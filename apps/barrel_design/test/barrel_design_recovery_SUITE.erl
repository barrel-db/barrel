%% Recovery: an indexer crash just before or just after a publication
%% batch, an application restart, a database recreated under the same
%% name, and restores and branches (new instance ids). Each recovered
%% index must equal a clean recomputation.
-module(barrel_design_recovery_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([crash_before_publication/1, crash_after_publication/1, app_restart_resumes/1,
         same_name_recreation/1, restore_rebuilds/1, branch_indexes_itself/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(barrel_design_test_util, [invoice/5, all_rows/2, recompute/2, recompute/3,
                                  normalized_view/2, wait_caught_up/2, guest_calls/0,
                                  guest/1, wait_until/2]).

-define(V, {<<"ar">>, <<"open">>}).
-define(SELECT, <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>).

all() ->
    [crash_before_publication, crash_after_publication, app_restart_resumes,
     same_name_recreation, restore_rebuilds, branch_indexes_itself].

init_per_suite(Config) ->
    ok = barrel_design_test_util:start(Config),
    Config.

end_per_suite(_Config) ->
    barrel_design_test_util:stop().

init_per_testcase(Case, Config) ->
    Db = <<"rc_", (atom_to_binary(Case))/binary>>,
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    _ = meck:unload(),
    Db = ?config(db, Config),
    _ = barrel_design:uninstall(Db, <<"ar">>),
    _ = barrel_docdb:delete_db(Db),
    ok.

design() ->
    barrel_design_test_util:design(<<"ar">>, 1, #{<<"open">> => #{
        select => ?SELECT,
        key => [#{path => [<<"customer">>], type => text, category => true},
                #{path => [<<"due_on">>], type => text}],
        value => #{sum => [<<"line_cents">>]}, reduce => '_sum'}}).

wasm_design(Digest) ->
    barrel_design_test_util:design(<<"ar">>, 1, #{<<"open">> => #{
        select => ?SELECT,
        map => #{module => Digest, fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                                              {[<<"line_cents">>], {array, i64}}]},
        key => [#{type => text, category => true}, #{type => text}],
        value => #{type => i64}, reduce => '_sum'}}).

inv(I) ->
    invoice(integer_to_binary(I), lists:nth(1 + I rem 3, [<<"Ada">>, <<"Bea">>, <<"Cy">>]),
            <<"open">>, iolist_to_binary(io_lib:format("2026-12-~2..0B", [1 + I rem 28])),
            [I, 1]).

seed(Db, From, To) ->
    [{ok, _} = barrel_docdb:put_doc(Db, inv(I)) || I <- lists:seq(From, To)],
    ok.

check(Db) ->
    ok = wait_caught_up(Db, ?V),
    {ok, Got} = all_rows(Db, ?V),
    ?assertEqual(recompute(Db, normalized_view(design(), <<"open">>)), Got),
    Got.

%%====================================================================
%% Cases
%%====================================================================

crash_before_publication(Config) ->
    crash_at_batch(?config(db, Config), before).

crash_after_publication(Config) ->
    crash_at_batch(?config(db, Config), 'after').

%% The third publication batch of the first build fails, before or after
%% it reaches the store. The indexer is restarted and replays from its
%% durable checkpoint.
crash_at_batch(Db, When) ->
    seed(Db, 1, 300),
    Count = counters:new(1, []),
    ok = meck:new(barrel_design_store, [passthrough, no_link]),
    ok = meck:expect(barrel_design_store, write, fun(Store, Ops) ->
        Rows = [K || {put, <<"r", _/binary>> = K, _} <- Ops],
        case Rows of
            [] -> meck:passthrough([Store, Ops]);
            _ ->
                counters:add(Count, 1, 1),
                case {counters:get(Count, 1), When} of
                    {3, before} -> exit(injected_crash);
                    {3, 'after'} -> ok = meck:passthrough([Store, Ops]), exit(injected_crash);
                    _ -> meck:passthrough([Store, Ops])
                end
        end
    end),
    {ok, _} = barrel_design:install(Db, design(), #{}),
    ok = wait_until(fun() -> counters:get(Count, 1) > 4 end, 20000),
    ?assertEqual(300, length(check(Db))).

%% Stopped, then started again: views resume from their checkpoints
%% (only the documents written meanwhile are mapped) without a reader.
app_restart_resumes(Config) ->
    Db = ?config(db, Config),
    Wasm = guest("open_by_customer.wasm"),
    D = crypto:hash(sha256, Wasm),
    seed(Db, 1, 50),
    {ok, _} = barrel_design:install(Db, wasm_design(D), #{D => Wasm}),
    check(Db),
    ok = application:stop(barrel_design),
    seed(Db, 51, 60),
    {ok, _} = application:ensure_all_started(barrel_design),
    Calls0 = guest_calls(),
    ok = wait_until(fun() -> barrel_design_dbs:lookup(Db) =/= {error, not_found} end, 5000),
    check(Db),
    ?assertEqual(10, guest_calls() - Calls0).

%% A database deleted and recreated under the same name starts without
%% designs: the old rows are never served and their directory goes.
same_name_recreation(Config) ->
    Db = ?config(db, Config),
    seed(Db, 1, 20),
    {ok, _} = barrel_design:install(Db, design(), #{}),
    check(Db),
    {ok, #{checkpoint := #{instance_id := Old}}} = barrel_design:status(Db, ?V),
    ok = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    seed(Db, 100, 104),
    %% the old instance's rows go with their directory
    ok = wait_until(fun() -> not filelib:is_dir(barrel_design_store:db_dir(Db)) end, 10000),
    {error, {unknown_view, _, _}} = barrel_design:query(Db, ?V, #{}),
    {ok, _} = barrel_design:install(Db, design(), #{}),
    ?assertEqual(5, length(check(Db))),
    {ok, #{checkpoint := #{instance_id := New}}} = barrel_design:status(Db, ?V),
    ?assertNotEqual(Old, New).

%% A restore is a new instance: the view is rebuilt over the restored
%% documents.
restore_rebuilds(Config) ->
    Db = ?config(db, Config),
    seed(Db, 1, 30),
    {ok, _} = barrel_design:install(Db, design(), #{}),
    check(Db),
    {ok, #{checkpoint := #{instance_id := Old}}} = barrel_design:status(Db, ?V),
    Dir = filename:join(?config(priv_dir, Config), "backup"),
    {ok, _} = barrel_docdb:backup_db(Db, Dir),
    seed(Db, 31, 35),
    ok = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:restore_db(Dir, Db),
    ok = wait_until(fun() ->
        case barrel_design:status(Db, ?V) of
            {ok, #{checkpoint := #{instance_id := I}}} -> I =/= Old;
            _ -> false
        end
    end, 10000),
    ?assertEqual(30, length(check(Db))).

%% A branch is its own database: it is attached on first use and builds
%% its own index over its documents.
branch_indexes_itself(Config) ->
    Db = ?config(db, Config),
    seed(Db, 1, 12),
    {ok, _} = barrel_design:install(Db, design(), #{}),
    check(Db),
    Branch = <<Db/binary, "_br">>,
    _ = barrel_docdb:delete_db(Branch),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        {ok, _} = barrel_docdb:put_doc(Branch, inv(500)),
        {ok, _} = barrel_design:refresh(Branch, ?V, #{timeout_ms => 10000}),
        {ok, Rows} = all_rows(Branch, ?V),
        ?assertEqual(recompute(Branch, normalized_view(design(), <<"open">>)), Rows),
        ?assertEqual(13, length(Rows)),
        %% the source is untouched
        ?assertEqual(12, length(check(Db)))
    after
        _ = barrel_design:uninstall(Branch, <<"ar">>),
        _ = barrel_docdb:delete_db(Branch)
    end.
