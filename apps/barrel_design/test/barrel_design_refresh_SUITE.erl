%% Explicit refresh: the indexer polls rarely here and processes one
%% document per batch, so a refresh is what moves it, and its progress
%% is observable batch by batch.
-module(barrel_design_refresh_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([concurrent_refreshes_share_work/1, accepted_target_is_fixed/1,
         timeout_leaves_others_running/1, current_refresh_runs_no_map/1,
         wait_for_observes_only/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(barrel_design_test_util, [invoice/5, all_rows/2, guest_calls/0, guest/1,
                                  wait_until/2, wait_caught_up/2]).

-define(V, {<<"ar">>, <<"open">>}).

all() ->
    [concurrent_refreshes_share_work, accepted_target_is_fixed,
     timeout_leaves_others_running, current_refresh_runs_no_map, wait_for_observes_only].

init_per_suite(Config) ->
    ok = barrel_design_test_util:start(Config, #{poll_ms => 600000, idle_poll_ms => 600000,
                                                 batch_size => 1}),
    Config.

end_per_suite(_Config) ->
    barrel_design_test_util:stop().

init_per_testcase(Case, Config) ->
    Db = <<"rf_", (atom_to_binary(Case))/binary>>,
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    Wasm = guest("open_by_customer.wasm"),
    D = crypto:hash(sha256, Wasm),
    put_invoices(Db, 1, 3),
    {ok, _} = barrel_design:install(Db, design(D), #{D => Wasm}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 10000}),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    _ = meck:unload(),
    Db = ?config(db, Config),
    _ = barrel_design:uninstall(Db, <<"ar">>),
    _ = barrel_docdb:delete_db(Db),
    ok.

design(Digest) ->
    barrel_design_test_util:design(<<"ar">>, 1, #{<<"open">> => #{
        select => <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>,
        map => #{module => Digest, fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                                              {[<<"line_cents">>], {array, i64}}]},
        key => [#{type => text, category => true}, #{type => text}],
        value => #{type => i64}, reduce => '_sum'}}).

put_invoices(Db, From, To) ->
    [{ok, _} = barrel_docdb:put_doc(Db, invoice(integer_to_binary(I), <<"Ada">>, <<"open">>,
                                                <<"2026-12-01">>, [I]))
     || I <- lists:seq(From, To)],
    ok.

seq(Db) ->
    {ok, #{last_seq := S}} = barrel_docdb:db_observed_version(Db),
    S.

%%====================================================================
%% Cases
%%====================================================================

%% Twelve refreshes after a 40-document import: each document is mapped
%% once, and every caller gets a checkpoint covering its own target.
concurrent_refreshes_share_work(Config) ->
    Db = ?config(db, Config),
    put_invoices(Db, 10, 49),
    Target = seq(Db),
    Calls0 = guest_calls(),
    Self = self(),
    Pids = [spawn_link(fun() -> Self ! {done, self(), barrel_design:refresh(Db, ?V, #{timeout_ms => 20000})} end)
            || _ <- lists:seq(1, 12)],
    Results = [receive {done, P, R} -> R end || P <- Pids],
    [?assertMatch({ok, #{last_seq := S}} when S >= Target, R) || R <- Results],
    ?assertEqual(40, guest_calls() - Calls0),
    {ok, Rows} = all_rows(Db, ?V),
    ?assertEqual(43, length(Rows)).

%% The target is the last write when the refresh is accepted. The
%% indexer is held at its next feed read until later writes land; with
%% one document per batch the reply still comes at the accepted target.
accepted_target_is_fixed(Config) ->
    Db = ?config(db, Config),
    put_invoices(Db, 10, 10),
    Target = seq(Db),
    Self = self(),
    ok = meck:new(barrel_docdb, [passthrough, no_link]),
    ok = meck:expect(barrel_docdb, read_feed, fun(D, S, O) ->
        Self ! {reading, self()},
        receive go -> ok end,
        meck:passthrough([D, S, O])
    end),
    P = spawn_link(fun() -> Self ! {done, self(), barrel_design:refresh(Db, ?V, #{timeout_ms => 20000})} end),
    Indexer = receive {reading, I} -> I end,
    put_invoices(Db, 11, 15),
    Later = seq(Db),
    ok = meck:expect(barrel_docdb, read_feed, fun(D, S, O) -> meck:passthrough([D, S, O]) end),
    Indexer ! go,
    {ok, #{last_seq := Reached}} = receive {done, P, R} -> R end,
    ?assertEqual(Target, Reached),
    ?assert(Reached < Later),
    {ok, #{last_seq := Later}} = barrel_design:refresh(Db, ?V, #{timeout_ms => 20000}).

%% A caller whose deadline passes gets the checkpoint reached; another
%% caller and the indexer carry on to the target.
timeout_leaves_others_running(Config) ->
    Db = ?config(db, Config),
    put_invoices(Db, 10, 19),
    Target = seq(Db),
    %% ten documents wait, one per batch: the first answer comes before
    %% the target is covered
    {error, {timeout, #{last_seq := Reached}}} = barrel_design:refresh(Db, ?V, #{timeout_ms => 0}),
    ?assert(Reached < Target),
    {ok, #{last_seq := S}} = barrel_design:refresh(Db, ?V, #{timeout_ms => 20000}),
    ?assert(S >= Target),
    {ok, Rows} = all_rows(Db, ?V),
    ?assertEqual(13, length(Rows)).

current_refresh_runs_no_map(Config) ->
    Db = ?config(db, Config),
    Calls0 = guest_calls(),
    {ok, Cp} = barrel_design:refresh(Db, ?V),
    {ok, Cp} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    ?assertEqual(Calls0, guest_calls()).

%% A freshness wait observes progress without starting a pass: with the
%% indexer idle it times out, and a refresh then satisfies it.
wait_for_observes_only(Config) ->
    Db = ?config(db, Config),
    put_invoices(Db, 10, 10),
    {ok, Target} = barrel_docdb:db_observed_version(Db),
    {error, {timeout, _}} = barrel_design:query(Db, ?V, #{freshness => {wait_for, Target},
                                                          timeout_ms => 200}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, [#{key := [<<"Ada">>], value := 16}], _} =
        barrel_design:query(Db, ?V, #{freshness => {wait_for, Target}, group_level => 1}).
