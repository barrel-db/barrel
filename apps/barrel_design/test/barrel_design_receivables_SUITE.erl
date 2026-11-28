%% The receivables design from the implementation prompt, end to end:
%% install over existing invoices, drill into a category, total it, pay
%% an invoice, wait for the payment, import one, refresh, and read the
%% same rows through BQL. Run with the Zig guest and the equivalent
%% native projection.
-module(barrel_design_receivables_SUITE).

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2, init_per_testcase/2, end_per_testcase/2]).
-export([statement/1, bql_reads/1, bql_pushdown_matches_scan/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(VIEW, {<<"receivables">>, <<"open_by_customer">>}).
-define(SELECT, <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>).

all() -> [{group, wasm}, {group, native}].

groups() ->
    Cases = [statement, bql_reads, bql_pushdown_matches_scan],
    [{wasm, [], Cases}, {native, [], Cases}].

init_per_suite(Config) ->
    ok = barrel_design_test_util:start(Config),
    Config.

end_per_suite(_Config) ->
    barrel_design_test_util:stop().

init_per_group(Mode, Config) -> [{mode, Mode} | Config].
end_per_group(_Mode, _Config) -> ok.

init_per_testcase(Case, Config) ->
    Db = iolist_to_binary(["inv_", atom_to_list(?config(mode, Config)), "_", atom_to_list(Case)]),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = seed(Db),
    %% counted before the install: its backfill may run at once
    Calls = maps:get(guest_calls, barrel_design_stats:get()),
    {ok, _} = install(Db, ?config(mode, Config)),
    [{db, Db}, {calls_before_install, Calls} | Config].

end_per_testcase(_Case, Config) ->
    Db = ?config(db, Config),
    _ = barrel_design:uninstall(Db, <<"receivables">>),
    _ = barrel_docdb:delete_db(Db),
    ok.

%%====================================================================
%% Cases
%%====================================================================

statement(Config) ->
    Db = ?config(db, Config),
    Calls0 = ?config(calls_before_install, Config),
    {ok, Target} = barrel_docdb:db_observed_version(Db),
    Ada = #{key_prefix => [<<"Ada">>], freshness => {wait_for, Target}, timeout_ms => 5000},

    %% expand Ada: her open invoices in due-date order
    {ok, [#{id := <<"inv-102">>, key := [<<"Ada">>, <<"2026-12-01">>], value := 1800},
          #{id := <<"inv-101">>, key := [<<"Ada">>, <<"2026-12-15">>], value := 750}],
     #{generation := 1, status := complete, checkpoint := #{last_seq := Seen}}} =
        barrel_design:query(Db, ?VIEW, Ada#{reduce => false, limit => 20}),
    ?assert(Seen >= maps:get(last_seq, Target)),

    %% collapse Ada: the category subtotal
    Total = Ada#{reduce => true, group_level => 1},
    {ok, [#{key := [<<"Ada">>], value := 2550}], _} = barrel_design:query(Db, ?VIEW, Total),

    %% pay inv-102 and read a subtotal that includes the payment
    {ok, Invoice} = barrel_docdb:get_doc(Db, <<"inv-102">>),
    {ok, _} = barrel_docdb:put_doc(Db, Invoice#{<<"status">> => <<"paid">>}),
    {ok, Paid} = barrel_docdb:db_observed_version(Db),
    {ok, [#{key := [<<"Ada">>], value := 750}], _} =
        barrel_design:query(Db, ?VIEW, Total#{freshness => {wait_for, Paid}}),
    {ok, [#{key := [<<"Bea">>], value := 900}], _} =
        barrel_design:query(Db, ?VIEW, #{key_prefix => [<<"Bea">>], group_level => 1}),

    %% import another invoice and refresh before the statement
    {ok, _} = barrel_docdb:put_doc(Db, invoice(<<"inv-103">>, <<"Ada">>, <<"open">>,
                                               <<"2026-12-20">>, [500])),
    {ok, #{instance_id := _, last_seq := _}} = barrel_design:refresh(Db, ?VIEW),
    #{guest_calls := Calls1} = barrel_design_stats:get(),

    %% reads and an already-current refresh run no map
    {ok, [#{key := [<<"Ada">>], value := 1250}], _} =
        barrel_design:query(Db, ?VIEW, #{key_prefix => [<<"Ada">>], reduce => true,
                                         group_level => 1}),
    {ok, [#{key := [<<"Bea">>], value := 900}], _} =
        barrel_design:query(Db, ?VIEW, #{key_prefix => [<<"Bea">>], group_level => 1}),
    {ok, [#{key := [<<"Ada">>], value := 1250}, #{key := [<<"Bea">>], value := 900}], _} =
        barrel_design:query(Db, ?VIEW, #{group_level => 1}),
    {ok, [#{key := null, value := 2150}], _} = barrel_design:query(Db, ?VIEW, #{}),
    {ok, _} = barrel_design:refresh(Db, ?VIEW, #{timeout_ms => 5000}),
    ?assertMatch(#{guest_calls := Calls1}, barrel_design_stats:get()),
    case ?config(mode, Config) of
        %% three open invoices at install, then inv-103: the paid one is
        %% not selected and runs no map
        wasm -> ?assertEqual(4, Calls1 - Calls0);
        native -> ?assertEqual(0, Calls1 - Calls0)
    end.

bql_reads(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_design:refresh(Db, ?VIEW, #{timeout_ms => 5000}),
    Rows = <<"SELECT v.id, v.key, v.value FROM view('receivables', 'open_by_customer') AS v "
             "WHERE v.key[0] = 'Ada' ORDER BY v.key LIMIT 20">>,
    Groups = <<"SELECT v.key, v.value FROM view('receivables', 'open_by_customer', "
               "group_level => 1) AS v WHERE v.key[0] = 'Ada'">>,
    WantRows = [#{<<"id">> => <<"inv-102">>, <<"key">> => [<<"Ada">>, <<"2026-12-01">>],
                  <<"value">> => 1800},
                #{<<"id">> => <<"inv-101">>, <<"key">> => [<<"Ada">>, <<"2026-12-15">>],
                  <<"value">> => 750}],
    WantGroups = [#{<<"key">> => [<<"Ada">>], <<"value">> => 2550}],
    #{guest_calls := Calls0} = barrel_design_stats:get(),
    {ok, WantRows, _} = barrel_docdb:query(Db, Rows),
    {ok, WantGroups, _} = barrel_docdb:query(Db, Groups),
    %% the barrel facade (and so the HTTP and MCP query routes)
    {ok, WantRows, _} = barrel:query(#{docdb => Db}, Rows, #{}),
    {ok, WantGroups, _} = barrel:query(#{docdb => Db}, Groups, #{}),
    {ok, #{source := <<"view">>}} = barrel:explain_query(#{docdb => Db}, Rows),
    %% a sort on another column, offset and projection
    {ok, [#{<<"id">> := <<"inv-200">>}, #{<<"id">> := <<"inv-101">>}], _} = barrel_docdb:query(Db,
        <<"SELECT v.id FROM view('receivables', 'open_by_customer') AS v "
          "ORDER BY v.value DESC LIMIT 2 OFFSET 1">>),
    %% chunked through query_fold, one row per chunk
    {ok, Ids, _} = barrel:query_fold(#{docdb => Db},
        <<"SELECT v.id FROM view('receivables', 'open_by_customer') AS v">>, #{chunk_size => 1},
        fun(#{<<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end, []),
    ?assertEqual([<<"inv-102">>, <<"inv-101">>, <<"inv-200">>], lists:reverse(Ids)),
    %% freshness through the query options
    {ok, _} = barrel_docdb:put_doc(Db, invoice(<<"inv-104">>, <<"Cy">>, <<"open">>,
                                               <<"2026-12-05">>, [42])),
    {ok, Wrote} = barrel_docdb:db_observed_version(Db),
    {ok, [#{<<"key">> := [<<"Cy">>], <<"value">> := 42}], _} = barrel_docdb:query(Db,
        <<"SELECT v.key, v.value FROM view('receivables', 'open_by_customer', group_level => 1) AS v "
          "WHERE v.key[0] = 'Cy'">>, #{freshness => {wait_for, Wrote}, timeout_ms => 5000}),
    #{guest_calls := Calls1} = barrel_design_stats:get(),
    case ?config(mode, Config) of
        wasm -> ?assertEqual(1, Calls1 - Calls0);
        native -> ?assertEqual(0, Calls1 - Calls0)
    end.

%% Pushed-down key conditions return what a full read filtered here does.
bql_pushdown_matches_scan(Config) ->
    Db = ?config(db, Config),
    Extra = [invoice(<<"x-", (integer_to_binary(I))/binary>>,
                     lists:nth(1 + I rem 4, [<<"Ada">>, <<"Adam">>, <<"Bea">>, <<"Ad">>]),
                     <<"open">>,
                     iolist_to_binary(io_lib:format("2026-12-~2..0B", [1 + I rem 28])),
                     [I, -1]) || I <- lists:seq(1, 40)],
    [{ok, _} = barrel_docdb:put_doc(Db, D) || D <- Extra],
    {ok, _} = barrel_design:refresh(Db, ?VIEW, #{timeout_ms => 5000}),
    {ok, All, _} = barrel_docdb:query(Db,
        <<"SELECT v.id, v.key, v.value FROM view('receivables', 'open_by_customer') AS v">>),
    Cases = [
        {<<"v.key[0] = 'Ada'">>, fun([C, _]) -> C =:= <<"Ada">> end},
        {<<"v.key[0] = 'Ada' AND v.key[1] >= '2026-12-10'">>,
         fun([C, D]) -> C =:= <<"Ada">> andalso D >= <<"2026-12-10">> end},
        {<<"v.key[0] = 'Ada' AND v.key[1] > '2026-12-10' AND v.key[1] <= '2026-12-20'">>,
         fun([C, D]) -> C =:= <<"Ada">> andalso D > <<"2026-12-10">> andalso D =< <<"2026-12-20">> end},
        {<<"v.key[0] LIKE 'Ad%'">>, fun([C, _]) -> binary:longest_common_prefix([C, <<"Ad">>]) =:= 2 end},
        {<<"v.key[0] < 'Adam'">>, fun([C, _]) -> C < <<"Adam">> end},
        {<<"v.key[1] = '2026-12-02'">>, fun([_, D]) -> D =:= <<"2026-12-02">> end},
        {<<"v.key = 'Bea'">>, fun(K) -> lists:member(<<"Bea">>, K) end},
        {<<"v.key[0] = 'Ada' AND v.value > 10">>, fun([C, _]) -> C =:= <<"Ada">> end}
    ],
    lists:foreach(fun({Where, Pred}) ->
        {ok, Got, _} = barrel_docdb:query(Db,
            <<"SELECT v.id, v.key, v.value FROM view('receivables', 'open_by_customer') AS v WHERE ",
              Where/binary>>),
        Want = [R || #{<<"key">> := K, <<"value">> := V} = R <- All, Pred(K),
                     binary:match(Where, <<"v.value">>) =:= nomatch orelse V > 10],
        ?assertEqual({Where, Want}, {Where, Got})
    end, Cases).

%%====================================================================
%% Fixture
%%====================================================================

seed(Db) ->
    Invoices = [{<<"inv-101">>, <<"Ada">>, <<"open">>, <<"2026-12-15">>, [1000, -250]},
                {<<"inv-099">>, <<"Ada">>, <<"paid">>, <<"2026-12-02">>, [9900]},
                {<<"inv-200">>, <<"Bea">>, <<"open">>, <<"2026-12-03">>, [900]},
                {<<"inv-102">>, <<"Ada">>, <<"open">>, <<"2026-12-01">>, [1250, 750, -200]}],
    lists:foreach(fun({Id, C, St, Due, Lines}) ->
        {ok, _} = barrel_docdb:put_doc(Db, invoice(Id, C, St, Due, Lines))
    end, Invoices).

invoice(Id, Customer, Status, Due, Lines) ->
    #{<<"id">> => Id, <<"type">> => <<"invoice">>, <<"customer">> => Customer,
      <<"status">> => Status, <<"due_on">> => Due, <<"line_cents">> => Lines}.

install(Db, wasm) ->
    Wasm = guest("open_by_customer.wasm"),
    Digest = crypto:hash(sha256, Wasm),
    Map = #{abi => 1, module => Digest, export => <<"map">>,
            fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                       {[<<"line_cents">>], {array, i64}}]},
    View = #{select => ?SELECT, map => Map,
             key => [#{type => text, category => true}, #{type => text}],
             value => #{type => i64}, reduce => '_sum'},
    barrel_design:install(Db, #{name => <<"receivables">>, version => 2,
                                views => #{<<"open_by_customer">> => View}}, #{Digest => Wasm});
install(Db, native) ->
    View = #{select => ?SELECT,
             key => [#{path => [<<"customer">>], type => text, category => true},
                     #{path => [<<"due_on">>], type => text}],
             value => #{sum => [<<"line_cents">>]}, reduce => '_sum'},
    barrel_design:install(Db, #{name => <<"receivables">>, version => 1,
                                views => #{<<"open_by_customer">> => View}}, #{}).

guest(Name) ->
    Here = filename:dirname(code:which(?MODULE)),
    Dirs = [filename:join(Here, "guests"),
            filename:join([Here, "..", "..", "..", "..", "apps", "barrel_design", "test", "guests"])],
    [Path | _] = [P || D <- Dirs, P <- [filename:join(D, Name)], filelib:is_file(P)],
    {ok, Bin} = file:read_file(Path),
    Bin.
