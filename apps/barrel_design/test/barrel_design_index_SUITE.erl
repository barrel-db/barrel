%% Index maintenance: no readers, repeated keys, category moves, TTL,
%% bulk, replicated and winner changes, writes during a backfill, native
%% and Wasm equivalence, definition changes with readers in flight, and
%% the read options. Every check compares the index with a clean
%% recomputation over the database's live documents.
-module(barrel_design_index_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([indexes_without_readers/1, repeated_keys/1, keys_move_between_categories/1,
         ttl_expiry/1, bulk_replicated_and_winner_changes/1, writes_during_backfill/1,
         native_and_wasm_agree/1, failed_map_removes_rows/1, caption_edit_reuses_index/1,
         selector_edit_rebuilds/1, upgrade_with_readers_in_flight/1, read_options/1,
         grouped_scan_budget/1, install_rules/1, encrypted_rows/1, install_from_wat/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(barrel_design_test_util, [invoice/5, all_rows/2, recompute/2, recompute/3,
                                  normalized_view/2, wait_caught_up/2, guest_calls/0, guest/1]).

-define(SELECT, <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>).
-define(V, {<<"ar">>, <<"open">>}).

all() ->
    [indexes_without_readers, repeated_keys, keys_move_between_categories, ttl_expiry,
     bulk_replicated_and_winner_changes, writes_during_backfill, native_and_wasm_agree,
     failed_map_removes_rows, caption_edit_reuses_index, selector_edit_rebuilds,
     upgrade_with_readers_in_flight, read_options, grouped_scan_budget, install_rules,
     encrypted_rows, install_from_wat].

init_per_suite(Config) ->
    ok = barrel_design_test_util:start(Config),
    Config.

end_per_suite(_Config) ->
    barrel_design_test_util:stop().

init_per_testcase(Case, Config) ->
    Db = <<"ix_", (atom_to_binary(Case))/binary>>,
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    Db = ?config(db, Config),
    {ok, Designs} = barrel_design:list(Db),
    [ok = barrel_design:uninstall(Db, N) || N <- maps:keys(Designs)],
    _ = barrel_docdb:delete_db(Db),
    ok.

%%====================================================================
%% Designs used here
%%====================================================================

native_design(Version) ->
    native_design(Version, ?SELECT, #{}).

native_design(Version, Select, Extra) ->
    barrel_design_test_util:design(<<"ar">>, Version, #{<<"open">> => maps:merge(#{
        select => Select,
        key => [#{path => [<<"customer">>], type => text, category => true},
                #{path => [<<"due_on">>], type => text}],
        value => #{sum => [<<"line_cents">>]}, reduce => '_sum'}, Extra)}).

install_native(Db) ->
    {ok, _} = barrel_design:install(Db, native_design(1), #{}).

check(Db, Design) ->
    ok = wait_caught_up(Db, ?V),
    Want = recompute(Db, normalized_view(Design, <<"open">>)),
    {ok, Got} = all_rows(Db, ?V),
    ?assertEqual(Want, Got),
    Got.

seed(Db, N) ->
    [{ok, _} = barrel_docdb:put_doc(Db, inv(I)) || I <- lists:seq(1, N)],
    ok.

inv(I) ->
    Status = case I rem 5 of 0 -> <<"paid">>; _ -> <<"open">> end,
    invoice(integer_to_binary(I), lists:nth(1 + I rem 3, [<<"Ada">>, <<"Bea">>, <<"Cy">>]),
            Status, iolist_to_binary(io_lib:format("2026-12-~2..0B", [1 + I rem 28])),
            [100 + I, -10]).

%%====================================================================
%% Cases
%%====================================================================

%% Installed over existing documents, then written and deleted with no
%% reader: the view catches up on its own (observed through status).
indexes_without_readers(Config) ->
    Db = ?config(db, Config),
    seed(Db, 30),
    install_native(Db),
    [{ok, _} = barrel_docdb:put_doc(Db, inv(I)) || I <- lists:seq(31, 40)],
    {ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(Db, <<"3">>),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"3">>, #{rev => Rev}),
    Rows = check(Db, native_design(1)),
    ?assertEqual(31, length(Rows)).

%% One document emits the same key twice: two rows, and an update
%% replaces both instead of adding to them.
repeated_keys(Config) ->
    Db = ?config(db, Config),
    Wasm = guest("dup.wasm"),
    D = crypto:hash(sha256, Wasm),
    Design = barrel_design_test_util:design(<<"ar">>, 1, #{<<"open">> => #{
        select => <<"SELECT * FROM db WHERE type = 'tag'">>,
        map => #{module => D, fields => [{[<<"tag">>], text}]},
        key => #{type => text}, value => #{type => i64}, reduce => '_count'}}),
    {ok, _} = barrel_design:install(Db, Design, #{D => Wasm}),
    {ok, #{<<"rev">> := R1}} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"t1">>, <<"type">> => <<"tag">>,
                                                           <<"tag">> => <<"x">>}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, [#{id := <<"t1">>, key := <<"x">>, value := 1}, #{id := <<"t1">>, key := <<"x">>, value := 1}]} =
        all_rows(Db, ?V),
    {ok, [#{key := null, value := 2}], _} = barrel_design:query(Db, ?V, #{}),
    {ok, #{<<"rev">> := R2}} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"t1">>, <<"_rev">> => R1,
                                                           <<"type">> => <<"tag">>, <<"tag">> => <<"y">>}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, [#{key := <<"y">>}, #{key := <<"y">>}]} = all_rows(Db, ?V),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"t1">>, #{rev => R2}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, []} = all_rows(Db, ?V),
    %% no rows, no group (as CouchDB)
    {ok, [], _} = barrel_design:query(Db, ?V, #{}).

keys_move_between_categories(Config) ->
    Db = ?config(db, Config),
    install_native(Db),
    {ok, #{<<"rev">> := R}} = barrel_docdb:put_doc(Db, invoice(<<"m">>, <<"Ada">>, <<"open">>,
                                                               <<"2026-12-01">>, [700])),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, [#{key := [<<"Ada">>], value := 700}], _} = barrel_design:query(Db, ?V, #{group_level => 1}),
    {ok, _} = barrel_docdb:put_doc(Db, (invoice(<<"m">>, <<"Bea">>, <<"open">>, <<"2026-12-02">>, [700]))#{<<"_rev">> => R}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, [#{key := [<<"Bea">>], value := 700}], _} = barrel_design:query(Db, ?V, #{group_level => 1}),
    {ok, [], _} = barrel_design:query(Db, ?V, #{key_prefix => [<<"Ada">>], reduce => false}),
    check(Db, native_design(1)).

%% An expired document leaves the rows and totals at once, before any
%% sweeper runs; a sweep then deletes its rows.
ttl_expiry(Config) ->
    Db = ?config(db, Config),
    install_native(Db),
    Expires = erlang:system_time(millisecond) + 300,
    {ok, _} = barrel_docdb:put_doc(Db, invoice(<<"t">>, <<"Ada">>, <<"open">>, <<"2026-12-01">>, [5]),
                                   #{expires_at => Expires}),
    {ok, _} = barrel_docdb:put_doc(Db, invoice(<<"k">>, <<"Ada">>, <<"open">>, <<"2026-12-02">>, [7])),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, [#{key := [<<"Ada">>], value := 12}], _} = barrel_design:query(Db, ?V, #{group_level => 1}),
    ok = barrel_design_test_util:wait_until(fun() -> erlang:system_time(millisecond) > Expires end, 2000),
    {ok, [#{key := [<<"Ada">>], value := 7}], _} = barrel_design:query(Db, ?V, #{group_level => 1}),
    {ok, [#{id := <<"k">>}]} = all_rows(Db, ?V),
    {ok, 1} = barrel_docdb:sweep_ttl(Db),
    check(Db, native_design(1)).

bulk_replicated_and_winner_changes(Config) ->
    Db = ?config(db, Config),
    install_native(Db),
    Docs = [inv(I) || I <- lists:seq(1, 12)],
    [{ok, _} | _] = barrel_docdb:put_docs(Db, Docs),
    check(Db, native_design(1)),
    %% a replicated arrival
    {Tok, VV} = remote_version(<<"peer_a">>),
    {ok, _, _} = barrel_docdb:put_version(Db, invoice(<<"r">>, <<"Cy">>, <<"open">>,
                                                      <<"2026-12-09">>, [33]), Tok, VV, false),
    check(Db, native_design(1)),
    %% a concurrent version: the winner may change, the view follows it
    {Tok2, VV2} = remote_version(<<"peer_b">>),
    {ok, _, _} = barrel_docdb:put_version(Db, invoice(<<"r">>, <<"Ada">>, <<"open">>,
                                                      <<"2026-12-10">>, [44]), Tok2, VV2, false),
    check(Db, native_design(1)),
    %% resolve the conflict to the loser's body
    {ok, #{<<"_rev">> := Winner}} = barrel_docdb:get_doc(Db, <<"r">>),
    Loser = case Winner of Tok -> Tok2; _ -> Tok end,
    {ok, _} = barrel_docdb:resolve_conflict(Db, <<"r">>, Winner, {choose, Loser}),
    check(Db, native_design(1)).

%% Writes, updates and deletes race the initial backfill.
writes_during_backfill(Config) ->
    Db = ?config(db, Config),
    seed(Db, 1500),
    Self = self(),
    Writer = spawn_link(fun() ->
        rand:seed(exsss, {1, 2, 3}),
        lists:foreach(fun(N) ->
            I = rand:uniform(1500),
            Id = integer_to_binary(I),
            case barrel_docdb:get_doc(Db, Id) of
                {ok, Doc} when N rem 7 =:= 0 ->
                    {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => maps:get(<<"_rev">>, Doc)});
                {ok, Doc} ->
                    {ok, _} = barrel_docdb:put_doc(Db, Doc#{<<"customer">> => <<"Di">>,
                                                            <<"status">> => <<"open">>});
                {error, not_found} ->
                    {ok, _} = barrel_docdb:put_doc(Db, inv(I))
            end
        end, lists:seq(1, 400)),
        Self ! {done, self()}
    end),
    install_native(Db),
    receive {done, Writer} -> ok end,
    check(Db, native_design(1)).

%% A native projection and an equivalent WAT map give the same rows as
%% documents enter, leave and move categories.
native_and_wasm_agree(Config) ->
    Db = ?config(db, Config),
    Wasm = guest("by_customer.wasm"),
    D = crypto:hash(sha256, Wasm),
    Select = <<"SELECT * FROM db WHERE type = 'sale'">>,
    Design = barrel_design_test_util:design(<<"sales">>, 1, #{
        <<"native">> => #{select => Select, key => #{path => [<<"customer">>], type => text},
                          value => #{path => [<<"cents">>], type => i64}, reduce => '_sum'},
        <<"wasm">> => #{select => Select,
                        map => #{module => D, fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]},
                        key => #{type => text}, value => #{type => i64}, reduce => '_sum'}}),
    {ok, _} = barrel_design:install(Db, Design, #{D => Wasm}),
    Sale = fun(Id, C, Cents) -> #{<<"id">> => Id, <<"type">> => <<"sale">>,
                                  <<"customer">> => C, <<"cents">> => Cents} end,
    Steps = [
        fun() -> [{ok, _} = barrel_docdb:put_doc(Db, Sale(integer_to_binary(I), lists:nth(1 + I rem 2, [<<"Ada">>, <<"Bea">>]), I * 10 - 25))
                  || I <- lists:seq(1, 20)] end,
        fun() -> update(Db, <<"3">>, #{<<"customer">> => <<"Cy">>}) end,
        fun() -> update(Db, <<"4">>, #{<<"type">> => <<"refund">>}) end,
        fun() -> update(Db, <<"4">>, #{<<"type">> => <<"sale">>, <<"cents">> => -5}) end,
        fun() -> update(Db, <<"5">>, #{<<"cents">> => <<"oops">>}) end,
        fun() -> {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db, <<"6">>),
                 {ok, _} = barrel_docdb:delete_doc(Db, <<"6">>, #{rev => R}) end
    ],
    lists:foreach(fun(Step) ->
        Step(),
        {ok, _} = barrel_design:refresh(Db, {<<"sales">>, <<"native">>}, #{timeout_ms => 5000}),
        {ok, _} = barrel_design:refresh(Db, {<<"sales">>, <<"wasm">>}, #{timeout_ms => 5000}),
        {ok, N} = all_rows(Db, {<<"sales">>, <<"native">>}),
        {ok, W} = all_rows(Db, {<<"sales">>, <<"wasm">>}),
        ?assertEqual(N, W),
        {ok, NG, _} = barrel_design:query(Db, {<<"sales">>, <<"native">>}, #{group_level => 1}),
        {ok, WG, _} = barrel_design:query(Db, {<<"sales">>, <<"wasm">>}, #{group_level => 1}),
        ?assertEqual(NG, WG)
    end, Steps).

%% A map that fails on a new revision removes the document's old rows
%% and records the error; a corrected revision indexes again.
failed_map_removes_rows(Config) ->
    Db = ?config(db, Config),
    Wasm = guest("by_customer.wasm"),
    D = crypto:hash(sha256, Wasm),
    Design = barrel_design_test_util:design(<<"ar">>, 1, #{<<"open">> => #{
        select => <<"SELECT * FROM db WHERE type = 'sale'">>,
        map => #{module => D, fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]},
        key => #{type => text}, value => #{type => i64}, reduce => '_sum',
        limits => #{max_field_bytes => 64}}}),
    {ok, _} = barrel_design:install(Db, Design, #{D => Wasm}),
    {ok, #{<<"rev">> := R1}} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"s">>, <<"type">> => <<"sale">>,
                                                           <<"customer">> => <<"Ada">>, <<"cents">> => 9}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, [#{value := 9}]} = all_rows(Db, ?V),
    {ok, #{<<"rev">> := R2}} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"s">>, <<"_rev">> => R1,
        <<"type">> => <<"sale">>, <<"customer">> => binary:copy(<<"A">>, 200), <<"cents">> => 9}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, []} = all_rows(Db, ?V),
    {ok, #{errors := 1}} = barrel_design:status(Db, ?V),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"s">>, <<"_rev">> => R2,
        <<"type">> => <<"sale">>, <<"customer">> => <<"Bea">>, <<"cents">> => 4}),
    {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
    {ok, [#{key := <<"Bea">>, value := 4}]} = all_rows(Db, ?V),
    {ok, #{errors := 0}} = barrel_design:status(Db, ?V).

caption_edit_reuses_index(Config) ->
    Db = ?config(db, Config),
    seed(Db, 20),
    install_native(Db),
    check(Db, native_design(1)),
    {ok, #{generation := 1}} = barrel_design:status(Db, ?V),
    Titled = native_design(2, ?SELECT, #{title => <<"Open invoices">>,
        key => [#{path => [<<"customer">>], type => text, category => true, title => <<"Client">>},
                #{path => [<<"due_on">>], type => text, title => <<"Due">>}]}),
    {ok, #{outcome := installed, version := 2}} = barrel_design:install(Db, Titled, #{}),
    {ok, #{generation := 1, version := 2}} = barrel_design:status(Db, ?V),
    {ok, #{outcome := unchanged}} = barrel_design:install(Db, Titled, #{}),
    check(Db, Titled).

selector_edit_rebuilds(Config) ->
    Db = ?config(db, Config),
    seed(Db, 20),
    install_native(Db),
    check(Db, native_design(1)),
    Paid = native_design(2, <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'paid'">>, #{}),
    {ok, _} = barrel_design:install(Db, Paid, #{}),
    Rows = check(Db, Paid),
    ?assertEqual(4, length(Rows)),
    {ok, #{generation := 2}} = barrel_design:status(Db, ?V).

%% Readers page through the view while a new definition builds and
%% takes over: every page comes from one generation, a cursor that
%% crosses the switch is refused, and the new rows match the new design.
upgrade_with_readers_in_flight(Config) ->
    Db = ?config(db, Config),
    seed(Db, 400),
    install_native(Db),
    check(Db, native_design(1)),
    Self = self(),
    Readers = [spawn_link(fun() -> Self ! {reader, self(), read_loop(Db, #{}, 0)} end)
               || _ <- lists:seq(1, 4)],
    Counted = native_design(2, ?SELECT, #{reduce => '_count'}),
    {ok, _} = barrel_design:install(Db, Counted, #{}),
    check(Db, Counted),
    [R ! stop || R <- Readers],
    Results = [receive {reader, R, Res} -> Res end || R <- Readers],
    ct:pal("reader outcomes: ~p", [Results]),
    [?assert(is_map(Res)) || Res <- Results],
    {ok, #{generation := 2}} = barrel_design:status(Db, ?V),
    {ok, [#{key := null, value := 320}], _} = barrel_design:query(Db, ?V, #{}).

%% Each page's generation is recorded; a stale cursor restarts the scan.
read_loop(Db, Seen, Pages) ->
    receive stop -> Seen#{pages => Pages}
    after 0 ->
        case barrel_design:query(Db, ?V, #{reduce => false, limit => 50}) of
            {ok, _, #{generation := G} = M} ->
                read_pages(Db, M, G, Seen#{G => true}, Pages + 1);
            {error, _} ->
                read_loop(Db, Seen, Pages)
        end
    end.

read_pages(Db, #{continuation := C}, G, Seen, Pages) ->
    case barrel_design:query(Db, ?V, #{reduce => false, limit => 50, continuation => C}) of
        {ok, _, #{generation := G} = M} -> read_pages(Db, M, G, Seen, Pages + 1);
        {ok, _, #{generation := Other}} -> error({mixed_generations, G, Other});
        {error, stale_cursor} -> read_loop(Db, Seen#{stale => true}, Pages)
    end;
read_pages(Db, _M, _G, Seen, Pages) ->
    read_loop(Db, Seen, Pages).

read_options(Config) ->
    Db = ?config(db, Config),
    seed(Db, 60),
    install_native(Db),
    All = check(Db, native_design(1)),
    Q = fun(O) -> {ok, R, _} = barrel_design:query(Db, ?V, O#{reduce => false, limit => 1000}), R end,
    Keyed = fun(F) -> [R || #{key := K} = R <- All, F(K)] end,
    ?assertEqual(Keyed(fun([C, _]) -> C =:= <<"Bea">> end), Q(#{key_prefix => [<<"Bea">>]})),
    [#{key := K1} | _] = All,
    ?assertEqual(Keyed(fun(K) -> K =:= K1 end), Q(#{key => K1})),
    S = [<<"Ada">>, <<"2026-12-10">>], E = [<<"Bea">>, <<"2026-12-05">>],
    ?assertEqual(Keyed(fun(K) -> K >= S andalso K =< E end), Q(#{start_key => S, end_key => E})),
    ?assertEqual(Keyed(fun(K) -> K >= S andalso K < E end),
                 Q(#{start_key => S, end_key => E, inclusive_end => false})),
    %% exact groups and totals
    {ok, Exact, _} = barrel_design:query(Db, ?V, #{group_level => exact, limit => 1000}),
    ?assertEqual(length(lists:usort([K || #{key := K} <- All])), length(Exact)),
    {ok, [#{value := Total}], _} = barrel_design:query(Db, ?V, #{}),
    ?assertEqual(lists:sum([V || #{value := V} <- All]), Total),
    %% errors
    {error, {invalid_limit, 0}} = barrel_design:query(Db, ?V, #{limit => 0}),
    {error, invalid_continuation} = barrel_design:query(Db, ?V, #{continuation => <<"x">>}),
    {error, {invalid_key, 1.5}} = barrel_design:query(Db, ?V, #{key => 1.5}),
    {error, {unknown_view, <<"ar">>, <<"nope">>}} = barrel_design:query(Db, {<<"ar">>, <<"nope">>}, #{}),
    {error, denied} = barrel_design:query(Db, ?V, #{authorize => fun(_) -> {error, denied} end}),
    {error, denied} = barrel_design:refresh(Db, ?V, #{authorize => fun(_) -> {error, denied} end}).

%% A group larger than one call's scan budget is totalled across calls.
grouped_scan_budget(Config) ->
    Db = ?config(db, Config),
    seed(Db, 90),
    install_native(Db),
    All = check(Db, native_design(1)),
    Want = lists:foldl(fun(#{key := [C, _], value := V}, Acc) ->
        maps:update_with(C, fun(X) -> X + V end, V, Acc)
    end, #{}, All),
    Got = groups(Db, #{group_level => 1, max_scan => 7, limit => 1}, []),
    ?assertEqual(lists:sort(maps:to_list(Want)), lists:sort([{K, V} || #{key := [K], value := V} <- Got])).

groups(Db, Opts, Acc) ->
    case barrel_design:query(Db, ?V, Opts) of
        {ok, Rows, #{continuation := C}} -> groups(Db, Opts#{continuation => C}, Acc ++ Rows);
        {ok, Rows, _} -> Acc ++ Rows
    end.

install_rules(Config) ->
    Db = ?config(db, Config),
    install_native(Db),
    {ok, #{outcome := unchanged}} = barrel_design:install(Db, native_design(1), #{}),
    {error, {version_conflict, <<"ar">>, 1}} =
        barrel_design:install(Db, native_design(1, <<"SELECT * FROM db WHERE type = 'x'">>, #{}), #{}),
    {ok, _} = barrel_design:install(Db, native_design(3), #{}),
    {error, {stale_version, <<"ar">>, 3}} = barrel_design:install(Db, native_design(2), #{}),
    Wasm = guest("by_customer.wasm"),
    D = crypto:hash(sha256, Wasm),
    WasmDesign = fun(Digest) -> barrel_design_test_util:design(<<"w">>, 1, #{<<"v">> => #{
        select => <<"SELECT * FROM db">>,
        map => #{module => Digest, fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]},
        key => #{type => text}, value => #{type => i64}}}) end,
    {error, {missing_module, _}} = barrel_design:install(Db, WasmDesign(D), #{}),
    {error, {digest_mismatch, _}} = barrel_design:install(Db, WasmDesign(D), #{D => <<Wasm/binary, 0>>}),
    {ok, _} = barrel_design:install(Db, WasmDesign(D), #{D => Wasm}),
    %% stored bytes need not be sent again
    {ok, #{outcome := unchanged}} = barrel_design:install(Db, WasmDesign(D), #{}),
    {ok, #{<<"ar">> := #{version := 3}, <<"w">> := #{version := 1}}} = barrel_design:list(Db),
    ok = barrel_design:uninstall(Db, <<"w">>),
    {error, not_found} = barrel_design:uninstall(Db, <<"w">>),
    {ok, []} = barrel_design_catalog:modules(Db).

%% A view installed from WAT gives the rows of the same view installed
%% from the wasm-tools binary; the key must be the digest of the
%% canonical encoding.
install_from_wat(Config) ->
    Db = ?config(db, Config),
    Wat = guest("by_customer.wat"),
    {ok, WatD, _} = barrel_design:module({wat, Wat}),
    Bin = guest("by_customer.wasm"),
    BinD = crypto:hash(sha256, Bin),
    View = fun(D) -> #{select => <<"SELECT * FROM db WHERE type = 'sale'">>,
                       map => #{module => D, fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]},
                       key => #{type => text}, value => #{type => i64}, reduce => '_sum'} end,
    Design = barrel_design_test_util:design(<<"sales">>, 1,
        #{<<"from_wat">> => View(WatD), <<"from_wasm">> => View(BinD)}),
    {error, {digest_mismatch, _}} = barrel_design:install(Db, Design, #{BinD => {wat, Wat}, WatD => Bin}),
    {error, {invalid_module, wat, _}} = barrel_design:install(Db, Design, #{WatD => {wat, <<"(module">>}}),
    {ok, _} = barrel_design:install(Db, Design, #{WatD => {wat, Wat}, BinD => Bin}),
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => integer_to_binary(I), <<"type">> => <<"sale">>,
                                          <<"customer">> => lists:nth(1 + I rem 3, [<<"Ada">>, <<"Bea">>, <<"Cy">>]),
                                          <<"cents">> => I * 7 - 20})
     || I <- lists:seq(1, 25)],
    [{ok, _} = barrel_design:refresh(Db, {<<"sales">>, V}, #{timeout_ms => 5000})
     || V <- [<<"from_wat">>, <<"from_wasm">>]],
    {ok, A} = all_rows(Db, {<<"sales">>, <<"from_wat">>}),
    {ok, B} = all_rows(Db, {<<"sales">>, <<"from_wasm">>}),
    ?assertEqual(25, length(A)),
    ?assertEqual(A, B),
    %% the stored bytes are the canonical encoding
    {ok, Stored} = barrel_design_catalog:module_bytes(Db, WatD),
    ?assertEqual(WatD, crypto:hash(sha256, Stored)).

%% Rows of an encrypted database are encrypted in the derived store.
encrypted_rows(Config) ->
    Db = <<(?config(db, Config))/binary, "_enc">>,
    _ = barrel_docdb:delete_db(Db),
    application:set_env(barrel_docdb, test_encryption_master, <<"design secret">>),
    Sentinel = <<"sentinel-7d3f1a-customer-name">>,
    try
        {ok, _} = barrel_docdb:create_db(Db, #{encryption => #{provider => barrel_docdb_test_keyprovider}}),
        {ok, _} = barrel_design:install(Db, native_design(1), #{}),
        {ok, _} = barrel_docdb:put_doc(Db, invoice(<<"e">>, Sentinel, <<"open">>, <<"2026-12-01">>, [3])),
        {ok, _} = barrel_design:refresh(Db, ?V, #{timeout_ms => 5000}),
        {ok, [#{key := [Sentinel]}], _} = barrel_design:query(Db, ?V, #{group_level => 1}),
        Dir = barrel_design_store:db_dir(Db),
        ?assertEqual([], [F || F <- filelib:wildcard(filename:join(Dir, "**/*")),
                               filelib:is_regular(F),
                               {ok, Bin} <- [file:read_file(F)],
                               binary:match(Bin, Sentinel) =/= nomatch])
    after
        _ = barrel_design:uninstall(Db, <<"ar">>),
        _ = barrel_docdb:delete_db(Db),
        application:unset_env(barrel_docdb, test_encryption_master)
    end.

%%====================================================================
%% Helpers
%%====================================================================

update(Db, Id, Fields) ->
    {ok, Doc} = barrel_docdb:get_doc(Db, Id),
    {ok, _} = barrel_docdb:put_doc(Db, maps:merge(Doc, Fields)).

remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.
