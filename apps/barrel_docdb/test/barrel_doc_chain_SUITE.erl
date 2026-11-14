%%%-------------------------------------------------------------------
%%% @doc Version chains of documents whose ids extend each other (`a',
%%% `a:b', `a:b:c') stay apart: conflicts, history, replication and
%%% forgets of one never see the rows of another. Rows written in the
%%% layout before 3.1 move at a writable open and read correctly on a
%%% read-only one.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc_chain_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([t_conflicts_stay_apart/1,
         t_replication_keeps_docs/1,
         t_history_stays_apart/1,
         t_owner_split/1,
         t_moves_old_rows/1,
         t_read_only_old_rows/1,
         t_owner_vector_ahead/1,
         t_owner_without_vector/1,
         t_refuses_leaked_versions/1,
         t_repairs_leaked_winner/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(IDS, [<<"a">>, <<"a:b">>, <<"a:b:c">>, <<"a:">>]).

all() ->
    [t_conflicts_stay_apart, t_replication_keeps_docs, t_history_stays_apart,
     t_owner_split, t_moves_old_rows, t_read_only_old_rows,
     t_owner_vector_ahead, t_owner_without_vector, t_refuses_leaked_versions,
     t_repairs_leaked_winner].

init_per_suite(Config) ->
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    A = <<"chain_", (atom_to_binary(Case))/binary, "_a">>,
    B = <<"chain_", (atom_to_binary(Case))/binary, "_b">>,
    {ok, _} = barrel_docdb:create_db(A),
    {ok, _} = barrel_docdb:create_db(B),
    [{a, A}, {b, B} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_docdb:delete_db(?config(a, Config)),
    _ = barrel_docdb:delete_db(?config(b, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% Every id gets a conflict: each document lists its own two versions,
%% never another document's.
t_conflicts_stay_apart(Config) ->
    {A, B} = {?config(a, Config), ?config(b, Config)},
    Revs = conflict_all(A, B),
    [begin
         {ok, Doc} = barrel_docdb:get_doc(A, Id, #{conflicts => true}),
         ?assertEqual(lists:sort(maps:get(Id, Revs)),
                      lists:sort(maps:get(<<"_observed">>, Doc)))
     end || Id <- ?IDS],
    ok.

%% With conflicts on every id and a newer delete of the longest one,
%% replication leaves every other document readable with its own body.
t_replication_keeps_docs(Config) ->
    {A, B} = {?config(a, Config), ?config(b, Config)},
    _ = conflict_all(A, B),
    {ok, _} = barrel_docdb:delete_doc(B, <<"a:b:c">>),
    {ok, _} = barrel_rep:replicate(B, A),
    {ok, _} = barrel_rep:replicate(A, B),
    {ok, _} = barrel_rep:replicate(B, A),
    [[begin
          {ok, #{<<"id">> := Id, <<"who">> := _}} = barrel_docdb:get_doc(Db, Id)
      end || Id <- ?IDS -- [<<"a:b:c">>]] || Db <- [A, B]],
    [?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"a:b:c">>))
     || Db <- [A, B]],
    ok.

%% Superseded versions: each document's history holds its own only.
t_history_stays_apart(Config) ->
    A = ?config(a, Config),
    Revs = maps:from_list([{Id, edit(A, Id, 3)} || Id <- ?IDS]),
    [begin
         {ok, Versions} = barrel_docdb:get_doc_versions(A, Id),
         ?assertEqual(lists:sort(maps:get(Id, Revs)),
                      lists:sort([maps:get(version, V) || V <- Versions]))
     end || Id <- ?IDS],
    ok.

%% An old key tail splits at the one `:' whose version its row names.
t_owner_split(_Config) ->
    Hlc = barrel_hlc:new_hlc(),
    VEnc = barrel_version:encode({Hlc, <<"w0123456789abcdef">>}),
    Row = <<1, 0, (barrel_vv:encode(#{<<"w0123456789abcdef">> => Hlc}))/binary>>,
    [?assertEqual({ok, Id, VEnc},
                  barrel_doc_chain:owner(<<Id/binary, ":", VEnc/binary>>, Row))
     || Id <- ?IDS],
    ?assertEqual(error, barrel_doc_chain:owner(VEnc, Row)),
    ok.

%% Rows written in the old layout move at open; conflicts and history
%% read the same before and after.
t_moves_old_rows(Config) ->
    {A, B} = {?config(a, Config), ?config(b, Config)},
    _ = conflict_all(A, B),
    [_ | _] = [edit(A, Id, 2) || Id <- ?IDS],
    Before = chains(A),
    N = to_old_layout(A),
    ?assert(N > 0),
    ok = barrel_docdb:close_db(A),
    {ok, _} = barrel_docdb:create_db(A),
    ?assertEqual(Before, chains(A)),
    ?assertEqual(0, count(A, barrel_store_keys:legacy_doc_version_db_prefix(A))),
    ok.

%% A read-only store still holding old rows reads them with the owner
%% check: no other document's rows.
t_read_only_old_rows(Config) ->
    {A, B} = {?config(a, Config), ?config(b, Config)},
    _ = conflict_all(A, B),
    Before = chains(A),
    _ = to_old_layout(A),
    ok = barrel_docdb:close_db(A),
    {ok, _} = barrel_docdb:create_db(A, #{read_only => true}),
    ?assertEqual(Before, chains(A)),
    ?assert(count(A, barrel_store_keys:legacy_doc_version_db_prefix(A)) > 0),
    ok = barrel_docdb:close_db(A),
    {ok, _} = barrel_docdb:create_db(A),
    ok.

%% A version from an old peer whose vector names its author at a later
%% clock still owns its row.
t_owner_vector_ahead(_Config) ->
    Hlc = barrel_hlc:new_hlc(),
    Later = barrel_hlc:add_logical(Hlc, 5),
    VEnc = barrel_version:encode({Hlc, <<"node-1">>}),
    Row = <<0, 0, (barrel_vv:encode(#{<<"node-1">> => Later}))/binary>>,
    ?assertEqual({ok, <<"a:b">>, VEnc},
                 barrel_doc_chain:owner(<<"a:b:", VEnc/binary>>, Row)),
    ok.

%% A row whose vector names no version goes to the longest doc id that
%% has a document.
t_owner_without_vector(_Config) ->
    VEnc = barrel_version:encode({barrel_hlc:new_hlc(), <<"node-1">>}),
    Row = <<0, 0, (barrel_vv:encode(#{}))/binary>>,
    Both = fun(Id) -> lists:member(Id, [<<"a">>, <<"a:b">>]) end,
    ?assertEqual({ok, <<"a:b">>, VEnc},
                 barrel_doc_chain:owner(<<"a:b:", VEnc/binary>>, Row, Both)),
    ?assertEqual({ok, <<"a">>, VEnc},
                 barrel_doc_chain:owner(<<"a:", VEnc/binary>>, Row, Both)),
    ?assertEqual(error, barrel_doc_chain:owner(<<"a:", VEnc/binary>>, Row,
                                               fun(_) -> false end)),
    ok.

%% A peer on docdb 3.0 sends, as a version of `a', a version it read
%% from the row of `a:b': it is refused.
t_refuses_leaked_versions(Config) ->
    {A, B} = {?config(a, Config), ?config(b, Config)},
    {ok, _} = barrel_docdb:put_doc(A, #{<<"id">> => <<"a">>, <<"who">> => <<"a">>}),
    {ok, _} = barrel_docdb:put_doc(A, #{<<"id">> => <<"a:b">>, <<"who">> => <<"a:b">>}),
    {Leaked, VV} = leaked_version(A),
    {ok, #{state := StateBin, bodies := Bodies}} = barrel_docdb:get_doc_state(A, <<"a">>),
    State = barrel_live:decode_state(StateBin),
    Forged = State#{live => (barrel_live:live(State))#{Leaked => #{deleted => false,
                                                                    vv => VV}}},
    {ok, _, _} = barrel_docdb:put_doc_state(
                   B, <<"a">>, barrel_live:encode_state(Forged),
                   Bodies#{barrel_version:to_token(Leaked) => #{}}),
    {ok, Doc} = barrel_docdb:get_doc(B, <<"a">>, #{conflicts => true}),
    ?assertEqual(<<"a">>, maps:get(<<"who">>, Doc)),
    ?assertNot(maps:is_key(<<"_conflicts">>, Doc)),
    ok.

%% A database from 3.0 whose `a' took a version of `a:b' as winner: the
%% move promotes a version of `a' of its own.
t_repairs_leaked_winner(Config) ->
    {A, B} = {?config(a, Config), ?config(b, Config)},
    _ = conflict_all(A, B),
    {ok, #{<<"who">> := Shown}} = barrel_docdb:get_doc(A, <<"a">>),
    Own = hd([A, B] -- [Shown]),
    {Leaked, VV} = leaked_version(A),
    Store = persistent_term:get({barrel_store, A}),
    Key = barrel_store_keys:doc_entity(barrel_keyspace:resolve(A), <<"a">>),
    {ok, Columns} = barrel_store_rocksdb:get_entity(Store, Key),
    ok = barrel_store_rocksdb:put_entity(
           Store, Key,
           lists:keystore(<<"vv">>, 1,
                          lists:keystore(<<"ver">>, 1, Columns,
                                         {<<"ver">>, barrel_version:encode(Leaked)}),
                          {<<"vv">>, barrel_vv:encode(VV)})),
    _ = to_old_layout(A),
    ok = barrel_docdb:close_db(A),
    {ok, _} = barrel_docdb:create_db(A),
    {ok, Doc} = barrel_docdb:get_doc(A, <<"a">>, #{conflicts => true}),
    ?assertEqual(Own, maps:get(<<"who">>, Doc)),
    ?assertNotEqual(barrel_version:to_token(Leaked), maps:get(<<"_rev">>, Doc)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% The version docdb 3.0 read, for `a', from the row of `a:b': the
%% tail `b:' + version of `a:b', with that version's vector.
leaked_version(Db) ->
    {ok, #{state := StateBin}} = barrel_docdb:get_doc_state(Db, <<"a:b">>),
    Live = barrel_live:live(barrel_live:decode_state(StateBin)),
    {V, #{vv := VV}} = lists:last(lists:sort(maps:to_list(Live))),
    Leaked = barrel_version:decode(<<"b:", (barrel_version:encode(V))/binary>>),
    true = barrel_live:leaked(Leaked, VV),
    {Leaked, VV}.

%% Each id written on A, copied to B, then edited on both sides.
conflict_all(A, B) ->
    [{ok, _} = barrel_docdb:put_doc(A, #{<<"id">> => Id, <<"who">> => <<"base">>})
     || Id <- ?IDS],
    {ok, _} = barrel_rep:replicate(A, B),
    Edit = fun(Db, Id) ->
               {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db, Id),
               {ok, #{<<"rev">> := New}} =
                   barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"who">> => Db,
                                              <<"_rev">> => R}),
               New
           end,
    Revs = [{Id, [Edit(A, Id), Edit(B, Id)]} || Id <- ?IDS],
    {ok, _} = barrel_rep:replicate(B, A),
    maps:from_list(Revs).

%% N + 1 local writes; their versions.
edit(Db, Id, N) ->
    {ok, #{<<"rev">> := R0}} =
        case barrel_docdb:get_doc(Db, Id) of
            {ok, #{<<"_rev">> := R}} ->
                barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"n">> => 0,
                                           <<"_rev">> => R});
            {error, not_found} ->
                barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"n">> => 0})
        end,
    lists:foldl(fun(I, [Prev | _] = Acc) ->
                    {ok, #{<<"rev">> := Next}} =
                        barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"n">> => I,
                                                   <<"_rev">> => Prev}),
                    [Next | Acc]
                end, [R0], lists:seq(1, N)).

%% What every document's chain reads as: conflicts and versions.
chains(Db) ->
    [{Id, element(2, barrel_docdb:get_doc(Db, Id, #{conflicts => true})),
      element(2, barrel_docdb:get_doc_versions(Db, Id))} || Id <- ?IDS].

%% Rewrite every chain row of Db in the layout before 3.1 and forget
%% the move, as a 3.0 store holds them. Returns the row count.
to_old_layout(Db) ->
    Store = persistent_term:get({barrel_store, Db}),
    Ks = barrel_keyspace:resolve(Db),
    Name = <<(byte_size(Ks)):16, Ks/binary>>,
    Prefix = <<16#21, Name/binary>>,
    Rows = barrel_store_rocksdb:fold_range(
             Store, Prefix, barrel_store_keys:prefix_successor(Prefix),
             fun(K, V, Acc) -> {ok, [{K, V} | Acc]} end, []),
    Ops = lists:append(
            [begin
                 <<_:(byte_size(Prefix))/binary, L:32, Id:L/binary,
                   VEnc/binary>> = K,
                 [{delete, K},
                  {put, <<16#1D, Name/binary, Id/binary, ":", VEnc/binary>>, V}]
             end || {K, V} <- Rows]),
    ok = barrel_store_rocksdb:write_batch(
           Store, [{delete, barrel_store_keys:db_meta(Ks, <<"doc_chain">>)}
                   | Ops], #{sync => true}),
    length(Rows).

count(Db, Prefix) ->
    Store = persistent_term:get({barrel_store, Db}),
    barrel_store_rocksdb:fold_range(
      Store, Prefix, barrel_store_keys:prefix_successor(Prefix),
      fun(_K, _V, N) -> {ok, N + 1} end, 0).
