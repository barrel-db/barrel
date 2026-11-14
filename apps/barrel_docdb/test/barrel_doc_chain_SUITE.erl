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
         t_read_only_old_rows/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(IDS, [<<"a">>, <<"a:b">>, <<"a:b:c">>, <<"a:">>]).

all() ->
    [t_conflicts_stay_apart, t_replication_keeps_docs, t_history_stays_apart,
     t_owner_split, t_moves_old_rows, t_read_only_old_rows].

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

%%====================================================================
%% Helpers
%%====================================================================

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
