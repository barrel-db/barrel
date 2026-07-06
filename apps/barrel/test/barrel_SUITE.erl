%%%-------------------------------------------------------------------
%%% @doc End-to-end tests for the barrel facade.
%%%
%%% Each case opens its own composed database (docdb + vectordb sharing a name
%%% and id space) under the suite priv_dir and exercises the public API through
%%% the real layers: documents, batches, attachments (incl. streaming), vectors,
%%% search, and the changes feed.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_doc_crud/1,
    t_batch_docs/1,
    t_attachments/1,
    t_attachment_stream/1,
    t_vectors/1,
    t_vector_batch/1,
    t_changes/1,
    t_delete_db/1,
    t_binary_names/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DIM, 8).

all() ->
    [t_doc_crud, t_batch_docs, t_attachments, t_attachment_stream,
     t_vectors, t_vector_batch, t_changes, t_delete_db,
     t_binary_names].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_vectordb),
    %% Set after start: docdb reads data_dir at each db open, and load resets
    %% env to the .app default, so this must happen post-start.
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Priv = ?config(priv_dir, Config),
    VPath = filename:join(Priv, atom_to_list(Case) ++ "_vec"),
    VCfg = #{dimension => ?DIM, db_path => VPath, bm25_backend => memory},
    {ok, Db} = barrel:open(Case, #{vectordb => VCfg}),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    ok = barrel:close(?config(db, Config)),
    ok.

%%====================================================================
%% Documents
%%====================================================================

t_doc_crud(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"title">> => <<"hello">>}),
    {ok, Doc} = barrel:get_doc(Db, <<"a">>),
    ?assertEqual(<<"hello">>, maps:get(<<"title">>, Doc)),
    ?assertMatch(#{<<"_rev">> := _}, Doc),
    {ok, _} = barrel:delete_doc(Db, <<"a">>),
    ?assertEqual({error, not_found}, barrel:get_doc(Db, <<"a">>)),
    ok.

t_batch_docs(Config) ->
    Db = ?config(db, Config),
    Docs = [#{<<"id">> => <<"b1">>, <<"n">> => 1},
            #{<<"id">> => <<"b2">>, <<"n">> => 2},
            #{<<"id">> => <<"b3">>, <<"n">> => 3}],
    PutRes = barrel:put_docs(Db, Docs),
    ?assertEqual(3, length(PutRes)),
    [?assertMatch({ok, _}, R) || R <- PutRes],

    GetRes = barrel:get_docs(Db, [<<"b1">>, <<"b2">>, <<"b3">>]),
    ?assertEqual(3, length(GetRes)),
    [?assertMatch({ok, _}, R) || R <- GetRes],
    [{ok, D1} | _] = GetRes,
    ?assertEqual(1, maps:get(<<"n">>, D1)),

    %% Missing ids report per-element, in order.
    Mixed = barrel:get_docs(Db, [<<"b1">>, <<"nope">>]),
    ?assertMatch([{ok, _}, {error, not_found}], Mixed),

    DelRes = barrel:delete_docs(Db, [<<"b1">>, <<"b2">>, <<"b3">>]),
    ?assertEqual(3, length(DelRes)),
    [?assertMatch({ok, _}, R) || R <- DelRes],
    ?assertEqual({error, not_found}, barrel:get_doc(Db, <<"b1">>)),
    ok.

%%====================================================================
%% Attachments
%%====================================================================

t_attachments(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel:put_attachment(Db, <<"a">>, <<"f.txt">>, <<"bytes">>),
    ?assertEqual({ok, <<"bytes">>}, barrel:get_attachment(Db, <<"a">>, <<"f.txt">>)),
    ?assertEqual([<<"f.txt">>], barrel:list_attachments(Db, <<"a">>)),
    {ok, Info} = barrel:attachment_info(Db, <<"a">>, <<"f.txt">>),
    ?assert(is_map(Info)),
    ok = barrel:delete_attachment(Db, <<"a">>, <<"f.txt">>),
    ?assertEqual([], barrel:list_attachments(Db, <<"a">>)),
    ok.

t_attachment_stream(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"big">>}),
    {ok, W0} = barrel:open_attachment_writer(Db, <<"big">>, <<"blob">>,
                                             <<"application/octet-stream">>),
    {ok, W1} = barrel:write_attachment(W0, <<"chunk-one;">>),
    {ok, W2} = barrel:write_attachment(W1, <<"chunk-two">>),
    {ok, _Info} = barrel:finish_attachment(W2),

    {ok, R0} = barrel:open_attachment_reader(Db, <<"big">>, <<"blob">>),
    {Bytes, RLast} = read_all(R0, []),
    ok = barrel:close_attachment_reader(RLast),
    ?assertEqual(<<"chunk-one;chunk-two">>, Bytes),
    ok.

read_all(Stream, Acc) ->
    case barrel:read_attachment(Stream) of
        {ok, Chunk, S2} -> read_all(S2, [Chunk | Acc]);
        eof -> {iolist_to_binary(lists:reverse(Acc)), Stream};
        {error, R} -> ct:fail({read_attachment, R})
    end.

%%====================================================================
%% Vectors and search
%%====================================================================

t_vectors(Config) ->
    Db = ?config(db, Config),
    V = vec(0.1),
    ok = barrel:vector_add(Db, <<"a">>, <<"hello world">>, #{<<"k">> => <<"v">>}, V),
    ?assertMatch({ok, _}, barrel:vector_get(Db, <<"a">>)),
    {ok, Hits} = barrel:search_vector(Db, V, #{k => 5}),
    ?assert(lists:any(fun(H) -> maps:get(key, H, undefined) =:= <<"a">> end, Hits)),
    %% BM25 keyword search (enabled via bm25_backend => memory) finds the doc
    %% indexed on the explicit-vector add path. Hits are {Id, Score} tuples.
    {ok, BHits} = barrel:search_bm25(Db, <<"hello">>, #{k => 5}),
    ?assert(lists:keymember(<<"a">>, 1, BHits)),
    %% Hybrid must vectorise the text query, so it needs an embedder; without a
    %% model configured it reports embedder_not_configured. The happy path is
    %% covered where a model is available.
    ?assertEqual({error, embedder_not_configured},
                 barrel:search_hybrid(Db, <<"hello">>, #{k => 5})),
    ok = barrel:vector_delete(Db, <<"a">>),
    ?assertEqual(not_found, barrel:vector_get(Db, <<"a">>)),
    ok.

t_vector_batch(Config) ->
    Db = ?config(db, Config),
    Batch = [{<<"v1">>, <<"t1">>, #{}, vec(0.1)},
             {<<"v2">>, <<"t2">>, #{}, vec(0.2)}],
    ?assertMatch({ok, #{inserted := 2}}, barrel:vector_add_batch(Db, Batch)),
    {ok, Hits} = barrel:search_vector(Db, vec(0.2), #{k => 5}),
    ?assert(length(Hits) >= 1),
    %% A batch must be all one shape.
    ?assertEqual({error, mixed_batch},
                 barrel:vector_add_batch(Db, [{<<"x">>, <<"t">>, #{}},
                                              {<<"y">>, <<"t">>, #{}, vec(0.3)}])),
    ok.

%%====================================================================
%% Changes feed + HLC cursor
%%====================================================================

t_changes(Config) ->
    Db = ?config(db, Config),
    [{ok, _}, {ok, _}] =
        barrel:put_docs(Db, [#{<<"id">> => <<"c1">>}, #{<<"id">> => <<"c2">>}]),
    {ok, Changes, Last} = barrel:changes(Db, first),
    ?assert(length(Changes) >= 2),

    %% Cursor round-trips through hlc_encode/decode.
    Cursor = barrel:hlc_encode(Last),
    ?assert(is_binary(Cursor)),
    ?assertEqual(Last, barrel:hlc_decode(Cursor)),

    %% Nothing has changed since the last cursor.
    {ok, Changes2, _} = barrel:changes(Db, Last),
    ?assertEqual(0, length(Changes2)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

vec(Base) ->
    [Base + (I / 100) || I <- lists:seq(1, ?DIM)].

%%====================================================================
%% Lifecycle
%%====================================================================

t_delete_db(Config) ->
    #{docdb := DbBin, vstore := Store} = Db = ?config(db, Config),
    Priv = ?config(priv_dir, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    ok = barrel:vector_add(Db, <<"a">>, <<"text">>, #{},
                           lists:duplicate(?DIM, 0.5)),
    {ok, VPath} = barrel_vectordb_server:get_db_path(Store),
    DocPath = filename:join(Priv, binary_to_list(DbBin)),
    ?assert(filelib:is_dir(VPath)),
    ?assert(filelib:is_dir(DocPath)),
    ok = barrel:delete(Db),
    %% both storage trees are gone and a fresh open starts empty
    ?assertNot(filelib:is_dir(VPath)),
    ?assertNot(filelib:is_dir(DocPath)),
    VCfg = #{dimension => ?DIM, db_path => VPath,
             bm25_backend => memory},
    %% the reopen registers the same names, so the standard
    %% teardown close applies to this fresh instance
    {ok, Db2} = barrel:open(t_delete_db, #{vectordb => VCfg}),
    {error, not_found} = barrel:get_doc(Db2, <<"a">>),
    ok.

%%====================================================================
%% Binary names
%%====================================================================

t_binary_names(Config) ->
    Priv = ?config(priv_dir, Config),
    Name = <<"binname_", (integer_to_binary(
        erlang:unique_integer([positive])))/binary>>,
    VPath = filename:join(Priv, binary_to_list(Name) ++ "_vec"),
    {ok, Db} = barrel:open(Name, #{vectordb => #{dimension => ?DIM,
                                                 db_path => VPath,
                                                 bm25_backend => memory}}),
    %% the handle is binary-named end to end
    ?assertMatch(#{name := Name, docdb := Name, vstore := Name}, Db),
    Vec = [1.0 | lists:duplicate(?DIM - 1, 0.0)],
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    {ok, #{<<"v">> := 1}} = barrel:get_doc(Db, <<"a">>),
    ok = barrel:vector_add(Db, <<"a">>, <<"hello">>, #{}, Vec),
    {ok, [_ | _]} = barrel:search_vector(Db, Vec, #{k => 1}),
    %% a branch by binary name works and stays binary
    Branch = <<Name/binary, "_b">>,
    BVPath = filename:join(Priv, binary_to_list(Branch) ++ "_vec"),
    {ok, BranchDb} = barrel:branch(Db, Branch,
                                   #{vectordb => #{dimension => ?DIM,
                                                   db_path => BVPath,
                                                   bm25_backend => memory}}),
    ?assertMatch(#{name := Branch}, BranchDb),
    {ok, _} = barrel:get_doc(BranchDb, <<"a">>),
    ok = barrel:delete(BranchDb),
    ok = barrel:close(Db),
    %% deterministic no-atom-leak proof for the whole facade path
    ?assertError(badarg, binary_to_existing_atom(Name, utf8)),
    ?assertError(badarg, binary_to_existing_atom(Branch, utf8)),
    ok.
