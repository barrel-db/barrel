%%%-------------------------------------------------------------------
%%% @doc A plain database opened with `vectordb => none': documents and
%%% queries without a vector store.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docs_only_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_documents_and_queries/1,
    t_vector_calls_refused/1,
    t_no_vector_store_started/1,
    t_record_mode_refused/1,
    t_dbs_keeps_the_handle/1,
    t_branch_and_export/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_documents_and_queries, t_vector_calls_refused,
     t_no_vector_store_started, t_record_mode_refused,
     t_dbs_keeps_the_handle, t_branch_and_export].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    ok.

t_documents_and_queries(_Config) ->
    {ok, Db} = barrel:open(<<"docs_only_q">>, #{vectordb => none}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"lang">> => <<"en">>,
                                   <<"title">> => <<"alpha">>}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"b">>, <<"lang">> => <<"fr">>,
                                   <<"title">> => <<"beta">>}),
    {ok, #{<<"title">> := <<"alpha">>}} = barrel:get_doc(Db, <<"a">>),
    {ok, [#{<<"title">> := <<"alpha">>}], _} =
        barrel:query(Db, "SELECT title FROM db WHERE lang = 'en'"),
    {ok, Info} = barrel:info(Db),
    ?assertNot(maps:is_key(embedder, Info)),
    ok = barrel:delete(Db).

t_vector_calls_refused(_Config) ->
    {ok, Db} = barrel:open(<<"docs_only_v">>, #{vectordb => none}),
    E = {error, no_vector_store},
    ?assertEqual(E, barrel:vector_add(Db, <<"v">>, <<"t">>, #{})),
    ?assertEqual(E, barrel:vector_add(Db, <<"v">>, <<"t">>, #{}, [0.1])),
    ?assertEqual(E, barrel:vector_add_batch(Db, [])),
    ?assertEqual(E, barrel:vector_get(Db, <<"v">>)),
    ?assertEqual(E, barrel:vector_delete(Db, <<"v">>)),
    ?assertEqual(E, barrel:search(Db, <<"q">>, #{})),
    ?assertEqual(E, barrel:search_vector(Db, [0.1], #{})),
    ?assertEqual(E, barrel:search_bm25(Db, <<"q">>, #{})),
    ?assertEqual(E, barrel:search_hybrid(Db, <<"q">>, #{})),
    ?assertEqual(E, barrel:embed(Db, <<"q">>)),
    ?assertEqual(E, barrel:embed_batch(Db, [<<"q">>])),
    ?assertEqual(E, barrel:embedder_info(Db)),
    ?assertEqual(E, barrel:vector_stats(Db)),
    ?assertEqual(E, barrel:query(Db, "SELECT * FROM vector_top_k('q', k => 3) AS v")),
    ?assertEqual(E, barrel:query(Db, "SELECT * FROM bm25_top_k('q', k => 3) AS v")),
    ok = barrel:close(Db).

%% No vector store process and no vector directory.
t_no_vector_store_started(_Config) ->
    Name = <<"docs_only_s">>,
    {ok, Db} = barrel:open(Name, #{vectordb => none}),
    ?assertEqual(undefined,
                 barrel_vectordb_registry:whereis_name({vstore, Name})),
    ?assertNot(filelib:is_dir("priv/barrel_vectordb_" ++ binary_to_list(Name))),
    ok = barrel:close(Db),
    ?assertEqual({error, not_found}, barrel_docdb:db_pid(Name)).

t_record_mode_refused(_Config) ->
    ?assertEqual({error, {invalid_option, vectordb}},
                 barrel:open(<<"docs_only_r">>,
                             #{vectordb => none,
                               embedding => #{fields => [<<"text">>]}})).

%% barrel_dbs keeps a handle without a vector store open: the same
%% database process serves every ensure.
t_dbs_keeps_the_handle(_Config) ->
    Name = <<"docs_only_d">>,
    {ok, Db} = barrel_dbs:ensure(Name, #{vectordb => none}),
    {ok, Pid} = barrel_docdb:db_pid(Name),
    {ok, Db} = barrel_dbs:ensure(Name, #{vectordb => none}),
    ?assertEqual({ok, Pid}, barrel_docdb:db_pid(Name)),
    ok = barrel_dbs:close(Name).

%% A branch has no vector store either; an export refuses the database.
t_branch_and_export(Config) ->
    {ok, Db} = barrel:open(<<"docs_only_b">>, #{vectordb => none}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, Branch} = barrel:branch(Db, <<"docs_only_b2">>),
    ?assertNot(maps:is_key(vstore, Branch)),
    {ok, _} = barrel:get_doc(Branch, <<"a">>),
    ok = barrel:delete(Branch),
    ok = barrel:close(Db),
    Dest = filename:join(?config(priv_dir, Config), "docs_only_export"),
    ?assertEqual({error, no_vector_store},
                 barrel_ctx_export:export(<<"docs_only_b">>, Dest,
                                          #{open_opts => #{vectordb => none}})).
