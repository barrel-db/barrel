%%% @doc barrel-backup on a barrel_server node: the server's databases
%%% are barrel databases, and their backups carry the vector store.
%%% Drives the functions the script calls on the node.
-module(barrel_server_backup_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([backup_keeps_vectors/1, list_and_prune/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [backup_keeps_vectors, list_and_prune].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, open_opts,
                        #{vectordb => #{dimension => 3, bm25_backend => memory}}),
    {ok, _} = application:ensure_all_started(barrel_server),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, open_opts),
    ok.

%% A database the server opened, with documents and vectors, comes back
%% from barrel-backup with both, and the server serves it.
backup_keeps_vectors(Config) ->
    {ok, Db} = barrel_server_dbs:ensure(<<"bk_src">>, #{}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    ok = barrel:vector_add(Db, <<"a">>, <<"alpha">>, #{}, [1.0, 0.0, 0.0]),
    ok = barrel:vector_add(Db, <<"b">>, <<"beta">>, #{}, [0.0, 1.0, 0.0]),
    Dir = filename:join(?config(priv_dir, Config), "backups"),
    {ok, [{<<"bk_src">>, Path}]} = barrel_backup_cli:create(Dir, [<<"bk_src">>]),
    ?assert(filelib:is_regular(filename:join(Path, "BARREL.json"))),
    {ok, _} = barrel_backup_cli:restore(binary_to_list(Path), <<"bk_dst">>, #{}),
    {ok, R} = barrel_server_dbs:ensure(<<"bk_dst">>, #{}),
    {ok, #{<<"v">> := 1}} = barrel:get_doc(R, <<"a">>),
    ?assertMatch({ok, [#{key := <<"b">>} | _]},
                 barrel:search_vector(R, [0.0, 1.0, 0.0], #{k => 1})).

list_and_prune(Config) ->
    {ok, _} = barrel_server_dbs:ensure(<<"bk_many">>, #{}),
    Dir = filename:join(?config(priv_dir, Config), "many"),
    {ok, _} = barrel_backup_cli:create(Dir, [<<"bk_many">>]),
    timer:sleep(1100),
    {ok, _} = barrel_backup_cli:create(Dir, [<<"bk_many">>]),
    {ok, Rows} = barrel_backup_cli:list(Dir, [<<"bk_many">>]),
    ?assertEqual(2, length(Rows)),
    {ok, [_]} = barrel_backup_cli:prune(Dir, 1),
    ?assertMatch({ok, [_]}, barrel_backup_cli:list(Dir, [<<"bk_many">>])).
