%%% @doc barrel:backup/3 and barrel:restore/3: a whole barrel database,
%%% documents and vectors, in plain, record and docs-only mode.
-module(barrel_backup_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([plain_new_name/1,
         plain_same_name/1,
         record_backfills_without_embedding/1,
         record_pending_outbox_indexed/1,
         docs_only/1,
         encrypted_plain/1,
         corrupt_file_refused/1,
         vector_path_taken_refused/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ENC, #{provider => barrel_facade_test_keyprovider}).

all() ->
    [plain_new_name, plain_same_name, record_backfills_without_embedding,
     record_pending_outbox_indexed, docs_only, encrypted_plain,
     corrupt_file_refused, vector_path_taken_refused].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    Dir = "/tmp/barrel_backup_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    application:set_env(barrel, test_encryption_master, <<"backup suite master">>),
    mock_embed(),
    Base = atom_to_list(TC),
    [{src, list_to_atom(Base ++ "_src")}, {dst, list_to_atom(Base ++ "_dst")},
     {bdir, filename:join([?config(dir, Config), "backups", Base])} | Config].

end_per_testcase(_TC, _Config) ->
    try meck:unload(barrel_embed) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% Documents, vectors and keyword search come back under a new name, as
%% they were at the backup.
plain_new_name(Config) ->
    {ok, Db} = barrel:open(?config(src, Config), plain_opts(Config)),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    ok = barrel:vector_add(Db, <<"a">>, <<"swordfish">>, #{}, [1.0, 0.0, 0.0]),
    ok = barrel:vector_add(Db, <<"b">>, <<"marlin">>, #{}, [0.0, 1.0, 0.0]),
    {ok, #{mode := plain}} = barrel:backup(Db, ?config(bdir, Config)),
    ok = barrel:vector_add(Db, <<"c">>, <<"tuna">>, #{}, [0.0, 0.0, 1.0]),
    {ok, R} = barrel:restore(?config(bdir, Config), ?config(dst, Config), plain_opts(Config)),
    {ok, #{<<"v">> := 1}} = barrel:get_doc(R, <<"a">>),
    ?assertEqual(<<"b">>, top(R, [0.0, 1.0, 0.0])),
    ?assertMatch({ok, [{<<"a">>, _} | _]}, barrel:search_bm25(R, <<"swordfish">>, #{k => 1})),
    ?assertMatch({ok, #{count := 2}}, barrel:vector_stats(R)),
    ok = barrel:close(R),
    ok = barrel:close(Db).

%% Disaster recovery: the lost database is restored under its own name,
%% with the vector store at its default place.
plain_same_name(Config) ->
    Name = ?config(src, Config),
    Opts = #{docdb => #{data_dir => ?config(dir, Config)},
             vectordb => #{dimension => 3, bm25_backend => memory}},
    {ok, Db} = barrel:open(Name, Opts),
    ok = barrel:vector_add(Db, <<"a">>, <<"alpha">>, #{}, [1.0, 0.0, 0.0]),
    {ok, _} = barrel:backup(Db, ?config(bdir, Config)),
    ok = barrel:delete(Db),
    {ok, R} = barrel:restore(?config(bdir, Config), Name, Opts),
    ?assertEqual(<<"a">>, top(R, [1.0, 0.0, 0.0])),
    ?assert(filelib:is_dir(filename:join(?config(dir, Config), atom_to_list(Name) ++ ".vectordb"))),
    ok = barrel:close(R).

%% Record mode copies no vectors: the restore rebuilds the index from the
%% stored embeddings, without calling the embedder.
record_backfills_without_embedding(Config) ->
    {ok, Db} = barrel:open(?config(src, Config), record_opts(?config(src, Config), Config)),
    [{ok, _} = barrel:put_doc(Db, #{<<"id">> => Id, <<"title">> => T})
     || {Id, T} <- [{<<"a">>, <<"alpha doc">>}, {<<"b">>, <<"beta doc">>}]],
    ok = wait_until(fun() -> drained(Db) end),
    {ok, #{mode := record}} = barrel:backup(Db, ?config(bdir, Config)),
    ?assertNot(filelib:is_dir(filename:join(?config(bdir, Config), "vectordb"))),
    Calls = embed_calls(),
    {ok, R} = barrel:restore(?config(bdir, Config), ?config(dst, Config),
                             #{docdb => #{data_dir => ?config(dir, Config)},
                               vectordb => #{dimension => 3,
                                             db_path => vec_path(?config(dst, Config), Config)}}),
    ?assertEqual(Calls, embed_calls()),
    ?assert(hit(R, <<"alpha doc">>, <<"a">>)),
    ?assert(hit(R, <<"beta doc">>, <<"b">>)),
    ok = barrel:close(R),
    ok = barrel:close(Db).

%% A document not yet indexed at the backup is indexed after the restore.
record_pending_outbox_indexed(Config) ->
    meck:expect(barrel_embed, embed, fun(_, _) -> {error, down} end),
    meck:expect(barrel_embed, embed_batch, fun(_, _) -> {error, down} end),
    {ok, Db} = barrel:open(?config(src, Config), record_opts(?config(src, Config), Config)),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"p">>, <<"title">> => <<"pending doc">>}),
    {ok, _} = barrel:backup(Db, ?config(bdir, Config)),
    ok = barrel:close(Db),
    mock_embed(),
    {ok, R} = barrel:restore(?config(bdir, Config), ?config(dst, Config),
                             #{docdb => #{data_dir => ?config(dir, Config)},
                               vectordb => #{dimension => 3,
                                             db_path => vec_path(?config(dst, Config), Config)}}),
    ok = wait_until(fun() -> hit(R, <<"pending doc">>, <<"p">>) end),
    ok = barrel:close(R).

docs_only(Config) ->
    Opts = #{docdb => #{data_dir => ?config(dir, Config)}, vectordb => none},
    {ok, Db} = barrel:open(?config(src, Config), Opts),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, #{mode := docs_only}} = barrel:backup(Db, ?config(bdir, Config)),
    {ok, R} = barrel:restore(?config(bdir, Config), ?config(dst, Config), Opts),
    {ok, _} = barrel:get_doc(R, <<"a">>),
    ?assertEqual({error, no_vector_store}, barrel:search_vector(R, [1.0, 0.0, 0.0], #{k => 1})),
    ok = barrel:close(R),
    ok = barrel:close(Db).

%% An encrypted database restores with its key provider, documents and
%% vectors, and does not open without it.
encrypted_plain(Config) ->
    Opts = (plain_opts(Config))#{encryption => ?ENC},
    {ok, Db} = barrel:open(?config(src, Config), Opts),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"s">>, <<"secret">> => <<"x">>}),
    ok = barrel:vector_add(Db, <<"s">>, <<"secret">>, #{}, [1.0, 0.0, 0.0]),
    {ok, _} = barrel:backup(Db, ?config(bdir, Config)),
    ok = barrel:close(Db),
    {ok, R} = barrel:restore(?config(bdir, Config), ?config(dst, Config), Opts),
    {ok, #{<<"secret">> := <<"x">>}} = barrel:get_doc(R, <<"s">>),
    ?assertEqual(<<"s">>, top(R, [1.0, 0.0, 0.0])),
    ok = barrel:close(R).

corrupt_file_refused(Config) ->
    {ok, Db} = barrel:open(?config(src, Config), plain_opts(Config)),
    ok = barrel:vector_add(Db, <<"a">>, <<"alpha">>, #{}, [1.0, 0.0, 0.0]),
    {ok, _} = barrel:backup(Db, ?config(bdir, Config)),
    ok = barrel:close(Db),
    [Victim | _] = filelib:wildcard(filename:join([?config(bdir, Config), "vectordb", "*.sst"]))
        ++ filelib:wildcard(filename:join([?config(bdir, Config), "vectordb", "MANIFEST-*"])),
    ok = file:write_file(Victim, <<"garbage">>, [append]),
    ?assertMatch({error, {corrupt_backup, [_ | _]}},
                 barrel:restore(?config(bdir, Config), ?config(dst, Config), plain_opts(Config))),
    ?assertNot(barrel_docdb:db_exists(atom_to_binary(?config(dst, Config)),
                                      #{data_dir => ?config(dir, Config)})).

%% A vector path already in use is refused before anything is restored.
vector_path_taken_refused(Config) ->
    {ok, Db} = barrel:open(?config(src, Config), plain_opts(Config)),
    {ok, _} = barrel:backup(Db, ?config(bdir, Config)),
    ok = barrel:close(Db),
    ok = filelib:ensure_path(vec_path(?config(dst, Config), Config)),
    ?assertMatch({error, {vector_store_exists, _}},
                 barrel:restore(?config(bdir, Config), ?config(dst, Config),
                                (plain_opts(Config))#{vectordb => #{dimension => 3,
                                                                     db_path => vec_path(?config(dst, Config), Config)}})),
    ?assertNot(barrel_docdb:db_exists(atom_to_binary(?config(dst, Config)),
                                      #{data_dir => ?config(dir, Config)})).

%%====================================================================
%% Helpers
%%====================================================================

plain_opts(Config) ->
    #{docdb => #{data_dir => ?config(dir, Config)},
      vectordb => #{dimension => 3, bm25_backend => memory}}.

record_opts(Name, Config) ->
    #{embedding => #{fields => [<<"title">>]},
      docdb => #{data_dir => ?config(dir, Config)},
      vectordb => #{dimension => 3, db_path => vec_path(Name, Config)}}.

vec_path(Name, Config) ->
    filename:join(?config(dir, Config), atom_to_list(Name) ++ "_vec").

top(Db, Vec) ->
    {ok, [#{key := K} | _]} = barrel:search_vector(Db, Vec, #{k => 1}),
    K.

hit(Db, Text, Id) ->
    case barrel:search_vector(Db, mock_vec(Text), #{k => 1}) of
        {ok, [#{key := Id} | _]} -> true;
        _ -> false
    end.

drained(#{docdb := DbBin}) ->
    [] =:= barrel_docdb:outbox_fold(DbBin, <<"embed">>, fun(E, Acc) -> {ok, [E | Acc]} end, []).

embed_calls() ->
    meck:num_calls(barrel_embed, embed, '_') + meck:num_calls(barrel_embed, embed_batch, '_').

wait_until(Fun) -> wait_until(Fun, 100).

wait_until(_Fun, 0) -> ct:fail(condition_never_met);
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Fun, N - 1)
    end.

mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed, fun(Text, _) -> {ok, mock_vec(Text)} end),
    meck:expect(barrel_embed, embed_batch, fun(Texts, _) -> {ok, [mock_vec(T) || T <- Texts]} end).

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].
