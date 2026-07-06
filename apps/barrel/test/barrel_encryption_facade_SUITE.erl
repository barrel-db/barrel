%%%-------------------------------------------------------------------
%%% @doc Facade encryption: one `encryption' spec encrypts the whole
%%% logical database (docdb stores + vector store) under one key,
%%% resolved once on the docdb keyspace. Branches inherit the spec
%%% from the parent handle and their fresh vector store encrypts under
%%% the parent's key.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_encryption_facade_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_plain_facade_encrypted/1,
    t_record_facade_encrypted/1,
    t_record_branch_encrypted/1,
    t_key_error_fails_open/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PROVIDER, barrel_facade_test_keyprovider).
-define(ENC, #{provider => ?PROVIDER}).

all() ->
    [t_plain_facade_encrypted, t_record_facade_encrypted,
     t_record_branch_encrypted, t_key_error_fails_open].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    Dir = "/tmp/barrel_encryption_facade_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    application:set_env(barrel, test_encryption_master,
                        <<"facade suite master">>),
    mock_embed(),
    Config.

end_per_testcase(_TC, _Config) ->
    application:set_env(barrel, test_encryption_master,
                        <<"facade suite master">>),
    try meck:unload(barrel_embed) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Helpers (mock embedder mirrors barrel_record_SUITE)
%%====================================================================

mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed, fun(Text, _State) ->
        {ok, mock_vec(Text)}
    end),
    meck:expect(barrel_embed, embed_batch, fun(Texts, _State) ->
        {ok, [mock_vec(T) || T <- Texts]}
    end).

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].

wait_until(Fun) ->
    wait_until(Fun, 100).

wait_until(_Fun, 0) ->
    ct:fail(condition_never_met);
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Fun, N - 1)
    end.

drained(#{docdb := DbBin}) ->
    [] =:= barrel_docdb:outbox_fold(
        DbBin, <<"embed">>, fun(E, Acc) -> {ok, [E | Acc]} end, []).

hit(Db, Text, Id) ->
    case barrel:search_vector(Db, mock_vec(Text), #{k => 1}) of
        {ok, [#{key := Id} | _]} -> true;
        _ -> false
    end.

open_plain(Name, Config) ->
    Dir = ?config(dir, Config),
    barrel:open(Name, #{
        encryption => ?ENC,
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3, bm25_backend => memory,
                      db_path => vec_path(Name, Config)}}).

open_record(Name, Config) ->
    Dir = ?config(dir, Config),
    barrel:open(Name, #{
        encryption => ?ENC,
        embedding => #{fields => [<<"title">>]},
        docdb => #{data_dir => Dir},
        %% memory BM25: the disk backend is rejected with crypto until
        %% its sector-cipher layer lands
        vectordb => #{dimension => 3, bm25_backend => memory,
                      db_path => vec_path(Name, Config)}}).

vec_path(Name, Config) ->
    ?config(dir, Config) ++ "/" ++ atom_to_list(Name) ++ "_vec".

doc_path(Name, Config) ->
    filename:join(?config(dir, Config), atom_to_list(Name)).

%%====================================================================
%% Cases
%%====================================================================

t_plain_facade_encrypted(Config) ->
    {ok, Db} = open_plain(enc_fp, Config),
    %% both sides carry a key-check marker
    ?assert(filelib:is_regular(
                filename:join(doc_path(enc_fp, Config), "CRYPTO"))),
    ?assert(filelib:is_regular(
                filename:join(vec_path(enc_fp, Config), "CRYPTO"))),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    ok = barrel:vector_add(Db, <<"a">>, <<"hello">>, #{},
                           [1.0, 0.0, 0.0]),
    {ok, [#{key := <<"a">>} | _]} =
        barrel:search_vector(Db, [1.0, 0.0, 0.0], #{k => 1}),
    ok = barrel:close(Db),
    %% reopen with the same spec
    {ok, Db2} = open_plain(enc_fp, Config),
    {ok, #{<<"v">> := 1}} = barrel:get_doc(Db2, <<"a">>),
    ok = barrel:close(Db2),
    %% a wrong master fails the docdb open first
    application:set_env(barrel, test_encryption_master, <<"other">>),
    ?assertEqual({error, wrong_encryption_key},
                 open_plain(enc_fp, Config)),
    ok.

t_record_facade_encrypted(Config) ->
    {ok, Db} = open_record(enc_fr, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"alpha doc">>}),
    ok = wait_until(fun() -> drained(Db) end),
    ?assert(hit(Db, <<"alpha doc">>, <<"a">>)),
    ?assert(filelib:is_regular(
                filename:join(vec_path(enc_fr, Config), "CRYPTO"))),
    ok = barrel:close(Db),
    {ok, Db2} = open_record(enc_fr, Config),
    ?assert(hit(Db2, <<"alpha doc">>, <<"a">>)),
    ok = barrel:close(Db2),
    ok.

t_record_branch_encrypted(Config) ->
    {ok, Db} = open_record(enc_fb, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"alpha doc">>}),
    ok = wait_until(fun() -> drained(Db) end),
    %% the branch handle inherits the spec; its fresh vector store
    %% encrypts under the PARENT's key (keyspace rule) and backfills
    {ok, Branch} = barrel:branch(Db, enc_fb_b,
        #{vectordb => #{dimension => 3,
                        db_path => vec_path(enc_fb_b, Config),
                        bm25_backend => memory}}),
    try
        ?assert(filelib:is_regular(
                    filename:join(vec_path(enc_fb_b, Config), "CRYPTO"))),
        ?assert(hit(Branch, <<"alpha doc">>, <<"a">>)),
        {ok, _} = barrel:put_doc(Branch, #{<<"id">> => <<"f">>,
                                           <<"title">> => <<"feature">>}),
        ok = wait_until(fun() -> drained(Branch) end),
        {ok, #{docs_written := 1}} = barrel:merge(Branch),
        {ok, _} = barrel:get_doc(Db, <<"f">>)
    after
        ok = barrel:delete(Branch)
    end,
    ok = barrel:close(Db),
    ok.

t_key_error_fails_open(Config) ->
    application:unset_env(barrel, test_encryption_master),
    ?assertMatch({error, {encryption_key_error,
                          {key_provider_error, ?PROVIDER, no_test_master}}},
                 open_plain(enc_fe, Config)),
    ok.
