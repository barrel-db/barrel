%%%-------------------------------------------------------------------
%%% @doc CT suite for record-mode building blocks: the read-through
%%% docstore adapter (barrel_record_docstore) against a real docdb.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_record_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([adapter_get/1,
         adapter_multi_get/1,
         adapter_missing_and_deleted/1,
         adapter_is_read_only/1,
         adapter_never_touches_doc/1,
         record_open_tags_writes/1,
         record_user_tags_preserved/1,
         record_policy_persisted/1,
         record_sync_mode_rejected/1,
         record_dimension_mismatch/1,
         record_plain_open_untagged/1,
         indexer_async_indexing/1,
         indexer_update_reembeds/1,
         indexer_delete_removes_vector/1,
         indexer_nonmatching_acked/1,
         indexer_crash_restart/1,
         indexer_poison_parked/1,
         sync_put_immediate_search/1,
         sync_embed_failure_fails_put/1,
         sync_index_failure_healed/1,
         sync_delete_immediate/1,
         sync_batch_put/1,
         explicit_vector_skips_embedder/1,
         explicit_vector_dimension_check/1,
         search_end_to_end/1,
         vector_add_guards/1,
         byo_embedding_async/1,
         byo_embedding_sync_and_precedence/1,
         byo_embedding_dimension_check/1,
         byo_embedding_not_path_indexed/1,
         computed_embedding_stored/1,
         computed_roundtrip_reembeds/1]).

all() ->
    [adapter_get,
     adapter_multi_get,
     adapter_missing_and_deleted,
     adapter_is_read_only,
     adapter_never_touches_doc,
     record_open_tags_writes,
     record_user_tags_preserved,
     record_policy_persisted,
     record_sync_mode_rejected,
     record_dimension_mismatch,
     record_plain_open_untagged,
     indexer_async_indexing,
     indexer_update_reembeds,
     indexer_delete_removes_vector,
     indexer_nonmatching_acked,
     indexer_crash_restart,
     indexer_poison_parked,
     sync_put_immediate_search,
     sync_embed_failure_fails_put,
     sync_index_failure_healed,
     sync_delete_immediate,
     sync_batch_put,
     explicit_vector_skips_embedder,
     explicit_vector_dimension_check,
     search_end_to_end,
     vector_add_guards,
     byo_embedding_async,
     byo_embedding_sync_and_precedence,
     byo_embedding_dimension_check,
     byo_embedding_not_path_indexed,
     computed_embedding_stored,
     computed_roundtrip_reembeds].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    Dir = "/tmp/barrel_record_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    case lists:member(TC, meck_cases()) of
        true -> mock_embed();
        false -> ok
    end,
    Db = atom_to_binary(TC, utf8),
    {ok, _Pid} = barrel_docdb:create_db(Db, #{data_dir => ?config(dir, Config)}),
    {ok, Policy} = barrel_embedding_policy:validate(#{
        fields => [<<"title">>, <<"body">>],
        metadata_fields => [<<"kind">>]
    }),
    {ok, Ctx} = barrel_record_docstore:init(
        binary_to_atom(Db, utf8), #{db => Db, policy => Policy}),
    [{db, Db}, {ctx, Ctx} | Config].

end_per_testcase(TC, Config) ->
    case lists:member(TC, meck_cases()) of
        true -> try meck:unload(barrel_embed) catch _:_ -> ok end;
        false -> ok
    end,
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

meck_cases() ->
    [indexer_async_indexing,
     indexer_update_reembeds,
     indexer_delete_removes_vector,
     indexer_nonmatching_acked,
     indexer_crash_restart,
     indexer_poison_parked,
     sync_put_immediate_search,
     sync_embed_failure_fails_put,
     sync_index_failure_healed,
     sync_delete_immediate,
     sync_batch_put,
     explicit_vector_skips_embedder,
     explicit_vector_dimension_check,
     search_end_to_end,
     vector_add_guards,
     byo_embedding_async,
     byo_embedding_sync_and_precedence,
     byo_embedding_dimension_check,
     byo_embedding_not_path_indexed,
     computed_embedding_stored,
     computed_roundtrip_reembeds].

%% Deterministic 3-dim embedder; texts containing "poison" fail.
mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed, fun(Text, _State) ->
        case binary:match(Text, <<"poison">>) of
            nomatch -> {ok, mock_vec(Text)};
            _ -> {error, poison}
        end
    end),
    meck:expect(barrel_embed, embed_batch, fun(Texts, _State) ->
        case lists:any(fun(T) -> binary:match(T, <<"poison">>) =/= nomatch end,
                       Texts) of
            true -> {error, poison};
            false -> {ok, [mock_vec(T) || T <- Texts]}
        end
    end).

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].

%% The stored column uses 32-bit floats; compare against the same width.
mock_vec32(Text) ->
    barrel_doc:decode_embedding(barrel_doc:encode_embedding(mock_vec(Text))).

%%====================================================================
%% Test cases
%%====================================================================

adapter_get(Config) ->
    Db = ?config(db, Config),
    Ctx = ?config(ctx, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"a">>, <<"title">> => <<"Hello">>,
        <<"body">> => <<"World">>, <<"kind">> => <<"note">>,
        <<"noise">> => 42}),
    {ok, Text, Meta} = barrel_record_docstore:get(Ctx, <<"a">>),
    ?assertEqual(<<"Hello\nWorld">>, Text),
    %% metadata_fields projection applies
    ?assertEqual(#{<<"kind">> => <<"note">>}, Meta).

adapter_multi_get(Config) ->
    Db = ?config(db, Config),
    Ctx = ?config(ctx, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"a">>, <<"title">> => <<"A">>, <<"kind">> => <<"x">>}),
    {ok, _} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"b">>, <<"title">> => <<"B">>, <<"kind">> => <<"y">>}),
    Results = barrel_record_docstore:multi_get(Ctx, [<<"b">>, <<"gone">>, <<"a">>]),
    [{ok, <<"B">>, #{<<"kind">> := <<"y">>}},
     not_found,
     {ok, <<"A">>, #{<<"kind">> := <<"x">>}}] = Results,
    ok.

adapter_missing_and_deleted(Config) ->
    Db = ?config(db, Config),
    Ctx = ?config(ctx, Config),
    ?assertEqual(not_found, barrel_record_docstore:get(Ctx, <<"nope">>)),
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"d">>, <<"title">> => <<"T">>}),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"d">>, #{rev => Rev}),
    %% Deleted documents read as missing through the adapter
    ?assertEqual(not_found, barrel_record_docstore:get(Ctx, <<"d">>)).

adapter_is_read_only(Config) ->
    Ctx = ?config(ctx, Config),
    ?assertEqual({error, read_only},
                 barrel_record_docstore:put(Ctx, <<"a">>, <<"t">>, #{})),
    ?assertEqual({error, read_only},
                 barrel_record_docstore:multi_put(Ctx, [{<<"a">>, <<"t">>, #{}}])).

adapter_never_touches_doc(Config) ->
    Db = ?config(db, Config),
    Ctx = ?config(ctx, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"keep">>, <<"title">> => <<"T">>}),
    %% delete on the adapter is a vector-side no-op: the doc survives
    ?assertEqual(ok, barrel_record_docstore:delete(Ctx, <<"keep">>)),
    {ok, _Doc} = barrel_docdb:get_doc(Db, <<"keep">>),
    %% terminate never closes the database
    ?assertEqual(ok, barrel_record_docstore:terminate(Ctx)),
    {ok, _Doc2} = barrel_docdb:get_doc(Db, <<"keep">>),
    ok.

%%====================================================================
%% Test cases: record-mode open + write tagging
%%====================================================================

record_open_tags_writes(Config) ->
    {ok, Db} = open_record(record_tags_db, Config, #{fields => [<<"title">>]}),
    %% Freeze the indexer so pending-entry assertions cannot race it
    ok = sys:suspend(erlang:whereis(barrel_record_indexer:name(record_tags_db))),
    DbBin = <<"record_tags_db">>,
    {ok, #{<<"rev">> := Rev}} = barrel:put_doc(Db, #{
        <<"id">> => <<"a">>, <<"title">> => <<"hello">>}),
    [Entry] = pending(DbBin),
    ?assertEqual(<<"a">>, maps:get(id, Entry)),
    ?assertEqual(false, maps:get(deleted, Entry)),
    %% Tagged delete replaces the entry with a deleted one
    {ok, _} = barrel:delete_doc(Db, <<"a">>),
    [Entry2] = pending(DbBin),
    ?assertEqual(true, maps:get(deleted, Entry2)),
    ?assertNotEqual(Rev, maps:get(rev, Entry2)),
    %% Batch writes tag every doc
    [_, _] = [R || {ok, _} = R <- barrel:put_docs(Db, [
        #{<<"id">> => <<"b">>, <<"title">> => <<"B">>},
        #{<<"id">> => <<"c">>, <<"title">> => <<"C">>}])],
    ?assertEqual(3, length(pending(DbBin))),
    ok = barrel:close(Db).

record_user_tags_preserved(Config) ->
    {ok, Db} = open_record(record_user_tags_db, Config,
                           #{fields => [<<"title">>]}),
    ok = sys:suspend(
        erlang:whereis(barrel_record_indexer:name(record_user_tags_db))),
    DbBin = <<"record_user_tags_db">>,
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"title">> => <<"T">>},
                             #{outbox => [<<"audit">>]}),
    ?assertEqual(1, length(pending(DbBin))),
    [AuditEntry] = lists:reverse(barrel_docdb:outbox_fold(
        DbBin, <<"audit">>, fun(E, Acc) -> {ok, [E | Acc]} end, [])),
    ?assertEqual(<<"a">>, maps:get(id, AuditEntry)),
    ok = barrel:close(Db).

record_policy_persisted(Config) ->
    Policy1 = #{fields => [<<"title">>]},
    {ok, Db} = open_record(record_policy_db, Config, Policy1),
    DbBin = <<"record_policy_db">>,
    {ok, #{<<"policy">> := Stored1}} =
        barrel_docdb:get_local_doc(DbBin, <<"_barrel/embedding">>),
    ?assertMatch(#{fields := [[<<"title">>]]}, binary_to_term(Stored1)),
    ok = barrel:close(Db),
    %% Reopen with a different policy: overwritten (a warning is logged)
    {ok, Db2} = open_record(record_policy_db, Config,
                            #{fields => [<<"body">>]}),
    {ok, #{<<"policy">> := Stored2}} =
        barrel_docdb:get_local_doc(DbBin, <<"_barrel/embedding">>),
    ?assertMatch(#{fields := [[<<"body">>]]}, binary_to_term(Stored2)),
    ok = barrel:close(Db2).

record_sync_mode_rejected(Config) ->
    %% Historical name: sync mode was rejected until the sync write path
    %% landed; opening a sync-mode database now succeeds.
    {ok, Db} = open_record(record_sync_db, Config,
                           #{fields => [<<"t">>], mode => sync}),
    ok = barrel:close(Db).

record_dimension_mismatch(Config) ->
    Dir = ?config(dir, Config),
    ?assertEqual({error, {dimension_mismatch, 3, 4}},
                 barrel:open(record_dim_db, #{
                     embedding => #{fields => [<<"t">>], dimensions => 3},
                     docdb => #{data_dir => Dir},
                     vectordb => #{dimension => 4,
                                   db_path => Dir ++ "/record_dim_vec"}})).

record_plain_open_untagged(Config) ->
    Dir = ?config(dir, Config),
    {ok, Db} = barrel:open(record_plain_db, #{
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3, db_path => Dir ++ "/record_plain_vec",
                      bm25_backend => memory}}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"title">> => <<"T">>}),
    ?assertEqual([], pending(<<"record_plain_db">>)),
    ok = barrel:close(Db).

%%====================================================================
%% Test cases: the record indexer (async vector indexing)
%%====================================================================

indexer_async_indexing(Config) ->
    {ok, Db} = open_record(idx_async_db, Config, #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"hello world">>}),
    ok = wait_until(fun() ->
        case barrel:search_vector(Db, mock_vec(<<"hello world">>), #{k => 1}) of
            {ok, [#{key := <<"a">>}]} -> true;
            _ -> false
        end
    end),
    %% Outbox drained
    ok = wait_until(fun() -> pending(<<"idx_async_db">>) =:= [] end),
    ok = barrel:close(Db).

indexer_update_reembeds(Config) ->
    {ok, Db} = open_record(idx_update_db, Config, #{fields => [<<"title">>]}),
    {ok, #{<<"rev">> := Rev}} = barrel:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"title">> => <<"first">>}),
    ok = wait_until(fun() -> hit(Db, <<"first">>, <<"a">>) end),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"second">>,
                                   <<"_rev">> => Rev}),
    ok = wait_until(fun() -> hit(Db, <<"second">>, <<"a">>) end),
    ok = barrel:close(Db).

indexer_delete_removes_vector(Config) ->
    {ok, Db} = open_record(idx_delete_db, Config, #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"to delete">>}),
    ok = wait_until(fun() -> barrel_vectordb:count(idx_delete_db) =:= 1 end),
    {ok, _} = barrel:delete_doc(Db, <<"a">>),
    ok = wait_until(fun() -> barrel_vectordb:count(idx_delete_db) =:= 0 end),
    ok = wait_until(fun() -> pending(<<"idx_delete_db">>) =:= [] end),
    ok = barrel:close(Db).

indexer_nonmatching_acked(Config) ->
    {ok, Db} = open_record(idx_nomatch_db, Config, #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"other">> => <<"x">>}),
    ok = wait_until(fun() -> pending(<<"idx_nomatch_db">>) =:= [] end),
    ?assertEqual(0, barrel_vectordb:count(idx_nomatch_db)),
    ok = barrel:close(Db).

indexer_crash_restart(Config) ->
    {ok, Db} = open_record(idx_crash_db, Config, #{fields => [<<"title">>]}),
    IndexerName = barrel_record_indexer:name(idx_crash_db),
    Pid0 = erlang:whereis(IndexerName),
    ?assert(is_pid(Pid0)),
    %% Freeze, enqueue work, kill mid-flight: the supervisor restarts the
    %% indexer and the pending entry converges.
    ok = sys:suspend(Pid0),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"survives">>}),
    exit(Pid0, kill),
    ok = wait_until(fun() ->
        case erlang:whereis(IndexerName) of
            undefined -> false;
            Pid -> Pid =/= Pid0
        end
    end),
    ok = wait_until(fun() -> hit(Db, <<"survives">>, <<"a">>) end),
    ok = barrel:close(Db).

indexer_poison_parked(Config) ->
    {ok, Db} = open_record(idx_poison_db, Config, #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"bad">>,
                                   <<"title">> => <<"poison pill">>}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"good">>,
                                   <<"title">> => <<"healthy doc">>}),
    %% The good doc indexes despite the poison one failing every round
    ok = wait_until(fun() -> hit(Db, <<"healthy doc">>, <<"good">>) end),
    %% The poison entry stays pending (parked, visible)
    ok = wait_until(fun() ->
        [E || #{id := Id} = E <- pending(<<"idx_poison_db">>),
              Id =:= <<"bad">>] =/= []
    end),
    ?assertEqual(1, barrel_vectordb:count(idx_poison_db)),
    ok = barrel:close(Db).

%%====================================================================
%% Test cases: sync mode + explicit vectors
%%====================================================================

sync_put_immediate_search(Config) ->
    {ok, Db} = open_record(sync_put_db, Config,
                           #{fields => [<<"title">>], mode => sync}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"read your writes">>}),
    %% No polling: the vector is searchable when put_doc returns
    ?assert(hit(Db, <<"read your writes">>, <<"a">>)),
    ?assertEqual([], pending(<<"sync_put_db">>)),
    ok = barrel:close(Db).

sync_embed_failure_fails_put(Config) ->
    {ok, Db} = open_record(sync_fail_db, Config,
                           #{fields => [<<"title">>], mode => sync}),
    ?assertEqual({error, {embed_failed, poison}},
                 barrel:put_doc(Db, #{<<"id">> => <<"bad">>,
                                      <<"title">> => <<"poison text">>})),
    %% Nothing was written
    ?assertEqual({error, not_found}, barrel:get_doc(Db, <<"bad">>)),
    ?assertEqual([], pending(<<"sync_fail_db">>)),
    ok = barrel:close(Db).

sync_index_failure_healed(Config) ->
    {ok, Db} = open_record(sync_heal_db, Config,
                           #{fields => [<<"title">>], mode => sync}),
    %% Inject one add_index_only failure: the doc commits, the entry
    %% stays pending, and the nudged indexer heals it.
    Counter = atomics:new(1, []),
    meck:new(barrel_vectordb, [passthrough, no_link]),
    meck:expect(barrel_vectordb, add_index_only,
        fun(Store, Id, Text, Vector) ->
            case atomics:add_get(Counter, 1, 1) of
                1 -> {error, injected};
                _ -> meck:passthrough([Store, Id, Text, Vector])
            end
        end),
    try
        {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                       <<"title">> => <<"healed">>}),
        %% Healed by the indexer (indexer uses the batch entry point,
        %% which passes through)
        ok = wait_until(fun() -> hit(Db, <<"healed">>, <<"a">>) end),
        ok = wait_until(fun() -> pending(<<"sync_heal_db">>) =:= [] end)
    after
        meck:unload(barrel_vectordb)
    end,
    ok = barrel:close(Db).

sync_delete_immediate(Config) ->
    {ok, Db} = open_record(sync_del_db, Config,
                           #{fields => [<<"title">>], mode => sync}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"short lived">>}),
    ?assertEqual(1, barrel_vectordb:count(sync_del_db)),
    {ok, _} = barrel:delete_doc(Db, <<"a">>),
    ?assertEqual(0, barrel_vectordb:count(sync_del_db)),
    ?assertEqual([], pending(<<"sync_del_db">>)),
    ok = barrel:close(Db).

sync_batch_put(Config) ->
    {ok, Db} = open_record(sync_batch_db, Config,
                           #{fields => [<<"title">>], mode => sync}),
    Results = barrel:put_docs(Db, [
        #{<<"id">> => <<"a">>, <<"title">> => <<"first entry">>},
        #{<<"id">> => <<"b">>, <<"title">> => <<"second entry">>},
        #{<<"id">> => <<"c">>, <<"other">> => <<"no title">>}]),
    ?assertEqual(3, length([ok || {ok, _} <- Results])),
    ?assert(hit(Db, <<"first entry">>, <<"a">>)),
    ?assert(hit(Db, <<"second entry">>, <<"b">>)),
    ?assertEqual(2, barrel_vectordb:count(sync_batch_db)),
    ?assertEqual([], pending(<<"sync_batch_db">>)),
    %% A poison doc fails the WHOLE batch before anything is written
    ?assertEqual({error, {embed_failed, poison}},
                 barrel:put_docs(Db, [
                     #{<<"id">> => <<"d">>, <<"title">> => <<"fine">>},
                     #{<<"id">> => <<"e">>, <<"title">> => <<"poison here">>}])),
    ?assertEqual({error, not_found}, barrel:get_doc(Db, <<"d">>)),
    ok = barrel:close(Db).

explicit_vector_skips_embedder(Config) ->
    {ok, Db} = open_record(explicit_vec_db, Config, #{fields => [<<"title">>]}),
    Vector = [0.25, 0.5, 0.75],
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"explicit">>},
                             #{vector => Vector}),
    %% Indexed synchronously at the supplied vector, entry acked
    {ok, [#{key := <<"a">>}]} = barrel:search_vector(Db, Vector, #{k => 1}),
    ?assertEqual([], pending(<<"explicit_vec_db">>)),
    %% The embedder was never called
    ?assertEqual(0, meck:num_calls(barrel_embed, embed, '_')),
    ?assertEqual(0, meck:num_calls(barrel_embed, embed_batch, '_')),
    ok = barrel:close(Db).

explicit_vector_dimension_check(Config) ->
    {ok, Db} = open_record(explicit_dim_db, Config, #{fields => [<<"title">>]}),
    ?assertEqual({error, {dimension_mismatch, 3, 2}},
                 barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                      <<"title">> => <<"T">>},
                                #{vector => [0.1, 0.2]})),
    ?assertEqual({error, not_found}, barrel:get_doc(Db, <<"a">>)),
    ok = barrel:close(Db).

%%====================================================================
%% Test cases: search surface + guards
%%====================================================================

search_end_to_end(Config) ->
    Dir = ?config(dir, Config),
    {ok, Db} = barrel:open(search_e2e_db, #{
        embedding => #{fields => [<<"title">>], mode => sync,
                       metadata_fields => [<<"kind">>]},
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3, db_path => Dir ++ "/search_e2e_vec"}}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"quick brown fox">>,
                                   <<"kind">> => <<"animal">>,
                                   <<"noise">> => 42}),
    %% barrel:search embeds the query through the facade and hydrates
    %% text/metadata from the document via the adapter
    {ok, [Top | _]} = barrel:search(Db, <<"quick brown fox">>, #{k => 1}),
    ?assertEqual(<<"a">>, maps:get(key, Top)),
    ?assertEqual(<<"quick brown fox">>, maps:get(text, Top)),
    ?assertEqual(#{<<"kind">> => <<"animal">>}, maps:get(metadata, Top)),
    %% Hybrid works on the embedder-less record store (facade embeds the
    %% query, BM25 leg matches terms)
    {ok, [HTop | _]} = barrel:search_hybrid(Db, <<"quick fox">>, #{k => 1}),
    ?assertEqual(<<"a">>, maps:get(key, HTop)),
    ?assertEqual(<<"quick brown fox">>, maps:get(text, HTop)),
    ?assertEqual(#{<<"kind">> => <<"animal">>}, maps:get(metadata, HTop)),
    ok = barrel:close(Db).

byo_embedding_async(Config) ->
    %% Fields-less policy: bring-your-own embeddings, no embedder needed.
    {ok, Db} = open_record(byo_async_db, Config, #{}),
    Vector = [0.5, 0.25, 0.75],  %% exactly representable in 32-bit floats
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"carried">>,
                                   <<"_embedding">> => Vector}),
    %% Stored as an entity column: absent from default reads, returned as
    %% the object form on request
    {ok, Doc} = barrel:get_doc(Db, <<"a">>),
    ?assertEqual(false, maps:is_key(<<"_embedding">>, Doc)),
    {ok, Doc2} = barrel:get_doc(Db, <<"a">>, #{include_embedding => true}),
    ?assertEqual(#{<<"vector">> => Vector, <<"source">> => <<"client">>},
                 maps:get(<<"_embedding">>, Doc2)),
    ok = wait_until(fun() ->
        case barrel:search_vector(Db, Vector, #{k => 1}) of
            {ok, [#{key := <<"a">>} = Hit]} ->
                %% _embedding never leaks into search metadata
                not maps:is_key(<<"_embedding">>, maps:get(metadata, Hit, #{}));
            _ -> false
        end
    end),
    ok = wait_until(fun() -> pending(<<"byo_async_db">>) =:= [] end),
    %% The embedder was never called
    ?assertEqual(0, meck:num_calls(barrel_embed, embed, '_')),
    ?assertEqual(0, meck:num_calls(barrel_embed, embed_batch, '_')),
    ok = barrel:close(Db).

computed_embedding_stored(Config) ->
    %% Policy-computed vectors are ALSO stored in _embedding: sync mode
    %% atomically with the write, async via the indexer write-back.
    {ok, Db} = open_record(computed_sync_db, Config,
                           #{fields => [<<"title">>], mode => sync}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"stored sync">>}),
    {ok, Doc} = barrel:get_doc(Db, <<"a">>, #{include_embedding => true}),
    #{<<"vector">> := V1, <<"source">> := <<"computed">>} =
        maps:get(<<"_embedding">>, Doc),
    ?assertEqual(3, length(V1)),
    ok = barrel:close(Db),
    %% Async: the indexer writes the computed vector back
    {ok, Db2} = open_record(computed_async_db, Config,
                            #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db2, #{<<"id">> => <<"b">>,
                                    <<"title">> => <<"stored async">>}),
    ok = wait_until(fun() ->
        case barrel:get_doc(Db2, <<"b">>, #{include_embedding => true}) of
            {ok, #{<<"_embedding">> := #{<<"source">> := <<"computed">>}}} ->
                true;
            _ ->
                false
        end
    end),
    ok = barrel:close(Db2).

computed_roundtrip_reembeds(Config) ->
    %% A read-modify-write that resends the computed _embedding object
    %% must NOT freeze the stale vector: provenance travels inside the
    %% object, so the policy re-embeds the changed text.
    {ok, Db} = open_record(roundtrip_db, Config,
                           #{fields => [<<"title">>], mode => sync}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"first text">>}),
    {ok, Doc} = barrel:get_doc(Db, <<"a">>, #{include_embedding => true}),
    %% Change the text, resend the doc INCLUDING the stale computed object
    {ok, _} = barrel:put_doc(Db, Doc#{<<"title">> => <<"second text">>}),
    %% The new text's vector wins the search, not the stale one
    {ok, [#{key := <<"a">>}]} =
        barrel:search_vector(Db, mock_vec(<<"second text">>), #{k => 1}),
    {ok, Doc2} = barrel:get_doc(Db, <<"a">>, #{include_embedding => true}),
    #{<<"vector">> := V2, <<"source">> := <<"computed">>} =
        maps:get(<<"_embedding">>, Doc2),
    ?assertEqual(mock_vec32(<<"second text">>), V2),
    ok = barrel:close(Db).

byo_embedding_sync_and_precedence(Config) ->
    %% Sync mode + fields: a carried _embedding wins over the policy
    {ok, Db} = open_record(byo_sync_db, Config,
                           #{fields => [<<"title">>], mode => sync}),
    Vector = [0.9, 0.8, 0.7],
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"has fields too">>,
                                   <<"_embedding">> => Vector}),
    %% Indexed at the carried vector immediately, embedder skipped
    {ok, [#{key := <<"a">>}]} = barrel:search_vector(Db, Vector, #{k => 1}),
    ?assertEqual([], pending(<<"byo_sync_db">>)),
    ?assertEqual(0, meck:num_calls(barrel_embed, embed, '_')),
    ok = barrel:close(Db).

byo_embedding_dimension_check(Config) ->
    {ok, Db} = open_record(byo_dim_db, Config, #{}),
    %% Single put: fails before the write
    ?assertEqual({error, {dimension_mismatch, 3, 2}},
                 barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                      <<"_embedding">> => [0.1, 0.2]})),
    ?assertEqual({error, not_found}, barrel:get_doc(Db, <<"a">>)),
    %% Batch: whole batch rejected before the write
    ?assertEqual({error, {dimension_mismatch, 3, 4}},
                 barrel:put_docs(Db, [
                     #{<<"id">> => <<"b">>, <<"_embedding">> => [0.1, 0.2, 0.3]},
                     #{<<"id">> => <<"c">>,
                       <<"_embedding">> => [0.1, 0.2, 0.3, 0.4]}])),
    ?assertEqual({error, not_found}, barrel:get_doc(Db, <<"b">>)),
    ok = barrel:close(Db).

byo_embedding_not_path_indexed(Config) ->
    {ok, Db} = open_record(byo_ars_db, Config, #{}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"kind">> => <<"note">>,
                                   <<"_embedding">> => [0.1, 0.2, 0.3]}),
    %% Regular fields query fine; the vector is not in the path index
    {ok, Rows, _} = barrel:find(Db, #{where => [{path, [<<"kind">>], <<"note">>}]}),
    ?assertEqual(1, length(Rows)),
    {ok, NoRows, _} = barrel:find(Db, #{where => [{exists, [<<"_embedding">>]}]}),
    ?assertEqual(0, length(NoRows)),
    ok = barrel:close(Db).

vector_add_guards(Config) ->
    {ok, Db} = open_record(guards_db, Config, #{fields => [<<"title">>]}),
    ?assertEqual({error, record_mode},
                 barrel:vector_add(Db, <<"x">>, <<"t">>, #{})),
    ?assertEqual({error, record_mode},
                 barrel:vector_add(Db, <<"x">>, <<"t">>, #{}, [0.1, 0.2, 0.3])),
    ?assertEqual({error, record_mode},
                 barrel:vector_add_batch(Db, [{<<"x">>, <<"t">>, #{}}])),
    %% info reports the embedding config
    {ok, Info} = barrel:info(Db),
    ?assertMatch(#{fields := [[<<"title">>]]}, maps:get(embedding, Info)),
    ?assertEqual(3, maps:get(dimensions, Info)),
    ok = barrel:close(Db),
    %% Plain databases keep the direct vector path
    Dir = ?config(dir, Config),
    {ok, Plain} = barrel:open(guards_plain_db, #{
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3, db_path => Dir ++ "/guards_plain_vec",
                      bm25_backend => memory}}),
    ok = barrel:vector_add(Plain, <<"x">>, <<"t">>, #{}, [0.1, 0.2, 0.3]),
    {ok, PlainInfo} = barrel:info(Plain),
    ?assertEqual(false, maps:is_key(embedding, PlainInfo)),
    ok = barrel:close(Plain).

%%====================================================================
%% Helpers
%%====================================================================

hit(Db, Text, ExpectedId) ->
    case barrel:search_vector(Db, mock_vec(Text), #{k => 1}) of
        {ok, [#{key := Key} | _]} -> Key =:= ExpectedId;
        _ -> false
    end.

wait_until(Fun) ->
    wait_until(Fun, 100).

wait_until(_Fun, 0) ->
    {error, timeout};
wait_until(Fun, Retries) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(50),
            wait_until(Fun, Retries - 1)
    end.

open_record(Name, Config, PolicyMap) ->
    Dir = ?config(dir, Config),
    barrel:open(Name, #{
        embedding => PolicyMap,
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3,
                      db_path => Dir ++ "/" ++ atom_to_list(Name) ++ "_vec"}}).

pending(DbBin) ->
    lists:reverse(barrel_docdb:outbox_fold(
        DbBin, <<"embed">>, fun(E, Acc) -> {ok, [E | Acc]} end, [])).
