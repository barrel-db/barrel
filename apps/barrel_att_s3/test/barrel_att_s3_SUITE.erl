%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_att_s3_store: the whole-blob API (Step 2 --
%%% open/close, put/get/delete/delete_all/fold, get_info, key scheme,
%%% expected_digest) and the streaming/multipart API (Step 3 --
%%% put_stream/write_chunk/finish_stream/abort_stream, get_stream/
%%% read_chunk, the buffer-until-threshold single-put-vs-multipart
%%% decision).
%%%
%%% Multipart tests use a 5 MiB part_size, not a tiny synthetic value:
%%% real S3 (and MinIO) reject non-final parts under 5 MiB with
%%% EntityTooSmall regardless of what part_size the *client* chooses to
%%% buffer to, so a genuine multipart round trip needs real S3-sized data.
%%% Garage is more lenient (a separate test below uses a 1 MiB part
%%% against Garage only, to exercise that the part_size knob genuinely
%%% changes behavior on a store that allows it).
%%%
%%% Runs against both MinIO and Garage (same test bodies, one per group),
%%% since the two stores have deliberately different capability profiles
%%% (see the plan) -- what's covered here is the part that must behave
%%% identically everywhere. Connection details come from OS env vars; a
%%% group skips cleanly if its store isn't configured/reachable, so this
%%% suite is safe to run without either service present.
%%%
%%% Local setup this suite was developed against:
%%%   MinIO:  docker run -p 19000:9000 -e MINIO_ROOT_USER=minioadmin \
%%%           -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
%%%   Garage: see apps/barrel_att_s3/test/README.md (bucket + key must be
%%%           pre-provisioned; Garage keys can't create buckets themselves).
%%%
%%% Env vars (all optional for MinIO, defaults match the setup above;
%%% GARAGE_S3_TEST_ACCESS_KEY/_SECRET_KEY have no default -- the garage
%%% group skips without them):
%%%   MINIO_S3_TEST_ENDPOINT/_ACCESS_KEY/_SECRET_KEY/_REGION/_BUCKET
%%%   GARAGE_S3_TEST_ENDPOINT/_ACCESS_KEY/_SECRET_KEY/_REGION/_BUCKET
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    open_missing_bucket/1,
    put_get_roundtrip/1,
    get_returns_not_found_for_missing/1,
    get_info_returns_metadata/1,
    get_info_not_found/1,
    content_type_override_persists/1,
    delete_removes_attachment/1,
    delete_missing_is_ok/1,
    fold_lists_attachment_names/1,
    fold_stop_early/1,
    delete_all_removes_every_attachment_for_doc/1,
    delete_all_does_not_affect_other_docs/1,
    expected_digest_match_succeeds/1,
    expected_digest_mismatch_rejected/1,
    key_too_long_rejected/1,
    stream_small_single_put/1,
    stream_large_multipart/1,
    stream_abort_mid_multipart/1,
    stream_digest_mismatch_small/1,
    stream_digest_mismatch_multipart_aborts_upload/1,
    stream_read_roundtrip/1,
    stream_read_not_found/1,
    stream_small_part_size_accepted_by_garage/1,
    conditional_writes_capability_reflects_store/1,
    default_put_stays_unconditional/1,
    create_only_succeeds_on_fresh_key/1,
    create_only_conflicts_on_existing_key/1,
    expected_etag_match_succeeds/1,
    expected_etag_stale_returns_conflict/1,
    stream_create_only_conflicts_small_path/1,
    stream_create_only_conflicts_multipart_path/1,
    garage_create_only_fails_fast/1,
    garage_expected_etag_fails_fast/1
]).

-define(CASES, [
    open_missing_bucket,
    put_get_roundtrip,
    get_returns_not_found_for_missing,
    get_info_returns_metadata,
    get_info_not_found,
    content_type_override_persists,
    delete_removes_attachment,
    delete_missing_is_ok,
    fold_lists_attachment_names,
    fold_stop_early,
    delete_all_removes_every_attachment_for_doc,
    delete_all_does_not_affect_other_docs,
    expected_digest_match_succeeds,
    expected_digest_mismatch_rejected,
    key_too_long_rejected,
    stream_small_single_put,
    stream_large_multipart,
    stream_abort_mid_multipart,
    stream_digest_mismatch_small,
    stream_digest_mismatch_multipart_aborts_upload,
    stream_read_roundtrip,
    stream_read_not_found,
    conditional_writes_capability_reflects_store,
    default_put_stays_unconditional
]).

%% MinIO has verifiably enforced If-Match/If-None-Match since 2023; Garage
%% cannot at all, by its own documented design. Run the actual
%% conflict-detection assertions only where they mean something.
-define(MINIO_ONLY_CASES, [
    create_only_succeeds_on_fresh_key,
    create_only_conflicts_on_existing_key,
    expected_etag_match_succeeds,
    expected_etag_stale_returns_conflict,
    stream_create_only_conflicts_small_path,
    stream_create_only_conflicts_multipart_path
]).

-define(GARAGE_ONLY_CASES, [
    stream_small_part_size_accepted_by_garage,
    garage_create_only_fails_fast,
    garage_expected_etag_fails_fast
]).

%% 5 MiB: the real S3/MinIO minimum for a non-final multipart part,
%% independent of whatever part_size the client buffers to.
-define(PART_SIZE, 5 * 1024 * 1024).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, minio}, {group, garage}].

groups() ->
    [
        {minio, [sequence], ?CASES ++ ?MINIO_ONLY_CASES},
        {garage, [sequence], ?CASES ++ ?GARAGE_ONLY_CASES}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_att_s3),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(minio, Config) ->
    S3Opts = #{
        endpoint => env_bin("MINIO_S3_TEST_ENDPOINT", <<"http://127.0.0.1:19000">>),
        region => env_bin("MINIO_S3_TEST_REGION", <<"us-east-1">>),
        access_key_id => env_bin("MINIO_S3_TEST_ACCESS_KEY", <<"minioadmin">>),
        secret_access_key => env_bin("MINIO_S3_TEST_SECRET_KEY", <<"minioadmin">>)
    },
    Bucket = env_bin("MINIO_S3_TEST_BUCKET", <<"barrel-att-s3-test">>),
    case reachable(S3Opts) of
        true ->
            Client = livery_s3:new(S3Opts),
            case livery_s3:create_bucket(Client, Bucket) of
                ok -> ok;
                {error, {s3, <<"BucketAlreadyOwnedByYou">>, _, _}} -> ok;
                {error, {s3, <<"BucketAlreadyExists">>, _, _}} -> ok;
                {error, Reason} -> ct:fail({minio_bucket_setup_failed, Reason})
            end,
            [{store, minio}, {s3_opts, S3Opts#{bucket => Bucket}} | Config];
        false ->
            {skip, {minio_not_reachable, maps:get(endpoint, S3Opts)}}
    end;
init_per_group(garage, Config) ->
    case {os:getenv("GARAGE_S3_TEST_ACCESS_KEY"), os:getenv("GARAGE_S3_TEST_SECRET_KEY")} of
        {false, _} ->
            {skip, garage_credentials_not_configured};
        {_, false} ->
            {skip, garage_credentials_not_configured};
        {AccessKey, SecretKey} ->
            S3Opts = #{
                endpoint => env_bin("GARAGE_S3_TEST_ENDPOINT", <<"http://127.0.0.1:13900">>),
                region => env_bin("GARAGE_S3_TEST_REGION", <<"garage">>),
                access_key_id => list_to_binary(AccessKey),
                secret_access_key => list_to_binary(SecretKey)
            },
            Bucket = env_bin("GARAGE_S3_TEST_BUCKET", <<"barrel-test">>),
            case reachable(S3Opts) of
                true -> [{store, garage}, {s3_opts, S3Opts#{bucket => Bucket}} | Config];
                false -> {skip, {garage_not_reachable, maps:get(endpoint, S3Opts)}}
            end
    end.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    S3Opts = ?config(s3_opts, Config),
    {ok, AttRef} = barrel_att_s3_store:open("/tmp/unused",
                                            #{s3 => S3Opts#{part_size => ?PART_SIZE}}),
    DbName = atom_to_binary(TestCase, utf8),
    [{att_ref, AttRef}, {db_name, DbName} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = barrel_att_s3_store:close(?config(att_ref, Config)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

env_bin(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> list_to_binary(Value)
    end.

%% Cheap reachability probe so a group skips cleanly instead of every
%% test case in it failing noisily when the store just isn't up.
reachable(S3Opts) ->
    Client = livery_s3:new(S3Opts),
    case livery_s3:list_buckets(Client) of
        {ok, _} -> true;
        {error, _} ->
            %% Some restricted keys (e.g. a scoped Garage key) can't list
            %% all buckets; a HEAD on a bucket that may not exist yet still
            %% proves the endpoint answers, since not_found is still a
            %% valid, connected response.
            case livery_s3:head_bucket(Client, <<"__reachability_probe__">>) of
                {error, not_found} -> true;
                {error, _} -> false;
                ok -> true
            end
    end.

%%====================================================================
%% Test Cases
%%====================================================================

open_missing_bucket(_Config) ->
    ?assertEqual({error, missing_bucket},
                 barrel_att_s3_store:open("/tmp/unused", #{s3 => #{}})).

put_get_roundtrip(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Data = <<"hello world">>,
    {ok, Info} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"note.txt">>, Data),
    ?assertMatch(#{name := <<"note.txt">>, length := 11, chunked := false}, Info),
    ?assertEqual({ok, Data},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"note.txt">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"note.txt">>).

get_returns_not_found_for_missing(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, <<"doc1">>, <<"nope.txt">>)).

get_info_returns_metadata(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Data = <<"some bytes">>,
    {ok, #{digest := Digest}} =
        barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.bin">>, Data),
    {ok, InfoResult} = barrel_att_s3_store:get_info(AttRef, DbName, DocId, <<"a.bin">>),
    ?assertMatch(#{name := <<"a.bin">>, length := 10, digest := Digest},
                 InfoResult),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.bin">>).

get_info_not_found(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get_info(AttRef, DbName, <<"doc1">>, <<"nope">>)).

%% barrel_att_store_blob's get_info recomputes content-type from the
%% filename for small attachments, ignoring any override the caller gave
%% put/6 -- this backend persists it as a real S3 header, so an override
%% survives the round trip. Documented divergence, not a bug.
content_type_override_persists(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"data.bin">>,
                                      <<"x">>, #{content_type => <<"application/custom">>}),
    {ok, #{content_type := <<"application/custom">>}} =
        barrel_att_s3_store:get_info(AttRef, DbName, DocId, <<"data.bin">>),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"data.bin">>).

delete_removes_attachment(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v">>),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)).

delete_missing_is_ok(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual(ok, barrel_att_s3_store:delete(AttRef, DbName, <<"doc1">>, <<"nope">>)).

fold_lists_attachment_names(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a">>, <<"1">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b">>, <<"2">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"c">>, <<"3">>),
    Names = barrel_att_s3_store:fold(AttRef, DbName, DocId,
                                     fun(Name, _Data, Acc) -> {ok, [Name | Acc]} end, []),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], lists:sort(Names)),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, DocId).

fold_stop_early(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a">>, <<"1">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b">>, <<"2">>),
    Result = barrel_att_s3_store:fold(AttRef, DbName, DocId,
                                      fun(Name, _Data, Acc) -> {stop, [Name | Acc]} end, []),
    ?assertEqual(1, length(Result)),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, DocId).

delete_all_removes_every_attachment_for_doc(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a">>, <<"1">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b">>, <<"2">>),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, DocId),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"a">>)),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"b">>)).

%% Two different DocIds sharing an AttName must not collide -- proves the
%% hex-encoded-DocId key scheme actually isolates documents.
delete_all_does_not_affect_other_docs(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, <<"docA">>, <<"f">>, <<"a">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, <<"docB">>, <<"f">>, <<"b">>),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, <<"docA">>),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, <<"docA">>, <<"f">>)),
    ?assertEqual({ok, <<"b">>},
                 barrel_att_s3_store:get(AttRef, DbName, <<"docB">>, <<"f">>)),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, <<"docB">>).

expected_digest_match_succeeds(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Data = <<"payload">>,
    Digest = <<"sha256-", (binary:encode_hex(crypto:hash(sha256, Data), lowercase))/binary>>,
    ?assertMatch({ok, _},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, Data,
                                        #{expected_digest => Digest})),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

expected_digest_mismatch_rejected(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    ?assertEqual({error, digest_mismatch},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"payload">>,
                                        #{expected_digest => <<"sha256-wrong">>})),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)).

key_too_long_rejected(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    HugeDocId = binary:copy(<<"a">>, 2048),
    ?assertEqual({error, key_too_long},
                 barrel_att_s3_store:put(AttRef, DbName, HugeDocId, <<"f">>, <<"v">>)),
    ?assertEqual({error, key_too_long},
                 barrel_att_s3_store:get(AttRef, DbName, HugeDocId, <<"f">>)).

%%====================================================================
%% Streaming / multipart test cases
%%====================================================================

stream_small_single_put(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
                                              <<"small.txt">>, <<"text/plain">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, <<"hello">>),
    {ok, Info} = barrel_att_s3_store:finish_stream(S2),
    ?assertMatch(#{chunked := false, length := 5}, Info),
    ?assertEqual({ok, <<"hello">>},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"small.txt">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"small.txt">>).

stream_large_multipart(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Part1 = binary:copy(<<"a">>, ?PART_SIZE),
    Part2 = binary:copy(<<"b">>, 100),
    Expected = <<Part1/binary, Part2/binary>>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
                                              <<"large.bin">>, <<"application/octet-stream">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, Part1),
    ?assertNotEqual(undefined, maps:get(multipart, S2)),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, Part2),
    {ok, Info} = barrel_att_s3_store:finish_stream(S3),
    ?assertMatch(#{chunked := true, length := 5242980}, Info),
    ?assertEqual({ok, Expected},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"large.bin">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"large.bin">>).

stream_abort_mid_multipart(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
                                              <<"aborted.bin">>, <<"application/octet-stream">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, binary:copy(<<"x">>, ?PART_SIZE)),
    ?assertNotEqual(undefined, maps:get(multipart, S2)),
    ?assertEqual(ok, barrel_att_s3_store:abort_stream(S2)),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"aborted.bin">>)).

stream_digest_mismatch_small(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"bad.txt">>,
                                              <<"text/plain">>,
                                              #{expected_digest => <<"sha256-wrong">>}),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, <<"hello">>),
    ?assertEqual({error, digest_mismatch}, barrel_att_s3_store:finish_stream(S2)),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"bad.txt">>)).

%% A mismatch discovered only at finish time (the digest can't be known
%% until every part is in) must abort the whole multipart upload, not just
%% report an error -- confirmed here by asserting no in-progress upload is
%% left behind for anyone to accidentally complete later.
stream_digest_mismatch_multipart_aborts_upload(Config) ->
    AttRef = #{client := Client, bucket := Bucket} = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"bad2.bin">>,
                                              <<"application/octet-stream">>,
                                              #{expected_digest => <<"sha256-wrong">>}),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, binary:copy(<<"z">>, ?PART_SIZE)),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, binary:copy(<<"z">>, 100)),
    ?assertEqual({error, digest_mismatch}, barrel_att_s3_store:finish_stream(S3)),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"bad2.bin">>)),
    {ok, #{uploads := Uploads}} = livery_s3:list_multipart_uploads(Client, Bucket),
    ?assertNot(lists:any(fun(#{key := K}) -> K =:= maps:get(key, S3) end, Uploads)).

stream_read_roundtrip(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Part1 = binary:copy(<<"q">>, ?PART_SIZE),
    Expected = <<Part1/binary, "tail">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"stream.bin">>,
                                              <<"application/octet-stream">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, Part1),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, <<"tail">>),
    {ok, _} = barrel_att_s3_store:finish_stream(S3),
    {ok, R1} = barrel_att_s3_store:get_stream(AttRef, DbName, DocId, <<"stream.bin">>),
    Got = drain(R1, []),
    ?assertEqual(Expected, Got),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"stream.bin">>).

stream_read_not_found(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get_stream(AttRef, DbName, <<"doc1">>, <<"nope">>)).

%% Garage-only: unlike AWS/MinIO's strict 5 MiB minimum for a non-final
%% part, Garage accepts a much smaller one (confirmed empirically at 1 MiB
%% this session) -- exercises that the part_size config knob genuinely
%% changes upload behavior on a store lenient enough to allow it, rather
%% than a knob that's accepted but ignored.
stream_small_part_size_accepted_by_garage(Config) ->
    S3Opts = ?config(s3_opts, Config),
    SmallPartSize = 1024 * 1024,
    {ok, AttRef} = barrel_att_s3_store:open("/tmp/unused",
                                            #{s3 => S3Opts#{part_size => SmallPartSize}}),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Part1 = binary:copy(<<"a">>, SmallPartSize),
    Part2 = binary:copy(<<"b">>, 50),
    Expected = <<Part1/binary, Part2/binary>>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"garage-small.bin">>,
                                              <<"application/octet-stream">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, Part1),
    ?assertNotEqual(undefined, maps:get(multipart, S2)),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, Part2),
    {ok, Info} = barrel_att_s3_store:finish_stream(S3),
    ?assertMatch(#{chunked := true}, Info),
    ?assertEqual({ok, Expected},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"garage-small.bin">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"garage-small.bin">>),
    ok = barrel_att_s3_store:close(AttRef).

%%====================================================================
%% Streaming helpers
%%====================================================================

drain(Stream, Acc) ->
    case barrel_att_s3_store:read_chunk(Stream) of
        {ok, Data, Next} -> drain(Next, [Data | Acc]);
        eof -> iolist_to_binary(lists:reverse(Acc))
    end.

%%====================================================================
%% Write-conflict detection test cases
%%====================================================================

conditional_writes_capability_reflects_store(Config) ->
    AttRef = ?config(att_ref, Config),
    Expected = case ?config(store, Config) of
        minio -> supported;
        garage -> unsupported
    end,
    ?assertEqual(Expected, maps:get(conditional_writes, AttRef)).

default_put_stays_unconditional(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    ?assertMatch({ok, _}, barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>)),
    ?assertEqual({ok, <<"v2">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

create_only_succeeds_on_fresh_key(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    ?assertMatch({ok, _},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>,
                                        #{create_only => true})),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

create_only_conflicts_on_existing_key(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>,
                                      #{create_only => true}),
    Result = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>,
                                     #{create_only => true}),
    ?assertMatch({error, {conflict, #{name := <<"f">>, length := 2}}}, Result),
    %% the rejected write never landed
    ?assertEqual({ok, <<"v1">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

expected_etag_match_succeeds(Config) ->
    #{client := Client, bucket := Bucket} = AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    Key = s3_key(DbName, DocId, <<"f">>),
    {ok, #{etag := Etag}} = livery_s3:head_object(Client, Bucket, Key),
    ?assertMatch({ok, _},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>,
                                        #{expected_etag => Etag})),
    ?assertEqual({ok, <<"v2">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

expected_etag_stale_returns_conflict(Config) ->
    #{client := Client, bucket := Bucket} = AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    Key = s3_key(DbName, DocId, <<"f">>),
    {ok, #{etag := StaleEtag}} = livery_s3:head_object(Client, Bucket, Key),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>),
    Result = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v3">>,
                                     #{expected_etag => StaleEtag}),
    ?assertMatch({error, {conflict, #{name := <<"f">>}}}, Result),
    ?assertEqual({ok, <<"v2">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

stream_create_only_conflicts_small_path(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"f">>,
                                              <<"text/plain">>, #{create_only => true}),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, <<"v2">>),
    Result = barrel_att_s3_store:finish_stream(S2),
    ?assertMatch({error, {conflict, #{name := <<"f">>}}}, Result),
    ?assertEqual({ok, <<"v1">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

%% create_only applies at completion, not create_multipart_upload (see the
%% barrel_att_s3_store module doc): a doc already carrying "big.bin" makes
%% the *second* stream's completion lose an If-None-Match race, not its
%% first write_chunk -- unlike the single-put path, the multipart upload
%% itself succeeds right up to completion before the conflict surfaces.
stream_create_only_conflicts_multipart_path(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Part1 = binary:copy(<<"a">>, ?PART_SIZE),
    {ok, First1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"big.bin">>,
                                                  <<"application/octet-stream">>,
                                                  #{create_only => true}),
    {ok, First2} = barrel_att_s3_store:write_chunk(First1, Part1),
    {ok, _} = barrel_att_s3_store:finish_stream(First2),

    {ok, Second1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"big.bin">>,
                                                   <<"application/octet-stream">>,
                                                   #{create_only => true}),
    {ok, Second2} = barrel_att_s3_store:write_chunk(Second1, Part1),
    Result = barrel_att_s3_store:finish_stream(Second2),
    ?assertMatch({error, {conflict, #{name := <<"big.bin">>}}}, Result),
    ?assertEqual({ok, Part1}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"big.bin">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"big.bin">>).

garage_create_only_fails_fast(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    ?assertEqual({error, conditional_writes_unsupported},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>,
                                        #{create_only => true})),
    ?assertEqual({error, not_found}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)).

garage_expected_etag_fails_fast(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    ?assertEqual({error, conditional_writes_unsupported},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>,
                                        #{expected_etag => <<"whatever">>})),
    ?assertEqual({ok, <<"v1">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

s3_key(DbName, DocId, AttName) ->
    <<DbName/binary, "/", (binary:encode_hex(DocId, lowercase))/binary, "/", AttName/binary>>.
