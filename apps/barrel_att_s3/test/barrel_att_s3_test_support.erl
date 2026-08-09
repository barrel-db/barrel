%%%-------------------------------------------------------------------
%%% @doc Shared MinIO/Garage test-fixture helpers for CT suites exercising
%%% the barrel_att_s3 backend against real S3-compatible stores. Used by
%%% both barrel_att_s3_SUITE.erl and barrel_att_s3_rep_SUITE.erl in this
%%% same test directory -- kept in one place since the env var names and
%%% default endpoints/buckets need to agree with
%%% test/e2e/attachments-s3-setup.sh either way.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_test_support).

-export([env_bin/2, minio_opts/0, garage_opts/0, reachable/1]).

%% @doc `Var''s value as a binary, or `Default' if unset.
-spec env_bin(string(), binary()) -> binary().
env_bin(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> list_to_binary(Value)
    end.

%% @doc MinIO connection options (endpoint/region/credentials/bucket),
%% from env vars with defaults matching test/e2e/attachments-s3-setup.sh.
-spec minio_opts() -> map().
minio_opts() ->
    #{
        bucket => env_bin("MINIO_S3_TEST_BUCKET", <<"barrel-att-s3-test">>),
        endpoint => env_bin("MINIO_S3_TEST_ENDPOINT", <<"http://127.0.0.1:19000">>),
        region => env_bin("MINIO_S3_TEST_REGION", <<"us-east-1">>),
        access_key_id => env_bin("MINIO_S3_TEST_ACCESS_KEY", <<"minioadmin">>),
        secret_access_key => env_bin("MINIO_S3_TEST_SECRET_KEY", <<"minioadmin">>)
    }.

%% @doc Same, for Garage -- `undefined' if `GARAGE_S3_TEST_ACCESS_KEY'/
%% `_SECRET_KEY' aren't set (no usable default credentials for a store
%% that must be provisioned externally; see attachments-s3-setup.sh).
-spec garage_opts() -> map() | undefined.
garage_opts() ->
    case {os:getenv("GARAGE_S3_TEST_ACCESS_KEY"), os:getenv("GARAGE_S3_TEST_SECRET_KEY")} of
        {false, _} -> undefined;
        {_, false} -> undefined;
        {AccessKey, SecretKey} ->
            #{
                bucket => env_bin("GARAGE_S3_TEST_BUCKET", <<"barrel-test">>),
                endpoint => env_bin("GARAGE_S3_TEST_ENDPOINT", <<"http://127.0.0.1:13900">>),
                region => env_bin("GARAGE_S3_TEST_REGION", <<"garage">>),
                access_key_id => list_to_binary(AccessKey),
                secret_access_key => list_to_binary(SecretKey)
            }
    end.

%% @doc Cheap reachability probe, so a test skips cleanly instead of
%% failing noisily when the store just isn't up. No bucket creation --
%% callers that need the bucket to exist handle that themselves, since
%% how strictly to treat a creation failure differs by caller (some fail
%% loudly on an unexpected error, some treat it as best-effort). A
%% working `list_buckets' proves the endpoint answers; some restricted
%% keys (e.g. a scoped Garage key) can't list all buckets, so a `HEAD' on
%% a bucket that may not exist yet is a fallback probe -- `not_found' is
%% still a valid, connected response.
-spec reachable(map()) -> boolean().
reachable(S3Opts) ->
    Client = livery_s3:new(maps:without([bucket], S3Opts)),
    case livery_s3:list_buckets(Client) of
        {ok, _} -> true;
        {error, _} ->
            case livery_s3:head_bucket(Client, <<"__reachability_probe__">>) of
                {error, not_found} -> true;
                {error, _} -> false;
                ok -> true
            end
    end.
