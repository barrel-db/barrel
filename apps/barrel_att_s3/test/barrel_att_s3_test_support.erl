%%%-------------------------------------------------------------------
%%% @doc Shared fixture helpers for the CT suites exercising barrel_att_s3
%%% against real RustFS and Garage stores. Env var names and defaults must
%%% agree with test/e2e/attachments-s3-setup.sh.
%%%
%%% RustFS enforces conditional writes, Garage cannot; `mock_conditional_writes/0'
%%% emulates them on Garage so the enforcing branch does not rest on one
%%% server. With `BARREL_S3_REQUIRED=1' an unusable store fails the suite
%%% instead of skipping it.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_test_support).

-export([env_bin/2, rustfs_opts/0, garage_opts/0, reachable/1,
         required/0, store_opts/1, unavailable/1,
         mock_conditional_writes/0, unmock_conditional_writes/0]).

%% @doc `Var''s value as a binary, or `Default' if unset.
-spec env_bin(string(), binary()) -> binary().
env_bin(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> list_to_binary(Value)
    end.

%% @doc RustFS connection options, defaults matching attachments-s3-setup.sh.
-spec rustfs_opts() -> map().
rustfs_opts() ->
    #{
        bucket => env_bin("RUSTFS_S3_TEST_BUCKET", <<"barrel-att-s3-test">>),
        endpoint => env_bin("RUSTFS_S3_TEST_ENDPOINT", <<"http://127.0.0.1:19000">>),
        region => env_bin("RUSTFS_S3_TEST_REGION", <<"us-east-1">>),
        access_key_id => env_bin("RUSTFS_S3_TEST_ACCESS_KEY", <<"s3testadmin">>),
        secret_access_key => env_bin("RUSTFS_S3_TEST_SECRET_KEY", <<"s3testsecret">>)
    }.

%% @doc Garage connection options: `undefined' without
%% `GARAGE_S3_TEST_ACCESS_KEY'/`_SECRET_KEY' (the key only exists once
%% provisioned by attachments-s3-setup.sh).
-spec garage_opts() -> map() | undefined.
garage_opts() ->
    garage_opts(os:getenv("GARAGE_S3_TEST_ACCESS_KEY"),
                os:getenv("GARAGE_S3_TEST_SECRET_KEY")).

garage_opts(false, _) -> undefined;
garage_opts(_, false) -> undefined;
garage_opts(AccessKey, SecretKey) ->
    #{
        bucket => env_bin("GARAGE_S3_TEST_BUCKET", <<"barrel-att-s3-test">>),
        endpoint => env_bin("GARAGE_S3_TEST_ENDPOINT", <<"http://127.0.0.1:13900">>),
        region => env_bin("GARAGE_S3_TEST_REGION", <<"garage">>),
        access_key_id => list_to_binary(AccessKey),
        secret_access_key => list_to_binary(SecretKey)
    }.

%% @doc Cheap reachability probe. A scoped key (Garage) may not list all
%% buckets, so a `HEAD' on a missing bucket answering `not_found' counts.
-spec reachable(map()) -> boolean().
reachable(S3Opts) ->
    Client = livery_s3:new(maps:without([bucket], S3Opts)),
    case livery_s3:list_buckets(Client) of
        {ok, _} -> true;
        {error, _} -> head_reachable(livery_s3:head_bucket(Client, <<"__reachability_probe__">>))
    end.

head_reachable(ok) -> true;
head_reachable({error, not_found}) -> true;
head_reachable({error, _}) -> false.

%% @doc True when the environment promises the stores are up (CI).
-spec required() -> boolean().
required() ->
    case os:getenv("BARREL_S3_REQUIRED") of
        "1" -> true;
        "true" -> true;
        _ -> false
    end.

%% @doc Options for a reachable, provisioned store, or why it is unusable.
-spec store_opts(rustfs | garage) -> {ok, map()} | {unavailable, term()}.
store_opts(rustfs) ->
    check_store(rustfs, rustfs_opts());
store_opts(garage) ->
    check_store(garage, garage_opts()).

check_store(garage, undefined) ->
    {unavailable, garage_credentials_not_configured};
check_store(Store, S3Opts) ->
    case reachable(S3Opts) of
        true -> check_bucket(Store, S3Opts);
        false -> {unavailable, {Store, not_reachable, maps:get(endpoint, S3Opts)}}
    end.

%% The bucket is provisioned by attachments-s3-setup.sh, never created here.
check_bucket(Store, #{bucket := Bucket} = S3Opts) ->
    Client = livery_s3:new(maps:without([bucket], S3Opts)),
    case livery_s3:head_bucket(Client, Bucket) of
        ok -> {ok, S3Opts};
        {error, Reason} -> {unavailable, {Store, bucket_not_provisioned, Bucket, Reason}}
    end.

%% @doc Skip, or fail when `BARREL_S3_REQUIRED' is set.
-spec unavailable(term()) -> {skip, term()} | no_return().
unavailable(Reason) ->
    unavailable(required(), Reason).

unavailable(true, Reason) -> ct:fail({s3_store_required, Reason});
unavailable(false, Reason) -> {skip, Reason}.

%%====================================================================
%% Conditional-write emulation
%%====================================================================

%% @doc Makes `livery_s3' enforce `if_none_match'/`if_match' on
%% `put_object' and `complete_multipart_upload' with a HEAD check, then
%% forwards the unconditional request to the real store. Not atomic, which
%% is fine for single-writer tests.
-spec mock_conditional_writes() -> ok.
mock_conditional_writes() ->
    ok = meck:new(livery_s3, [passthrough, no_link]),
    ok = meck:expect(livery_s3, put_object,
        fun(Client, Bucket, Key, Body, Opts) ->
            conditional(Client, Bucket, Key, Opts,
                        fun(Plain) -> meck:passthrough([Client, Bucket, Key, Body, Plain]) end)
        end),
    ok = meck:expect(livery_s3, complete_multipart_upload,
        fun(Client, Bucket, Key, UploadId, Parts, Opts) ->
            conditional(Client, Bucket, Key, Opts,
                        fun(Plain) ->
                            meck:passthrough([Client, Bucket, Key, UploadId, Parts, Plain])
                        end)
        end).

-spec unmock_conditional_writes() -> ok.
unmock_conditional_writes() ->
    meck:unload(livery_s3).

conditional(Client, Bucket, Key, Opts, Forward) ->
    Plain = maps:without([if_none_match, if_match], Opts),
    Current = livery_s3:head_object(Client, Bucket, Key),
    case precondition(Opts, Current) of
        ok -> Forward(Plain);
        {error, _} = Error -> Error
    end.

precondition(#{if_none_match := <<"*">>}, {ok, _}) -> {error, precondition_failed};
precondition(#{if_match := _}, {error, not_found}) -> {error, not_found};
precondition(#{if_match := Expected}, {ok, #{etag := Etag}}) ->
    etag_match(unquote(Expected), unquote(Etag));
precondition(_Opts, _Current) -> ok.

etag_match(Same, Same) -> ok;
etag_match(_, _) -> {error, precondition_failed}.

unquote(Etag) -> string:trim(Etag, both, "\"").
