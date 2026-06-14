%% @doc Object storage backend behaviour.
%%
%% A backend stores opaque object bytes under binary keys. `barrel_objectdb_fs'
%% is the built-in local-disk backend; an S3 backend (`barrel_objectdb_s3', via
%% `livery_s3') and a remote backend are opt-in and added later. The `Ctx' is a
%% backend-private value returned by {@link init/1} and threaded through calls.
%% Streaming put/get for large objects is added in a later phase.
%% @end
-module(barrel_objectdb_backend).

-callback init(Config :: map()) -> {ok, Ctx :: term()} | {error, term()}.

-callback put(Ctx :: term(), Key :: binary(), Value :: binary()) ->
    ok | {error, term()}.

-callback get(Ctx :: term(), Key :: binary()) ->
    {ok, binary()} | {error, not_found} | {error, term()}.

-callback delete(Ctx :: term(), Key :: binary()) ->
    ok | {error, term()}.

-callback list(Ctx :: term(), Prefix :: binary()) ->
    {ok, [binary()]} | {error, term()}.
