%% @doc Object storage facade.
%%
%% Opens a store backed by a pluggable {@link barrel_objectdb_backend} and
%% dispatches object operations to it. The default backend is
%% `barrel_objectdb_fs' (local disk); an S3 backend and a remote backend are
%% opt-in. Keys and values are opaque binaries. This is the foundation for
%% document attachments (via the docdb attachment backend) and a future agent
%% filesystem.
%% @end
-module(barrel_objectdb).

-export([
    open/2,
    put/3,
    get/2,
    delete/2,
    list/2
]).

-export_type([store/0]).

-type store() :: #{mod := module(), ctx := term()}.
%% A handle bundling the backend module and its private context.

%% @doc Open an object store using `Backend' (a {@link barrel_objectdb_backend})
%% with backend-specific `Config'.
-spec open(module(), map()) -> {ok, store()} | {error, term()}.
open(Backend, Config) when is_atom(Backend), is_map(Config) ->
    case Backend:init(Config) of
        {ok, Ctx} -> {ok, #{mod => Backend, ctx => Ctx}};
        {error, _} = Err -> Err
    end.

%% @doc Store an object.
-spec put(store(), binary(), binary()) -> ok | {error, term()}.
put(#{mod := Mod, ctx := Ctx}, Key, Value) ->
    Mod:put(Ctx, Key, Value).

%% @doc Fetch an object.
-spec get(store(), binary()) -> {ok, binary()} | {error, not_found} | {error, term()}.
get(#{mod := Mod, ctx := Ctx}, Key) ->
    Mod:get(Ctx, Key).

%% @doc Delete an object (deleting a missing object is not an error).
-spec delete(store(), binary()) -> ok | {error, term()}.
delete(#{mod := Mod, ctx := Ctx}, Key) ->
    Mod:delete(Ctx, Key).

%% @doc List object keys with the given prefix.
-spec list(store(), binary()) -> {ok, [binary()]} | {error, term()}.
list(#{mod := Mod, ctx := Ctx}, Prefix) ->
    Mod:list(Ctx, Prefix).
