%%%-------------------------------------------------------------------
%%% @doc Spaces: shared context containers for agents. A space IS a
%%% barrel database created through this layer: sharing context means
%%% holding a capability for the space (see barrel_caps), and every
%%% barrel feature (documents, search, channels, timeline, sync,
%%% per-database encryption) works inside one unchanged.
%%%
%%% Space metadata lives as regular documents in the registry database
%%% `_barrel_spaces' (regular docs, not local docs: discovery needs
%%% folds and the changes feed). Space database names are generated
%%% (`sp_' + 16 base32 chars) so the human label never constrains the
%%% database name rules; the label lives in the registry doc.
%%%
%%% Space databases open through Barrel's database lifecycle manager
%%% (barrel_dbs), so idle spaces close automatically and hundreds of
%%% ephemeral spaces stay cheap. Runtime config (the encryption spec of
%%% an encrypted space) must be supplied again on every open, exactly
%%% as for any barrel database.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces).

-export([ensure_registry/0,
         create_space/1,
         open_space/1, open_space/2,
         close_space/1,
         drop_space/1, drop_space/2,
         list_spaces/0,
         space_info/1]).

%% Shared helpers for the other agent-layer modules
-export([registry_db/0, new_id/1, now_ms/0, base32/1]).

-define(REGISTRY_DB, <<"_barrel_spaces">>).

-type space() :: #{id := binary(), db := barrel:db()}.
-export_type([space/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Open or create the registry database (docdb only: the registry
%% holds small metadata docs and needs no vector store).
-spec ensure_registry() -> ok.
ensure_registry() ->
    Name = configured_registry(),
    case barrel_docdb:open_db(Name) of
        {ok, _} ->
            ok;
        {error, _} ->
            case barrel_docdb:create_db(Name) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end
    end.

%% @doc The registry database name (documents: `space:Id', `grant:Id',
%% `handoff:Id', `handoff_token:TokenId'). Configurable through the
%% `registry_db' application env of barrel_spaces (default
%% `_barrel_spaces'); set it before first use. The registry is a
%% regular barrel database: opening it through barrel_dbs alongside
%% this layer, and replicating it, are both supported (see the spaces
%% guide on what replicating grants means).
-spec registry_db() -> binary().
registry_db() ->
    ok = ensure_registry(),
    configured_registry().

configured_registry() ->
    application:get_env(barrel_spaces, registry_db, ?REGISTRY_DB).

%% @doc Create a space. Options:
%% <ul>
%% <li>`label', `purpose', `owner' - metadata binaries</li>
%% <li>`encryption' - a barrel_keyprovider spec; per-space keys are the
%%     agent isolation story (runtime config: pass it to open_space
%%     again after a restart)</li>
%% <li>`session_ttl' - default session TTL in seconds (3600)</li>
%% <li>`ttl_sweep_interval' - doc TTL sweep of the space db in ms
%%     (60000; sessions rely on it)</li>
%% <li>`docdb', `vectordb' - extra store config; `vectordb => none' for a
%%     space without a vector store (documents and queries only)</li>
%% <li>`embedding' - a record-mode policy (see barrel:open/2). The space
%%     document records its fields and the embedder's identity, never the
%%     embedder config, so any node can reopen the space in record mode
%%     with its own config of the same model</li>
%% </ul>
%% The vector store is per node: by default it lives at
%% `<data_dir>/<id>_vec', resolved from the local `data_dir' on every
%% open. A custom `vectordb => #{db_path => ...}' is kept as given and
%% must exist on every node that opens the space.
-spec create_space(map()) -> {ok, space()} | {error, term()}.
create_space(Opts) when is_map(Opts) ->
    Registry = registry_db(),
    Id = new_id(<<"sp_">>),
    SessionTtl = maps:get(session_ttl, Opts, 3600),
    TtlSweep = maps:get(ttl_sweep_interval, Opts, 60000),
    VecOpts = vec_opts(Id, maps:get(vectordb, Opts, #{})),
    Embedding = maps:get(embedding, Opts, undefined),
    case open_db(Id, Opts, TtlSweep, VecOpts, Embedding) of
        {ok, Db} ->
            Doc0 = #{
                <<"id">> => <<"space:", Id/binary>>,
                <<"type">> => <<"space">>,
                <<"space">> => Id,
                <<"label">> => maps:get(label, Opts, <<>>),
                <<"purpose">> => maps:get(purpose, Opts, <<>>),
                <<"owner">> => maps:get(owner, Opts, <<>>),
                <<"status">> => <<"active">>,
                <<"created_at">> => now_ms(),
                <<"session_ttl">> => SessionTtl,
                <<"ttl_sweep_interval">> => TtlSweep,
                <<"encrypted">> =>
                    maps:get(encryption, Opts, disabled) =/= disabled
            },
            Doc = maps:merge(maps:merge(Doc0, recorded_vec_path(Id, VecOpts)),
                             recorded_embedding(Db)),
            {ok, _} = barrel_docdb:put_doc(Registry, Doc),
            {ok, #{id => Id, db => Db}};
        {error, _} = Err ->
            Err
    end.

%% @doc Open an existing space.
-spec open_space(binary()) -> {ok, space()} | {error, term()}.
open_space(Id) ->
    open_space(Id, #{}).

%% @doc Open an existing space with runtime options (`encryption' for
%% encrypted spaces, extra `docdb'/`vectordb' config, `embedding').
%% A space created in record mode reopens in record mode: with the
%% `embedding' given, else with its recorded fields and the embedder of
%% the `embedder' application env of barrel_spaces. The embedder must
%% have the recorded fingerprint: `{error, {embedder_mismatch, _}}'
%% otherwise, `{error, {embedder_required, Fingerprint}}' without one.
-spec open_space(binary(), map()) -> {ok, space()} | {error, term()}.
open_space(Id, RuntimeOpts) when is_binary(Id), is_map(RuntimeOpts) ->
    case space_info(Id) of
        {ok, #{<<"status">> := <<"active">>} = Info} ->
            case space_embedding(maps:get(embedding, RuntimeOpts, undefined),
                                 maps:get(<<"embedding">>, Info, undefined)) of
                {ok, Embedding} ->
                    open_active(Id, Info, RuntimeOpts, Embedding);
                {error, _} = Err ->
                    Err
            end;
        {ok, _Dropped} ->
            {error, space_dropped};
        {error, _} = Err ->
            Err
    end.

%% @doc Close a space's database (idempotent; reopen with open_space).
-spec close_space(binary()) -> ok.
close_space(Id) when is_binary(Id) ->
    barrel_dbs:close(Id).

%% @doc Drop a space: delete its database, mark the registry doc, and
%% revoke every capability grant for it.
-spec drop_space(binary()) -> ok | {error, term()}.
drop_space(Id) ->
    drop_space(Id, #{}).

%% @doc Like drop_space/1 with runtime open options (an encrypted
%% space needs its encryption spec to open for deletion).
-spec drop_space(binary(), map()) -> ok | {error, term()}.
drop_space(Id, RuntimeOpts) when is_binary(Id), is_map(RuntimeOpts) ->
    Registry = registry_db(),
    case space_info(Id) of
        {ok, #{<<"status">> := <<"active">>} = Info} ->
            %% plain open: dropping needs the files, not the embedder
            case open_active(Id, Info, RuntimeOpts, undefined) of
                {ok, _} ->
                    _ = barrel_dbs:destroy(Id),
                    Updated = Info#{<<"status">> => <<"dropped">>,
                                    <<"dropped_at">> => now_ms()},
                    {ok, _} = barrel_docdb:put_doc(Registry, Updated),
                    revoke_grants(Id),
                    ok;
                {error, _} = Err ->
                    Err
            end;
        {ok, _} ->
            ok;
        {error, _} = Err ->
            Err
    end.

%% @doc Active spaces: `[#{id, label, purpose, owner, created_at}]'.
-spec list_spaces() -> {ok, [map()]}.
list_spaces() ->
    Registry = registry_db(),
    {ok, Docs} = barrel_docdb:fold_docs(
        Registry,
        fun(#{<<"type">> := <<"space">>,
              <<"status">> := <<"active">>} = Doc, Acc) ->
                {ok, [maps:with([<<"space">>, <<"label">>, <<"purpose">>,
                                 <<"owner">>, <<"created_at">>,
                                 <<"encrypted">>], Doc) | Acc]};
           (_Doc, Acc) ->
                {ok, Acc}
        end, [], #{id_prefix => <<"space:">>}),
    {ok, lists:reverse(Docs)}.

%% @doc The registry document of a space.
-spec space_info(binary()) -> {ok, map()} | {error, term()}.
space_info(Id) when is_binary(Id) ->
    barrel_docdb:get_doc(registry_db(), <<"space:", Id/binary>>).

%%====================================================================
%% Shared helpers
%%====================================================================

%% @doc A generated identifier: Prefix + 16 lowercase base32 chars
%% (10 random bytes), valid as a database name.
-spec new_id(binary()) -> binary().
new_id(Prefix) ->
    <<Prefix/binary, (base32(crypto:strong_rand_bytes(10)))/binary>>.

-spec now_ms() -> non_neg_integer().
now_ms() ->
    erlang:system_time(millisecond).

%%====================================================================
%% Internal
%%====================================================================

open_active(Id, Info, RuntimeOpts, Embedding) ->
    TtlSweep = maps:get(<<"ttl_sweep_interval">>, Info, 60000),
    VecOpts = vec_opts_from(Id, Info, maps:get(vectordb, RuntimeOpts, #{})),
    case open_db(Id, RuntimeOpts, TtlSweep, VecOpts, Embedding) of
        {ok, Db} -> {ok, #{id => Id, db => Db}};
        {error, _} = Err -> Err
    end.

open_db(Id, Opts, TtlSweep, VecOpts, Embedding) ->
    DocOpts0 = maps:get(docdb, Opts, #{}),
    OpenOpts0 = with_embedding(Embedding, #{
        docdb => DocOpts0#{ttl_sweep_interval => TtlSweep},
        vectordb => VecOpts,
        owner => barrel_spaces
    }),
    OpenOpts = case maps:get(encryption, Opts, disabled) of
        disabled -> OpenOpts0;
        Spec -> OpenOpts0#{encryption => Spec}
    end,
    barrel_dbs:ensure(Id, OpenOpts).

with_embedding(undefined, OpenOpts) -> OpenOpts;
with_embedding(Policy, OpenOpts) -> OpenOpts#{embedding => Policy}.

%% The policy's plain fields and the embedder identity: the registry
%% replicates, and an embedder config can hold paths and API keys.
recorded_embedding(#{embedding := Policy} = Db) ->
    {ok, Info} = barrel:embedder_info(Db),
    Fields = maps:with([fields, join, metadata_fields], Policy),
    Ident = maps:with([provider, model, revision, dimensions, distance,
                       fingerprint], Info),
    Rec = maps:merge(Fields, Ident#{mode => maps:get(mode, Policy)}),
    #{<<"embedding">> => maps:fold(fun(K, V, Acc) ->
                                       Acc#{atom_to_binary(K) => wire(V)}
                                   end, #{}, Rec)};
recorded_embedding(_PlainDb) ->
    #{}.

wire(V) when is_atom(V) -> atom_to_binary(V);
wire(V) -> V.

%% Explicit policy, recorded policy, or none.
space_embedding(undefined, undefined) ->
    {ok, undefined};
space_embedding(undefined, Recorded) ->
    verified(recorded_policy(Recorded), Recorded);
space_embedding(Policy, undefined) ->
    {ok, Policy};
space_embedding(Policy, Recorded) ->
    verified(Policy, Recorded).

recorded_policy(Recorded) ->
    Base = #{fields => maps:get(<<"fields">>, Recorded, []),
             join => maps:get(<<"join">>, Recorded, <<"\n">>),
             mode => mode(maps:get(<<"mode">>, Recorded, <<"async">>))},
    Policy = maps:merge(Base, maps:fold(fun recorded_key/3, #{}, Recorded)),
    case application:get_env(barrel_spaces, embedder, undefined) of
        undefined -> Policy;
        Embedder -> Policy#{embedder => Embedder}
    end.

recorded_key(<<"metadata_fields">>, V, Acc) -> Acc#{metadata_fields => V};
recorded_key(<<"dimensions">>, V, Acc) -> Acc#{dimensions => V};
recorded_key(_K, _V, Acc) -> Acc.

mode(<<"sync">>) -> sync;
mode(<<"async">>) -> async.

verified(Policy, #{<<"fingerprint">> := Fp, <<"dimensions">> := Dim,
                   <<"distance">> := Distance}) ->
    case barrel:embedding_fingerprint(Policy, Dim, Distance) of
        {ok, Fp} -> {ok, Policy};
        {ok, undefined} -> {error, {embedder_required, Fp}};
        {ok, Other} ->
            {error, {embedder_mismatch, #{recorded => Fp, given => Other}}};
        {error, _} = Err -> Err
    end;
verified(Policy, _NoFingerprint) ->
    {ok, Policy}.

vec_opts(_Id, none) ->
    none;
vec_opts(_Id, #{db_path := _} = VecOpts) ->
    VecOpts;
vec_opts(Id, VecOpts) ->
    VecOpts#{db_path => default_vec_path(Id)}.

%% The vector store is not replicated: record the default layout
%% relative to data_dir so each node resolves it locally.
recorded_vec_path(_Id, none) ->
    #{<<"vectordb">> => <<"none">>};
recorded_vec_path(Id, #{db_path := Path}) ->
    Abs = filename:absname(iolist_to_binary(Path)),
    case Abs =:= filename:absname(iolist_to_binary(default_vec_path(Id))) of
        true -> #{<<"vec_path">> => list_to_binary(vec_dir_name(Id))};
        false -> #{<<"vec_path">> => Abs, <<"vec_custom">> => true}
    end.

vec_opts_from(_Id, #{<<"vectordb">> := <<"none">>}, _VecOpts) ->
    none;
vec_opts_from(_Id, _Info, #{db_path := _} = VecOpts) ->
    VecOpts;
vec_opts_from(Id, Info, VecOpts) ->
    VecOpts#{db_path => resolve_vec_path(Id, Info)}.

resolve_vec_path(_Id, #{<<"vec_custom">> := true, <<"vec_path">> := Path}) ->
    binary_to_list(Path);
resolve_vec_path(Id, #{<<"vec_path">> := Path}) ->
    resolve_vec_path(Id, binary_to_list(Path), filename:pathtype(Path));
resolve_vec_path(Id, _Info) ->
    default_vec_path(Id).

resolve_vec_path(_Id, Path, relative) ->
    filename:join(data_dir(), Path);
resolve_vec_path(Id, Path, _Absolute) ->
    %% 1.2.1 doc: an absolute default path may come from another node.
    case {under_data_dir(Path), filename:basename(Path) =:= vec_dir_name(Id)} of
        {false, true} -> default_vec_path(Id);
        _ -> Path
    end.

under_data_dir(Path) ->
    lists:prefix(filename:split(filename:absname(data_dir())),
                 filename:split(filename:absname(Path))).

default_vec_path(Id) ->
    filename:join(data_dir(), vec_dir_name(Id)).

vec_dir_name(Id) ->
    binary_to_list(Id) ++ "_vec".

data_dir() ->
    unicode:characters_to_list(
      application:get_env(barrel_docdb, data_dir, "/tmp/barrel_data")).

%% Revocation is owned by barrel_caps (next step); tolerate its absence
%% so this module stays independently testable.
revoke_grants(Id) ->
    case erlang:function_exported(barrel_caps, revoke_all, 1) of
        true -> barrel_caps:revoke_all(Id);
        false -> ok
    end.

%% @doc RFC 4648 base32, lowercase, no padding (callers pass sizes
%% that are multiples of 5 bits: 10 bytes -> 16 chars, 25 -> 40).
-spec base32(binary()) -> binary().
base32(Bin) ->
    << <<(b32_char(C))>> || <<C:5>> <= Bin >>.

b32_char(C) when C < 26 -> $a + C;
b32_char(C) -> $2 + C - 26.
