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
%%% Space databases open through the facade's lifecycle manager
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
    case barrel_docdb:open_db(?REGISTRY_DB) of
        {ok, _} ->
            ok;
        {error, _} ->
            case barrel_docdb:create_db(?REGISTRY_DB) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end
    end.

%% @doc The registry database name (documents: `space:Id', `grant:Id',
%% `handoff:Id').
-spec registry_db() -> binary().
registry_db() ->
    ok = ensure_registry(),
    ?REGISTRY_DB.

%% @doc Create a space. Options:
%% <ul>
%% <li>`label', `purpose', `owner' - metadata binaries</li>
%% <li>`encryption' - a barrel_keyprovider spec; per-space keys are the
%%     agent isolation story (runtime config: pass it to open_space
%%     again after a restart)</li>
%% <li>`session_ttl' - default session TTL in seconds (3600)</li>
%% <li>`ttl_sweep_interval' - doc TTL sweep of the space db in ms
%%     (60000; sessions rely on it)</li>
%% <li>`docdb', `vectordb' - extra store config</li>
%% </ul>
-spec create_space(map()) -> {ok, space()} | {error, term()}.
create_space(Opts) when is_map(Opts) ->
    Registry = registry_db(),
    Id = new_id(<<"sp_">>),
    SessionTtl = maps:get(session_ttl, Opts, 3600),
    TtlSweep = maps:get(ttl_sweep_interval, Opts, 60000),
    VecOpts = vec_opts(Id, maps:get(vectordb, Opts, #{})),
    case open_db(Id, Opts, TtlSweep, VecOpts) of
        {ok, Db} ->
            Doc = #{
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
                <<"vec_path">> => iolist_to_binary(
                    maps:get(db_path, VecOpts)),
                <<"encrypted">> =>
                    maps:get(encryption, Opts, disabled) =/= disabled
            },
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
%% encrypted spaces, extra `docdb'/`vectordb' config).
-spec open_space(binary(), map()) -> {ok, space()} | {error, term()}.
open_space(Id, RuntimeOpts) when is_binary(Id), is_map(RuntimeOpts) ->
    case space_info(Id) of
        {ok, #{<<"status">> := <<"active">>} = Info} ->
            TtlSweep = maps:get(<<"ttl_sweep_interval">>, Info, 60000),
            VecOpts = vec_opts_from(Info, maps:get(vectordb, RuntimeOpts,
                                                   #{})),
            case open_db(Id, RuntimeOpts, TtlSweep, VecOpts) of
                {ok, Db} -> {ok, #{id => Id, db => Db}};
                {error, _} = Err -> Err
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
            case open_space(Id, RuntimeOpts) of
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

open_db(Id, Opts, TtlSweep, VecOpts) ->
    DocOpts0 = maps:get(docdb, Opts, #{}),
    OpenOpts0 = #{
        docdb => DocOpts0#{ttl_sweep_interval => TtlSweep},
        vectordb => VecOpts,
        owner => barrel_spaces
    },
    OpenOpts = case maps:get(encryption, Opts, disabled) of
        disabled -> OpenOpts0;
        Spec -> OpenOpts0#{encryption => Spec}
    end,
    barrel_dbs:ensure(Id, OpenOpts).

vec_opts(Id, VecOpts) ->
    case maps:is_key(db_path, VecOpts) of
        true ->
            VecOpts;
        false ->
            DataDir = application:get_env(barrel_docdb, data_dir,
                                          "/tmp/barrel_data"),
            Path = filename:join(DataDir, binary_to_list(Id) ++ "_vec"),
            VecOpts#{db_path => Path}
    end.

vec_opts_from(Info, VecOpts) ->
    case maps:is_key(db_path, VecOpts) of
        true -> VecOpts;
        false -> VecOpts#{db_path => binary_to_list(
                              maps:get(<<"vec_path">>, Info))}
    end.

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
