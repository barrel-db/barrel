%%%-------------------------------------------------------------------
%%% @doc Quiesced export of a composed database to a checksummed
%%% directory and read-only import of such a generation.
%%%
%%% Export takes exclusive access through {@link barrel_dbs:hold/2},
%%% copies the docdb directory (documents, blob attachments, CRYPTO,
%%% TIMELINE) and the vector store directory into `Dest', and writes a
%%% manifest with a sha256 per file. Encrypted databases export
%%% as ciphertext; the importer resolves the key through its
%%% keyprovider under the source keyspace.
%%%
%%% A source whose stores need an upgrade (a read-only open answers
%%% `read_only_upgrade_needed') gets one writable open and close under
%%% the hold before the copy, so the export carries files that open read
%%% only and its checksums are those of the files the importer opens.
%%%
%%% Import copies and verifies every artifact into a partial directory,
%%% resumes from the files already verified, writes an import sidecar
%%% (TIMELINE `kind => import': keys keep the source name, the copy gets
%%% a fresh source id minted into the sidecar), and renames the directory
%%% into place only when complete. Imports open read only through
%%% `barrel_dbs' and write nothing to the copied files.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_export).

-export([export/3,
         import/2,
         open/1,
         open_opts/1,
         import_info/1,
         list_imports/0,
         remove_import/1,
         imports_dir/0]).

-define(IMPORT_RECORD, "IMPORT").
-define(POLICY_DOC, <<"_barrel/embedding">>).
%% Keys whose value is a secret, at any depth of a persisted policy.
-define(SECRET_KEYS, [<<"api_key">>, <<"apikey">>, <<"token">>,
                      <<"access_token">>, <<"secret">>, <<"secret_key">>,
                      <<"secret_access_key">>, <<"access_key_id">>,
                      <<"password">>, <<"authorization">>,
                      <<"credentials">>, <<"private_key">>]).
-define(PARTIAL_PREFIX, ".partial-").

%%====================================================================
%% Export
%%====================================================================

%% @doc Export `DbName' into the empty or absent directory `Dest'.
%%
%% Options: `owner' (barrel_dbs owner tag allowed to be closed),
%% `open_opts' (barrel:open options when the database is not open in
%% barrel_dbs), `context' (context id, minted when absent),
%% `generation' (default 1), `parent_generation'.
-spec export(barrel:db_name(), file:filename(), map()) ->
    {ok, map()} | {error, term()}.
export(DbName0, Dest, Opts) ->
    DbName = to_name(DbName0),
    case dest_ready(Dest) of
        ok ->
            case barrel_dbs:hold(DbName, maps:with([owner], Opts)) of
                {ok, Held} ->
                    try
                        export_held(DbName, Dest, Opts, Held)
                    after
                        ok = barrel_dbs:unhold(DbName),
                        reopen(DbName, Opts, Held)
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

dest_ready(Dest) ->
    case file:list_dir(Dest) of
        {ok, []} -> ok;
        {ok, _} -> {error, {dest_not_empty, Dest}};
        {error, enoent} -> filelib:ensure_path(Dest);
        {error, Reason} -> {error, {dest, Reason}}
    end.

reopen(_DbName, _Opts, #{was_open := false}) ->
    ok;
reopen(DbName, Opts, #{was_open := true, opts := OpenOpts}) ->
    Owner = maps:with([owner], Opts),
    case barrel_dbs:ensure(DbName, maps:merge(OpenOpts, Owner)) of
        {ok, _} -> ok;
        {error, Reason} ->
            logger:warning("barrel_ctx_export: reopen of ~ts failed: ~p",
                           [DbName, Reason])
    end.

%% Open privately and read only to learn paths and identity (nothing
%% can write: barrel_dbs refuses the name, the stores refuse writes),
%% close, then copy.
export_held(DbName, Dest, Opts, #{was_open := WasOpen, opts := HeldOpts}) ->
    OpenOpts = case WasOpen of
        true -> HeldOpts;
        false -> maps:get(open_opts, Opts, #{})
    end,
    case open_source(DbName, OpenOpts) of
        {ok, Db} ->
            Checked = try
                          case policy_secret(DbName) of
                              none -> {ok, facts(Db, OpenOpts)};
                              Key -> {error, {policy_holds_secret, Key}}
                          end
                      after barrel:close(Db)
                      end,
            case Checked of
                {ok, Facts} -> copy_and_manifest(Dest, Facts, Opts);
                {error, _} = Err -> Err
            end;
        {error, Reason} ->
            {error, {open_failed, Reason}}
    end.

%% Read only; a store an older version wrote is upgraded by one writable
%% open and close of the held source (never of a read-only source). The
%% vector store is supervised: a failed open never exits the caller.
open_source(DbName, OpenOpts0) ->
    OpenOpts = OpenOpts0#{store_supervised => true},
    case barrel:open(DbName, OpenOpts#{read_only => true}) of
        {ok, _} = Ok ->
            Ok;
        {error, Reason} = Err ->
            case upgrade_needed(Reason) andalso
                     maps:get(read_only, OpenOpts, false) =/= true of
                true -> upgrade_source(DbName, OpenOpts);
                false -> Err
            end
    end.

upgrade_source(DbName, OpenOpts) ->
    case barrel:open(DbName, OpenOpts) of
        {ok, Db} ->
            ok = barrel:close(Db),
            barrel:open(DbName, OpenOpts#{read_only => true});
        {error, _} = Err ->
            Err
    end.

upgrade_needed({read_only_upgrade_needed, _}) -> true;
upgrade_needed(T) when is_tuple(T) ->
    lists:any(fun upgrade_needed/1, tuple_to_list(T));
upgrade_needed(_) -> false.

facts(#{docdb := DbBin, vstore := VStore} = Db, OpenOpts) ->
    {ok, Info} = barrel_docdb:db_info(DbBin),
    {ok, VPath} = barrel_vectordb_server:get_db_path(VStore),
    {ok, VStats} = barrel_vectordb:stats(VStore),
    {ok, InstanceId} = barrel_docdb:db_instance_id(DbBin),
    Config = maps:get(config, Info, #{}),
    #{db => DbBin,
      keyspace => maps:get(keyspace, Info),
      db_path => maps:get(db_path, Info),
      vector_path => filename:absname(VPath),
      vector_config => portable_vector_config(maps:get(config, VStats, #{})),
      vector_count => maps:get(count, VStats, 0),
      att_backend => maps:get(backend, maps:get(att_opts, Config, #{}), blob),
      instance_id => InstanceId,
      last_seq => last_seq(DbBin),
      record_mode => maps:is_key(embedding, Db),
      dimensions => maps:get(dimension, VStats),
      encrypted => maps:get(encryption, OpenOpts, disabled) =/= disabled}.

%% A record-mode database persists its policy, embedder config included,
%% inside the files an export copies: refuse when it names a secret.
policy_secret(DbBin) ->
    case barrel_docdb:get_local_doc(DbBin, ?POLICY_DOC) of
        {ok, #{<<"policy">> := Bin}} ->
            secret_key(binary_to_term(Bin, [safe]));
        _ ->
            none
    end.

secret_key(Map) when is_map(Map) ->
    first_secret(maps:to_list(Map));
secret_key(List) when is_list(List) ->
    first_secret([{none, V} || V <- List]);
secret_key(Tuple) when is_tuple(Tuple) ->
    first_secret([{none, V} || V <- tuple_to_list(Tuple)]);
secret_key(_Scalar) ->
    none.

first_secret([]) ->
    none;
first_secret([{K, V} | Rest]) ->
    case is_secret_key(K) of
        true -> key_bin(K);
        false ->
            case secret_key(V) of
                none -> first_secret(Rest);
                Found -> Found
            end
    end.

is_secret_key(K) when is_atom(K), K =/= none ->
    is_secret_key(atom_to_binary(K));
is_secret_key(K) when is_binary(K) ->
    lists:member(string:lowercase(K), ?SECRET_KEYS);
is_secret_key(_K) ->
    false.

key_bin(K) when is_atom(K) -> atom_to_binary(K);
key_bin(K) -> K.

%% The vector config an importer needs to load the persisted graph (the
%% index fingerprint must match); never keys, embedders or docstores.
portable_vector_config(Config) ->
    maps:with([dimension, backend, hnsw, faiss, diskann, bm25_backend, bm25,
               bm25_disk], Config).

last_seq(DbBin) ->
    {ok, Pid} = barrel_docdb:db_pid(DbBin),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    b64(barrel_changes:get_last_seq(StoreRef, DbBin)).

copy_and_manifest(_Dest, #{att_backend := Backend}, _Opts)
        when Backend =/= blob, Backend =/= none ->
    {error, {unsupported_att_backend, Backend}};
copy_and_manifest(Dest, Facts, Opts) ->
    #{db_path := DbPath, vector_path := VPath} = Facts,
    T0 = erlang:monotonic_time(millisecond),
    ok = copy_tree(DbPath, filename:join(Dest, "docdb")),
    ok = copy_tree(VPath, filename:join(Dest, "vectordb")),
    Artifacts = barrel_ctx_manifest:scan(Dest),
    Root = root(Facts, Opts),
    case barrel_ctx_manifest:write(Dest, Root, Artifacts) of
        {ok, Written} ->
            {ok, #{dest => Dest,
                   context => maps:get(<<"context">>, Written),
                   generation => maps:get(<<"generation">>, Written),
                   artifacts => length(Artifacts),
                   bytes => lists:sum([B || #{bytes := B} <- Artifacts]),
                   elapsed_ms => erlang:monotonic_time(millisecond) - T0,
                   manifest => Written}};
        {error, _} = Err ->
            Err
    end.

root(Facts, Opts) ->
    #{db := Db, keyspace := Ks, instance_id := Iid, last_seq := Seq,
      vector_config := VCfg} = Facts,
    Ctx = maps:get(context, Opts, barrel_ctx_manifest:new_id(<<"ctx_">>)),
    #{<<"context">> => Ctx,
      <<"generation">> => maps:get(generation, Opts, 1),
      <<"parent_generation">> => maps:get(parent_generation, Opts, null),
      <<"published_at">> => iso8601_now(),
      <<"publisher">> => node_ref(),
      <<"engine">> => #{<<"barrel">> => vsn(barrel),
                        <<"barrel_docdb">> => vsn(barrel_docdb),
                        <<"barrel_vectordb">> => vsn(barrel_vectordb)},
      <<"sources">> => [#{<<"db">> => Db, <<"keyspace">> => Ks,
                          <<"instance_id">> => Iid, <<"last_seq">> => Seq,
                          <<"quiesced">> => true}],
      <<"layout">> => #{
          <<"docdb">> => <<"docdb">>,
          <<"vectordb">> => <<"vectordb">>,
          <<"mode">> => mode(maps:get(record_mode, Facts)),
          <<"att_backend">> => atom_to_binary(maps:get(att_backend, Facts)),
          <<"encrypted">> => maps:get(encrypted, Facts),
          <<"dimensions">> => maps:get(dimensions, Facts),
          <<"vectors">> => maps:get(vector_count, Facts),
          %% Erlang term (atoms, tuples) carried opaquely for the
          %% importer; it holds no secret (see portable_vector_config)
          <<"vector_config_etf">> => base64:encode(term_to_binary(VCfg))}}.

mode(true) -> <<"record">>;
mode(false) -> <<"plain">>.

%%====================================================================
%% Import
%%====================================================================

%% @doc Import the generation exported in directory `Src'.
%%
%% Options: `name' (local database name, default `wsnap_<ctx>_<gen>'),
%% `encryption' (keyprovider spec for an encrypted generation),
%% `embedding' (policy override for record mode, default the stored
%% one), `vectordb' (extra store config such as an embedder),
%% `open' (default true), `owner' (barrel_dbs owner tag),
%% `stop_after' (test hook: stop after copying that many artifacts).
-spec import(file:filename(), map()) -> {ok, map()} | {error, term()}.
import(Src, Opts) ->
    case barrel_ctx_manifest:read(Src) of
        {ok, Root, Artifacts} ->
            Name = local_name(Root, Opts),
            case check_name(Name, Root) of
                ok -> import_as(Src, Root, Artifacts, Name, Opts);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%% The source's own names are refused: the copy would reuse the
%% source id stored under that name (two authors, one id).
check_name(Name, #{<<"sources">> := [#{<<"db">> := Db,
                                       <<"keyspace">> := Ks} | _]})
        when Name =:= Db; Name =:= Ks ->
    {error, {same_name_as_source, Name}};
check_name(Name, _Root) ->
    barrel_docdb:validate_db_name(Name).

local_name(Root, Opts) ->
    case maps:find(name, Opts) of
        {ok, Name} ->
            to_name(Name);
        error ->
            #{<<"context">> := Ctx, <<"generation">> := Gen} = Root,
            Short = binary:part(strip_prefix(Ctx), 0,
                                min(8, byte_size(strip_prefix(Ctx)))),
            <<"wsnap_", Short/binary, "_", (integer_to_binary(Gen))/binary>>
    end.

strip_prefix(<<"ctx_", Rest/binary>>) -> Rest;
strip_prefix(Other) -> Other.

import_as(Src, Root, Artifacts, Name, Opts) ->
    Final = import_path(Name),
    case filelib:is_regular(filename:join(Final, ?IMPORT_RECORD)) of
        true ->
            same_generation(Name, Root, Opts);
        false ->
            Partial = filename:join(imports_dir(), ?PARTIAL_PREFIX ++
                                        binary_to_list(Name)),
            case copy_verified(Src, Partial, Name, Artifacts,
                               maps:get(stop_after, Opts, infinity),
                               {0, 0}) of
                {ok, {Copied, Reused}} ->
                    ok = seal(Partial, Final, Name, Root, Opts),
                    maybe_open(Name, Opts, #{copied => Copied,
                                             reused => Reused});
                {error, _} = Err ->
                    Err
            end
    end.

%% A finished import of the same context generation is a no-op.
same_generation(Name, Root, Opts) ->
    {ok, Rec} = import_info(Name),
    case {maps:get(context, Rec), maps:get(generation, Rec)} of
        {Ctx, Gen} when Ctx =:= map_get(<<"context">>, Root),
                        Gen =:= map_get(<<"generation">>, Root) ->
            maybe_open(Name, Opts, #{copied => 0, reused => 0});
        Other ->
            {error, {name_taken, Name, Other}}
    end.

%% Copy each artifact to its place under Partial unless an already
%% verified copy is there; each copy lands through a temp file and is
%% verified before the rename.
copy_verified(_Src, _Partial, _Name, _Arts, 0, Counts) ->
    {error, {interrupted, Counts}};
copy_verified(_Src, _Partial, _Name, [], _Stop, Counts) ->
    {ok, Counts};
copy_verified(Src, Partial, Name, [#{path := P} = Art | Rest], Stop,
              {Copied, Reused}) ->
    Target = unicode:characters_to_list(
                 filename:join(Partial, local_path(P, Name))),
    case barrel_ctx_manifest:verify_file(Target, Art) of
        true ->
            copy_verified(Src, Partial, Name, Rest, Stop,
                          {Copied, Reused + 1});
        false ->
            ok = filelib:ensure_dir(Target),
            Tmp = Target ++ ".part",
            {ok, _} = file:copy(filename:join(Src, P), Tmp),
            case barrel_ctx_manifest:verify_file(Tmp, Art) of
                true ->
                    ok = file:rename(Tmp, Target),
                    copy_verified(Src, Partial, Name, Rest, dec(Stop),
                                  {Copied + 1, Reused});
                false ->
                    _ = file:delete(Tmp),
                    {error, {checksum_mismatch, P}}
            end
    end.

dec(infinity) -> infinity;
dec(N) -> N - 1.

%% docdb/X lands in docdb/<Name>/X so the docdb data_dir is docdb/.
local_path(<<"docdb/", Rest/binary>>, Name) ->
    filename:join(["docdb", Name, Rest]);
local_path(P, _Name) ->
    binary_to_list(P).

%% Import sidecar and record, then the atomic rename into place.
seal(Partial, Final, Name, Root, Opts) ->
    #{<<"sources">> := [#{<<"keyspace">> := Ks} | _],
      <<"layout">> := Layout} = Root,
    DocDir = filename:join([Partial, "docdb", binary_to_list(Name)]),
    %% The copy's source id is minted here: the read-only store cannot
    %% persist one, and the copied files stay as the manifest lists them.
    ok = barrel_keyspace:write_meta(DocDir, #{
        keyspace => Ks, parent => Ks, kind => import,
        fork_hlc => barrel_hlc:encode(barrel_hlc:new_hlc()),
        source_id => binary:encode_hex(crypto:strong_rand_bytes(8),
                                       lowercase)}),
    Rec = #{name => Name,
            context => maps:get(<<"context">>, Root),
            generation => maps:get(<<"generation">>, Root),
            source => hd(maps:get(<<"sources">>, Root)),
            mode => maps:get(<<"mode">>, Layout),
            encrypted => maps:get(<<"encrypted">>, Layout),
            vector_config => binary_to_term(
                               base64:decode(maps:get(<<"vector_config_etf">>,
                                                      Layout)), [safe]),
            open => maps:with([encryption, embedding, vectordb], Opts),
            imported_at => iso8601_now()},
    ok = file:write_file(filename:join(Partial, ?IMPORT_RECORD),
                         io_lib:format("~p.~n", [Rec]), [sync]),
    _ = file:del_dir_r(Final),
    file:rename(Partial, Final).

maybe_open(Name, Opts, Counts) ->
    {ok, Rec} = import_info(Name),
    Base = maps:merge(Counts, #{name => Name, path => import_path(Name),
                                context => maps:get(context, Rec),
                                version => #{kind => generation,
                                             generation =>
                                                 maps:get(generation, Rec)}}),
    case maps:get(open, Opts, true) of
        false ->
            {ok, Base};
        true ->
            case open(Name, maps:with([owner], Opts)) of
                {ok, Db} -> {ok, Base#{db => Db}};
                {error, _} = Err -> Err
            end
    end.

%%====================================================================
%% Imported generations
%%====================================================================

%% @doc Open an imported generation read only through barrel_dbs.
-spec open(barrel:db_name()) -> {ok, barrel:db()} | {error, term()}.
open(Name) ->
    open(Name, #{}).

open(Name, Extra) ->
    case open_opts(Name) of
        {ok, OpenOpts} -> barrel_dbs:ensure(Name, maps:merge(OpenOpts, Extra));
        {error, _} = Err -> Err
    end.

%% @doc barrel:open options for an imported generation; `not_imported'
%% for a name whose import never completed.
-spec open_opts(barrel:db_name()) -> {ok, map()} | {error, term()}.
open_opts(Name0) ->
    Name = to_name(Name0),
    case import_info(Name) of
        {ok, #{mode := Mode, vector_config := VCfg, open := Open}} ->
            Path = import_path(Name),
            %% the source's BM25 backend: a disk index is copied as
            %% written, a memory one is rebuilt at open
            VecOpts = maps:merge(VCfg#{db_path => filename:join(Path,
                                                                "vectordb")},
                                 maps:get(vectordb, Open, #{})),
            Base = #{read_only => true,
                     docdb => #{data_dir => filename:join(Path, "docdb")},
                     vectordb => VecOpts},
            {ok, maps:merge(with_mode(Mode, Open, Base),
                            maps:with([encryption], Open))};
        {error, _} = Err ->
            Err
    end.

with_mode(<<"record">>, Open, Base) ->
    Base#{embedding => maps:get(embedding, Open, stored)};
with_mode(<<"plain">>, _Open, Base) ->
    Base.

%% @doc The import record of a completed import.
-spec import_info(barrel:db_name()) -> {ok, map()} | {error, not_imported}.
import_info(Name) ->
    File = filename:join(import_path(to_name(Name)), ?IMPORT_RECORD),
    case file:consult(File) of
        {ok, [Rec]} when is_map(Rec) -> {ok, Rec};
        _ -> {error, not_imported}
    end.

%% @doc Names of the completed imports on this node.
-spec list_imports() -> [binary()].
list_imports() ->
    case file:list_dir(imports_dir()) of
        {ok, Names} ->
            lists:sort([list_to_binary(N) || N <- Names,
                        filelib:is_regular(filename:join([imports_dir(), N,
                                                          ?IMPORT_RECORD]))]);
        {error, _} ->
            []
    end.

%% @doc Close and delete an imported generation (and any partial copy).
-spec remove_import(barrel:db_name()) -> ok.
remove_import(Name0) ->
    Name = to_name(Name0),
    ok = barrel_dbs:close(Name),
    _ = file:del_dir_r(import_path(Name)),
    _ = file:del_dir_r(filename:join(imports_dir(),
                                     ?PARTIAL_PREFIX ++ binary_to_list(Name))),
    ok.

%% @doc Where imports live: `barrel' env `ctx_dir' (default
%% `<docdb data_dir>/_ctx') plus `imports'.
-spec imports_dir() -> file:filename().
imports_dir() ->
    Default = filename:join(application:get_env(barrel_docdb, data_dir,
                                                "/tmp/barrel_data"), "_ctx"),
    filename:join(application:get_env(barrel, ctx_dir, Default), "imports").

import_path(Name) ->
    filename:join(imports_dir(), binary_to_list(Name)).

%%====================================================================
%% Internal
%%====================================================================

copy_tree(Src, Dst) ->
    case filelib:is_dir(Src) of
        true ->
            ok = filelib:ensure_path(Dst),
            {ok, Names} = file:list_dir(Src),
            lists:foreach(fun(N) -> ok = copy_tree(filename:join(Src, N),
                                                   filename:join(Dst, N))
                          end, lists:sort(Names));
        false ->
            case filelib:is_regular(Src) of
                true -> {ok, _} = file:copy(Src, Dst), ok;
                false -> ok
            end
    end.

b64(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

vsn(App) ->
    case application:get_key(App, vsn) of
        {ok, V} -> list_to_binary(V);
        undefined -> null
    end.

node_ref() ->
    try <<"node:", (barrel_docdb:node_id())/binary>>
    catch _:_ -> null
    end.

iso8601_now() ->
    list_to_binary(calendar:system_time_to_rfc3339(
                     erlang:system_time(second), [{offset, "Z"}])).

to_name(Name) when is_binary(Name) -> Name;
to_name(Name) when is_atom(Name) -> atom_to_binary(Name, utf8).
