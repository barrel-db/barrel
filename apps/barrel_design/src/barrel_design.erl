%% @doc Versioned designs with automatically maintained views.
%%
%% A design packages named views for a database. Each view selects
%% documents with a BQL predicate and builds ordered key/value rows,
%% either with a native projection or with a Wasm map run under fresh
%% state, finite fuel, a deadline and memory limits.
-module(barrel_design).

-export([install/3, uninstall/2, list/1]).
-export([query/3, refresh/2, refresh/3, rebuild/2, status/2]).
-export([preview/3]).

-define(DEFAULT_TIMEOUT, 1000).

-type view_ref() :: {Design :: binary(), View :: binary()}.
-type checkpoint() :: #{instance_id := binary(), last_seq := binary()}.
-export_type([view_ref/0, checkpoint/0]).

%%====================================================================
%% Designs
%%====================================================================

%% @doc Install a named, versioned design in an open database. Modules
%% maps each referenced module digest (SHA-256) to its `.wasm' bytes;
%% bytes already stored need not be passed again. Everything is checked
%% before anything is written: the manifest, the digests, and each
%% module against the views that use it.
%%
%% Returns once the design is stored and its views are scheduled to
%% build. Installing the same version again changes nothing; a higher
%% version keeps the rows of every view whose signature is unchanged and
%% rebuilds the others in a new generation, the old one serving reads
%% until the new one has caught up.
-spec install(binary(), map(), #{binary() => binary()}) -> {ok, map()} | {error, term()}.
install(Db, Design, Modules) when is_binary(Db), is_map(Design), is_map(Modules) ->
    case barrel_design_dbs:ensure(Db, install) of
        {ok, Pid} -> barrel_design_db:install(Pid, Design, Modules);
        {error, _} = Error -> Error
    end.

%% @doc Remove a design, its views and their rows.
-spec uninstall(binary(), binary()) -> ok | {error, term()}.
uninstall(Db, Name) ->
    case attached(Db) of
        {ok, #{pid := Pid}} -> barrel_design_db:uninstall(Pid, Name);
        {error, _} = Error -> Error
    end.

%% @doc Installed designs by name.
-spec list(binary()) -> {ok, #{binary() => map()}} | {error, term()}.
list(Db) ->
    case attached(Db) of
        {ok, #{designs := Designs}} -> {ok, Designs};
        {error, _} -> barrel_design_catalog:designs(Db)
    end.

%%====================================================================
%% Reads
%%====================================================================

%% @doc Read a view. Selection: `key', `key_prefix' (whole leading
%% components), `start_key' and `end_key' (with `inclusive_end', default
%% true). `limit' rows (default 100, at most 10000) and a `continuation'
%% from the previous page. `reduce' (default: the view has a reducer)
%% totals with `_sum' or `_count', per group of the first `group_level'
%% key components (0, the default, for one total; `exact' for whole
%% keys); `max_scan' bounds the rows one call folds.
%%
%% `freshness': `stale' (default) answers from the last complete
%% generation at once; `{wait_for, Checkpoint}' first waits, up to
%% `timeout_ms' (default 1000), until the view covers that database
%% write. `authorize', a fun of the database name returning `ok', must
%% allow the caller to read the whole database: a view exposes data from
%% every document it indexes.
%%
%% Returns rows and `#{generation, checkpoint, status, continuation}'.
%% Before the first build completes: `{error, {building, Checkpoint}}'.
%% A continuation from a generation that has since been replaced:
%% `{error, stale_cursor}'.
-spec query(binary(), view_ref(), map()) -> {ok, [map()], map()} | {error, term()}.
query(Db, {Design, View}, Opts) when is_binary(Db), is_map(Opts) ->
    case authorize(Db, Opts) of
        ok ->
            case view_entry(Db, Design, View) of
                {ok, #{pid := Pid} = Entry} ->
                    case freshness(Pid, Opts) of
                        ok -> read(Entry, Opts);
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error -> Error
            end;
        {error, _} = Error -> Error
    end.

authorize(Db, #{authorize := Fun}) when is_function(Fun, 1) ->
    case Fun(Db) of
        ok -> ok;
        {error, _} = Error -> Error;
        Other -> {error, {unauthorized, Other}}
    end;
authorize(_Db, _Opts) ->
    ok.

freshness(Pid, #{freshness := {wait_for, Target}} = Opts) ->
    case barrel_design_view:wait_for(Pid, Target, deadline(Opts)) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end;
freshness(_Pid, #{freshness := stale}) -> ok;
freshness(_Pid, #{freshness := Other}) -> {error, {invalid_freshness, Other}};
freshness(_Pid, _Opts) -> ok.

read(#{store := Store, vid := Vid, instance := Inst}, Opts) ->
    try barrel_design_store:snapshot(Store) of
        {ok, Snap} ->
            try
                case barrel_design_index:read_state(Store, Vid, Snap) of
                    {ok, #{current := #{complete := true, gen := G, def := Def, checkpoint := Cp}}} ->
                        Opts1 = with_pushdown(Def, Opts),
                        case barrel_design_index:query(Store, Snap, Vid, G, Def, Opts1) of
                            {ok, Rows, Meta} ->
                                {ok, Rows, Meta#{generation => G, status => complete,
                                                 checkpoint => #{instance_id => Inst, last_seq => Cp}}};
                            {error, _} = Error -> Error
                        end;
                    {ok, #{current := #{checkpoint := Cp}}} ->
                        {error, {building, #{instance_id => Inst, last_seq => Cp}}};
                    not_found ->
                        {error, {building, #{instance_id => Inst, last_seq => <<0:96>>}}}
                end
            after
                barrel_design_store:release(Snap)
            end;
        {error, _} = Error -> Error
    catch
        error:badarg -> {error, unavailable}
    end.

%% BQL conditions (from the `view' source) narrow the scan.
with_pushdown(#{key := KeySpec, reduce := R}, #{where := Where} = Opts) ->
    Mode = case maps:get(reduce, Opts, R =/= none) of
        true -> {reduce, maps:get(group_level, Opts, 0)};
        false -> map
    end,
    case barrel_design_pushdown:bounds(KeySpec, Where, Mode) of
        none -> maps:remove(where, Opts);
        Bounds -> (maps:remove(where, Opts))#{bounds => Bounds}
    end;
with_pushdown(_Def, Opts) ->
    Opts.

%% @doc Catch up with the database's last write as of this call; see
%% refresh/3.
-spec refresh(binary(), view_ref()) -> {ok, checkpoint()} | {error, term()}.
refresh(Db, View) ->
    refresh(Db, View, #{}).

%% @doc Catch up with the database's last write as of this call. The
%% target is captured once, so later writes cannot move it; concurrent
%% refreshes share the indexer's batches. Returns the checkpoint reached
%% (it may include later writes) or, after `timeout_ms' (default 1000),
%% `{error, {timeout, Reached}}' while indexing continues. A view
%% already current answers at once without running any map.
-spec refresh(binary(), view_ref(), map()) -> {ok, checkpoint()} | {error, term()}.
refresh(Db, {Design, View}, Opts) ->
    case authorize(Db, Opts) of
        ok ->
            case view_entry(Db, Design, View) of
                {ok, #{pid := Pid}} -> barrel_design_view:refresh(Pid, deadline(Opts));
                {error, _} = Error -> Error
            end;
        {error, _} = Error -> Error
    end.

%% @doc Build the view again from every document into a new generation;
%% the current one serves reads until the new one has caught up.
-spec rebuild(binary(), view_ref()) -> {ok, non_neg_integer()} | {error, term()}.
rebuild(Db, {Design, View}) ->
    case view_entry(Db, Design, View) of
        {ok, #{pid := Pid}} -> barrel_design_view:rebuild(Pid);
        {error, _} = Error -> Error
    end.

%% @doc Generation, build status, checkpoint, error count and the
%% node's guest-call counters for a view.
-spec status(binary(), view_ref()) -> {ok, map()} | {error, term()}.
status(Db, {Design, View}) ->
    case view_entry(Db, Design, View) of
        {ok, #{pid := Pid, store := Store}} ->
            S = barrel_design_view:status(Pid),
            {ok, S#{guest => barrel_design_stats:get(),
                    modules => maps:get(resident, barrel_design_modules:info()),
                    store_bytes => barrel_design_store:bytes(Store)}};
        {error, _} = Error -> Error
    end.

view_entry(Db, Design, View) ->
    case attached(Db) of
        {ok, #{views := Views, store := Store, instance := Inst}} ->
            case maps:find({Design, View}, Views) of
                {ok, #{pid := Pid, vid := Vid}} ->
                    {ok, #{pid => Pid, vid => Vid, store => Store, instance => Inst}};
                error -> {error, {unknown_view, Design, View}}
            end;
        {error, not_found} ->
            {error, {unknown_view, Design, View}}
    end.

%% A database restored or branched with designs in its catalog has no
%% indexers until something asks for it: attach it on first use.
attached(Db) ->
    case barrel_design_dbs:lookup(Db) of
        {ok, _} = Found -> Found;
        {error, not_found} ->
            case {barrel_docdb:db_pid(Db), barrel_design_catalog:designs(Db)} of
                {{ok, _}, {ok, Designs}} when map_size(Designs) > 0 ->
                    {ok, Pid} = barrel_design_dbs:ensure(Db, restore),
                    _ = barrel_design_db:list(Pid),
                    barrel_design_dbs:lookup(Db);
                _ ->
                    {error, not_found}
            end
    end.

deadline(Opts) ->
    Timeout = case maps:get(timeout_ms, Opts, ?DEFAULT_TIMEOUT) of
        T when is_integer(T), T >= 0 -> T;
        _ -> ?DEFAULT_TIMEOUT
    end,
    erlang:monotonic_time(millisecond) + Timeout.

%%====================================================================
%% Preview
%%====================================================================

%% @doc Run a view definition over sample documents without installing
%% anything. Source is `{wat, Text}' (parsed in Erlang, never cached) or
%% `{wasm, Bytes}'. The view is a manifest view whose `map' omits
%% `module'. Returns, per document, its rows, its error or that the
%% selector skipped it.
-spec preview({wat, binary() | string()} | {wasm, binary()}, map(), [map()]) ->
    {ok, [map()]} | {error, term()}.
preview(Source, View, Docs) when is_map(View), is_list(Docs) ->
    case preview_module(Source) of
        {ok, Module, Digest} ->
            Map = maps:get(map, View, maps:get(<<"map">>, View, #{})),
            Design = #{name => <<"preview">>, version => 1,
                       views => #{<<"preview">> =>
                                      maps:remove(<<"map">>, View#{map => Map#{module => Digest}})}},
            case barrel_design_manifest:normalize(Design) of
                {ok, #{views := #{<<"preview">> := V}}} -> preview_run(Module, V, Docs);
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

preview_module({wat, Text}) when is_list(Text) ->
    preview_module({wat, unicode:characters_to_binary(Text)});
preview_module({wat, Text}) when is_binary(Text) ->
    case wasm:compile({wat, Text}) of
        {ok, Module} -> {ok, Module, <<0:256>>};
        {error, E} -> {error, {invalid_module, wat, E}}
    end;
preview_module({wasm, Bytes}) when is_binary(Bytes) ->
    case wasm:compile(Bytes) of
        {ok, Module} -> {ok, Module, crypto:hash(sha256, Bytes)};
        {error, E} -> {error, {invalid_module, wasm, E}}
    end.

preview_run(Module, #{map := #{export := Export} = Map, limits := Limits} = View, Docs) ->
    case barrel_design_module:check(Module, preview, #{export => Export,
                                                       max_memory_pages => maps:get(max_memory_pages, Limits)}) of
        {ok, _} ->
            {ok, Sel} = barrel_design_manifest:selector(maps:get(select, View)),
            Spec = guest_spec(View),
            Selected = [D || D <- Docs, barrel_design_manifest:selected(Sel, D)],
            Results = maps:from_list(lists:zip([id(D) || D <- Selected],
                                               barrel_design_guest:run(Module, Spec, Selected, #{}))),
            _ = Map,
            {ok, [preview_row(id(D), maps:find(id(D), Results)) || D <- Docs]};
        {error, _} = Error ->
            Error
    end.

preview_row(Id, error) -> #{id => Id, selected => false};
preview_row(Id, {ok, {ok, Rows}}) ->
    #{id => Id, selected => true, rows => [#{key => K, value => V} || {K, V} <- Rows]};
preview_row(Id, {ok, {error, Why}}) -> #{id => Id, selected => true, error => Why}.

id(#{<<"id">> := Id}) -> Id;
id(_) -> undefined.

%% @private The runner's view of a normalized Wasm view.
guest_spec(#{map := #{export := Export, fields := Fields}, key := Key, value := Value,
             limits := Limits}) ->
    #{export => Export, fields => Fields, key => Key, value => Value, limits => Limits}.
