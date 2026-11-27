%% @doc One source database: its derived store, its installed designs
%% and one indexer per active view.
%%
%% docdb has no database lifecycle events, so this process polls until
%% the database is open, then binds to its instance id. The store lives
%% in a directory named after that instance: a database deleted and
%% recreated under the same name, restored or branched gets a new id and
%% therefore an empty store, and the old directory is removed. When the
%% database closes, the indexers stop and the process waits for it to
%% open again; they resume from their durable checkpoints.
-module(barrel_design_db).
-behaviour(gen_server).

-export([start_link/2, install/3, uninstall/2, view/2, list/1, call/2]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(TABLE, barrel_design_dbs).

start_link(Db, Mode) ->
    gen_server:start_link(?MODULE, {Db, Mode}, []).

install(Pid, Design, Modules) -> call(Pid, {install, Design, Modules}).
uninstall(Pid, Name) -> call(Pid, {uninstall, Name}).
view(Pid, Key) -> call(Pid, {view, Key}).
list(Pid) -> call(Pid, list).

call(Pid, Msg) ->
    try gen_server:call(Pid, Msg, 60000)
    catch exit:{_, {gen_server, call, _}} -> {error, not_found}
    end.

init({Db, Mode}) ->
    process_flag(trap_exit, true),
    {ok, #{db => Db, mode => Mode, phase => waiting, views => #{}, designs => #{},
           store => undefined, instance => undefined, db_mon => undefined,
           backoff => 50}, {continue, open}}.

handle_continue(open, S) ->
    open(S).

%% Poll until the database is open, then bind to it.
open(#{db := Db, backoff := Backoff} = S) ->
    case {barrel_docdb:db_pid(Db), barrel_docdb:db_instance_id(Db)} of
        {{ok, Pid}, {ok, Inst}} -> bind(Pid, Inst, S);
        _ ->
            erlang:send_after(Backoff, self(), open),
            {noreply, S#{backoff := min(Backoff * 2, 1000)}}
    end.

bind(Pid, Inst, #{db := Db, mode := Mode} = S) ->
    case barrel_design_catalog:designs(Db) of
        {ok, Designs} when map_size(Designs) =:= 0, Mode =/= install ->
            %% nothing installed (or a fresh database under this name)
            _ = file:del_dir_r(barrel_design_store:db_dir(Db)),
            {stop, normal, S};
        {ok, Designs} ->
            remove_other_instances(Db, Inst),
            {ok, Env} = barrel_docdb:store_env(Db),
            case barrel_design_store:open(barrel_design_store:instance_dir(Db, Inst), Env) of
                {ok, Store} ->
                    S1 = S#{phase := open, store := Store, instance := Inst, designs := Designs,
                            db_mon := erlang:monitor(process, Pid), backoff := 50,
                            mode := restore},
                    S2 = maps:fold(fun(_Name, Design, Acc) -> start_design(Design, Acc) end,
                                   S1, Designs),
                    publish(S2),
                    {noreply, S2};
                {error, Reason} ->
                    %% the store may still be closing after a restart
                    logger:notice("barrel_design: ~s store not open yet: ~p", [Db, Reason]),
                    Backoff = maps:get(backoff, S),
                    erlang:send_after(Backoff, self(), open),
                    {noreply, S#{backoff := min(Backoff * 2, 1000)}}
            end;
        {error, _} ->
            erlang:send_after(50, self(), open),
            {noreply, S}
    end.

remove_other_instances(Db, Inst) ->
    Dir = barrel_design_store:db_dir(Db),
    case file:list_dir(Dir) of
        {ok, Names} ->
            [file:del_dir_r(filename:join(Dir, N)) || N <- Names, N =/= binary_to_list(Inst)],
            ok;
        {error, _} -> ok
    end.

handle_call(_Msg, _From, #{phase := waiting} = S) ->
    {reply, {error, db_not_open}, S};
handle_call({install, Design, Modules}, _From, S) ->
    case do_install(Design, Modules, S) of
        {ok, Reply, S1} -> publish(S1), {reply, {ok, Reply}, S1};
        {error, _} = Error -> {reply, Error, S}
    end;
handle_call({uninstall, Name}, _From, #{db := Db, designs := Designs} = S) ->
    case maps:take(Name, Designs) of
        {Design, Rest} ->
            S1 = stop_design(Design, S#{designs := Rest}),
            ok = barrel_design_catalog:delete_design(Db, Name),
            ok = gc_modules(S1),
            publish(S1),
            {reply, ok, S1};
        error ->
            {reply, {error, not_found}, S}
    end;
handle_call({view, Key}, _From, #{views := Views, store := Store, instance := Inst} = S) ->
    case maps:find(Key, Views) of
        {ok, #{pid := Pid, vid := Vid}} ->
            {reply, {ok, #{pid => Pid, vid => Vid, store => Store, instance => Inst}}, S};
        error -> {reply, {error, not_found}, S}
    end;
handle_call(list, _From, #{designs := Designs} = S) ->
    {reply, {ok, Designs}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(open, #{phase := waiting} = S) ->
    open(S);
handle_info(open, S) ->
    {noreply, S};
handle_info({'DOWN', Mon, process, _, _}, #{db_mon := Mon} = S) ->
    {noreply, unbind(S)};
handle_info({'EXIT', Pid, Reason}, #{views := Views} = S) ->
    case [K || {K, #{pid := P}} <- maps:to_list(Views), P =:= Pid] of
        [Key] ->
            case Reason of
                {shutdown, {instance_changed, _}} -> {noreply, unbind(S)};
                {shutdown, {db_unavailable, _}} -> {noreply, unbind(S)};
                _ ->
                    %% restart it; it resumes from its checkpoint
                    logger:warning("barrel_design: indexer ~p exited: ~p", [Key, Reason]),
                    erlang:send_after(1000, self(), {restart, Key}),
                    {noreply, S#{views := maps:remove(Key, Views)}}
            end;
        [] ->
            {noreply, S}
    end;
handle_info({restart, {Name, _} = Key}, #{phase := open, designs := Designs} = S) ->
    case maps:find(Name, Designs) of
        {ok, #{views := Vs, version := Ver}} ->
            {_, VName} = Key,
            case maps:find(VName, Vs) of
                {ok, Def} -> S1 = start_view(Name, VName, Def, Ver, S), publish(S1), {noreply, S1};
                error -> {noreply, S}
            end;
        error -> {noreply, S}
    end;
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, S) ->
    _ = unbind(S),
    ok.

%% The database went away (or was replaced): stop indexing and wait.
unbind(#{phase := waiting} = S) -> S;
unbind(#{db := Db, views := Views, store := Store, db_mon := Mon} = S) ->
    [stop_pid(P) || #{pid := P} <- maps:values(Views)],
    _ = Mon =/= undefined andalso erlang:demonitor(Mon, [flush]),
    _ = Store =/= undefined andalso barrel_design_store:close(Store),
    ets:delete(?TABLE, Db),
    erlang:send_after(50, self(), open),
    S#{phase := waiting, views := #{}, store := undefined, instance := undefined,
       db_mon := undefined}.

stop_pid(Pid) ->
    unlink(Pid),
    Ref = erlang:monitor(process, Pid),
    exit(Pid, shutdown),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 5000 -> exit(Pid, kill)
    end.

%%====================================================================
%% Install
%%====================================================================

do_install(Design0, Modules, #{db := Db, designs := Designs} = S) ->
    case barrel_design_manifest:normalize(Design0) of
        {error, _} = Error -> Error;
        {ok, #{name := Name, version := Ver} = Design} ->
            case maps:find(Name, Designs) of
                {ok, Design} ->
                    {ok, reply(Design, unchanged, S), S};
                {ok, #{version := Ver}} ->
                    {error, {version_conflict, Name, Ver}};
                {ok, #{version := Old}} when Old > Ver ->
                    {error, {stale_version, Name, Old}};
                _ ->
                    case check_modules(Design, Modules, Db) of
                        ok ->
                            ok = store_modules(Design, Modules, Db),
                            ok = barrel_design_catalog:put_design(Db, Design),
                            S1 = case maps:find(Name, Designs) of
                                {ok, Previous} -> upgrade(Previous, Design, S);
                                error -> start_design(Design, S)
                            end,
                            S2 = S1#{designs := Designs#{Name => Design}},
                            ok = gc_modules(S2),
                            {ok, reply(Design, installed, S2), S2};
                        {error, _} = Error ->
                            Error
                    end
            end
    end.

reply(#{name := Name, version := Ver, views := Vs}, Outcome, #{views := Views}) ->
    #{name => Name, version => Ver, outcome => Outcome,
      views => maps:map(fun(V, Def) ->
          Status = case maps:find({Name, V}, Views) of
              {ok, #{pid := Pid}} -> maps:with([generation, status, rebuilding], barrel_design_view:status(Pid));
              error -> #{}
          end,
          Status#{signature => binary:encode_hex(barrel_design_manifest:signature(Def), lowercase)}
      end, Vs)}.

%% Every referenced module must come with its bytes or already be
%% stored, and pass admission for every view that uses it.
check_modules(#{views := Views}, Modules, Db) ->
    Uses = [{D, E, maps:get(max_memory_pages, L)}
            || #{map := #{module := D, export := E}, limits := L} <- maps:values(Views)],
    lists:foldl(fun({D, Export, Pages}, ok) ->
                        case module_source(D, Modules, Db) of
                            {ok, Bytes} ->
                                case barrel_design_module:check_bytes(
                                         Bytes, D, #{export => Export, max_memory_pages => Pages}) of
                                    {ok, _} -> ok;
                                    {error, _} = E -> E
                                end;
                            error -> {error, {missing_module, binary:encode_hex(D, lowercase)}}
                        end;
                   (_, Error) -> Error
                end, ok, Uses).

module_source(D, Modules, Db) ->
    case maps:find(D, Modules) of
        {ok, Bytes} when is_binary(Bytes) -> {ok, Bytes};
        _ ->
            case barrel_design_catalog:module_bytes(Db, D) of
                {ok, Bytes} -> {ok, Bytes};
                {error, _} -> error
            end
    end.

store_modules(Design, Modules, Db) ->
    lists:foreach(fun(D) ->
        case maps:find(D, Modules) of
            {ok, Bytes} -> ok = barrel_design_catalog:put_module(Db, D, Bytes);
            error -> ok
        end
    end, barrel_design_manifest:modules(Design)).

%% Drop module bytes no installed design references.
gc_modules(#{db := Db, designs := Designs}) ->
    Used = lists:usort(lists:append([barrel_design_manifest:modules(D) || D <- maps:values(Designs)])),
    {ok, Stored} = barrel_design_catalog:modules(Db),
    [ok = barrel_design_catalog:delete_module(Db, D) || D <- Stored, not lists:member(D, Used)],
    ok.

start_design(#{name := Name, version := Ver, views := Vs}, S) ->
    maps:fold(fun(V, Def, Acc) -> start_view(Name, V, Def, Ver, Acc) end, S, Vs).

start_view(Name, V, Def, Ver, #{db := Db, store := Store, instance := Inst, views := Views} = S) ->
    {ok, Pid} = barrel_design_view:start_link(
        #{db => Db, store => Store, design => Name, view => V, instance => Inst,
          def => Def, signature => barrel_design_manifest:signature(Def), version => Ver}),
    S#{views := Views#{{Name, V} => #{pid => Pid, vid => barrel_design_index:view_id(Name, V)}}}.

%% Same-named views get the new definition (kept or rebuilt by their
%% indexer); removed views stop and lose their rows; new views start.
upgrade(#{name := Name, views := OldVs}, #{views := NewVs, version := Ver}, #{views := Views} = S) ->
    S1 = maps:fold(fun(V, _Def, Acc) ->
        case maps:is_key(V, NewVs) of
            true -> Acc;
            false -> drop_view(Name, V, Acc)
        end
    end, S, OldVs),
    maps:fold(fun(V, Def, Acc) ->
        case maps:find({Name, V}, Views) of
            {ok, #{pid := Pid}} ->
                {ok, _} = barrel_design_view:set_definition(
                    Pid, Def, barrel_design_manifest:signature(Def), Ver),
                Acc;
            error ->
                start_view(Name, V, Def, Ver, Acc)
        end
    end, S1, NewVs).

stop_design(#{name := Name, views := Vs}, S) ->
    maps:fold(fun(V, _Def, Acc) -> drop_view(Name, V, Acc) end, S, Vs).

drop_view(Name, V, #{views := Views, store := Store} = S) ->
    case maps:take({Name, V}, Views) of
        {#{pid := Pid, vid := Vid}, Rest} ->
            stop_pid(Pid),
            {ok, State} = barrel_design_index:read_state(Store, Vid),
            Gens = [G || #{gen := G} <- [maps:get(current, State), maps:get(building, State)]],
            [ok = barrel_design_store:delete_range(Store, L, U)
             || G <- Gens, {L, U} <- barrel_design_index:generation_ranges(Vid, G)],
            ok = barrel_design_store:write(Store, [{delete, barrel_design_index:state_key(Vid)}]),
            S#{views := Rest};
        error ->
            S
    end.

%% Readers find the store and the indexers here without a call.
publish(#{db := Db, store := Store, instance := Inst, views := Views, designs := Designs}) ->
    true = ets:insert(?TABLE, {Db, #{pid => self(), store => Store, instance => Inst,
                                     views => Views, designs => Designs}}),
    ok.
