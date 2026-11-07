%%%-------------------------------------------------------------------
%%% @doc Tracks per-database supervisors: finds a restarted writer by
%%% name and records a database whose writer ran out of restarts as
%%% failed, until reopen_db/1 or delete_db/1 clears it.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_watch).

-behaviour(gen_server).

-export([start_link/0]).
-export([writer_started/3, lookup/1, clear_failure/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TAB, ?MODULE).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Called by a supervised writer once open.
-spec writer_started(binary(), map(), pid()) -> ok.
writer_started(Name, Config, Sup) ->
    try gen_server:call(?MODULE, {writer_started, Name, Config, Sup, self()},
                        infinity)
    catch exit:{noproc, _} -> ok
    end.

%% @doc Where a database stands when its registered writer is gone:
%% the restarted writer, a recorded failure, or nothing.
-spec lookup(binary()) -> {ok, pid()} | {failed, term()} | not_found.
lookup(Name) ->
    try ets:lookup(?TAB, {inst, Name}) of
        [{_, Sup}] ->
            case barrel_db_instance_sup:writer(Sup) of
                {ok, Pid} ->
                    {ok, Pid};
                {error, not_found} ->
                    %% the supervisor just died: let its DOWN be handled
                    ok = gen_server:call(?MODULE, {settle, Name}, infinity),
                    failure(Name)
            end;
        [] ->
            failure(Name)
    catch
        error:badarg -> not_found
    end.

%% @doc Forget a recorded failure; returns the config it ran with.
-spec clear_failure(binary()) -> {ok, map()} | not_found.
clear_failure(Name) ->
    try gen_server:call(?MODULE, {clear_failure, Name}, infinity)
    catch exit:{noproc, _} -> not_found
    end.

failure(Name) ->
    case ets:lookup(?TAB, {failed, Name}) of
        [{_, Reason, _Config}] -> {failed, Reason};
        [] -> not_found
    end.

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    _ = ets:new(?TAB, [named_table, protected, set, {read_concurrency, true}]),
    %% Sup => #{name, config, mref, writer => {Pid, MRef} | undefined, last}
    {ok, #{}}.

handle_call({writer_started, Name, Config, Sup, Writer}, _From, Sups) ->
    Entry0 = case Sups of
        #{Sup := E} -> E;
        _ -> #{name => Name, config => Config,
               mref => erlang:monitor(process, Sup), last => undefined}
    end,
    true = ets:insert(?TAB, {{inst, Name}, Sup}),
    true = ets:delete(?TAB, {failed, Name}),
    Entry = Entry0#{writer => {Writer, erlang:monitor(process, Writer)}},
    {reply, ok, Sups#{Sup => Entry}};
handle_call({settle, Name}, _From, Sups) ->
    Sups1 = case ets:lookup(?TAB, {inst, Name}) of
        [{_, Sup}] -> settle(Sup, Sups);
        [] -> Sups
    end,
    {reply, ok, Sups1};
handle_call({clear_failure, Name}, _From, Sups) ->
    case ets:lookup(?TAB, {failed, Name}) of
        [{_, _Reason, Config}] ->
            true = ets:delete(?TAB, {failed, Name}),
            {reply, {ok, Config}, Sups};
        [] ->
            {reply, not_found, Sups}
    end;
handle_call(_Req, _From, Sups) ->
    {reply, {error, unknown_call}, Sups}.

handle_cast(_Msg, Sups) ->
    {noreply, Sups}.

handle_info({'DOWN', MRef, process, Pid, Reason}, Sups) ->
    case Sups of
        #{Pid := #{mref := MRef} = Entry} ->
            {noreply, sup_down(Pid, Entry, maps:remove(Pid, Sups))};
        _ ->
            {noreply, writer_down(MRef, Reason, Sups)}
    end;
handle_info(_Info, Sups) ->
    {noreply, Sups}.

%%====================================================================
%% Internal
%%====================================================================

%% Handle a dead supervisor's DOWN now (it is on its way).
settle(Sup, Sups) ->
    case {Sups, is_process_alive(Sup)} of
        {#{Sup := #{mref := MRef} = Entry}, false} ->
            receive {'DOWN', MRef, process, Sup, _} -> ok end,
            sup_down(Sup, Entry, maps:remove(Sup, Sups));
        _ ->
            Sups
    end.

writer_down(MRef, Reason, Sups) ->
    maps:map(fun(_Sup, #{writer := {_, R}} = E) when R =:= MRef ->
                     E#{writer => undefined, last => Reason};
                (_Sup, E) ->
                     E
             end, Sups).

%% The supervisor only exits once its writer is dead: take that DOWN.
sup_down(Sup, #{name := Name, config := Config} = Entry, Sups) ->
    Last = case Entry of
        #{writer := {Writer, WRef}} ->
            receive {'DOWN', WRef, process, Writer, R} -> R end;
        #{last := L} ->
            L
    end,
    case ets:lookup(?TAB, {inst, Name}) of
        [{_, Sup}] ->
            true = ets:delete(?TAB, {inst, Name}),
            case clean_exit(Last) of
                true ->
                    ok;
                false ->
                    logger:error("Database ~s failed after repeated "
                                 "writer crashes: ~p", [Name, Last]),
                    _ = try barrel_db_server:release_stale_refs(Name)
                        catch _:_ -> ok
                        end,
                    true = ets:insert(?TAB, {{failed, Name}, Last, Config})
            end;
        _ ->
            %% a newer instance owns the name
            ok
    end,
    Sups.

clean_exit(normal) -> true;
clean_exit(shutdown) -> true;
clean_exit({shutdown, _}) -> true;
clean_exit(_) -> false.
