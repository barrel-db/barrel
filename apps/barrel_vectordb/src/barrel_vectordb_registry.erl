%%%-------------------------------------------------------------------
%%% @doc Local name registry for vector stores and their companions.
%%%
%%% Names are arbitrary terms (in practice `{vstore, NameBin}' and
%%% friends), so dynamically named ephemeral stores never grow the
%%% atom table the way `{local, Atom}' registration does. Implements
%%% the `{via, Module, Name}' callbacks over a public ETS table; the
%%% owner process monitors registered pids and removes dead entries,
%%% so a crashed store never leaves a stale registration behind.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_registry).
-behaviour(gen_server).

-export([start_link/0, ensure/0]).
%% via callbacks
-export([register_name/2, unregister_name/1, whereis_name/1, send/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TAB, ?MODULE).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Start the registry if it is not running. Covers callers that
%% start stores without the barrel_vectordb application (eunit,
%% embedded use); under the application the supervisor owns it.
-spec ensure() -> ok.
ensure() ->
    case whereis(?MODULE) of
        undefined ->
            case gen_server:start({local, ?MODULE}, ?MODULE, [], []) of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok
            end;
        _ ->
            ok
    end.

%% @doc via callback: register Name to Pid. `yes' on success, `no' if
%% the name is taken by a live process.
-spec register_name(term(), pid()) -> yes | no.
register_name(Name, Pid) when is_pid(Pid) ->
    gen_server:call(?MODULE, {register, Name, Pid}, infinity).

%% @doc via callback.
-spec unregister_name(term()) -> ok.
unregister_name(Name) ->
    gen_server:call(?MODULE, {unregister, Name}, infinity).

%% @doc via callback: resolve Name to a live pid or `undefined'.
-spec whereis_name(term()) -> pid() | undefined.
whereis_name(Name) ->
    case ets:lookup(?TAB, Name) of
        [{_, Pid}] ->
            case is_process_alive(Pid) of
                true -> Pid;
                false -> undefined
            end;
        [] ->
            undefined
    end.

%% @doc via callback.
-spec send(term(), term()) -> pid().
send(Name, Msg) ->
    case whereis_name(Name) of
        undefined -> exit({badarg, {Name, Msg}});
        Pid -> Pid ! Msg, Pid
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ?TAB = ets:new(?TAB, [named_table, public, set,
                          {read_concurrency, true}]),
    %% state: monitor ref -> registered name
    {ok, #{}}.

handle_call({register, Name, Pid}, _From, Mons) ->
    case whereis_name(Name) of
        undefined ->
            Ref = erlang:monitor(process, Pid),
            ets:insert(?TAB, {Name, Pid}),
            {reply, yes, Mons#{Ref => Name}};
        _Live ->
            {reply, no, Mons}
    end;
handle_call({unregister, Name}, _From, Mons) ->
    ets:delete(?TAB, Name),
    %% Drop the monitor too, or a process that registers and unregisters
    %% many names accumulates live monitors until it finally dies.
    Mons1 = case [R || {R, N} <- maps:to_list(Mons), N =:= Name] of
        [Ref | _] ->
            erlang:demonitor(Ref, [flush]),
            maps:remove(Ref, Mons);
        [] ->
            Mons
    end,
    {reply, ok, Mons1}.

handle_cast(_Msg, Mons) ->
    {noreply, Mons}.

handle_info({'DOWN', Ref, process, Pid, _Reason}, Mons) ->
    case maps:take(Ref, Mons) of
        {Name, Rest} ->
            %% only clear if the name still maps to the dead pid (a
            %% re-registration may have raced the DOWN)
            case ets:lookup(?TAB, Name) of
                [{Name, Pid}] -> ets:delete(?TAB, Name);
                _ -> ok
            end,
            {noreply, Rest};
        error ->
            {noreply, Mons}
    end;
handle_info(_Info, Mons) ->
    {noreply, Mons}.
