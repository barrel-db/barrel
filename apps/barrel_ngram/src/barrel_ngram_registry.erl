%%%-------------------------------------------------------------------
%%% @doc Local name registry for ngram shards.
%%%
%%% Corpus and shard names are arbitrary terms (`{shard, Corpus}'), so a
%%% dynamically named shard never grows the atom table the way
%%% `{local, Atom}' registration would. Implements the `{via, Module,
%%% Name}' callbacks over a public ETS table; the owner monitors
%%% registered pids and clears dead entries so a crashed shard never
%%% leaves a stale registration behind.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_registry).
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
%% start shards without the barrel_ngram application (eunit, embedded
%% use); under the application the supervisor owns it.
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
