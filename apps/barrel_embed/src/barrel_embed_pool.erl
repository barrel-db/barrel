%%%-------------------------------------------------------------------
%%% @doc One model process per model, shared by every database that uses
%%% it. A provider acquires its process by key; the pool starts it under
%%% barrel_embed_pool_sup on first use, counts references, and stops it
%%% when the last holder releases it. A process that dies is forgotten;
%%% the next acquire starts a new one.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_pool).

-behaviour(gen_server).

-export([start_link/0, acquire/2, release/1, port_server/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {by_key = #{} :: #{term() => pid()},
                by_pid = #{} :: #{pid() => {term(), pos_integer(), reference()}}}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc The process for `Key', started with `MFA' when none runs.
-spec acquire(term(), {module(), atom(), [term()]}) ->
    {ok, pid()} | {error, term()}.
acquire(Key, MFA) ->
    gen_server:call(?MODULE, {acquire, Key, MFA}, infinity).

%% @doc Drop one reference; the process stops with the last one. A pid
%% the pool does not hold is ignored.
-spec release(pid()) -> ok.
release(Pid) when is_pid(Pid) ->
    gen_server:call(?MODULE, {release, Pid}, infinity).

%% @doc The Python port server for this command line, shared.
-spec port_server(string(), [string()], proplists:proplist()) ->
    {ok, pid()} | {error, term()}.
port_server(Python, Args, Opts) ->
    acquire({port_server, Python, Args, proplists:get_value(venv, Opts)},
            {barrel_embed_port_server, start_link, [Python, Args, Opts]}).

init([]) ->
    {ok, #state{}}.

handle_call({acquire, Key, MFA}, _From, #state{by_key = ByKey} = State) ->
    case ByKey of
        #{Key := Pid} -> reuse(is_process_alive(Pid), Pid, Key, MFA, State);
        _ -> start(Key, MFA, State)
    end;
handle_call({release, Pid}, _From, #state{by_pid = ByPid} = State) ->
    case ByPid of
        #{Pid := {Key, 1, MRef}} ->
            demonitor(MRef, [flush]),
            _ = supervisor:terminate_child(barrel_embed_pool_sup, Pid),
            {reply, ok, forget(Pid, Key, State)};
        #{Pid := {Key, N, MRef}} ->
            {reply, ok, State#state{by_pid = ByPid#{Pid => {Key, N - 1, MRef}}}};
        _ ->
            {reply, ok, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            #state{by_pid = ByPid} = State) ->
    case ByPid of
        #{Pid := {Key, _N, _MRef}} -> {noreply, forget(Pid, Key, State)};
        _ -> {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%% A process that died before its 'DOWN' reached the pool is replaced.
reuse(true, Pid, _Key, _MFA, State) ->
    {reply, {ok, Pid}, ref(Pid, State)};
reuse(false, Pid, Key, MFA, #state{by_pid = ByPid} = State) ->
    #{Pid := {Key, _N, MRef}} = ByPid,
    demonitor(MRef, [flush]),
    start(Key, MFA, forget(Pid, Key, State)).

start(Key, MFA, #state{by_key = ByKey, by_pid = ByPid} = State) ->
    case supervisor:start_child(barrel_embed_pool_sup, [MFA]) of
        {ok, Pid} ->
            MRef = monitor(process, Pid),
            {reply, {ok, Pid}, State#state{by_key = ByKey#{Key => Pid},
                                           by_pid = ByPid#{Pid => {Key, 1, MRef}}}};
        {error, _} = Err ->
            {reply, Err, State}
    end.

ref(Pid, #state{by_pid = ByPid} = State) ->
    #{Pid := {Key, N, MRef}} = ByPid,
    State#state{by_pid = ByPid#{Pid => {Key, N + 1, MRef}}}.

forget(Pid, Key, #state{by_key = ByKey, by_pid = ByPid} = State) ->
    State#state{by_key = maps:remove(Key, ByKey), by_pid = maps:remove(Pid, ByPid)}.
