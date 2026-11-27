%% @doc Registry of the per-database processes.
%%
%% Owns the public table readers use to find a database's store and
%% indexers. At startup it starts one process for each database that has
%% a directory under the data_dir, so views keep following their
%% databases after a restart without anyone querying them.
-module(barrel_design_dbs).
-behaviour(gen_server).

-export([start_link/0, ensure/2, lookup/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TABLE, barrel_design_dbs).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc The process for Db, started if needed. Mode `install' keeps it
%% running when the database has no design yet.
-spec ensure(binary(), install | restore) -> {ok, pid()} | {error, term()}.
ensure(Db, Mode) ->
    gen_server:call(?MODULE, {ensure, Db, Mode}, 60000).

%% @doc What the database's process published: its store, instance,
%% indexers and designs.
-spec lookup(binary()) -> {ok, map()} | {error, not_found}.
lookup(Db) ->
    case ets:lookup(?TABLE, Db) of
        [{_, Entry}] -> {ok, Entry};
        [] -> {error, not_found}
    end.

-spec stop(binary()) -> ok.
stop(Db) ->
    gen_server:call(?MODULE, {stop, Db}, 60000).

init([]) ->
    process_flag(trap_exit, true),
    ?TABLE = ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
    self() ! restore,
    {ok, #{pids => #{}}}.

handle_call({ensure, Db, Mode}, _From, #{pids := Pids} = S) ->
    case maps:find(Db, Pids) of
        {ok, Pid} -> {reply, {ok, Pid}, S};
        error ->
            {ok, Pid} = barrel_design_db:start_link(Db, Mode),
            {reply, {ok, Pid}, S#{pids := Pids#{Db => Pid}}}
    end;
handle_call({stop, Db}, _From, #{pids := Pids} = S) ->
    case maps:take(Db, Pids) of
        {Pid, Rest} ->
            ok = gen_server:stop(Pid),
            {reply, ok, S#{pids := Rest}};
        error ->
            {reply, ok, S}
    end.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(restore, #{pids := Pids} = S) ->
    Root = barrel_design_store:root(),
    Names = case file:list_dir(Root) of
        {ok, Ns} -> [N || N <- Ns, filelib:is_dir(filename:join(Root, N))];
        {error, _} -> []
    end,
    Pids1 = lists:foldl(fun(N, Acc) ->
        Db = barrel_design_store:db_name(N),
        case maps:is_key(Db, Acc) of
            true -> Acc;
            false ->
                {ok, Pid} = barrel_design_db:start_link(Db, restore),
                Acc#{Db => Pid}
        end
    end, Pids, Names),
    {noreply, S#{pids := Pids1}};
handle_info({'EXIT', Pid, _Reason}, #{pids := Pids} = S) ->
    Pids1 = maps:filter(fun(Db, P) ->
        case P of
            Pid -> ets:delete(?TABLE, Db), false;
            _ -> true
        end
    end, Pids),
    {noreply, S#{pids := Pids1}};
handle_info(_Info, S) ->
    {noreply, S}.

%% Stop every database process and wait: each closes its store, which a
%% restart must be able to open again.
terminate(_Reason, #{pids := Pids}) ->
    Refs = [{P, erlang:monitor(process, P)} || P <- maps:values(Pids)],
    [exit(P, shutdown) || {P, _} <- Refs],
    [receive {'DOWN', R, process, P, _} -> ok
     after 30000 -> exit(P, kill)
     end || {P, R} <- Refs],
    ok.
