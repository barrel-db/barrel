%% @doc Concurrency caps for guest jobs, per node and per database.
%%
%% A view indexer takes a slot before it runs a batch of guest maps and
%% returns it afterwards. When either cap is reached the call answers
%% `busy' at once and the indexer backs off; it never blocks here. A slot
%% held by a process that exits is returned.
-module(barrel_design_slots).
-behaviour(gen_server).

-export([start_link/0, try_acquire/1, release/1, info/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec try_acquire(binary()) -> ok | busy.
try_acquire(Db) ->
    gen_server:call(?MODULE, {acquire, Db, self()}).

-spec release(binary()) -> ok.
release(Db) ->
    gen_server:call(?MODULE, {release, Db, self()}).

-spec info() -> #{node := non_neg_integer(), node_max := pos_integer(),
                  db_max := pos_integer(), dbs := #{binary() => pos_integer()}}.
info() ->
    gen_server:call(?MODULE, info).

init([]) ->
    NodeMax = case application:get_env(barrel_design, node_jobs, 0) of
        N when is_integer(N), N > 0 -> N;
        _ -> erlang:system_info(schedulers_online)
    end,
    DbMax = case application:get_env(barrel_design, db_jobs, 2) of
        D when is_integer(D), D > 0 -> D;
        _ -> 2
    end,
    {ok, #{node_max => NodeMax, db_max => DbMax, held => #{}, used => 0, dbs => #{}}}.

handle_call({acquire, Db, Pid}, _From,
            #{node_max := NodeMax, db_max := DbMax, used := Used, dbs := Dbs,
              held := Held} = State) ->
    InDb = maps:get(Db, Dbs, 0),
    case Used < NodeMax andalso InDb < DbMax of
        true ->
            Ref = erlang:monitor(process, Pid),
            {reply, ok, State#{used := Used + 1, dbs := Dbs#{Db => InDb + 1},
                               held := Held#{Ref => {Pid, Db}}}};
        false ->
            {reply, busy, State}
    end;
handle_call({release, Db, Pid}, _From, #{held := Held} = State) ->
    case [R || {R, {P, D}} <- maps:to_list(Held), P =:= Pid, D =:= Db] of
        [Ref | _] ->
            erlang:demonitor(Ref, [flush]),
            {reply, ok, free(Ref, Db, State)};
        [] ->
            {reply, ok, State}
    end;
handle_call(info, _From, #{used := Used, node_max := NodeMax, db_max := DbMax,
                           dbs := Dbs} = State) ->
    {reply, #{node => Used, node_max => NodeMax, db_max => DbMax, dbs => Dbs}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, _}, #{held := Held} = State) ->
    case maps:find(Ref, Held) of
        {ok, {_, Db}} -> {noreply, free(Ref, Db, State)};
        error -> {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

free(Ref, Db, #{held := Held, used := Used, dbs := Dbs} = State) ->
    Dbs1 = case maps:get(Db, Dbs, 0) of
        N when N > 1 -> Dbs#{Db => N - 1};
        _ -> maps:remove(Db, Dbs)
    end,
    State#{held := maps:remove(Ref, Held), used := Used - 1, dbs := Dbs1}.
