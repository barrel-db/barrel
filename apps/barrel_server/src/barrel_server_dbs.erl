%%%-------------------------------------------------------------------
%%% @doc Database lifecycle manager.
%%%
%%% Owns the open {@link barrel} database handles for the server. A barrel
%%% database links its vector store to the process that opened it, so a
%%% long-lived owner is required: HTTP handlers ask this manager for a handle
%%% rather than opening databases themselves.
%%%
%%% Handles are opened lazily on first use and cached by name. The manager does
%%% not trap exits: if an owned store crashes, the manager crashes with it and is
%%% restarted by its supervisor with an empty cache, and databases are reopened
%%% on the next request.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_dbs).
-behaviour(gen_server).

-export([start_link/0, ensure/1, close/1, branch/3, destroy/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(MAX_NAME_LEN, 128).

-record(state, {dbs = #{} :: #{binary() => barrel:db()}}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Return the (possibly newly opened) handle for `Name'.
-spec ensure(binary()) -> {ok, barrel:db()} | {error, term()}.
ensure(Name) when is_binary(Name) ->
    gen_server:call(?SERVER, {ensure, Name}, infinity).

%% @doc Close and forget the database `Name'. Idempotent.
-spec close(binary()) -> ok.
close(Name) when is_binary(Name) ->
    gen_server:call(?SERVER, {close, Name}, infinity).

%% @doc Fork `Parent' into `BranchName' and own the branch handle.
%% Runs inside the manager because a barrel database links its vector
%% store to the process that opens it. Opts: at => now | HlcT.
-spec branch(binary(), binary(), map()) ->
    {ok, barrel:db()} | {error, term()}.
branch(Parent, BranchName, Opts) when is_binary(Parent),
                                      is_binary(BranchName),
                                      is_map(Opts) ->
    gen_server:call(?SERVER, {branch, Parent, BranchName, Opts},
                    infinity).

%% @doc Destroy the database `Name': close it and delete its files
%% (docdb and vector store).
-spec destroy(binary()) -> ok | {error, term()}.
destroy(Name) when is_binary(Name) ->
    gen_server:call(?SERVER, {destroy, Name}, infinity).

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    case application:get_env(barrel_server, data_dir) of
        {ok, Dir} -> application:set_env(barrel_docdb, data_dir, Dir);
        undefined -> ok
    end,
    {ok, #state{}}.

handle_call({ensure, Name}, _From, State) ->
    case do_ensure(Name, State) of
        {ok, Db, State1} -> {reply, {ok, Db}, State1};
        {error, Reason, State1} -> {reply, {error, Reason}, State1}
    end;
handle_call({branch, Parent, BranchName, Opts}, _From, State) ->
    case valid_name(BranchName) of
        false ->
            {reply, {error, invalid_name}, State};
        true ->
            case do_ensure(Parent, State) of
                {ok, ParentDb, State1} ->
                    case barrel:branch(ParentDb, BranchName, Opts) of
                        {ok, BranchDb} ->
                            Dbs = maps:put(BranchName, BranchDb,
                                           State1#state.dbs),
                            {reply, {ok, BranchDb},
                             State1#state{dbs = Dbs}};
                        {error, _} = Err ->
                            {reply, Err, State1}
                    end;
                {error, Reason, State1} ->
                    {reply, {error, Reason}, State1}
            end
    end;
handle_call({destroy, Name}, _From, State) ->
    case do_ensure(Name, State) of
        {ok, Db, State1} ->
            Dbs = maps:remove(Name, State1#state.dbs),
            Result = try barrel:delete(Db)
                     catch _:Reason -> {error, Reason}
                     end,
            {reply, Result, State1#state{dbs = Dbs}};
        {error, Reason, State1} ->
            {reply, {error, Reason}, State1}
    end;
handle_call({close, Name}, _From, State) ->
    case maps:take(Name, State#state.dbs) of
        {Db, Dbs} ->
            _ = barrel:close(Db),
            {reply, ok, State#state{dbs = Dbs}};
        error ->
            {reply, ok, State}
    end;
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Close = fun(_Name, Db) ->
        try barrel:close(Db) catch _:_ -> ok end
    end,
    maps:foreach(Close, State#state.dbs),
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_ensure(Name, State) ->
    case valid_name(Name) of
        false ->
            {error, invalid_name, State};
        true ->
            case maps:find(Name, State#state.dbs) of
                {ok, Db} ->
                    {ok, Db, State};
                error ->
                    %% Default open options from the barrel_server app env
                    %% (e.g. a server-wide embedding policy or vectordb
                    %% config for record-mode databases).
                    OpenOpts = application:get_env(barrel_server,
                                                   open_opts, #{}),
                    case barrel:open(Name, OpenOpts) of
                        {ok, Db} ->
                            Dbs = maps:put(Name, Db, State#state.dbs),
                            {ok, Db, State#state{dbs = Dbs}};
                        {error, Reason} ->
                            {error, Reason, State}
                    end
            end
    end.

%% @private Accept only short, filesystem- and atom-safe names.
-spec valid_name(binary()) -> boolean().
valid_name(Name) ->
    Size = byte_size(Name),
    Size >= 1 andalso Size =< ?MAX_NAME_LEN
        andalso match =:= re:run(Name, "^[A-Za-z0-9_-]+$", [{capture, none}]).
