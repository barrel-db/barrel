%%%-------------------------------------------------------------------
%%% @doc Async cross-encoder reranking server with request multiplexing
%%%
%%% A gen_server that manages communication with a Python cross-encoder
%%% server. Supports concurrent requests by tracking request IDs and
%%% routing responses back to the correct callers.
%%%
%%% Cross-encoders score query-document pairs directly, providing more
%%% accurate relevance scores than bi-encoder similarity for reranking
%%% candidate results.
%%%
%%% == Requirements ==
%%% ```
%%% pip install transformers torch
%%% '''
%%%
%%% == Usage ==
%%% ```
%%% %% Start the reranker
%%% {ok, Server} = barrel_rerank:start_link(#{}).
%%%
%%% %% Make concurrent rerank requests
%%% spawn(fun() -> barrel_rerank:rerank(Server, Query1, Docs1) end).
%%% spawn(fun() -> barrel_rerank:rerank(Server, Query2, Docs2) end).
%%%
%%% %% Stop when done
%%% barrel_rerank:stop(Server).
%%% '''
%%%
%%% == Request Multiplexing ==
%%% Multiple Erlang processes can call rerank/3,4 concurrently.
%%% Each request is assigned a unique ID and sent to Python. The
%%% Python server includes the ID in its response, allowing this
%%% gen_server to route responses to the correct waiting callers.
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     python => "python3",                               %% Python executable (fallback)
%%%     model => "cross-encoder/ms-marco-MiniLM-L-6-v2",   %% Model name
%%%     timeout => 120000                                  %% Timeout in ms
%%% }.
%%% '''
%%%
%%% == Managed Venv ==
%%% The rerank server uses the managed venv from barrel_rerank_venv.
%%% Dependencies (transformers, torch, uvloop) are auto-installed
%%% when the application starts.
%%%
%%% == Supported Models ==
%%% - `"cross-encoder/ms-marco-MiniLM-L-6-v2"' - Default, fast, good quality
%%% - `"cross-encoder/ms-marco-MiniLM-L-12-v2"' - Better quality, slower
%%% - `"BAAI/bge-reranker-base"' - Good quality
%%% - `"BAAI/bge-reranker-large"' - Best quality, slowest
%%%
%%% == Integration with Search ==
%%% Typical two-stage retrieval:
%%% ```
%%% %% Stage 1: Fast vector search (top 100)
%%% {ok, Candidates} = barrel_vectordb:search(Store, Query, #{k => 100}),
%%%
%%% %% Stage 2: Rerank top candidates
%%% Docs = [maps:get(text, C) || C <- Candidates],
%%% {ok, Ranked} = barrel_rerank:rerank(Server, Query, Docs),
%%%
%%% %% Get top 10 after reranking
%%% Top10Indices = [Idx || {Idx, _Score} <- lists:sublist(Ranked, 10)],
%%% Top10 = [lists:nth(Idx + 1, Candidates) || Idx <- Top10Indices].
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rerank).
-behaviour(gen_server).

%% API
-export([
    start_link/1,
    rerank/3,
    rerank/4,
    info/1,
    info/2,
    available/1,
    stop/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
%% exposed for unit tests: the pure decoder of the python sidecar protocol
-export([decode_response/1]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "cross-encoder/ms-marco-MiniLM-L-6-v2").
-define(DEFAULT_TIMEOUT, 120000).

-record(state, {
    port :: port(),
    pending = #{} :: #{integer() => {pid(), reference()}},
    next_id = 1 :: integer(),
    timeout :: timeout(),
    buffer = <<>> :: binary(),
    model :: binary() | undefined
}).

-type rerank_result() :: {Index :: non_neg_integer(), Score :: float()}.
-export_type([rerank_result/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the rerank server.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

%% @doc Rerank documents by relevance to query.
%% Returns list of {Index, Score} tuples sorted by score descending.
-spec rerank(pid(), binary(), [binary()]) ->
    {ok, [rerank_result()]} | {error, term()}.
rerank(Server, Query, Documents) ->
    rerank(Server, Query, Documents, #{}).

%% @doc Rerank documents with options.
%% Options:
%%   - top_k: Limit number of results returned
%%   - timeout: Override default timeout
-spec rerank(pid(), binary(), [binary()], map()) ->
    {ok, [rerank_result()]} | {error, term()}.
rerank(Server, Query, Documents, Options) ->
    Timeout = maps:get(timeout, Options, ?DEFAULT_TIMEOUT),
    gen_server:call(Server, {rerank, Query, Documents, Options}, Timeout).

%% @doc Get model info.
-spec info(pid()) -> {ok, map()} | {error, term()}.
info(Server) ->
    info(Server, ?DEFAULT_TIMEOUT).

%% @doc Get model info with timeout.
-spec info(pid(), timeout()) -> {ok, map()} | {error, term()}.
info(Server, Timeout) ->
    gen_server:call(Server, info, Timeout).

%% @doc Check if reranker is available.
-spec available(pid()) -> boolean().
available(Server) ->
    is_process_alive(Server).

%% @doc Stop the reranker and close the port.
-spec stop(pid()) -> ok.
stop(Server) ->
    gen_server:stop(Server).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Use managed venv if available
    Venv = case application:get_env(barrel_rerank, managed_venv_path) of
        {ok, Path} -> Path;
        undefined -> undefined
    end,

    case find_rerank_script() of
        {ok, Script} ->
            PortOpts = [
                {args, ["-u", Script, Model]},
                {line, 10000000},
                binary,
                use_stdio,
                exit_status,
                {env, build_env(Venv)}
            ],
            PythonExe = get_python_exe(Python, Venv),
            try
                Port = open_port({spawn_executable, PythonExe}, PortOpts),
                State = #state{port = Port, timeout = Timeout},
                %% Get info to verify server started
                Self = self(),
                From = {Self, make_ref()},
                case send_request(info, #{}, From, State) of
                    {noreply, State1} ->
                        %% Wait for the info response
                        receive
                            {Port, {data, {eol, Line}}} ->
                                case json:decode(Line) of
                                    #{<<"ok">> := true, <<"model">> := ModelName} ->
                                        %% Clear pending since we handled inline
                                        {ok, State1#state{model = ModelName, pending = #{}}};
                                    #{<<"ok">> := false, <<"error">> := Err} ->
                                        port_close(Port),
                                        {stop, {python_error, Err}}
                                end
                        after Timeout ->
                            port_close(Port),
                            {stop, timeout}
                        end
                end
            catch
                error:PortReason ->
                    {stop, {port_open_failed, PortReason}}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({rerank, Query, Documents, Options}, From, State) ->
    Request = #{query => Query, documents => Documents},
    Request1 = case maps:get(top_k, Options, undefined) of
        undefined -> Request;
        TopK -> Request#{top_k => TopK}
    end,
    send_request(rerank, Request1, From, State);

handle_call(info, From, State) ->
    send_request(info, #{}, From, State).

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Port, {data, {eol, Line}}}, #state{port = Port, buffer = Buffer} = State) ->
    %% Combine any buffered partial line with this line
    FullLine = <<Buffer/binary, Line/binary>>,
    handle_response(FullLine, State#state{buffer = <<>>});

handle_info({Port, {data, {noeol, Chunk}}}, #state{port = Port, buffer = Buffer} = State) ->
    %% Accumulate partial data
    {noreply, State#state{buffer = <<Buffer/binary, Chunk/binary>>}};

handle_info({Port, {exit_status, Status}}, #state{port = Port, pending = Pending} = State) ->
    %% Fail all pending requests
    maps:foreach(fun(_, From) ->
        gen_server:reply(From, {error, {port_exited, Status}})
    end, Pending),
    {stop, {port_exited, Status}, State#state{pending = #{}}};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{port = Port}) ->
    catch port_close(Port),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
send_request(Action, Params, From, #state{port = Port, pending = Pending, next_id = Id} = State) ->
    Request = Params#{id => Id, action => Action},
    Json = json:encode(Request),
    port_command(Port, [Json, "\n"]),
    NewPending = Pending#{Id => From},
    {noreply, State#state{pending = NewPending, next_id = Id + 1}}.

%% @private
handle_response(Line, #state{pending = Pending} = State) ->
    try
        Response = json:decode(Line),
        case maps:get(<<"id">>, Response, undefined) of
            undefined ->
                %% No ID in response, ignore
                {noreply, State};
            Id ->
                case maps:take(Id, Pending) of
                    {From, NewPending} ->
                        Result = decode_response(Response),
                        gen_server:reply(From, Result),
                        {noreply, State#state{pending = NewPending}};
                    error ->
                        %% Unknown ID, ignore
                        {noreply, State}
                end
        end
    catch
        _:_ ->
            %% JSON decode failed, ignore
            {noreply, State}
    end.

%% @private
decode_response(#{<<"ok">> := true, <<"results">> := Results}) ->
    {ok, [{maps:get(<<"index">>, R), maps:get(<<"score">>, R)} || R <- Results]};
decode_response(#{<<"ok">> := true, <<"model">> := Model, <<"type">> := Type}) ->
    {ok, #{model => Model, type => Type}};
decode_response(#{<<"ok">> := true} = R) ->
    {ok, R};
decode_response(#{<<"ok">> := false, <<"error">> := Err}) ->
    {error, {python_error, Err}};
decode_response(_) ->
    {error, invalid_response}.

%% @private
find_rerank_script() ->
    case code:priv_dir(barrel_rerank) of
        {error, bad_name} ->
            case filelib:is_file("priv/rerank_server.py") of
                true -> {ok, "priv/rerank_server.py"};
                false ->
                    case filelib:is_file("../priv/rerank_server.py") of
                        true -> {ok, "../priv/rerank_server.py"};
                        false -> {error, script_not_found}
                    end
            end;
        PrivDir ->
            Script = filename:join(PrivDir, "rerank_server.py"),
            case filelib:is_file(Script) of
                true -> {ok, Script};
                false -> {error, script_not_found}
            end
    end.

%% @private Build environment variables for Python port
build_env(undefined) ->
    [];
build_env(Venv) ->
    VenvBin = venv_bin_dir(Venv),
    CurrentPath = os:getenv("PATH", ""),
    [
        {"VIRTUAL_ENV", Venv},
        {"PATH", VenvBin ++ ":" ++ CurrentPath}
    ].

%% @private Get Python executable path
get_python_exe(Python, undefined) ->
    Python;
get_python_exe(_Python, Venv) ->
    filename:join(venv_bin_dir(Venv), "python").

%% @private Get venv bin directory (cross-platform)
venv_bin_dir(Venv) ->
    case os:type() of
        {win32, _} -> filename:join(Venv, "Scripts");
        _ -> filename:join(Venv, "bin")
    end.
