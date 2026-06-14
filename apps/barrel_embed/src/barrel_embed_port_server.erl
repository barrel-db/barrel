%%%-------------------------------------------------------------------
%%% @doc Async Python port server with request multiplexing
%%%
%%% A gen_server that manages communication with a Python embedding
%%% server. Supports concurrent requests by tracking request IDs and
%%% routing responses back to the correct callers.
%%%
%%% == Usage ==
%%% ```
%%% %% Start the server
%%% {ok, Server} = barrel_embed_port_server:start_link(Python, Args, Opts).
%%%
%%% %% Make concurrent embedding requests
%%% spawn(fun() -> barrel_embed_port_server:embed_batch(Server, Texts1, 60000) end).
%%% spawn(fun() -> barrel_embed_port_server:embed_batch(Server, Texts2, 60000) end).
%%%
%%% %% Stop when done
%%% barrel_embed_port_server:stop(Server).
%%% '''
%%%
%%% == Request Multiplexing ==
%%% Multiple Erlang processes can call embed_batch/3 concurrently.
%%% Each request is assigned a unique ID and sent to Python. The
%%% Python server includes the ID in its response, allowing this
%%% gen_server to route responses to the correct waiting callers.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_port_server).
-behaviour(gen_server).

-export([start_link/3, embed_batch/3, info/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Image embedding support
-export([embed_image_batch/3]).

%% Sparse/multi-vector support
-export([embed_sparse_batch/3, embed_multi_batch/3]).

-record(state, {
    port :: port(),
    pending = #{} :: #{integer() => {pid(), reference()}},
    next_id = 1 :: integer(),
    timeout :: timeout(),
    buffer = <<>> :: binary()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the port server.
%% Args are passed to python -m barrel_embed.
-spec start_link(string(), [string()], proplists:proplist()) ->
    {ok, pid()} | {error, term()}.
start_link(Python, Args, Opts) ->
    gen_server:start_link(?MODULE, {Python, Args, Opts}, []).

%% @doc Generate embeddings for texts.
-spec embed_batch(pid(), [binary()], timeout()) ->
    {ok, [[float()]]} | {error, term()}.
embed_batch(Server, Texts, Timeout) ->
    gen_server:call(Server, {embed_batch, Texts}, Timeout).

%% @doc Generate embeddings for images (base64 encoded).
-spec embed_image_batch(pid(), [binary()], timeout()) ->
    {ok, [[float()]]} | {error, term()}.
embed_image_batch(Server, Images, Timeout) ->
    gen_server:call(Server, {embed_image_batch, Images}, Timeout).

%% @doc Generate sparse embeddings.
-spec embed_sparse_batch(pid(), [binary()], timeout()) ->
    {ok, [map()]} | {error, term()}.
embed_sparse_batch(Server, Texts, Timeout) ->
    gen_server:call(Server, {embed_sparse_batch, Texts}, Timeout).

%% @doc Generate multi-vector embeddings.
-spec embed_multi_batch(pid(), [binary()], timeout()) ->
    {ok, [[[float()]]]} | {error, term()}.
embed_multi_batch(Server, Texts, Timeout) ->
    gen_server:call(Server, {embed_multi_batch, Texts}, Timeout).

%% @doc Get model info.
-spec info(pid(), timeout()) -> {ok, map()} | {error, term()}.
info(Server, Timeout) ->
    gen_server:call(Server, info, Timeout).

%% @doc Stop the server and close the port.
-spec stop(pid()) -> ok.
stop(Server) ->
    gen_server:stop(Server).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({Python, Args, Opts}) ->
    Timeout = proplists:get_value(timeout, Opts, 120000),
    PrivDir = proplists:get_value(priv_dir, Opts, get_priv_dir()),
    Venv = proplists:get_value(venv, Opts, undefined),

    %% Build environment - activate venv if specified
    Env = build_env(PrivDir, Venv),

    %% Get Python executable - use venv's python if specified
    PythonExe = get_python_exe(Python, Venv),

    PortOpts = [
        {args, Args},
        {env, Env},
        {line, 10000000},  %% Large buffer for embeddings
        binary,
        use_stdio,
        exit_status
    ],

    try
        Port = open_port({spawn_executable, PythonExe}, PortOpts),
        {ok, #state{port = Port, timeout = Timeout}}
    catch
        error:Reason ->
            {stop, {port_open_failed, Reason}}
    end.

handle_call({embed_batch, Texts}, From, State) ->
    send_request(embed, #{texts => Texts}, From, State);

handle_call({embed_image_batch, Images}, From, State) ->
    send_request(embed_image, #{images => Images}, From, State);

handle_call({embed_sparse_batch, Texts}, From, State) ->
    send_request(embed, #{texts => Texts}, From, State);

handle_call({embed_multi_batch, Texts}, From, State) ->
    send_request(embed, #{texts => Texts}, From, State);

handle_call(info, From, State) ->
    send_request(info, #{}, From, State).

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Port, {data, {eol, Line}}}, #state{port = Port, buffer = Buffer} = State) ->
    %% Combine any buffered partial line with this line
    FullLine = <<Buffer/binary, Line/binary>>,
    handle_response(FullLine, State#state{buffer = <<>>});

handle_info({Port, {data, {noeol, Partial}}}, #state{port = Port, buffer = Buffer} = State) ->
    %% Accumulate partial data
    {noreply, State#state{buffer = <<Buffer/binary, Partial/binary>>}};

handle_info({Port, {exit_status, Status}}, #state{port = Port, pending = Pending} = State) ->
    %% Fail all pending requests
    maps:foreach(fun(_, From) ->
        gen_server:reply(From, {error, {port_exited, Status}})
    end, Pending),
    {stop, {port_exited, Status}, State#state{pending = #{}}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{port = Port}) ->
    catch port_close(Port),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

send_request(Action, Params, From, #state{port = Port, pending = Pending, next_id = Id} = State) ->
    Request = Params#{id => Id, action => Action},
    Json = json:encode(Request),
    port_command(Port, [Json, "\n"]),
    NewPending = Pending#{Id => From},
    {noreply, State#state{pending = NewPending, next_id = Id + 1}}.

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

decode_response(#{<<"ok">> := true, <<"embeddings">> := Embeddings}) ->
    {ok, Embeddings};
decode_response(#{<<"ok">> := true, <<"dimensions">> := Dims, <<"model">> := Model} = Response) ->
    {ok, #{
        dimensions => Dims,
        model => Model,
        backend => maps:get(<<"backend">>, Response, undefined),
        type => maps:get(<<"type">>, Response, undefined),
        vocab_size => maps:get(<<"vocab_size">>, Response, undefined)
    }};
decode_response(#{<<"ok">> := true} = Response) ->
    {ok, Response};
decode_response(#{<<"ok">> := false, <<"error">> := Err}) ->
    {error, {python_error, Err}};
decode_response(_) ->
    {error, invalid_response}.

get_priv_dir() ->
    case code:priv_dir(barrel_embed) of
        {error, bad_name} -> "priv";
        Dir -> Dir
    end.

%% @private
%% Build environment variables for port.
%% When venv is specified, set up environment to activate it.
build_env(PrivDir, undefined) ->
    [{"PYTHONPATH", PrivDir}];
build_env(PrivDir, Venv) ->
    VenvBin = venv_bin_dir(Venv),
    CurrentPath = os:getenv("PATH", ""),
    [
        {"VIRTUAL_ENV", Venv},
        {"PATH", VenvBin ++ ":" ++ CurrentPath},
        {"PYTHONPATH", PrivDir}
        %% Note: PYTHONHOME must NOT be set for venv to work
    ].

%% @private
%% Get Python executable path.
%% When venv is specified, use the venv's python.
get_python_exe(Python, undefined) ->
    Python;
get_python_exe(_Python, Venv) ->
    %% When venv specified, always use its python
    filename:join(venv_bin_dir(Venv), "python").

%% @private
%% Get the bin directory path for a venv (cross-platform).
venv_bin_dir(Venv) ->
    case os:type() of
        {win32, _} -> filename:join(Venv, "Scripts");
        _ -> filename:join(Venv, "bin")
    end.
