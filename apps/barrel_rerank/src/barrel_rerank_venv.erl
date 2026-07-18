%%%-------------------------------------------------------------------
%%% @doc Managed Python virtual environment for barrel_rerank
%%%
%%% Creates and manages a Python venv with all dependencies required
%%% for the rerank server (transformers, torch, uvloop).
%%%
%%% == Usage ==
%%% ```
%%% %% Ensure venv exists and has dependencies
%%% {ok, VenvPath} = barrel_rerank_venv:ensure_venv().
%%%
%%% %% Check if venv is valid
%%% true = barrel_rerank_venv:is_valid().
%%%
%%% %% Check for uvloop support
%%% true = barrel_rerank_venv:has_uvloop().
%%%
%%% %% Force refresh (reinstall deps)
%%% ok = barrel_rerank_venv:refresh().
%%% '''
%%%
%%% == Configuration ==
%%% The venv path can be configured via application env:
%%% ```
%%% {barrel_rerank, [{venv_path, "/custom/path/.venv"}]}
%%% '''
%%%
%%% Default path is "priv/.venv" relative to the application.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rerank_venv).

-export([
    venv_path/0,
    ensure_venv/0,
    create_venv/0,
    install_deps/0,
    is_valid/0,
    has_uvloop/0,
    refresh/0
]).

%% Required Python packages
-define(BASE_DEPS, ["uvloop"]).
-define(RERANK_DEPS, ["transformers", "torch"]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get the venv path (configured or default).
-spec venv_path() -> string().
venv_path() ->
    case application:get_env(barrel_rerank, venv_path) of
        {ok, Path} -> Path;
        undefined -> default_venv_path()
    end.

%% @doc Ensure venv exists with all dependencies.
%% Creates venv and installs deps if needed.
-spec ensure_venv() -> {ok, string()} | {error, term()}.
ensure_venv() ->
    Path = venv_path(),
    case is_valid_path(Path) of
        true ->
            %% Venv exists, store path and return
            application:set_env(barrel_rerank, managed_venv_path, Path),
            {ok, Path};
        false ->
            %% Create venv and install deps
            case create_venv() of
                ok ->
                    case install_deps() of
                        ok ->
                            application:set_env(barrel_rerank, managed_venv_path, Path),
                            {ok, Path};
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end
    end.

%% @doc Create a new Python venv.
-spec create_venv() -> ok | {error, term()}.
create_venv() ->
    Path = venv_path(),
    case find_python() of
        {ok, Python} ->
            %% Ensure parent directory exists
            ok = filelib:ensure_dir(filename:join(Path, "dummy")),
            %% Create venv
            Cmd = io_lib:format("~s -m venv ~s", [Python, Path]),
            case run_cmd(lists:flatten(Cmd)) of
                {ok, _} -> ok;
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Install all required dependencies.
-spec install_deps() -> ok | {error, term()}.
install_deps() ->
    Path = venv_path(),
    Pip = pip_exe(Path),

    %% Upgrade pip first
    case run_cmd(Pip ++ " install --upgrade pip") of
        {ok, _} -> ok;
        {error, _} ->
            %% Continue even if pip upgrade fails
            ok
    end,

    %% Install base deps (uvloop - Unix only)
    case os:type() of
        {unix, _} ->
            install_packages(Pip, ?BASE_DEPS);
        _ ->
            ok
    end,

    %% Install rerank deps
    install_packages(Pip, ?RERANK_DEPS).

%% @doc Check if the venv is valid (exists with Python).
-spec is_valid() -> boolean().
is_valid() ->
    is_valid_path(venv_path()).

%% @doc Check if uvloop is installed.
-spec has_uvloop() -> boolean().
has_uvloop() ->
    Path = venv_path(),
    Python = python_exe(Path),
    case run_cmd(Python ++ " -c \"import uvloop\"") of
        {ok, _} -> true;
        {error, _} -> false
    end.

%% @doc Force refresh - reinstall all dependencies.
-spec refresh() -> ok | {error, term()}.
refresh() ->
    install_deps().

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Get default venv path
default_venv_path() ->
    case code:priv_dir(barrel_rerank) of
        {error, bad_name} ->
            %% Development - use local priv dir
            filename:absname("priv/.venv");
        PrivDir ->
            filename:join(PrivDir, ".venv")
    end.

%% @private Check if path has valid venv
is_valid_path(Path) ->
    Python = python_exe(Path),
    filelib:is_file(Python).

%% @private Find system Python
find_python() ->
    Candidates = ["python3", "python"],
    find_first_exe(Candidates).

find_first_exe([]) ->
    {error, python_not_found};
find_first_exe([Cmd | Rest]) ->
    case run_cmd(Cmd ++ " --version") of
        {ok, _} -> {ok, Cmd};
        {error, _} -> find_first_exe(Rest)
    end.

%% @private Get Python executable path
python_exe(VenvPath) ->
    filename:join(venv_bin_dir(VenvPath), "python").

%% @private Get pip executable path
pip_exe(VenvPath) ->
    filename:join(venv_bin_dir(VenvPath), "pip").

%% @private Get venv bin directory
venv_bin_dir(VenvPath) ->
    case os:type() of
        {win32, _} -> filename:join(VenvPath, "Scripts");
        _ -> filename:join(VenvPath, "bin")
    end.

%% @private Install packages using pip
install_packages(_Pip, []) ->
    ok;
install_packages(Pip, Packages) ->
    PackageStr = string:join(Packages, " "),
    Cmd = io_lib:format("~s install ~s", [Pip, PackageStr]),
    case run_cmd(lists:flatten(Cmd)) of
        {ok, _} -> ok;
        {error, _} = Err -> Err
    end.

%% @private Run a shell command
run_cmd(Cmd) ->
    Port = open_port({spawn, Cmd}, [exit_status, stderr_to_stdout, binary]),
    collect_output(Port, []).

collect_output(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            collect_output(Port, [Data | Acc]);
        {Port, {exit_status, 0}} ->
            {ok, iolist_to_binary(lists:reverse(Acc))};
        {Port, {exit_status, Code}} ->
            Output = iolist_to_binary(lists:reverse(Acc)),
            {error, {exit_code, Code, Output}}
    after 600000 ->
        _ = try port_close(Port) catch _:_ -> ok end,
        {error, timeout}
    end.
