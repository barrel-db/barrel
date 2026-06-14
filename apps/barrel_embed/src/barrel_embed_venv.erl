%%%-------------------------------------------------------------------
%%% @doc Managed virtualenv for barrel_embed
%%%
%%% Automatically creates and manages a Python virtualenv with
%%% dependencies required by embedding providers.
%%%
%%% == Venv Location ==
%%% Default: `priv/barrel_embed/.venv'
%%% Configurable via app env: `{barrel_embed, [{venv_dir, "/custom/path"}]}'
%%%
%%% == Provider Dependencies ==
%%% - `fastembed': fastembed (~100MB)
%%% - `local': sentence-transformers (~2GB)
%%% - `splade': transformers, torch
%%% - `colbert': transformers, torch
%%% - `clip': transformers, torch, pillow
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_venv).

-export([
    ensure_venv/0,
    create_venv/0,
    install_deps/1,
    refresh/0,
    venv_path/0,
    is_valid/0,
    has_uvloop/0
]).

-define(DEFAULT_VENV_SUBDIR, ".venv").

%%====================================================================
%% API
%%====================================================================

%% @doc Get the managed venv path.
%% Returns custom path from app env or default priv/.venv
-spec venv_path() -> string().
venv_path() ->
    case application:get_env(barrel_embed, venv_dir) of
        {ok, Path} -> Path;
        undefined -> default_venv_path()
    end.

%% @doc Ensure venv exists and is valid.
%% Creates if missing, returns path on success.
-spec ensure_venv() -> {ok, string()} | {error, term()}.
ensure_venv() ->
    Path = venv_path(),
    case is_valid_venv(Path) of
        true ->
            application:set_env(barrel_embed, managed_venv_path, Path),
            {ok, Path};
        false ->
            case create_venv() of
                {ok, _} = Ok ->
                    application:set_env(barrel_embed, managed_venv_path, Path),
                    Ok;
                Error ->
                    Error
            end
    end.

%% @doc Create the managed venv.
%% Finds Python3 and creates venv at configured path.
-spec create_venv() -> {ok, string()} | {error, term()}.
create_venv() ->
    Path = venv_path(),
    Python = find_python(),
    case Python of
        {ok, PythonExe} ->
            %% Ensure parent directory exists
            ok = filelib:ensure_dir(filename:join(Path, "dummy")),
            %% Create venv
            Cmd = PythonExe ++ " -m venv " ++ Path,
            case run_cmd(Cmd) of
                {ok, _} ->
                    %% Install base deps (uvloop on unix)
                    install_base_deps(Path);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Check if managed venv is valid.
-spec is_valid() -> boolean().
is_valid() ->
    is_valid_venv(venv_path()).

%% @doc Check if uvloop is installed in the managed venv.
-spec has_uvloop() -> boolean().
has_uvloop() ->
    Path = venv_path(),
    case is_valid_venv(Path) of
        true ->
            Python = venv_python(Path),
            Cmd = Python ++ " -c \"import uvloop; print('ok')\"",
            case run_cmd(Cmd) of
                {ok, "ok\n"} -> true;
                {ok, "ok"} -> true;
                _ -> false
            end;
        false ->
            false
    end.

%% @doc Install dependencies for a provider.
%% Automatically installs required packages for the given provider.
-spec install_deps(atom()) -> ok | {error, term()}.
install_deps(Provider) ->
    Path = venv_path(),
    case is_valid_venv(Path) of
        true ->
            Deps = provider_deps(Provider),
            case Deps of
                [] -> ok;
                _ -> pip_install(Path, Deps)
            end;
        false ->
            {error, venv_not_found}
    end.

%% @doc Refresh the managed venv.
%% Deletes and recreates the venv.
-spec refresh() -> {ok, string()} | {error, term()}.
refresh() ->
    Path = venv_path(),
    %% Remove existing venv
    _ = remove_dir(Path),
    %% Create fresh venv
    create_venv().

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
default_venv_path() ->
    PrivDir = get_priv_dir(),
    filename:join(PrivDir, ?DEFAULT_VENV_SUBDIR).

%% @private
get_priv_dir() ->
    case code:priv_dir(barrel_embed) of
        {error, bad_name} ->
            %% Not yet loaded, use relative path
            "priv";
        Dir ->
            Dir
    end.

%% @private
%% Check if venv exists and has python executable
is_valid_venv(Path) ->
    PythonPath = venv_python(Path),
    filelib:is_file(PythonPath).

%% @private
%% Get path to python executable in venv
venv_python(VenvPath) ->
    BinDir = venv_bin_dir(VenvPath),
    filename:join(BinDir, "python").

%% @private
%% Get bin directory for venv (platform-specific)
venv_bin_dir(VenvPath) ->
    case os:type() of
        {win32, _} -> filename:join(VenvPath, "Scripts");
        _ -> filename:join(VenvPath, "bin")
    end.

%% @private
%% Find Python 3 executable
find_python() ->
    Candidates = ["python3", "python"],
    find_python(Candidates).

find_python([]) ->
    {error, python_not_found};
find_python([Candidate | Rest]) ->
    case os:find_executable(Candidate) of
        false ->
            find_python(Rest);
        Path ->
            %% Verify it's Python 3
            case check_python_version(Path) of
                true -> {ok, Path};
                false -> find_python(Rest)
            end
    end.

%% @private
check_python_version(Python) ->
    Cmd = Python ++ " -c \"import sys; print(sys.version_info.major)\"",
    case run_cmd(Cmd) of
        {ok, "3\n"} -> true;
        {ok, "3"} -> true;
        _ -> false
    end.

%% @private
%% Install base dependencies (uvloop on unix)
install_base_deps(VenvPath) ->
    case os:type() of
        {unix, _} ->
            case pip_install(VenvPath, ["uvloop"]) of
                ok ->
                    error_logger:info_msg("barrel_embed: uvloop installed~n"),
                    {ok, VenvPath};
                {error, Reason} ->
                    error_logger:error_msg(
                        "barrel_embed: failed to install uvloop: ~p~n",
                        [Reason]
                    ),
                    {error, {uvloop_install_failed, Reason}}
            end;
        _ ->
            %% uvloop not available on Windows
            {ok, VenvPath}
    end.

%% @private
%% Get dependencies for a provider
provider_deps(fastembed) ->
    ["fastembed"];
provider_deps(local) ->
    ["sentence-transformers"];
provider_deps(splade) ->
    ["transformers", "torch"];
provider_deps(colbert) ->
    ["transformers", "torch"];
provider_deps(clip) ->
    ["transformers", "torch", "pillow"];
provider_deps(_) ->
    [].

%% @private
%% Install packages using pip
pip_install(VenvPath, Packages) ->
    Pip = filename:join(venv_bin_dir(VenvPath), "pip"),
    PackageStr = string:join(Packages, " "),
    Cmd = Pip ++ " install " ++ PackageStr,
    case run_cmd(Cmd) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.

%% @private
%% Run a shell command and return output
run_cmd(Cmd) ->
    run_cmd(Cmd, 60000).  %% 60 second default timeout

run_cmd(Cmd, Timeout) ->
    Port = open_port(
        {spawn, Cmd},
        [exit_status, stderr_to_stdout, binary]
    ),
    collect_output(Port, [], Timeout).

collect_output(Port, Acc, Timeout) ->
    receive
        {Port, {data, Data}} ->
            collect_output(Port, [Data | Acc], Timeout);
        {Port, {exit_status, 0}} ->
            Output = iolist_to_binary(lists:reverse(Acc)),
            {ok, binary_to_list(Output)};
        {Port, {exit_status, Status}} ->
            Output = iolist_to_binary(lists:reverse(Acc)),
            {error, {exit_status, Status, binary_to_list(Output)}}
    after Timeout ->
        port_close(Port),
        {error, timeout}
    end.

%% @private
%% Remove a directory recursively
remove_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            case os:type() of
                {win32, _} ->
                    os:cmd("rmdir /s /q \"" ++ Dir ++ "\"");
                _ ->
                    os:cmd("rm -rf \"" ++ Dir ++ "\"")
            end,
            ok;
        false ->
            ok
    end.
