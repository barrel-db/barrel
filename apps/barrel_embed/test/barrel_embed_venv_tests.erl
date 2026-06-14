%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_embed_venv module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_venv_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

venv_path_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       {"venv_path returns default path", fun test_venv_path_default/0},
       {"venv_path returns custom path from env", fun test_venv_path_custom/0}
     ]
    }.

venv_lifecycle_test_() ->
    {foreach,
     fun setup_with_temp_dir/0,
     fun cleanup_with_temp_dir/1,
     [
       {"is_valid returns false for non-existent venv", fun test_is_valid_false/0},
       {timeout, 30, {"create_venv creates valid venv", fun test_create_venv/0}},
       {timeout, 30, {"ensure_venv creates venv if missing", fun test_ensure_venv_create/0}},
       {timeout, 30, {"ensure_venv returns existing venv", fun test_ensure_venv_existing/0}},
       {timeout, 60, {"refresh deletes and recreates venv", fun test_refresh/0}}
     ]
    }.

install_deps_test_() ->
    {foreach,
     fun setup_with_venv/0,
     fun cleanup_with_temp_dir/1,
     [
       {"install_deps returns error when venv not found", fun test_install_deps_no_venv/0},
       {"install_deps returns ok for unknown provider", fun test_install_deps_unknown/0}
     ]
    }.

%% Tests for barrel_embed wrapper functions
barrel_embed_api_test_() ->
    {foreach,
     fun setup_with_temp_dir/0,
     fun cleanup_with_temp_dir/1,
     [
       {"barrel_embed:venv_path returns path", fun test_barrel_embed_venv_path/0},
       {timeout, 60, {"barrel_embed:refresh_venv delegates to venv module", fun test_barrel_embed_refresh_venv/0}},
       {timeout, 30, {"barrel_embed:install_provider delegates to venv module", fun test_barrel_embed_install_provider/0}},
       {timeout, 30, {"has_uvloop returns true after venv creation", fun test_has_uvloop/0}}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    %% Clear any existing env settings
    application:unset_env(barrel_embed, venv_dir),
    application:unset_env(barrel_embed, managed_venv_path),
    ok.

cleanup(_) ->
    application:unset_env(barrel_embed, venv_dir),
    application:unset_env(barrel_embed, managed_venv_path),
    ok.

setup_with_temp_dir() ->
    setup(),
    %% Create a unique temp directory for testing
    TempDir = create_temp_dir(),
    VenvDir = filename:join(TempDir, ".venv"),
    application:set_env(barrel_embed, venv_dir, VenvDir),
    TempDir.

cleanup_with_temp_dir(TempDir) ->
    cleanup(ok),
    %% Remove temp directory
    remove_dir_recursive(TempDir),
    ok.

setup_with_venv() ->
    TempDir = setup_with_temp_dir(),
    %% Create the venv
    _ = barrel_embed_venv:create_venv(),
    TempDir.

%%====================================================================
%% Test Cases - venv_path
%%====================================================================

test_venv_path_default() ->
    Path = barrel_embed_venv:venv_path(),
    %% Should end with .venv
    ?assertEqual(".venv", filename:basename(Path)),
    %% Should be under priv dir or "priv"
    ParentBase = filename:basename(filename:dirname(Path)),
    ?assert(ParentBase =:= "priv" orelse ParentBase =:= "barrel_embed").

test_venv_path_custom() ->
    CustomPath = "/custom/venv/path",
    application:set_env(barrel_embed, venv_dir, CustomPath),
    ?assertEqual(CustomPath, barrel_embed_venv:venv_path()).

%%====================================================================
%% Test Cases - venv lifecycle
%%====================================================================

test_is_valid_false() ->
    ?assertEqual(false, barrel_embed_venv:is_valid()).

test_create_venv() ->
    case find_python() of
        {ok, _} ->
            Result = barrel_embed_venv:create_venv(),
            ?assertMatch({ok, _}, Result),
            {ok, Path} = Result,
            ?assertEqual(true, barrel_embed_venv:is_valid()),
            %% Check python executable exists
            PythonPath = filename:join([Path, "bin", "python"]),
            ?assertEqual(true, filelib:is_file(PythonPath));
        {error, _} ->
            %% Skip test if Python not available
            ?debugMsg("Skipping test_create_venv: Python not found"),
            ok
    end.

test_ensure_venv_create() ->
    case find_python() of
        {ok, _} ->
            ?assertEqual(false, barrel_embed_venv:is_valid()),
            Result = barrel_embed_venv:ensure_venv(),
            ?assertMatch({ok, _}, Result),
            ?assertEqual(true, barrel_embed_venv:is_valid()),
            %% Check managed_venv_path was set
            {ok, ManagedPath} = application:get_env(barrel_embed, managed_venv_path),
            ?assertEqual(barrel_embed_venv:venv_path(), ManagedPath);
        {error, _} ->
            ?debugMsg("Skipping test_ensure_venv_create: Python not found"),
            ok
    end.

test_ensure_venv_existing() ->
    case find_python() of
        {ok, _} ->
            %% Create venv first
            {ok, Path1} = barrel_embed_venv:create_venv(),
            %% ensure_venv should return existing
            {ok, Path2} = barrel_embed_venv:ensure_venv(),
            ?assertEqual(Path1, Path2);
        {error, _} ->
            ?debugMsg("Skipping test_ensure_venv_existing: Python not found"),
            ok
    end.

test_refresh() ->
    case find_python() of
        {ok, _} ->
            %% Create initial venv
            {ok, Path1} = barrel_embed_venv:create_venv(),
            ?assertEqual(true, barrel_embed_venv:is_valid()),
            %% Create a marker file
            MarkerFile = filename:join(Path1, "test_marker"),
            ok = file:write_file(MarkerFile, <<"test">>),
            ?assertEqual(true, filelib:is_file(MarkerFile)),
            %% Refresh should delete and recreate
            {ok, Path2} = barrel_embed_venv:refresh(),
            ?assertEqual(Path1, Path2),
            ?assertEqual(true, barrel_embed_venv:is_valid()),
            %% Marker file should be gone
            ?assertEqual(false, filelib:is_file(MarkerFile));
        {error, _} ->
            ?debugMsg("Skipping test_refresh: Python not found"),
            ok
    end.

%%====================================================================
%% Test Cases - install_deps
%%====================================================================

test_install_deps_no_venv() ->
    %% Use a non-existent venv path
    application:set_env(barrel_embed, venv_dir, "/nonexistent/path"),
    Result = barrel_embed_venv:install_deps(fastembed),
    ?assertEqual({error, venv_not_found}, Result).

test_install_deps_unknown() ->
    %% Unknown provider should return ok (no deps to install)
    Result = barrel_embed_venv:install_deps(unknown_provider),
    ?assertEqual(ok, Result).

%%====================================================================
%% Test Cases - barrel_embed API
%%====================================================================

test_barrel_embed_venv_path() ->
    %% Should return same path as barrel_embed_venv:venv_path()
    ?assertEqual(barrel_embed_venv:venv_path(), barrel_embed:venv_path()).

test_barrel_embed_refresh_venv() ->
    case find_python() of
        {ok, _} ->
            %% Create initial venv
            {ok, _} = barrel_embed_venv:create_venv(),
            %% Use barrel_embed:refresh_venv
            Result = barrel_embed:refresh_venv(),
            ?assertMatch({ok, _}, Result),
            ?assertEqual(true, barrel_embed_venv:is_valid());
        {error, _} ->
            ?debugMsg("Skipping test_barrel_embed_refresh_venv: Python not found"),
            ok
    end.

test_barrel_embed_install_provider() ->
    case find_python() of
        {ok, _} ->
            %% Create venv first
            {ok, _} = barrel_embed_venv:create_venv(),
            %% Unknown provider should return ok
            ?assertEqual(ok, barrel_embed:install_provider(unknown_provider));
        {error, _} ->
            ?debugMsg("Skipping test_barrel_embed_install_provider: Python not found"),
            ok
    end.

test_has_uvloop() ->
    case find_python() of
        {ok, _} ->
            %% Before venv creation, should return false
            ?assertEqual(false, barrel_embed_venv:has_uvloop()),
            %% Create venv (which installs uvloop on unix)
            {ok, _} = barrel_embed_venv:create_venv(),
            %% On unix, should return true; on windows, false
            case os:type() of
                {unix, _} ->
                    ?assertEqual(true, barrel_embed_venv:has_uvloop()),
                    ?assertEqual(true, barrel_embed:has_uvloop());
                _ ->
                    ?assertEqual(false, barrel_embed_venv:has_uvloop())
            end;
        {error, _} ->
            ?debugMsg("Skipping test_has_uvloop: Python not found"),
            ok
    end.

%%====================================================================
%% Helpers
%%====================================================================

create_temp_dir() ->
    Timestamp = erlang:system_time(microsecond),
    Rand = rand:uniform(1000000),
    TempBase = filename:join(["/tmp", "barrel_embed_test_" ++ integer_to_list(Timestamp) ++ "_" ++ integer_to_list(Rand)]),
    ok = filelib:ensure_dir(filename:join(TempBase, "dummy")),
    TempBase.

remove_dir_recursive(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            case os:type() of
                {win32, _} ->
                    os:cmd("rmdir /s /q \"" ++ Dir ++ "\"");
                _ ->
                    os:cmd("rm -rf \"" ++ Dir ++ "\"")
            end;
        false ->
            ok
    end.

find_python() ->
    case os:find_executable("python3") of
        false ->
            case os:find_executable("python") of
                false -> {error, not_found};
                Path -> check_python3(Path)
            end;
        Path ->
            {ok, Path}
    end.

check_python3(Python) ->
    Cmd = Python ++ " -c \"import sys; print(sys.version_info.major)\"",
    case os:cmd(Cmd) of
        "3\n" -> {ok, Python};
        "3" -> {ok, Python};
        _ -> {error, not_python3}
    end.
