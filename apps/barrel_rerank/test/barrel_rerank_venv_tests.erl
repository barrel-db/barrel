%%%-------------------------------------------------------------------
%%% @doc Unit tests for barrel_rerank_venv
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rerank_venv_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

venv_path_default_test() ->
    %% Should return a valid path
    Path = barrel_rerank_venv:venv_path(),
    ?assert(is_list(Path)),
    ?assert(length(Path) > 0).

venv_path_configured_test() ->
    %% Set a custom path
    CustomPath = "/tmp/test_venv",
    application:set_env(barrel_rerank, venv_path, CustomPath),
    try
        ?assertEqual(CustomPath, barrel_rerank_venv:venv_path())
    after
        application:unset_env(barrel_rerank, venv_path)
    end.

is_valid_nonexistent_test() ->
    %% A non-existent venv should not be valid
    application:set_env(barrel_rerank, venv_path, "/nonexistent/venv/path"),
    try
        ?assertEqual(false, barrel_rerank_venv:is_valid())
    after
        application:unset_env(barrel_rerank, venv_path)
    end.
