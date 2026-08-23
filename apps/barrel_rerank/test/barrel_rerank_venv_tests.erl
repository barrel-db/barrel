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

%% run_cmd/2 must never leave port or 'EXIT' messages in the caller's
%% mailbox (a trap_exit caller would otherwise see them as stray messages).
run_cmd_mailbox_ok_test() ->
    {Result, Msgs} = run_cmd_trapping("echo hello", 5000),
    ?assertEqual({ok, <<"hello\n">>}, Result),
    ?assertEqual([], Msgs).

run_cmd_mailbox_error_test() ->
    {Result, Msgs} = run_cmd_trapping("sh -c 'echo oops; exit 3'", 5000),
    ?assertEqual({error, {exit_code, 3, <<"oops\n">>}}, Result),
    ?assertEqual([], Msgs).

run_cmd_mailbox_timeout_test() ->
    {Result, Msgs} = run_cmd_trapping("sleep 5", 200),
    ?assertEqual({error, timeout}, Result),
    ?assertEqual([], Msgs).

run_cmd_trapping(Cmd, Timeout) ->
    Parent = self(),
    Ref = make_ref(),
    spawn(fun() ->
        process_flag(trap_exit, true),
        Result = barrel_rerank_venv:run_cmd(Cmd, Timeout),
        timer:sleep(200),
        {messages, Msgs} = erlang:process_info(self(), messages),
        Parent ! {Ref, Result, Msgs}
    end),
    receive
        {Ref, Result, Msgs} -> {Result, Msgs}
    after 15000 ->
        error(run_cmd_test_timeout)
    end.
