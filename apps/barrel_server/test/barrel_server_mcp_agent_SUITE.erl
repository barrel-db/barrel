%%%-------------------------------------------------------------------
%%% @doc The agent layer over MCP, end to end on a locked server: the
%%% operator creates a space and grants alice admin; alice works in a
%%% session and hands off to bob; bob accepts with nothing but the
%%% handoff token, reads alice's context in place, completes, and his
%%% token dies with the completion. Plus the deny matrix for
%%% capability principals on the agent tools.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_agent_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_agent_tools_registered/1,
    t_agent_flow/1,
    t_capability_denies/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ROOT, <<"agent-root-token">>).

all() ->
    [t_agent_tools_registered, t_agent_flow, t_capability_denies].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, auth, #{tokens => [?ROOT]}),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, auth),
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_agent_tools_registered(_Config) ->
    C = connect(?ROOT),
    {ok, Tools} = barrel_mcp_client:list_tools(C),
    Names = [maps:get(<<"name">>, T) || T <- Tools],
    Expected = [<<"space_create">>, <<"space_info">>, <<"space_grant">>,
                <<"space_revoke">>, <<"session_create">>,
                <<"session_touch">>, <<"session_add_message">>,
                <<"session_get_messages">>, <<"handoff_create">>,
                <<"handoff_accept">>, <<"handoff_complete">>,
                <<"handoff_list">>],
    ?assertEqual([], Expected -- Names),
    barrel_mcp_client:close(C),
    ok.

t_agent_flow(_Config) ->
    %% the operator creates the space and hands alice the keys
    Server = connect(?ROOT),
    {false, #{<<"space">> := Sp}} =
        call(Server, <<"space_create">>,
             #{<<"label">> => <<"joint work">>,
               <<"vectordb">> => #{<<"dimension">> => 3,
                                   <<"bm25_backend">> => <<"memory">>}}),
    {false, #{<<"token">> := AliceToken}} =
        call(Server, <<"space_grant">>,
             #{<<"space">> => Sp,
               <<"rights">> => [<<"read">>, <<"write">>, <<"admin">>],
               <<"subject">> => <<"alice">>}),
    barrel_mcp_client:close(Server),
    %% alice reconnects with her capability and works in a session
    Alice = connect(AliceToken),
    {false, #{<<"space">> := Sp}} =
        call(Alice, <<"space_info">>, #{<<"space">> => Sp}),
    {false, #{<<"session">> := ASid}} =
        call(Alice, <<"session_create">>, #{<<"space">> => Sp,
                                            <<"agent">> => <<"alice">>}),
    {false, _} = call(Alice, <<"session_add_message">>,
                      #{<<"space">> => Sp, <<"session">> => ASid,
                        <<"role">> => <<"user">>,
                        <<"content">> => <<"draft is half done">>}),
    %% her doc writes carry her subject and session on the audit trail
    {false, _} = call(Alice, <<"doc_put">>,
                      #{<<"db">> => Sp,
                        <<"doc">> => #{<<"id">> => <<"draft">>,
                                       <<"state">> => <<"wip">>},
                        <<"session">> => ASid}),
    {ok, #{db := SpaceDb}} = barrel_spaces:open_space(Sp),
    {ok, Entries} = barrel:history(SpaceDb, #{id => <<"draft">>}),
    [Prov] = [maps:get(provenance, E) || E <- Entries,
                                         maps:is_key(provenance, E)],
    ?assertEqual(<<"alice">>, maps:get(actor, Prov)),
    ?assertEqual(ASid, maps:get(session, Prov)),
    %% alice hands the task off to bob
    {false, #{<<"handoff">> := Hid, <<"token">> := BobToken}} =
        call(Alice, <<"handoff_create">>,
             #{<<"space">> => Sp,
               <<"task_name">> => <<"finish the draft">>,
               <<"from_agent">> => <<"alice">>,
               <<"to_agent">> => <<"bob">>,
               <<"from_session">> => ASid}),
    {false, #{<<"handoffs">> := [Pending]}} =
        call(Alice, <<"handoff_list">>, #{<<"status">> => <<"pending">>}),
    ?assertEqual(Hid, maps:get(<<"handoff">>, Pending)),
    barrel_mcp_client:close(Alice),
    %% bob has nothing but the handoff token
    Bob = connect(BobToken),
    {false, #{<<"handoff">> := Accepted, <<"space">> := Sp,
              <<"session">> := BSid}} =
        call(Bob, <<"handoff_accept">>, #{<<"token">> => BobToken,
                                          <<"agent">> => <<"bob">>}),
    ?assertEqual(<<"accepted">>, maps:get(<<"status">>, Accepted)),
    %% alice's context read in place: her doc and her session history
    {false, #{<<"state">> := <<"wip">>}} =
        call(Bob, <<"doc_get">>, #{<<"db">> => Sp,
                                   <<"id">> => <<"draft">>}),
    {false, #{<<"messages">> := [Msg]}} =
        call(Bob, <<"session_get_messages">>,
             #{<<"space">> => Sp, <<"session">> => ASid}),
    ?assertEqual(<<"draft is half done">>, maps:get(<<"content">>, Msg)),
    {false, _} = call(Bob, <<"session_add_message">>,
                      #{<<"space">> => Sp, <<"session">> => BSid,
                        <<"role">> => <<"assistant">>,
                        <<"content">> => <<"picking this up">>}),
    {false, #{<<"handoff">> := Done}} =
        call(Bob, <<"handoff_complete">>,
             #{<<"token">> => BobToken,
               <<"result">> => <<"shipped">>}),
    ?assertEqual(<<"completed">>, maps:get(<<"status">>, Done)),
    %% completion revoked bob's token: the next call dies at auth
    %% (the client tears itself down on the 401)
    Denied = try barrel_mcp_client:call_tool(Bob, <<"space_info">>,
                                             #{<<"space">> => Sp}) of
        {error, _} -> true;
        {ok, _} -> false
    catch
        exit:_ -> true
    end,
    ?assert(Denied),
    barrel_mcp_client:close(Bob),
    ok.

t_capability_denies(_Config) ->
    Server = connect(?ROOT),
    {false, #{<<"space">> := Sp}} =
        call(Server, <<"space_create">>,
             #{<<"label">> => <<"deny">>,
               <<"vectordb">> => #{<<"dimension">> => 3,
                                   <<"bm25_backend">> => <<"memory">>}}),
    {false, #{<<"token">> := ReadToken, <<"grant">> := Grant}} =
        call(Server, <<"space_grant">>, #{<<"space">> => Sp,
                                          <<"rights">> => [<<"read">>]}),
    barrel_mcp_client:close(Server),
    R = connect(ReadToken),
    %% a capability principal never manages the space collection
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(R, <<"space_create">>, #{<<"label">> => <<"nope">>}),
    %% the right ladder holds on the agent tools
    {false, _} = call(R, <<"space_info">>, #{<<"space">> => Sp}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(R, <<"session_create">>, #{<<"space">> => Sp}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(R, <<"space_grant">>, #{<<"space">> => Sp,
                                     <<"rights">> => [<<"read">>]}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(R, <<"handoff_create">>, #{<<"space">> => Sp,
                                        <<"task_name">> => <<"t">>}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(R, <<"space_revoke">>,
             #{<<"space">> => Sp,
               <<"token_id">> => maps:get(<<"token_id">>, Grant)}),
    barrel_mcp_client:close(R),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

base_url() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    "http://127.0.0.1:" ++ integer_to_list(Port).

connect(Token) ->
    Base = base_url(),
    {ok, C} = barrel_mcp_client:start(#{
        transport => {http, list_to_binary(Base ++ "/mcp")},
        auth => {bearer, Token}
    }),
    ok = wait_ready(C, 100),
    C.

wait_ready(_C, 0) ->
    {error, not_ready};
wait_ready(C, N) ->
    case barrel_mcp_client:ping(C) of
        {ok, _} -> ok;
        {error, not_ready} ->
            timer:sleep(50),
            wait_ready(C, N - 1);
        {error, _} = Err ->
            Err
    end.

call(C, Name, Args) ->
    {ok, Res} = barrel_mcp_client:call_tool(C, Name, Args),
    IsErr = maps:get(<<"isError">>, Res, false),
    Data = case maps:get(<<"content">>, Res, []) of
        [#{<<"type">> := <<"text">>, <<"text">> := Text} | _] ->
            try json:decode(Text) catch _:_ -> Text end;
        Other ->
            Other
    end,
    {IsErr, Data}.
