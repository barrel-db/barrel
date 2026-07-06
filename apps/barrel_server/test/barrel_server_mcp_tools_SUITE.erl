%%%-------------------------------------------------------------------
%%% @doc The core MCP tools end to end through barrel_mcp_client.
%%% Open group: full access, db/doc roundtrips with provenance on the
%%% audit trail, query paging and SUBSCRIBE rejection, bm25 search,
%%% changes cursors, and a branch/merge flow. Locked group: the
%%% capability deny matrix (right ladder and space scoping) and the
%%% server token's full access.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_tools_SUITE).

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2]).
-export([
    t_tools_list/1,
    t_db_doc_roundtrip/1,
    t_query_paging/1,
    t_query_subscribe_rejected/1,
    t_search/1,
    t_changes_cursor/1,
    t_branch_merge/1,
    t_capability_scoping/1,
    t_server_token_full/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ROOT, <<"tools-root-token">>).

all() ->
    [{group, open}, {group, locked}].

groups() ->
    [
        {open, [], [t_tools_list, t_db_doc_roundtrip, t_query_paging,
                    t_query_subscribe_rejected, t_search,
                    t_changes_cursor, t_branch_merge]},
        {locked, [], [t_capability_scoping, t_server_token_full]}
    ].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    %% small vectors + memory bm25 keep search cases cheap
    application:set_env(barrel_server, open_opts,
                        #{vectordb => #{dimension => 3,
                                        bm25_backend => memory}}),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, open_opts),
    ok.

init_per_group(locked, Config) ->
    application:set_env(barrel_server, auth, #{tokens => [?ROOT]}),
    restart_http(),
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(locked, _Config) ->
    application:unset_env(barrel_server, auth),
    restart_http(),
    ok;
end_per_group(_Group, _Config) ->
    ok.

%%====================================================================
%% Open group
%%====================================================================

t_tools_list(_Config) ->
    C = connect(#{}),
    {ok, Tools} = barrel_mcp_client:list_tools(C),
    Names = [maps:get(<<"name">>, T) || T <- Tools],
    Expected = [<<"db_create">>, <<"db_list">>, <<"db_info">>,
                <<"doc_get">>, <<"doc_put">>, <<"doc_delete">>,
                <<"query">>, <<"search">>, <<"changes">>,
                <<"branch_create">>, <<"branch_list">>, <<"merge">>],
    ?assertEqual(lists:sort(Expected), lists:sort(Names)),
    %% annotations surface the behavioural hints
    [Delete] = [T || #{<<"name">> := <<"doc_delete">>} = T <- Tools],
    ?assertMatch(#{<<"destructiveHint">> := true},
                 maps:get(<<"annotations">>, Delete)),
    [Info] = [T || #{<<"name">> := <<"db_info">>} = T <- Tools],
    ?assertMatch(#{<<"readOnlyHint">> := true},
                 maps:get(<<"annotations">>, Info)),
    barrel_mcp_client:close(C),
    ok.

t_db_doc_roundtrip(_Config) ->
    C = connect(#{}),
    {false, #{<<"ok">> := true}} =
        call(C, <<"db_create">>, #{<<"db">> => <<"mcp_rt">>}),
    {false, _} = call(C, <<"doc_put">>,
                      #{<<"db">> => <<"mcp_rt">>,
                        <<"doc">> => #{<<"id">> => <<"d1">>,
                                       <<"v">> => 1},
                        <<"session">> => <<"sess-1">>}),
    {false, #{<<"id">> := <<"d1">>, <<"v">> := 1}} =
        call(C, <<"doc_get">>, #{<<"db">> => <<"mcp_rt">>,
                                 <<"id">> => <<"d1">>}),
    %% the write's provenance landed on the audit trail
    {ok, Db} = barrel_server_dbs:ensure(<<"mcp_rt">>),
    {ok, Entries} = barrel:history(Db, #{id => <<"d1">>}),
    [Prov] = [maps:get(provenance, E) || E <- Entries,
                                         maps:is_key(provenance, E)],
    ?assertEqual(<<"mcp">>, maps:get(source, Prov)),
    ?assertEqual(<<"anonymous">>, maps:get(actor, Prov)),
    ?assertEqual(<<"sess-1">>, maps:get(session, Prov)),
    %% a doc without an id is refused
    {true, #{<<"error">> := <<"missing_id">>}} =
        call(C, <<"doc_put">>, #{<<"db">> => <<"mcp_rt">>,
                                 <<"doc">> => #{<<"v">> => 2}}),
    {false, _} = call(C, <<"doc_delete">>, #{<<"db">> => <<"mcp_rt">>,
                                             <<"id">> => <<"d1">>}),
    {true, #{<<"error">> := <<"not_found">>}} =
        call(C, <<"doc_get">>, #{<<"db">> => <<"mcp_rt">>,
                                 <<"id">> => <<"d1">>}),
    barrel_mcp_client:close(C),
    ok.

t_query_paging(_Config) ->
    C = connect(#{}),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"mcp_q">>}),
    lists:foreach(
        fun(N) ->
            {false, _} = call(C, <<"doc_put">>,
                #{<<"db">> => <<"mcp_q">>,
                  <<"doc">> => #{<<"id">> => <<"q",
                                     (integer_to_binary(N))/binary>>,
                                 <<"kind">> => <<"row">>,
                                 <<"n">> => N}})
        end, lists:seq(1, 5)),
    {false, #{<<"rows">> := Page1, <<"has_more">> := true,
              <<"continuation">> := Cont}} =
        call(C, <<"query">>,
             #{<<"db">> => <<"mcp_q">>,
               <<"query">> => <<"SELECT * FROM db WHERE kind = 'row'">>,
               <<"max_rows">> => 2}),
    ?assertEqual(2, length(Page1)),
    {false, #{<<"rows">> := Page2}} =
        call(C, <<"query">>,
             #{<<"db">> => <<"mcp_q">>,
               <<"query">> => <<"SELECT * FROM db WHERE kind = 'row'">>,
               <<"max_rows">> => 10,
               <<"continuation">> => Cont}),
    ?assertEqual(3, length(Page2)),
    %% params work; bad BQL is a readable error
    {false, #{<<"rows">> := [_], <<"has_more">> := false}} =
        call(C, <<"query">>,
             #{<<"db">> => <<"mcp_q">>,
               <<"query">> => <<"SELECT * FROM db WHERE n = $n">>,
               <<"params">> => #{<<"n">> => 3}}),
    {true, #{<<"error">> := <<"invalid_query">>}} =
        call(C, <<"query">>, #{<<"db">> => <<"mcp_q">>,
                               <<"query">> => <<"SELECT FROM">>}),
    barrel_mcp_client:close(C),
    ok.

t_query_subscribe_rejected(_Config) ->
    C = connect(#{}),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"mcp_sub">>}),
    {true, #{<<"error">> := <<"subscribe_not_supported">>}} =
        call(C, <<"query">>,
             #{<<"db">> => <<"mcp_sub">>,
               <<"query">> =>
                   <<"SELECT * FROM db WHERE kind = 'x' SUBSCRIBE">>}),
    barrel_mcp_client:close(C),
    ok.

t_search(_Config) ->
    C = connect(#{}),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"mcp_s">>}),
    {ok, Db} = barrel_server_dbs:ensure(<<"mcp_s">>),
    ok = barrel:vector_add(Db, <<"v1">>, <<"the quick brown fox">>,
                           #{}, [0.1, 0.2, 0.3]),
    ok = barrel:vector_add(Db, <<"v2">>, <<"lazy dogs sleep">>,
                           #{}, [0.9, 0.8, 0.7]),
    {false, #{<<"hits">> := Hits}} =
        call(C, <<"search">>, #{<<"db">> => <<"mcp_s">>,
                                <<"mode">> => <<"bm25">>,
                                <<"query">> => <<"fox">>}),
    ?assertMatch([#{<<"key">> := <<"v1">>} | _], Hits),
    {true, #{<<"error">> := <<"bad_mode">>}} =
        call(C, <<"search">>, #{<<"db">> => <<"mcp_s">>,
                                <<"mode">> => <<"regex">>}),
    barrel_mcp_client:close(C),
    ok.

t_changes_cursor(_Config) ->
    C = connect(#{}),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"mcp_c">>}),
    {false, #{<<"last">> := Last0}} =
        call(C, <<"changes">>, #{<<"db">> => <<"mcp_c">>}),
    {false, _} = call(C, <<"doc_put">>,
                      #{<<"db">> => <<"mcp_c">>,
                        <<"doc">> => #{<<"id">> => <<"c1">>}}),
    {false, #{<<"changes">> := Changes}} =
        call(C, <<"changes">>, #{<<"db">> => <<"mcp_c">>,
                                 <<"since">> => Last0}),
    ?assertMatch([#{<<"id">> := <<"c1">>}], Changes),
    {true, #{<<"error">> := <<"bad_since">>}} =
        call(C, <<"changes">>, #{<<"db">> => <<"mcp_c">>,
                                 <<"since">> => <<"not a cursor">>}),
    barrel_mcp_client:close(C),
    ok.

t_branch_merge(_Config) ->
    C = connect(#{}),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"mcp_tl">>}),
    {false, _} = call(C, <<"doc_put">>,
                      #{<<"db">> => <<"mcp_tl">>,
                        <<"doc">> => #{<<"id">> => <<"base">>}}),
    {false, #{<<"ok">> := true, <<"branch">> := Branch,
              <<"parent">> := <<"mcp_tl">>}} =
        call(C, <<"branch_create">>, #{<<"db">> => <<"mcp_tl">>,
                                       <<"name">> => <<"b1">>}),
    {false, #{<<"branches">> := Branches}} =
        call(C, <<"branch_list">>, #{<<"db">> => <<"mcp_tl">>}),
    ?assert(lists:member(Branch, Branches)),
    %% work on the branch, then merge it back
    {false, _} = call(C, <<"doc_put">>,
                      #{<<"db">> => Branch,
                        <<"doc">> => #{<<"id">> => <<"feature">>,
                                       <<"done">> => true}}),
    {true, #{<<"error">> := <<"not_found">>}} =
        call(C, <<"doc_get">>, #{<<"db">> => <<"mcp_tl">>,
                                 <<"id">> => <<"feature">>}),
    {false, _Report} = call(C, <<"merge">>, #{<<"db">> => Branch}),
    {false, #{<<"done">> := true}} =
        call(C, <<"doc_get">>, #{<<"db">> => <<"mcp_tl">>,
                                 <<"id">> => <<"feature">>}),
    barrel_mcp_client:close(C),
    ok.

%%====================================================================
%% Locked group
%%====================================================================

t_capability_scoping(Config) ->
    Space = make_space(Config, "cap_scope"),
    {ok, ReadT, _} = barrel_caps:grant(Space, #{rights => [read],
                                                subject => <<"reader">>}),
    {ok, WriteT, _} = barrel_caps:grant(Space,
                                        #{rights => [read, write],
                                          subject => <<"writer">>}),
    {ok, AdminT, _} = barrel_caps:grant(Space, #{rights => [admin],
                                                 subject => <<"boss">>}),
    %% the read principal sees its space and nothing else
    R = connect(#{auth => {bearer, ReadT}}),
    {false, _} = call(R, <<"db_info">>, #{<<"db">> => Space}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(R, <<"db_info">>, #{<<"db">> => <<"someone_elses_db">>}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(R, <<"doc_put">>,
             #{<<"db">> => Space,
               <<"doc">> => #{<<"id">> => <<"nope">>}}),
    {false, #{<<"dbs">> := Visible}} = call(R, <<"db_list">>, #{}),
    ?assertEqual([Space], Visible),
    barrel_mcp_client:close(R),
    %% the write principal writes but cannot touch the timeline
    W = connect(#{auth => {bearer, WriteT}}),
    {false, _} = call(W, <<"doc_put">>,
                      #{<<"db">> => Space,
                        <<"doc">> => #{<<"id">> => <<"note">>,
                                       <<"by">> => <<"writer">>},
                        <<"session">> => <<"w-1">>}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(W, <<"branch_create">>, #{<<"db">> => Space,
                                       <<"name">> => <<"wb">>}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(W, <<"merge">>, #{<<"db">> => Space}),
    barrel_mcp_client:close(W),
    %% the grant subject is the audit actor
    {ok, SpaceHandle} = barrel_spaces:open_space(Space),
    #{db := Db} = SpaceHandle,
    {ok, Entries} = barrel:history(Db, #{id => <<"note">>}),
    [Prov] = [maps:get(provenance, E) || E <- Entries,
                                         maps:is_key(provenance, E)],
    ?assertEqual(<<"writer">>, maps:get(actor, Prov)),
    ?assertEqual(<<"mcp">>, maps:get(source, Prov)),
    %% admin branches its space, but a branch db is not the space:
    %% touching it stays denied in v1
    A = connect(#{auth => {bearer, AdminT}}),
    {false, #{<<"branch">> := Branch}} =
        call(A, <<"branch_create">>, #{<<"db">> => Space,
                                       <<"name">> => <<"ab">>}),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(A, <<"merge">>, #{<<"db">> => Branch}),
    barrel_mcp_client:close(A),
    ok = barrel_spaces:drop_space(Space),
    ok.

t_server_token_full(_Config) ->
    C = connect(#{auth => {bearer, ?ROOT}}),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"anywhere">>}),
    {false, _} = call(C, <<"doc_put">>,
                      #{<<"db">> => <<"anywhere">>,
                        <<"doc">> => #{<<"id">> => <<"root">>}}),
    {false, #{<<"dbs">> := Dbs}} = call(C, <<"db_list">>, #{}),
    ?assert(lists:member(<<"anywhere">>, Dbs)),
    barrel_mcp_client:close(C),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

make_space(Config, Tag) ->
    Vec = #{dimension => 3, bm25_backend => memory,
            db_path => filename:join(?config(priv_dir, Config),
                                     Tag ++ "_vec")},
    {ok, #{id := SpaceId}} = barrel_spaces:create_space(
        #{label => list_to_binary(Tag), vectordb => Vec}),
    SpaceId.

base_url() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    "http://127.0.0.1:" ++ integer_to_list(Port).

restart_http() ->
    ok = supervisor:terminate_child(barrel_server_sup, barrel_server_http),
    {ok, _} = supervisor:restart_child(barrel_server_sup, barrel_server_http),
    ok.

connect(Extra) ->
    Base = base_url(),
    Spec = Extra#{transport => {http, list_to_binary(Base ++ "/mcp")}},
    {ok, C} = barrel_mcp_client:start(Spec),
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

%% Run a tool and decode its first text content block:
%% {IsError, DecodedJson}.
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
