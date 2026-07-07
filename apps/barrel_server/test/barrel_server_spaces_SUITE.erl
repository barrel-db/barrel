%%%-------------------------------------------------------------------
%%% @doc The agent layer over REST under a locked server: global
%%% bearer manages spaces, capability tokens are scoped to one space
%%% and one right ladder, dead tokens answer 401, and a handoff runs
%%% end to end over HTTP on the handoff token alone.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_spaces_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_space_crud_global/1,
    t_read_token_scope/1,
    t_revoked_401/1,
    t_wrong_space_403/1,
    t_bsp_outside_agent_layer/1,
    t_sessions_http/1,
    t_handoff_flow_http/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ROOT, <<"root-token">>).

all() ->
    [t_space_crud_global, t_read_token_scope, t_revoked_401,
     t_wrong_space_403, t_bsp_outside_agent_layer, t_sessions_http,
     t_handoff_flow_http].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, auth, #{tokens => [?ROOT]}),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, auth),
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_space_crud_global(Config) ->
    B = ?config(base, Config),
    {201, #{<<"space">> := Sp}} = create_space(B, <<"crud">>),
    {200, #{<<"spaces">> := Spaces}} =
        req(get, B ++ "/spaces", none, auth(?ROOT)),
    ?assert(lists:any(
        fun(#{<<"space">> := S}) -> S =:= Sp end, Spaces)),
    {200, Info} = req(get, B ++ path(["/spaces/", Sp]), none, auth(?ROOT)),
    ?assertEqual(<<"crud">>, maps:get(<<"label">>, Info)),
    ?assertEqual(<<"active">>, maps:get(<<"status">>, Info)),
    {200, #{<<"ok">> := true}} =
        req(delete, B ++ path(["/spaces/", Sp]), none, auth(?ROOT)),
    {200, #{<<"spaces">> := After}} =
        req(get, B ++ "/spaces", none, auth(?ROOT)),
    ?assertNot(lists:any(
        fun(#{<<"space">> := S}) -> S =:= Sp end, After)),
    ok.

t_read_token_scope(Config) ->
    B = ?config(base, Config),
    {201, #{<<"space">> := Sp}} = create_space(B, <<"scope">>),
    {201, #{<<"token">> := T}} = grant(B, Sp, [<<"read">>]),
    %% read right: GETs pass, mutations and grant admin do not
    {200, _} = req(get, B ++ path(["/spaces/", Sp]), none, auth(T)),
    {200, #{<<"sessions">> := []}} =
        req(get, B ++ path(["/spaces/", Sp, "/sessions"]), none, auth(T)),
    {403, #{<<"error">> := <<"forbidden">>}} =
        req(post, B ++ path(["/spaces/", Sp, "/sessions"]), #{}, auth(T)),
    {403, _} = req(post, B ++ path(["/spaces/", Sp, "/grants"]),
                   #{<<"rights">> => [<<"read">>]}, auth(T)),
    ok.

t_revoked_401(Config) ->
    B = ?config(base, Config),
    {201, #{<<"space">> := Sp}} = create_space(B, <<"revoked">>),
    {201, #{<<"token">> := T, <<"grant">> := G}} =
        grant(B, Sp, [<<"read">>]),
    {200, _} = req(get, B ++ path(["/spaces/", Sp]), none, auth(T)),
    TokenId = maps:get(<<"token_id">>, G),
    {200, #{<<"ok">> := true}} =
        req(delete, B ++ path(["/spaces/", Sp, "/grants/", TokenId]),
            none, auth(?ROOT)),
    {401, _} = req(get, B ++ path(["/spaces/", Sp]), none, auth(T)),
    ok.

t_wrong_space_403(Config) ->
    B = ?config(base, Config),
    {201, #{<<"space">> := SpA}} = create_space(B, <<"a">>),
    {201, #{<<"space">> := SpB}} = create_space(B, <<"b">>),
    {201, #{<<"token">> := T}} = grant(B, SpA, [<<"admin">>]),
    {200, _} = req(get, B ++ path(["/spaces/", SpA]), none, auth(T)),
    {403, _} = req(get, B ++ path(["/spaces/", SpB]), none, auth(T)),
    ok.

t_bsp_outside_agent_layer(Config) ->
    B = ?config(base, Config),
    {201, #{<<"space">> := Sp}} = create_space(B, <<"outside">>),
    {201, #{<<"token">> := T}} = grant(B, Sp, [<<"admin">>]),
    %% capability tokens reach only their granted space's /db routes:
    %% another db answers 403 (wrong space), db lifecycle stays off
    {403, _} = req(get, B ++ "/db/somedb", none, auth(T)),
    {403, _} = req(put, B ++ "/db/somedb", #{}, auth(T)),
    %% and never manage the space collection itself
    {403, _} = req(post, B ++ "/spaces",
                   #{<<"label">> => <<"nope">>}, auth(T)),
    {403, _} = req(get, B ++ "/spaces", none, auth(T)),
    {403, _} = req(delete, B ++ path(["/spaces/", Sp]), none, auth(T)),
    ok.

t_sessions_http(Config) ->
    B = ?config(base, Config),
    {201, #{<<"space">> := Sp}} = create_space(B, <<"sessions">>),
    {201, #{<<"token">> := T}} = grant(B, Sp, [<<"read">>, <<"write">>]),
    {201, #{<<"session">> := Sid}} =
        req(post, B ++ path(["/spaces/", Sp, "/sessions"]),
            #{<<"agent">> => <<"alice">>}, auth(T)),
    {200, Sess} = req(get, B ++ path(["/spaces/", Sp, "/sessions/", Sid]),
                      none, auth(T)),
    ?assertEqual(<<"alice">>, maps:get(<<"agent">>, Sess)),
    {200, #{<<"expires_at">> := E}} =
        req(post, B ++ path(["/spaces/", Sp, "/sessions/", Sid, "/touch"]),
            #{}, auth(T)),
    ?assert(is_integer(E)),
    lists:foreach(
        fun(N) ->
            {201, _} = req(
                post,
                B ++ path(["/spaces/", Sp, "/sessions/", Sid, "/messages"]),
                #{<<"role">> => <<"user">>,
                  <<"content">> => <<"m", (integer_to_binary(N))/binary>>},
                auth(T))
        end, lists:seq(1, 3)),
    {200, #{<<"messages">> := Msgs}} =
        req(get, B ++ path(["/spaces/", Sp, "/sessions/", Sid, "/messages"]),
            none, auth(T)),
    ?assertEqual([<<"m1">>, <<"m2">>, <<"m3">>],
                 [maps:get(<<"content">>, M) || M <- Msgs]),
    {200, #{<<"messages">> := [Newest]}} =
        req(get, B ++ path(["/spaces/", Sp, "/sessions/", Sid,
                            "/messages?order=desc&limit=1"]),
            none, auth(T)),
    ?assertEqual(<<"m3">>, maps:get(<<"content">>, Newest)),
    {200, _} = req(put, B ++ path(["/spaces/", Sp, "/sessions/", Sid,
                                   "/data/cursor"]),
                   #{<<"value">> => 42}, auth(T)),
    {200, #{<<"value">> := 42}} =
        req(get, B ++ path(["/spaces/", Sp, "/sessions/", Sid,
                            "/data/cursor"]),
            none, auth(T)),
    {200, #{<<"ok">> := true}} =
        req(delete, B ++ path(["/spaces/", Sp, "/sessions/", Sid]),
            none, auth(T)),
    {404, _} = req(get, B ++ path(["/spaces/", Sp, "/sessions/", Sid]),
                   none, auth(T)),
    ok.

t_handoff_flow_http(Config) ->
    B = ?config(base, Config),
    %% alice's side runs on the global bearer
    {201, #{<<"space">> := Sp}} = create_space(B, <<"handoff">>),
    {201, #{<<"handoff">> := Hid, <<"token">> := HT}} =
        req(post, B ++ "/handoffs",
            #{<<"space">> => Sp,
              <<"task_name">> => <<"finish the draft">>,
              <<"from_agent">> => <<"alice">>,
              <<"to_agent">> => <<"bob">>,
              <<"pending">> => [<<"polish conclusion">>]},
            auth(?ROOT)),
    {200, #{<<"handoffs">> := Pending}} =
        req(get, B ++ "/handoffs?status=pending&space=" ++
                binary_to_list(Sp), none, auth(?ROOT)),
    ?assert(lists:any(
        fun(#{<<"handoff">> := H}) -> H =:= Hid end, Pending)),
    %% bob accepts and works with nothing but the handoff token
    {200, #{<<"handoff">> := Accepted, <<"space">> := Sp,
            <<"session">> := Sid}} =
        req(post, B ++ "/handoffs/accept",
            #{<<"token">> => HT, <<"agent">> => <<"bob">>}, auth(HT)),
    ?assertEqual(<<"accepted">>, maps:get(<<"status">>, Accepted)),
    {409, #{<<"error">> := <<"already_accepted">>}} =
        req(post, B ++ "/handoffs/accept",
            #{<<"token">> => HT, <<"agent">> => <<"mallory">>}, auth(HT)),
    %% a capability caller only sees its own space's handoffs
    {200, #{<<"handoffs">> := Mine}} =
        req(get, B ++ "/handoffs", none, auth(HT)),
    ?assert(lists:all(
        fun(#{<<"space">> := S}) -> S =:= Sp end, Mine)),
    {201, _} = req(
        post, B ++ path(["/spaces/", Sp, "/sessions/", Sid, "/messages"]),
        #{<<"role">> => <<"assistant">>,
          <<"content">> => <<"picking this up">>}, auth(HT)),
    {200, #{<<"handoff">> := Done}} =
        req(post, B ++ "/handoffs/complete",
            #{<<"token">> => HT, <<"result">> => <<"shipped">>}, auth(HT)),
    ?assertEqual(<<"completed">>, maps:get(<<"status">>, Done)),
    ?assertEqual(<<"shipped">>, maps:get(<<"result">>, Done)),
    %% completion revoked the token: bob is out
    {401, _} = req(get, B ++ path(["/spaces/", Sp]), none, auth(HT)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

create_space(B, Label) ->
    req(post, B ++ "/spaces",
        #{<<"label">> => Label,
          <<"vectordb">> => #{<<"dimension">> => 3,
                              <<"bm25_backend">> => <<"memory">>}},
        auth(?ROOT)).

grant(B, Sp, Rights) ->
    req(post, B ++ path(["/spaces/", Sp, "/grants"]),
        #{<<"rights">> => Rights}, auth(?ROOT)).

auth(Token) ->
    [{<<"authorization">>, <<"Bearer ", Token/binary>>}].

path(Parts) ->
    binary_to_list(iolist_to_binary(Parts)).

req(Method, Url, none, Headers) ->
    {ok, S, _H, RespBody} = hackney:request(Method, list_to_binary(Url),
                                            Headers, <<>>, [with_body]),
    {S, decode(RespBody)};
req(Method, Url, JsonTerm, Headers) ->
    {ok, S, _H, RespBody} = hackney:request(
        Method, list_to_binary(Url),
        [{<<"content-type">>, <<"application/json">>} | Headers],
        json:encode(JsonTerm), [with_body]),
    {S, decode(RespBody)}.

decode(<<>>) -> #{};
decode(Bin) ->
    try json:decode(Bin) catch _:_ -> Bin end.
