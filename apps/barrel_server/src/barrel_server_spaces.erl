%%%-------------------------------------------------------------------
%%% @doc Agent-layer REST surface: spaces, capability grants, sessions,
%%% and handoffs over HTTP. Space and handoff creation are management
%%% operations (global bearer only); everything scoped to one space
%%% accepts a capability token checked per method through
%%% barrel_server_caps (GETs need read, mutations write, grant
%%% administration admin).
%%%
%%% Spaces open with default runtime options: an encrypted space needs
%%% its key spec on open, which HTTP callers cannot supply in v1, so
%%% encrypted spaces are Erlang-API-only for now.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_spaces).

-export([routes/0]).

%% Route handlers (invoked by the livery router as Module:Function/1)
-export([
    create_space/1,
    list_spaces/1,
    space_info/1,
    drop_space/1,
    create_grant/1,
    list_grants/1,
    revoke_grant/1,
    create_session/1,
    list_sessions/1,
    get_session/1,
    delete_session/1,
    touch_session/1,
    add_message/1,
    get_messages/1,
    set_data/1,
    get_data/1,
    create_handoff/1,
    list_handoffs/1,
    accept_handoff/1,
    complete_handoff/1
]).

routes() ->
    [
        {<<"POST">>,   <<"/spaces">>,                    {?MODULE, create_space}},
        {<<"GET">>,    <<"/spaces">>,                    {?MODULE, list_spaces}},
        {<<"GET">>,    <<"/spaces/:space">>,             {?MODULE, space_info}},
        {<<"DELETE">>, <<"/spaces/:space">>,             {?MODULE, drop_space}},

        {<<"POST">>,   <<"/spaces/:space/grants">>,      {?MODULE, create_grant}},
        {<<"GET">>,    <<"/spaces/:space/grants">>,      {?MODULE, list_grants}},
        {<<"DELETE">>, <<"/spaces/:space/grants/:token_id">>, {?MODULE, revoke_grant}},

        {<<"POST">>,   <<"/spaces/:space/sessions">>,    {?MODULE, create_session}},
        {<<"GET">>,    <<"/spaces/:space/sessions">>,    {?MODULE, list_sessions}},
        {<<"GET">>,    <<"/spaces/:space/sessions/:sid">>, {?MODULE, get_session}},
        {<<"DELETE">>, <<"/spaces/:space/sessions/:sid">>, {?MODULE, delete_session}},
        {<<"POST">>,   <<"/spaces/:space/sessions/:sid/touch">>, {?MODULE, touch_session}},
        {<<"POST">>,   <<"/spaces/:space/sessions/:sid/messages">>, {?MODULE, add_message}},
        {<<"GET">>,    <<"/spaces/:space/sessions/:sid/messages">>, {?MODULE, get_messages}},
        {<<"PUT">>,    <<"/spaces/:space/sessions/:sid/data/:key">>, {?MODULE, set_data}},
        {<<"GET">>,    <<"/spaces/:space/sessions/:sid/data/:key">>, {?MODULE, get_data}},

        {<<"POST">>,   <<"/handoffs">>,                  {?MODULE, create_handoff}},
        {<<"GET">>,    <<"/handoffs">>,                  {?MODULE, list_handoffs}},
        {<<"POST">>,   <<"/handoffs/accept">>,           {?MODULE, accept_handoff}},
        {<<"POST">>,   <<"/handoffs/complete">>,         {?MODULE, complete_handoff}}
    ].

%%====================================================================
%% Spaces
%%====================================================================

create_space(Req) ->
    with_global(Req, fun() ->
        with_json(Req, fun(Body) ->
            case barrel_spaces:create_space(space_opts(Body)) of
                {ok, #{id := Id}} ->
                    json(201, #{ok => true, space => Id});
                Err ->
                    err(Err)
            end
        end)
    end).

list_spaces(Req) ->
    with_global(Req, fun() ->
        {ok, Spaces} = barrel_spaces:list_spaces(),
        json(200, #{spaces => Spaces})
    end).

space_info(Req) ->
    with_right(Req, read, fun(SpaceId) ->
        case barrel_spaces:space_info(SpaceId) of
            {ok, Info} -> json(200, public(Info));
            Err -> err(Err)
        end
    end).

drop_space(Req) ->
    with_global(Req, fun() ->
        SpaceId = livery_req:binding(<<"space">>, Req),
        case barrel_spaces:drop_space(SpaceId) of
            ok -> json(200, #{ok => true});
            Err -> err(Err)
        end
    end).

%%====================================================================
%% Grants
%%====================================================================

create_grant(Req) ->
    with_right(Req, admin, fun(SpaceId) ->
        with_json(Req, fun(Body) ->
            case grant_opts(Body) of
                {ok, Opts} ->
                    case barrel_caps:grant(SpaceId, Opts) of
                        {ok, Token, Grant} ->
                            json(201, #{token => Token,
                                        grant => Grant});
                        Err ->
                            err(Err)
                    end;
                {error, _} = Err ->
                    err(Err)
            end
        end)
    end).

list_grants(Req) ->
    with_right(Req, admin, fun(SpaceId) ->
        {ok, Grants} = barrel_caps:list(SpaceId),
        json(200, #{grants => Grants})
    end).

revoke_grant(Req) ->
    with_right(Req, admin, fun(SpaceId) ->
        TokenId = livery_req:binding(<<"token_id">>, Req),
        {ok, Grants} = barrel_caps:list(SpaceId),
        case [G || #{<<"token_id">> := T} = G <- Grants,
                   T =:= TokenId] of
            [_] ->
                ok = barrel_caps:revoke(TokenId),
                json(200, #{ok => true});
            [] ->
                err(not_found)
        end
    end).

%%====================================================================
%% Sessions
%%====================================================================

create_session(Req) ->
    with_space(Req, write, fun(Space) ->
        with_json(Req, fun(Body) ->
            case barrel_session:create(Space, session_opts(Body)) of
                {ok, Sid} -> json(201, #{ok => true, session => Sid});
                Err -> err(Err)
            end
        end)
    end).

list_sessions(Req) ->
    with_space(Req, read, fun(Space) ->
        Opts = case param(<<"agent">>, Req) of
            undefined -> #{};
            Agent -> #{agent => Agent}
        end,
        {ok, Sessions} = barrel_session:list(Space, Opts),
        json(200, #{sessions => [public(S) || S <- Sessions]})
    end).

get_session(Req) ->
    with_session(Req, read, fun(Space, Sid) ->
        case barrel_session:get(Space, Sid) of
            {ok, Doc} -> json(200, public(Doc));
            Err -> err(Err)
        end
    end).

delete_session(Req) ->
    with_session(Req, write, fun(Space, Sid) ->
        ok = barrel_session:delete(Space, Sid),
        json(200, #{ok => true})
    end).

touch_session(Req) ->
    with_session(Req, write, fun(Space, Sid) ->
        case barrel_session:touch(Space, Sid) of
            {ok, ExpiresAt} ->
                json(200, #{ok => true, expires_at => ExpiresAt});
            Err ->
                err(Err)
        end
    end).

add_message(Req) ->
    with_session(Req, write, fun(Space, Sid) ->
        with_json(Req, fun(Body) ->
            case message_opts(Body) of
                {ok, Msg} ->
                    case barrel_session:add_message(Space, Sid, Msg) of
                        {ok, MsgId} ->
                            json(201, #{ok => true, message => MsgId});
                        Err ->
                            err(Err)
                    end;
                {error, _} = Err ->
                    err(Err)
            end
        end)
    end).

get_messages(Req) ->
    with_session(Req, read, fun(Space, Sid) ->
        case message_range(Req) of
            {ok, Opts} ->
                {ok, Messages} =
                    barrel_session:get_messages(Space, Sid, Opts),
                json(200, #{messages => [public(M) || M <- Messages]});
            {error, _} = Err ->
                err(Err)
        end
    end).

set_data(Req) ->
    with_session(Req, write, fun(Space, Sid) ->
        with_json(Req, fun(Body) ->
            Key = livery_req:binding(<<"key">>, Req),
            Value = maps:get(<<"value">>, Body, null),
            case barrel_session:set_data(Space, Sid, Key, Value) of
                {ok, _} -> json(200, #{ok => true});
                Err -> err(Err)
            end
        end)
    end).

get_data(Req) ->
    with_session(Req, read, fun(Space, Sid) ->
        Key = livery_req:binding(<<"key">>, Req),
        case barrel_session:get_data(Space, Sid, Key) of
            {ok, Value} -> json(200, #{value => Value});
            Err -> err(Err)
        end
    end).

%%====================================================================
%% Handoffs
%%====================================================================

create_handoff(Req) ->
    with_json(Req, fun(Body) ->
        case maps:get(<<"space">>, Body, undefined) of
            SpaceId when is_binary(SpaceId) ->
                case barrel_server_caps:require(Req, SpaceId, admin) of
                    ok ->
                        do_create_handoff(SpaceId, Body);
                    Err ->
                        err(Err)
                end;
            undefined ->
                err(missing_space)
        end
    end).

do_create_handoff(SpaceId, Body) ->
    case handoff_opts(Body) of
        {ok, Opts} ->
            case barrel_handoff:create(SpaceId, Opts) of
                {ok, #{handoff_id := Hid, token := Token}} ->
                    json(201, #{ok => true, handoff => Hid,
                                token => Token});
                Err ->
                    err(Err)
            end;
        {error, _} = Err ->
            err(Err)
    end.

%% A capability caller only sees its own space's handoffs; the query
%% filters compose on top.
list_handoffs(Req) ->
    Filter0 = maps:from_list(
        [{K, V} || {K, P} <- [{space, <<"space">>},
                              {status, <<"status">>},
                              {to_agent, <<"to_agent">>},
                              {from_agent, <<"from_agent">>}],
                   V <- [param(P, Req)], V =/= undefined]),
    case livery_ext:bearer_token(Req) of
        <<"bsp_", _/binary>> = Token ->
            case barrel_caps:auth_context(Token) of
                {ok, #{space := SpaceId}} ->
                    {ok, Handoffs} =
                        barrel_handoff:list(Filter0#{space => SpaceId}),
                    json(200, #{handoffs => [public(H) || H <- Handoffs]});
                Err ->
                    err(Err)
            end;
        _Global ->
            {ok, Handoffs} = barrel_handoff:list(Filter0),
            json(200, #{handoffs => [public(H) || H <- Handoffs]})
    end.

accept_handoff(Req) ->
    with_json(Req, fun(Body) ->
        case maps:get(<<"token">>, Body, undefined) of
            Token when is_binary(Token) ->
                Opts = #{agent => maps:get(<<"agent">>, Body, <<>>)},
                case barrel_handoff:accept(Token, Opts) of
                    {ok, #{handoff := H, space := #{id := SpaceId},
                           session := Sid}} ->
                        json(200, #{handoff => H, space => SpaceId,
                                    session => Sid});
                    Err ->
                        err(Err)
                end;
            undefined ->
                err(missing_token)
        end
    end).

complete_handoff(Req) ->
    with_json(Req, fun(Body) ->
        case maps:get(<<"token">>, Body, undefined) of
            Token when is_binary(Token) ->
                Opts0 = #{result => maps:get(<<"result">>, Body, <<>>),
                          notes => maps:get(<<"notes">>, Body, <<>>)},
                Opts = case maps:get(<<"revoke">>, Body, true) of
                    Rev when is_boolean(Rev) -> Opts0#{revoke => Rev};
                    _ -> Opts0
                end,
                case barrel_handoff:complete(Token, Opts) of
                    {ok, Done} -> json(200, #{handoff => Done});
                    Err -> err(Err)
                end;
            undefined ->
                err(missing_token)
        end
    end).

%%====================================================================
%% Authorization wrappers
%%====================================================================

with_global(Req, Fun) ->
    case barrel_server_caps:require_global(Req) of
        ok -> Fun();
        Err -> err(Err)
    end.

with_right(Req, Right, Fun) ->
    SpaceId = livery_req:binding(<<"space">>, Req),
    case barrel_server_caps:require(Req, SpaceId, Right) of
        ok -> Fun(SpaceId);
        Err -> err(Err)
    end.

with_space(Req, Right, Fun) ->
    with_right(Req, Right, fun(SpaceId) ->
        case barrel_spaces:open_space(SpaceId) of
            {ok, Space} -> Fun(Space);
            Err -> err(Err)
        end
    end).

with_session(Req, Right, Fun) ->
    with_space(Req, Right, fun(Space) ->
        Fun(Space, livery_req:binding(<<"sid">>, Req))
    end).

%%====================================================================
%% Body and query decoding
%%====================================================================

space_opts(Body) ->
    Opts0 = maps:from_list(
        [{K, V} || {K, P} <- [{label, <<"label">>},
                              {purpose, <<"purpose">>},
                              {owner, <<"owner">>}],
                   V <- [maps:get(P, Body, undefined)], is_binary(V)]),
    Opts1 = maps:merge(Opts0, maps:from_list(
        [{K, V} || {K, P} <- [{session_ttl, <<"session_ttl">>},
                              {ttl_sweep_interval,
                               <<"ttl_sweep_interval">>}],
                   V <- [maps:get(P, Body, undefined)], is_integer(V)])),
    case maps:get(<<"vectordb">>, Body, undefined) of
        Vec when is_map(Vec) -> Opts1#{vectordb => vec_opts(Vec)};
        _ -> Opts1
    end.

%% Only the safe vector knobs cross HTTP; paths and backends beyond
%% memory/disk stay server-side.
vec_opts(Vec) ->
    Opts0 = case maps:get(<<"dimension">>, Vec, undefined) of
        D when is_integer(D), D > 0 -> #{dimension => D};
        _ -> #{}
    end,
    case maps:get(<<"bm25_backend">>, Vec, undefined) of
        <<"memory">> -> Opts0#{bm25_backend => memory};
        <<"disk">> -> Opts0#{bm25_backend => disk};
        _ -> Opts0
    end.

grant_opts(Body) ->
    case rights(maps:get(<<"rights">>, Body, [<<"read">>])) of
        {ok, Rights} ->
            Opts0 = #{rights => Rights,
                      subject => maps:get(<<"subject">>, Body, <<>>),
                      label => maps:get(<<"label">>, Body, <<>>),
                      issued_by => maps:get(<<"issued_by">>, Body, <<>>)},
            case maps:get(<<"expires_at">>, Body, undefined) of
                E when is_integer(E), E >= 0 ->
                    {ok, Opts0#{expires_at => E}};
                undefined ->
                    {ok, Opts0};
                _ ->
                    {error, invalid_expires_at}
            end;
        {error, _} = Err ->
            Err
    end.

rights(L) when is_list(L) ->
    try {ok, [right(R) || R <- L]}
    catch error:_ -> {error, invalid_rights}
    end;
rights(_) ->
    {error, invalid_rights}.

right(<<"read">>) -> read;
right(<<"write">>) -> write;
right(<<"admin">>) -> admin.

session_opts(Body) ->
    Opts0 = case maps:get(<<"agent">>, Body, undefined) of
        A when is_binary(A) -> #{agent => A};
        _ -> #{}
    end,
    Opts1 = case maps:get(<<"ttl">>, Body, undefined) of
        T when is_integer(T), T > 0 -> Opts0#{ttl => T};
        _ -> Opts0
    end,
    Opts2 = case maps:get(<<"data">>, Body, undefined) of
        D when is_map(D) -> Opts1#{data => D};
        _ -> Opts1
    end,
    case maps:get(<<"metadata">>, Body, undefined) of
        M when is_map(M) -> Opts2#{metadata => M};
        _ -> Opts2
    end.

message_opts(#{<<"role">> := Role, <<"content">> := Content} = Body)
  when is_binary(Role) ->
    Msg0 = #{role => Role, content => Content},
    case maps:get(<<"metadata">>, Body, undefined) of
        M when is_map(M) -> {ok, Msg0#{metadata => M}};
        _ -> {ok, Msg0}
    end;
message_opts(_) ->
    {error, invalid_message}.

message_range(Req) ->
    try
        Opts0 = maps:from_list(
            [{K, binary_to_integer(V)}
             || {K, P} <- [{since, <<"since">>},
                           {before, <<"before">>},
                           {limit, <<"limit">>}],
                V <- [param(P, Req)], V =/= undefined]),
        case param(<<"order">>, Req) of
            <<"desc">> -> {ok, Opts0#{order => desc}};
            _ -> {ok, Opts0}
        end
    catch
        error:badarg -> {error, bad_range}
    end.

handoff_opts(Body) ->
    case maps:get(<<"task_name">>, Body, undefined) of
        Task when is_binary(Task) ->
            Opts0 = #{task_name => Task},
            Opts1 = maps:merge(Opts0, maps:from_list(
                [{K, V} || {K, P} <- [{from_agent, <<"from_agent">>},
                                      {to_agent, <<"to_agent">>},
                                      {from_session, <<"from_session">>},
                                      {priority, <<"priority">>}],
                           V <- [maps:get(P, Body, undefined)],
                           is_binary(V)])),
            Opts2 = maps:merge(Opts1, maps:from_list(
                [{K, V} || {K, P} <- [{completed, <<"completed">>},
                                      {pending, <<"pending">>},
                                      {blockers, <<"blockers">>},
                                      {files, <<"files">>}],
                           V <- [maps:get(P, Body, undefined)],
                           is_list(V)])),
            Opts3 = case maps:get(<<"context">>, Body, undefined) of
                undefined -> Opts2;
                Ctx -> Opts2#{context => Ctx}
            end,
            Opts4 = case maps:get(<<"expires_at">>, Body, undefined) of
                E when is_integer(E) -> Opts3#{expires_at => E};
                _ -> Opts3
            end,
            case rights(maps:get(<<"rights">>, Body,
                                 [<<"read">>, <<"write">>])) of
                {ok, Rights} -> {ok, Opts4#{rights => Rights}};
                {error, _} = Err -> Err
            end;
        undefined ->
            {error, task_name_required}
    end.

%%====================================================================
%% Rendering
%%====================================================================

json(Status, Term) ->
    barrel_server_http:json_resp(Status, Term).

with_json(Req, Fun) ->
    barrel_server_http:with_json(Req, Fun).

param(Key, Req) ->
    barrel_server_http:param(Key, Req).

public(Doc) ->
    maps:without([<<"_rev">>], Doc).

err({error, Reason}) -> err(Reason);
err(forbidden) -> json(403, #{error => <<"forbidden">>});
err(wrong_space) -> json(403, #{error => <<"forbidden">>});
err(invalid_token) -> json(401, #{error => <<"unauthorized">>});
err(revoked) -> json(401, #{error => <<"unauthorized">>});
err(expired) -> json(401, #{error => <<"unauthorized">>});
err(space_dropped) -> json(404, #{error => <<"space_dropped">>});
err(already_accepted) -> json(409, #{error => <<"already_accepted">>});
err(already_completed) -> json(409, #{error => <<"already_completed">>});
err(task_name_required) -> json(400, #{error => <<"task_name_required">>});
err(missing_space) -> json(400, #{error => <<"missing_space">>});
err(missing_token) -> json(400, #{error => <<"missing_token">>});
err(invalid_message) -> json(400, #{error => <<"invalid_message">>});
err(invalid_rights) -> json(400, #{error => <<"invalid_rights">>});
err(empty_rights) -> json(400, #{error => <<"invalid_rights">>});
err(invalid_expires_at) -> json(400, #{error => <<"invalid_expires_at">>});
err(bad_range) -> json(400, #{error => <<"bad_range">>});
err(Other) -> barrel_server_http:error_resp(Other).
