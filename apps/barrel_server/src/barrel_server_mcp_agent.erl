%%%-------------------------------------------------------------------
%%% @doc The agent-layer MCP tools: spaces, capability grants,
%%% sessions, and handoffs, calling barrel_spaces / barrel_caps /
%%% barrel_session / barrel_handoff directly. Authorization mirrors
%%% the REST surface: space creation is management-only (global
%%% principals), grant administration needs admin on the space,
%%% session reads/writes follow the right ladder, and handoff
%%% accept/complete need nothing but the handoff token: possession is
%%% the right.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_agent).

-export([register_all/0, unregister_all/0]).

%% Tool handlers (invoked by the barrel_mcp registry as M:F(Args, Ctx))
-export([
    space_create/2,
    space_info/2,
    space_grant/2,
    space_revoke/2,
    session_create/2,
    session_touch/2,
    session_add_message/2,
    session_get_messages/2,
    handoff_create/2,
    handoff_accept/2,
    handoff_complete/2,
    handoff_list/2
]).

%%====================================================================
%% Registration
%%====================================================================

register_all() ->
    lists:foreach(
        fun({Name, Fun, Opts}) ->
            ok = barrel_mcp:unreg_tool(Name),
            ok = barrel_mcp:reg_tool(Name, ?MODULE, Fun,
                                     Opts#{validate_input => true})
        end, specs()).

unregister_all() ->
    lists:foreach(
        fun({Name, _Fun, _Opts}) -> barrel_mcp:unreg_tool(Name) end,
        specs()).

specs() ->
    [
        {<<"space_create">>, space_create, #{
            description => <<"Create a space: a shared context "
                             "database agents reach with capability "
                             "tokens. Management only.">>,
            input_schema => schema(#{<<"label">> => str(),
                                     <<"purpose">> => str(),
                                     <<"owner">> => str(),
                                     <<"session_ttl">> => int(),
                                     <<"vectordb">> => obj()}, [])
        }},
        {<<"space_info">>, space_info, #{
            description => <<"The registry document of a space.">>,
            input_schema => schema(#{<<"space">> => str()},
                                   [<<"space">>]),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"space_grant">>, space_grant, #{
            description => <<"Issue a capability token for a space "
                             "(rights: read, write, admin). The token "
                             "is shown once.">>,
            input_schema => schema(#{<<"space">> => str(),
                                     <<"rights">> => arr(),
                                     <<"subject">> => str(),
                                     <<"label">> => str(),
                                     <<"expires_at">> => int()},
                                   [<<"space">>])
        }},
        {<<"space_revoke">>, space_revoke, #{
            description => <<"Revoke a grant of a space by token id.">>,
            input_schema => schema(#{<<"space">> => str(),
                                     <<"token_id">> => str()},
                                   [<<"space">>, <<"token_id">>]),
            annotations => #{<<"destructiveHint">> => true,
                             <<"idempotentHint">> => true}
        }},
        {<<"session_create">>, session_create, #{
            description => <<"Create a working session in a space "
                             "(sliding TTL).">>,
            input_schema => schema(#{<<"space">> => str(),
                                     <<"agent">> => str(),
                                     <<"ttl">> => int(),
                                     <<"data">> => obj()},
                                   [<<"space">>])
        }},
        {<<"session_touch">>, session_touch, #{
            description => <<"Slide a session's TTL.">>,
            input_schema => schema(#{<<"space">> => str(),
                                     <<"session">> => str()},
                                   [<<"space">>, <<"session">>])
        }},
        {<<"session_add_message">>, session_add_message, #{
            description => <<"Append a message to a session's "
                             "history.">>,
            input_schema => schema(#{<<"space">> => str(),
                                     <<"session">> => str(),
                                     <<"role">> => str(),
                                     <<"content">> => #{},
                                     <<"metadata">> => obj()},
                                   [<<"space">>, <<"session">>,
                                    <<"role">>, <<"content">>])
        }},
        {<<"session_get_messages">>, session_get_messages, #{
            description => <<"A session's messages in chronological "
                             "order (since/before unix ms, limit, "
                             "order asc|desc).">>,
            input_schema => schema(#{<<"space">> => str(),
                                     <<"session">> => str(),
                                     <<"since">> => int(),
                                     <<"before">> => int(),
                                     <<"limit">> => int(),
                                     <<"order">> => str()},
                                   [<<"space">>, <<"session">>]),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"handoff_create">>, handoff_create, #{
            description => <<"Hand work off: issues a capability for "
                             "the space and returns its token once; "
                             "whoever holds the token accepts.">>,
            input_schema => schema(#{<<"space">> => str(),
                                     <<"task_name">> => str(),
                                     <<"from_agent">> => str(),
                                     <<"to_agent">> => str(),
                                     <<"from_session">> => str(),
                                     <<"context">> => #{},
                                     <<"pending">> => arr(),
                                     <<"completed">> => arr(),
                                     <<"rights">> => arr(),
                                     <<"expires_at">> => int()},
                                   [<<"space">>, <<"task_name">>])
        }},
        {<<"handoff_accept">>, handoff_accept, #{
            description => <<"Accept a handoff by presenting its "
                             "token: opens the shared space and "
                             "creates a session in it (context is "
                             "read in place).">>,
            input_schema => schema(#{<<"token">> => str(),
                                     <<"agent">> => str()},
                                   [<<"token">>])
        }},
        {<<"handoff_complete">>, handoff_complete, #{
            description => <<"Complete a handoff (revokes its token "
                             "by default).">>,
            input_schema => schema(#{<<"token">> => str(),
                                     <<"result">> => #{},
                                     <<"notes">> => str(),
                                     <<"revoke">> => bool()},
                                   [<<"token">>])
        }},
        {<<"handoff_list">>, handoff_list, #{
            description => <<"Handoffs, filtered by space, status, "
                             "to_agent, from_agent (capability "
                             "callers see their own space only).">>,
            input_schema => schema(#{<<"space">> => str(),
                                     <<"status">> => str(),
                                     <<"to_agent">> => str(),
                                     <<"from_agent">> => str()}, []),
            annotations => #{<<"readOnlyHint">> => true}
        }}
    ].

%%====================================================================
%% Spaces
%%====================================================================

space_create(Args, Ctx) ->
    with_global(Ctx, fun() ->
        case barrel_spaces:create_space(space_opts(Args)) of
            {ok, #{id := Id}} -> reply(#{ok => true, space => Id});
            Err -> err(Err)
        end
    end).

space_info(#{<<"space">> := SpaceId}, Ctx) ->
    with_right(SpaceId, Ctx, read, fun() ->
        case barrel_spaces:space_info(SpaceId) of
            {ok, Info} -> reply(maps:without([<<"_rev">>], Info));
            Err -> err(Err)
        end
    end).

space_grant(#{<<"space">> := SpaceId} = Args, Ctx) ->
    with_right(SpaceId, Ctx, admin, fun() ->
        case rights(maps:get(<<"rights">>, Args, [<<"read">>])) of
            {ok, Rights} ->
                Opts0 = #{rights => Rights,
                          subject => maps:get(<<"subject">>, Args, <<>>),
                          label => maps:get(<<"label">>, Args, <<>>)},
                Opts = case maps:get(<<"expires_at">>, Args, undefined) of
                    E when is_integer(E), E >= 0 ->
                        Opts0#{expires_at => E};
                    _ ->
                        Opts0
                end,
                case barrel_caps:grant(SpaceId, Opts) of
                    {ok, Token, Grant} ->
                        reply(#{token => Token, grant => Grant});
                    Err ->
                        err(Err)
                end;
            {error, _} = Err ->
                err(Err)
        end
    end).

space_revoke(#{<<"space">> := SpaceId, <<"token_id">> := TokenId},
             Ctx) ->
    with_right(SpaceId, Ctx, admin, fun() ->
        {ok, Grants} = barrel_caps:list(SpaceId),
        case [G || #{<<"token_id">> := T} = G <- Grants,
                   T =:= TokenId] of
            [_] ->
                ok = barrel_caps:revoke(TokenId),
                reply(#{ok => true});
            [] ->
                err(not_found)
        end
    end).

%%====================================================================
%% Sessions
%%====================================================================

session_create(#{<<"space">> := SpaceId} = Args, Ctx) ->
    with_space(SpaceId, Ctx, write, fun(Space) ->
        Opts0 = case maps:get(<<"agent">>, Args, undefined) of
            A when is_binary(A) -> #{agent => A};
            _ -> #{}
        end,
        Opts1 = case maps:get(<<"ttl">>, Args, undefined) of
            T when is_integer(T), T > 0 -> Opts0#{ttl => T};
            _ -> Opts0
        end,
        Opts = case maps:get(<<"data">>, Args, undefined) of
            D when is_map(D) -> Opts1#{data => D};
            _ -> Opts1
        end,
        case barrel_session:create(Space, Opts) of
            {ok, Sid} -> reply(#{ok => true, session => Sid});
            Err -> err(Err)
        end
    end).

session_touch(#{<<"space">> := SpaceId, <<"session">> := Sid}, Ctx) ->
    with_space(SpaceId, Ctx, write, fun(Space) ->
        case barrel_session:touch(Space, Sid) of
            {ok, ExpiresAt} ->
                reply(#{ok => true, expires_at => ExpiresAt});
            Err ->
                err(Err)
        end
    end).

session_add_message(#{<<"space">> := SpaceId, <<"session">> := Sid,
                      <<"role">> := Role, <<"content">> := Content}
                        = Args, Ctx) ->
    with_space(SpaceId, Ctx, write, fun(Space) ->
        Msg0 = #{role => Role, content => Content},
        Msg = case maps:get(<<"metadata">>, Args, undefined) of
            M when is_map(M) -> Msg0#{metadata => M};
            _ -> Msg0
        end,
        case barrel_session:add_message(Space, Sid, Msg) of
            {ok, MsgId} -> reply(#{ok => true, message => MsgId});
            Err -> err(Err)
        end
    end).

session_get_messages(#{<<"space">> := SpaceId, <<"session">> := Sid}
                         = Args, Ctx) ->
    with_space(SpaceId, Ctx, read, fun(Space) ->
        Opts0 = maps:from_list(
            [{K, V} || {K, P} <- [{since, <<"since">>},
                                  {before, <<"before">>},
                                  {limit, <<"limit">>}],
                       V <- [maps:get(P, Args, undefined)],
                       is_integer(V)]),
        Opts = case maps:get(<<"order">>, Args, undefined) of
            <<"desc">> -> Opts0#{order => desc};
            _ -> Opts0
        end,
        {ok, Messages} = barrel_session:get_messages(Space, Sid, Opts),
        reply(#{messages =>
                    [maps:without([<<"_rev">>], M) || M <- Messages]})
    end).

%%====================================================================
%% Handoffs
%%====================================================================

handoff_create(#{<<"space">> := SpaceId, <<"task_name">> := Task}
                   = Args, Ctx) ->
    with_right(SpaceId, Ctx, admin, fun() ->
        Opts0 = #{task_name => Task},
        Opts1 = maps:merge(Opts0, maps:from_list(
            [{K, V} || {K, P} <- [{from_agent, <<"from_agent">>},
                                  {to_agent, <<"to_agent">>},
                                  {from_session, <<"from_session">>}],
                       V <- [maps:get(P, Args, undefined)],
                       is_binary(V)])),
        Opts2 = maps:merge(Opts1, maps:from_list(
            [{K, V} || {K, P} <- [{pending, <<"pending">>},
                                  {completed, <<"completed">>}],
                       V <- [maps:get(P, Args, undefined)],
                       is_list(V)])),
        Opts3 = case maps:get(<<"context">>, Args, undefined) of
            undefined -> Opts2;
            Context -> Opts2#{context => Context}
        end,
        Opts4 = case maps:get(<<"expires_at">>, Args, undefined) of
            E when is_integer(E) -> Opts3#{expires_at => E};
            _ -> Opts3
        end,
        case rights(maps:get(<<"rights">>, Args,
                             [<<"read">>, <<"write">>])) of
            {ok, Rights} ->
                case barrel_handoff:create(SpaceId,
                                           Opts4#{rights => Rights}) of
                    {ok, #{handoff_id := Hid, token := Token}} ->
                        reply(#{ok => true, handoff => Hid,
                                token => Token});
                    Err ->
                        err(Err)
                end;
            {error, _} = Err ->
                err(Err)
        end
    end).

handoff_accept(#{<<"token">> := Token} = Args, _Ctx) ->
    Opts = #{agent => maps:get(<<"agent">>, Args, <<>>)},
    case barrel_handoff:accept(Token, Opts) of
        {ok, #{handoff := H, space := #{id := SpaceId},
               session := Sid}} ->
            reply(#{handoff => H, space => SpaceId, session => Sid});
        Err ->
            err(Err)
    end.

handoff_complete(#{<<"token">> := Token} = Args, _Ctx) ->
    Opts0 = #{result => maps:get(<<"result">>, Args, <<>>),
              notes => maps:get(<<"notes">>, Args, <<>>)},
    Opts = case maps:get(<<"revoke">>, Args, true) of
        Rev when is_boolean(Rev) -> Opts0#{revoke => Rev};
        _ -> Opts0
    end,
    case barrel_handoff:complete(Token, Opts) of
        {ok, Done} -> reply(#{handoff => Done});
        Err -> err(Err)
    end.

handoff_list(Args, Ctx) ->
    Filter0 = maps:from_list(
        [{K, V} || {K, P} <- [{space, <<"space">>},
                              {status, <<"status">>},
                              {to_agent, <<"to_agent">>},
                              {from_agent, <<"from_agent">>}],
                   V <- [maps:get(P, Args, undefined)],
                   is_binary(V)]),
    Filter = case principal_space(Ctx) of
        undefined -> Filter0;
        Space -> Filter0#{space => Space}
    end,
    {ok, Handoffs} = barrel_handoff:list(Filter),
    reply(#{handoffs =>
                [maps:without([<<"_rev">>], H) || H <- Handoffs]}).

%%====================================================================
%% Authorization wrappers
%%====================================================================

%% management-only: a capability principal is scoped to one space and
%% never manages the space collection
with_global(Ctx, Fun) ->
    case principal_space(Ctx) of
        undefined -> Fun();
        _Space -> {tool_error, #{error => <<"forbidden">>}}
    end.

with_right(SpaceId, Ctx, Right, Fun) ->
    case barrel_server_mcp_auth:allow(Ctx, SpaceId, Right) of
        ok -> Fun();
        {error, forbidden} ->
            {tool_error, #{error => <<"forbidden">>, space => SpaceId}}
    end.

with_space(SpaceId, Ctx, Right, Fun) ->
    with_right(SpaceId, Ctx, Right, fun() ->
        case barrel_spaces:open_space(SpaceId) of
            {ok, Space} -> Fun(Space);
            Err -> err(Err)
        end
    end).

principal_space(Ctx) ->
    case maps:get(auth_info, Ctx, undefined) of
        undefined ->
            undefined;
        AuthInfo ->
            case maps:get(claims, AuthInfo, #{}) of
                #{<<"space">> := Space} -> Space;
                _ -> undefined
            end
    end.

%%====================================================================
%% Internal
%%====================================================================

space_opts(Args) ->
    Opts0 = maps:from_list(
        [{K, V} || {K, P} <- [{label, <<"label">>},
                              {purpose, <<"purpose">>},
                              {owner, <<"owner">>}],
                   V <- [maps:get(P, Args, undefined)], is_binary(V)]),
    Opts1 = case maps:get(<<"session_ttl">>, Args, undefined) of
        T when is_integer(T), T > 0 -> Opts0#{session_ttl => T};
        _ -> Opts0
    end,
    case maps:get(<<"vectordb">>, Args, undefined) of
        Vec when is_map(Vec) -> Opts1#{vectordb => vec_opts(Vec)};
        _ -> Opts1
    end.

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

rights(L) when is_list(L) ->
    try {ok, [right(R) || R <- L]}
    catch error:_ -> {error, invalid_rights}
    end;
rights(_) ->
    {error, invalid_rights}.

right(<<"read">>) -> read;
right(<<"write">>) -> write;
right(<<"admin">>) -> admin.

%% Tool results are JSON-encoded here: the framework passes a map
%% carrying a <<"type">> key through as a pre-built content block, and
%% space/handoff/session documents all carry one.
reply(Map) ->
    iolist_to_binary(json:encode(Map)).

err({error, Reason}) ->
    err(Reason);
err(Reason) when is_atom(Reason) ->
    {tool_error, #{error => atom_to_binary(Reason, utf8)}};
err(Reason) ->
    {tool_error,
     #{error => iolist_to_binary(io_lib:format("~p", [Reason]))}}.

schema(Props, Required) ->
    Base = #{<<"type">> => <<"object">>,
             <<"properties">> => Props},
    case Required of
        [] -> Base;
        _ -> Base#{<<"required">> => Required}
    end.

str() -> #{<<"type">> => <<"string">>}.
int() -> #{<<"type">> => <<"integer">>}.
obj() -> #{<<"type">> => <<"object">>}.
arr() -> #{<<"type">> => <<"array">>}.
bool() -> #{<<"type">> => <<"boolean">>}.
