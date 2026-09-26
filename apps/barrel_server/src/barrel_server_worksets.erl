%%%-------------------------------------------------------------------
%%% @doc REST adapter for working sets: create, list, read, delete,
%%% attach, detach, materialize a retrieved set, import a snapshot.
%%% Global auth only (the auth middleware refuses capability tokens on
%%% /worksets). The MCP tools share the decoders below, so both
%%% surfaces return the same maps.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_worksets).

-compile({no_auto_import, [get/1]}).

-export([routes/0]).
-export([create/1, list/1, get/1, delete/1, attach/1, detach/1,
         materialize/1, import/1]).
-export([do_create/1, do_attach/2, do_materialize/2, do_import/2]).

routes() ->
    [
        {<<"POST">>,   <<"/worksets">>,                  {?MODULE, create}},
        {<<"GET">>,    <<"/worksets">>,                  {?MODULE, list}},
        {<<"GET">>,    <<"/worksets/:ws">>,              {?MODULE, get}},
        {<<"DELETE">>, <<"/worksets/:ws">>,              {?MODULE, delete}},
        {<<"POST">>,   <<"/worksets/:ws/members">>,      {?MODULE, attach}},
        {<<"DELETE">>, <<"/worksets/:ws/members/:ctx">>, {?MODULE, detach}},
        {<<"POST">>,   <<"/worksets/:ws/_materialize">>, {?MODULE, materialize}},
        {<<"POST">>,   <<"/worksets/:ws/_import">>,      {?MODULE, import}}
    ].

%%====================================================================
%% Handlers
%%====================================================================

create(Req) ->
    with_body(Req, fun(Body) -> reply(201, do_create(Body)) end).

list(_Req) ->
    barrel_server_http:json_resp(200, #{working_sets => barrel_ctx:list_ws()}).

get(Req) ->
    reply(200, barrel_ctx:get_ws(ws(Req))).

delete(Req) ->
    case barrel_ctx:delete_ws(ws(Req)) of
        ok -> barrel_server_http:json_resp(200, #{ok => true});
        {error, _} = Err -> reply(200, Err)
    end.

attach(Req) ->
    with_body(Req, fun(Body) -> reply(200, do_attach(ws(Req), Body)) end).

detach(Req) ->
    Ctx = uri_string:percent_decode(livery_req:binding(<<"ctx">>, Req)),
    reply(200, barrel_ctx:detach(ws(Req), Ctx)).

materialize(Req) ->
    with_body(Req, fun(Body) ->
        reply(200, do_materialize(ws(Req), Body))
    end).

import(Req) ->
    with_body(Req, fun(Body) -> reply(200, do_import(ws(Req), Body)) end).

%%====================================================================
%% Shared with MCP (`WsId' may be `new')
%%====================================================================

%% @doc Create a working set from `{owner, budget}' and return its view.
-spec do_create(map()) -> {ok, map()} | {error, term()}.
do_create(Body) ->
    Opts0 = case maps:get(<<"owner">>, Body, null) of
        Owner when is_binary(Owner) -> #{owner => Owner};
        _ -> #{}
    end,
    Opts = case maps:get(<<"budget">>, Body, null) of
        null -> Opts0;
        B -> Opts0#{budget => B}
    end,
    case barrel_ctx:create_ws(Opts) of
        {ok, WsId} -> barrel_ctx:get_ws(WsId);
        {error, _} = Err -> Err
    end.

-spec do_attach(barrel_ctx:ws_ref(), map()) -> {ok, map()} | {error, term()}.
do_attach(WsId, #{<<"context">> := Ctx} = Body) when is_binary(Ctx) ->
    case mode(maps:get(<<"mode">>, Body, null)) of
        {ok, Opts0} ->
            Opts = case maps:get(<<"credential_ref">>, Body, null) of
                Ref when is_binary(Ref) -> Opts0#{credential_ref => Ref};
                _ -> Opts0
            end,
            barrel_ctx:attach(WsId, Ctx, Opts);
        {error, _} = Err ->
            Err
    end;
do_attach(_WsId, _Body) ->
    {error, {invalid_argument, #{field => context,
                                 expected => <<"a context name or id">>}}}.

-spec do_materialize(barrel_ctx:ws_ref(), map()) ->
    {ok, map()} | {error, term()}.
do_materialize(WsId, #{<<"from_query">> := #{<<"query">> := Q,
                                           <<"contexts">> := Ctxs}} = Body)
        when is_binary(Q), is_list(Ctxs) ->
    Req0 = #{from_query => #{query => Q, contexts => Ctxs},
             open_opts => barrel_server_dbs:ensure_opts()},
    Req = lists:foldl(
        fun({Json, Key}, Acc) ->
            case maps:get(Json, Body, null) of
                null -> Acc;
                V -> Acc#{Key => V}
            end
        end, Req0, [{<<"max_bytes">>, max_bytes}, {<<"timeout_ms">>, timeout},
                    {<<"embedding">>, embedding}, {<<"offline">>, offline}]),
    Req1 = case maps:get(<<"include">>, Body, null) of
        #{} = Inc -> Req#{include => include(Inc)};
        _ -> Req
    end,
    barrel_ctx:materialize(WsId, Req1);
do_materialize(_WsId, _Body) ->
    {error, {invalid_argument,
             #{field => from_query,
               expected => <<"{\"query\": \"SELECT b.id ...\", "
                             "\"contexts\": [name or id, ...]}">>}}}.

-spec do_import(barrel_ctx:ws_ref(), map()) -> {ok, map()} | {error, term()}.
do_import(WsId, #{<<"dir">> := Dir} = Body) when is_binary(Dir) ->
    Opts = case maps:get(<<"name">>, Body, null) of
        Name when is_binary(Name) -> #{name => Name};
        _ -> #{}
    end,
    barrel_ctx:import(WsId, binary_to_list(Dir), Opts);
do_import(_WsId, _Body) ->
    {error, {invalid_argument,
             #{field => dir, expected => <<"the export directory path on "
                                           "this node">>}}}.

%%====================================================================
%% Internal
%%====================================================================

mode(null) -> {ok, #{}};
mode(<<"local">>) -> {ok, #{mode => local}};
mode(<<"remote">>) -> {ok, #{mode => remote}};
mode(_Other) ->
    {error, {invalid_argument, #{field => mode,
                                 allowed => [<<"local">>, <<"remote">>]}}}.

include(Inc) ->
    maps:from_list([{K, V} || K <- [embeddings, attachments],
                              V <- [maps:get(atom_to_binary(K), Inc,
                                             undefined)],
                              is_boolean(V)]).

ws(Req) ->
    livery_req:binding(<<"ws">>, Req).

with_body(Req, Fun) ->
    barrel_server_contexts:with_body(Req, Fun).

reply(Status, Result) ->
    barrel_server_contexts:reply(Status, Result).
