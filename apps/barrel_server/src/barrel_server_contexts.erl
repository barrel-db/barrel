%%%-------------------------------------------------------------------
%%% @doc REST adapter for contexts: cards, capabilities, offline mode
%%% and federated queries. Errors use one body everywhere
%%% (`{"error", "message", "hint", "details"}', see barrel_ctx_error);
%%% the MCP tools share the decoders and error mapping below.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_contexts).

-export([routes/0]).
-export([register_card/1, list_cards/1, get_card/1, delete_card/1,
         capabilities/1, get_offline/1, put_offline/1, query/1]).
-export([run_query/2, run_query/3, discover/1, list/1, error_body/1,
         with_body/2, reply/2, reply_error/1]).

routes() ->
    [
        {<<"POST">>,   <<"/contexts">>,               {?MODULE, register_card}},
        {<<"GET">>,    <<"/contexts">>,               {?MODULE, list_cards}},
        {<<"GET">>,    <<"/contexts/_capabilities">>, {?MODULE, capabilities}},
        {<<"GET">>,    <<"/contexts/_offline">>,      {?MODULE, get_offline}},
        {<<"PUT">>,    <<"/contexts/_offline">>,      {?MODULE, put_offline}},
        {<<"POST">>,   <<"/contexts/_query">>,        {?MODULE, query}},
        {<<"GET">>,    <<"/contexts/:id">>,           {?MODULE, get_card}},
        {<<"DELETE">>, <<"/contexts/:id">>,           {?MODULE, delete_card}}
    ] ++ barrel_server_worksets:routes().

%%====================================================================
%% Handlers
%%====================================================================

%% Global auth only: the auth middleware refuses capability tokens here.
register_card(Req) ->
    with_body(Req, fun(Card) -> reply(201, barrel_ctx:register(Card)) end).

%% `?q=' filters by words (discover); without it, list.
list_cards(Req) ->
    Opts = #{include_unlisted =>
                 barrel_server_http:param(<<"unlisted">>, Req) =:= <<"true">>,
             name_prefix =>
                 case barrel_server_http:param(<<"prefix">>, Req) of
                     undefined -> <<>>;
                     Prefix -> Prefix
                 end},
    case barrel_server_http:param(<<"q">>, Req) of
        undefined -> barrel_server_http:json_resp(200, list(Opts));
        Q -> barrel_server_http:json_resp(200, discover(Q))
    end.

get_card(Req) ->
    reply(200, barrel_ctx:inspect(binding(<<"id">>, Req))).

delete_card(Req) ->
    case barrel_ctx:unregister(binding(<<"id">>, Req)) of
        ok -> barrel_server_http:json_resp(200, #{ok => true});
        {error, _} = Err -> reply(200, Err)
    end.

capabilities(_Req) ->
    barrel_server_http:json_resp(200, barrel_ctx:capabilities()).

get_offline(_Req) ->
    barrel_server_http:json_resp(200, #{offline => barrel_ctx:offline()}).

%% Node offline mode: remote members are skipped, never contacted.
put_offline(Req) ->
    with_body(Req, fun
        (#{<<"offline">> := Flag}) when is_boolean(Flag) ->
            ok = barrel_ctx:set_offline(Flag),
            barrel_server_http:json_resp(200, #{offline => Flag});
        (_Body) ->
            reply_error({invalid_argument,
                         #{field => offline, expected => <<"true or false">>}})
    end).

query(Req) ->
    with_body(Req, fun(Body) ->
        Authorize = barrel_server_auth:member_authorizer(Req),
        Global = barrel_server_auth:is_global(Req),
        reply(200, run_query(Body, Authorize, Global))
    end).

%%====================================================================
%% Shared with MCP
%%====================================================================

%% @doc Decode a JSON request body and run it. `Authorize' checks each
%% local member db for the calling principal.
-spec run_query(map(), fun((binary()) -> ok | {error, term()})) ->
    {ok, map()} | {error, term()}.
run_query(Body, Authorize) ->
    run_query(Body, Authorize, true).

%% @doc Like run_query/2; a working set needs a global principal (its
%% local copies and remote legs are not scoped to a space).
-spec run_query(map(), fun((binary()) -> ok | {error, term()}), boolean()) ->
    {ok, map()} | {error, term()}.
run_query(#{<<"working_set">> := _}, _Authorize, false) ->
    {error, {forbidden, #{operation => <<"query a working set">>}}};
run_query(Body, Authorize, _Global) when is_map(Body) ->
    Req = decode(Body),
    barrel_ctx:query(Req#{authorize => Authorize,
                          open_opts => barrel_server_dbs:ensure_opts()});
run_query(_Body, _Authorize, _Global) ->
    {error, {invalid_argument, #{}}}.

-spec list(map()) -> map().
list(Opts) ->
    {ok, Cards} = barrel_ctx:list(Opts),
    #{contexts => Cards}.

%% @doc Discover answer: matching cards and a summary.
-spec discover(binary()) -> map().
discover(Q) ->
    {ok, Cards} = barrel_ctx:discover(Q, #{}),
    {ok, All} = barrel_ctx:list(#{}),
    #{contexts => Cards,
      summary => barrel_ctx_explain:discover(Cards, Q, length(All))}.

%% @doc HTTP status and JSON body for a contexts error.
-spec error_body(term()) -> {pos_integer(), map()}.
error_body(Reason) ->
    #{error := Code} = Body = barrel_ctx_error:to_map(Reason),
    {proplists:get_value(Code, barrel_ctx_error:codes(), 500), Body}.

%% @doc Read a JSON object body; a body that is not one answers the
%% contexts error shape.
-spec with_body(term(), fun((map()) -> term())) -> term().
with_body(Req, Fun) ->
    case barrel_server_http:read_json(Req) of
        {ok, Body} when is_map(Body) ->
            Fun(Body);
        _ ->
            reply_error({invalid_argument,
                         #{field => body, expected => <<"a JSON object">>}})
    end.

-spec reply(pos_integer(), ok | {ok, map()} | {error, term()}) -> term().
reply(Status, {ok, Map}) -> barrel_server_http:json_resp(Status, Map);
reply(_Status, {error, Reason}) -> reply_error(Reason).

-spec reply_error(term()) -> term().
reply_error(Reason) ->
    {Status, Body} = error_body(Reason),
    barrel_server_http:json_resp(Status, Body).

%%====================================================================
%% Internal
%%====================================================================

%% Path segments arrive percent-encoded (a name may hold a slash).
binding(Name, Req) ->
    uri_string:percent_decode(livery_req:binding(Name, Req)).

%% Known JSON fields to request keys; barrel_ctx checks the values.
decode(Body) ->
    Fields = [{<<"query">>, query}, {<<"contexts">>, contexts},
              {<<"merge">>, merge}, {<<"params">>, params},
              {<<"deadline_ms">>, deadline_ms},
              {<<"per_context_timeout_ms">>, per_context_timeout_ms},
              {<<"max_parallel">>, max_parallel},
              {<<"working_set">>, working_set}, {<<"offline">>, offline},
              {<<"continuation">>, continuation}],
    decode(Fields, Body, #{}).

decode([], _Body, Acc) ->
    Acc;
decode([{Json, Key} | Rest], Body, Acc) ->
    case maps:find(Json, Body) of
        {ok, null} -> decode(Rest, Body, Acc);
        {ok, Value} -> decode(Rest, Body, Acc#{Key => Value});
        error -> decode(Rest, Body, Acc)
    end.
