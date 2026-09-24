%%%-------------------------------------------------------------------
%%% @doc MCP tools for contexts. Each description says what the tool
%%% does, when to use it, what it returns, with one example; every
%%% input property has a description. Arguments are checked here, so a
%%% bad call answers the contexts error shape (code, message, hint,
%%% details) like every other failure.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_contexts).

-export([register_all/0, unregister_all/0, specs/0]).
-export([context_capabilities/2, context_list/2, context_discover/2,
         context_inspect/2, context_query/2, context_attach/2,
         context_detach/2, context_materialize/2, context_import/2,
         context_working_sets/2, context_working_set_delete/2,
         context_offline/2]).

%%====================================================================
%% Registration
%%====================================================================

register_all() ->
    lists:foreach(
        fun({Name, Fun, Opts}) ->
            ok = barrel_mcp:unreg_tool(Name),
            ok = barrel_mcp:reg_tool(Name, ?MODULE, Fun,
                                     (maps:remove(check, Opts))#{
                                       validate_input => false})
        end, specs()).

unregister_all() ->
    lists:foreach(fun({Name, _Fun, _Opts}) -> barrel_mcp:unreg_tool(Name) end,
                  specs()).

%% @doc Tool name, handler and registration options.
-spec specs() -> [{binary(), atom(), map()}].
specs() ->
    [
     {<<"context_capabilities">>, context_capabilities, #{
        description =>
            <<"What a context query may look like: the accepted statement "
              "shapes with examples, the merges, the limits (contexts per "
              "query, largest LIMIT, timeouts), working-set budgets and "
              "whether the node is offline. Call it before a first "
              "context_query. No arguments.">>,
        input_schema => schema(#{}, []),
        annotations => read_only()}},
     {<<"context_list">>, context_list, #{
        description =>
            <<"List the registered contexts: named datasets, each a "
              "database on this node or on a remote Barrel server. "
              "Returns {contexts: [card]}; pass a card's name (or id) to "
              "the other context tools. Example: {\"prefix\": \"otp/\"}.">>,
        input_schema => schema(
            #{<<"prefix">> => str(<<"Only names starting with this text, "
                                    "e.g. \"otp/\".">>),
              <<"unlisted">> => bool(<<"Include cards registered as "
                                       "unlisted.">>, false)}, []),
        annotations => read_only()}},
     {<<"context_discover">>, context_discover, #{
        description =>
            <<"Find contexts about a topic: every word of q must appear in "
              "a card's name, title, description or topics (a plural "
              "matches its stem). A filter, not a ranking: other contexts "
              "may still hold answers. Returns {contexts, summary}. "
              "Example: {\"q\": \"release\"}.">>,
        input_schema => schema(
            #{<<"q">> => (str(<<"Words to look for, e.g. \"release "
                                "handling\".">>))#{
                           <<"examples">> => [<<"release">>, <<"testing">>]}},
            [<<"q">>]),
        annotations => read_only()}},
     {<<"context_inspect">>, context_inspect, #{
        description =>
            <<"Read one context card: where it lives (local db, or remote "
              "endpoint and db), title, topics, embedding. Example: "
              "{\"context\": \"otp/sasl\"}.">>,
        input_schema => schema(#{<<"context">> => ctx_ref()},
                               [<<"context">>]),
        annotations => read_only()}},
     {<<"context_query">>, context_query, #{
        description =>
            <<"Run one BQL statement over several contexts (names or ids) "
              "or over a working set. Returns rows (or groups per "
              "context), one sources entry per context saying whether it "
              "answered and what its answer covers, and a summary: read "
              "it first. Row queries need LIMIT (<= 1000); add ORDER BY a "
              "selected field to merge rows in order. isError is true "
              "when no context answered. Example: {\"query\": \"SELECT id, "
              "lines FROM c ORDER BY lines DESC LIMIT 10\", \"contexts\": "
              "[\"otp/tools\", \"otp/sasl\"]}.">>,
        input_schema => schema(
            #{<<"query">> =>
                  (str(<<"One BQL statement. Strings use single quotes. "
                         "Shapes: see context_capabilities.">>))#{
                    <<"examples">> =>
                        [<<"SELECT id, path FROM c ORDER BY path LIMIT 20">>,
                         <<"SELECT b.id, b._score FROM bm25_top_k('release "
                           "upgrade', k => 5) AS b">>]},
              <<"contexts">> =>
                  #{<<"type">> => <<"array">>,
                    <<"items">> => #{<<"type">> => <<"string">>},
                    <<"minItems">> => 1, <<"maxItems">> => 8,
                    <<"description">> =>
                        <<"Context names or ids. Give this or "
                          "working_set, not both.">>,
                    <<"examples">> => [[<<"otp/tools">>, <<"otp/sasl">>]]},
              <<"working_set">> =>
                  str(<<"A working-set id (ws_...) instead of contexts: "
                        "queries its members, local copies first.">>),
              <<"merge">> =>
                  (str(<<"Leave unset for the default: ordered with ORDER "
                         "BY, grouped otherwise, score for comparable "
                         "vector_top_k.">>))#{
                    <<"enum">> => [<<"ordered">>, <<"grouped">>,
                                   <<"interleave">>, <<"score">>]},
              <<"offline">> =>
                  bool(<<"true: answer from local copies only, never "
                         "contact a remote server. Default: the node's "
                         "offline mode.">>, undefined),
              <<"params">> =>
                  obj(<<"Values for $name parameters in the statement.">>),
              <<"deadline_ms">> =>
                  int(<<"Deadline for the whole request, at most 60000.">>,
                      5000),
              <<"per_context_timeout_ms">> =>
                  int(<<"Deadline per context, at most 60000.">>, 4000),
              <<"max_parallel">> =>
                  int(<<"Remote contexts queried at once, at most 16.">>,
                      8)},
            [<<"query">>]),
        annotations => read_only()}},
     {<<"context_attach">>, context_attach, #{
        description =>
            <<"Add a context to a working set: the contexts you work with, "
              "which context_materialize and context_import can copy "
              "locally for offline use. Omit working_set to create one; "
              "the answer's id is the working set to pass next. Logical: "
              "nothing is opened or copied. Example: {\"context\": "
              "\"otp/tools\"}.">>,
        input_schema => schema(
            #{<<"working_set">> => ws_ref(),
              <<"context">> => ctx_ref(),
              <<"mode">> =>
                  (str(<<"Which location of the card to use. Default: "
                         "local if the card has one, else remote.">>))#{
                    <<"enum">> => [<<"local">>, <<"remote">>]},
              <<"credential_ref">> =>
                  str(<<"Name of a token in the node's ctx_credentials, "
                        "for a remote location.">>)},
            [<<"context">>])}},
     {<<"context_detach">>, context_detach, #{
        description =>
            <<"Remove a context from a working set; a saved slice of it is "
              "deleted. Returns the working set. Example: {\"working_set\": "
              "\"ws_...\", \"context\": \"otp/eunit\"}.">>,
        input_schema => schema(#{<<"working_set">> => ws_id(),
                                 <<"context">> => ctx_ref()},
                               [<<"working_set">>, <<"context">>]),
        annotations => #{<<"destructiveHint">> => true}}},
     {<<"context_materialize">>, context_materialize, #{
        description =>
            <<"Save the documents a query returns into a working set so "
              "they answer offline: one frozen slice per answering "
              "context, holding exactly those documents (by id). Omit "
              "working_set to create one. A context already in the "
              "working set must be detached first. Example: "
              "{\"from_query\": {\"query\": \"SELECT b.id FROM "
              "bm25_top_k('test', k => 5) AS b\", \"contexts\": "
              "[\"otp/eunit\"]}}.">>,
        input_schema => schema(
            #{<<"working_set">> => ws_ref(),
              <<"from_query">> =>
                  #{<<"type">> => <<"object">>,
                    <<"description">> =>
                        <<"The query whose rows (by id) are saved.">>,
                    <<"properties">> =>
                        #{<<"query">> => str(<<"A BQL statement returning "
                                               "the id of each document.">>),
                          <<"contexts">> =>
                              #{<<"type">> => <<"array">>,
                                <<"items">> => #{<<"type">> => <<"string">>},
                                <<"description">> =>
                                    <<"Context names or ids.">>}},
                    <<"required">> => [<<"query">>, <<"contexts">>]},
              <<"include">> =>
                  #{<<"type">> => <<"object">>,
                    <<"description">> => <<"What to copy besides documents.">>,
                    <<"properties">> =>
                        #{<<"embeddings">> =>
                              bool(<<"Copy vectors (needed for vector "
                                     "search on the slice).">>, true),
                          <<"attachments">> =>
                              bool(<<"Not supported; true is refused.">>,
                                   false)}},
              <<"max_bytes">> =>
                  int(<<"Refuse the slice when the fetched documents are "
                        "larger than this.">>, undefined)},
            [<<"from_query">>])}},
     {<<"context_import">>, context_import, #{
        description =>
            <<"Import a published snapshot (a directory written by an "
              "export, holding manifest.json) into this node, read only, "
              "as a working-set member that answers offline for that "
              "generation. Omit working_set to create one. Example: "
              "{\"dir\": \"/srv/export_sasl_g1\"}.">>,
        input_schema => schema(
            #{<<"dir">> => str(<<"Path of the export directory on this "
                                 "node.">>),
              <<"working_set">> => ws_ref(),
              <<"name">> => str(<<"Local database name for the copy. "
                                  "Default: derived from the context and "
                                  "generation.">>)},
            [<<"dir">>])}},
     {<<"context_working_sets">>, context_working_sets, #{
        description =>
            <<"List the working sets on this node with their member names, "
              "or, given working_set, read one in full: each member's "
              "mode, membership (what its answers cover), answers_offline, "
              "budget, usage and a summary. Example: {} or "
              "{\"working_set\": \"ws_...\"}.">>,
        input_schema => schema(#{<<"working_set">> => ws_id()}, []),
        annotations => read_only()}},
     {<<"context_working_set_delete">>, context_working_set_delete, #{
        description =>
            <<"Delete a working set and the slices it saved (imported "
              "snapshots stay). Example: {\"working_set\": \"ws_...\"}.">>,
        input_schema => schema(#{<<"working_set">> => ws_id()},
                               [<<"working_set">>]),
        annotations => #{<<"destructiveHint">> => true}}},
     {<<"context_offline">>, context_offline, #{
        description =>
            <<"Read or switch the node's offline mode. Offline, remote "
              "contexts are skipped and never contacted; local databases, "
              "slices and snapshots still answer. Omit offline to read the "
              "mode. Example: {\"offline\": true}.">>,
        input_schema => schema(
            #{<<"offline">> => bool(<<"true to go offline, false to go back "
                                      "online.">>, undefined)}, []),
        annotations => #{<<"idempotentHint">> => true}}}
    ].

%%====================================================================
%% Handlers
%%====================================================================

context_capabilities(Args, _Ctx) ->
    run(<<"context_capabilities">>, Args,
        fun() -> {ok, barrel_ctx:capabilities()} end).

context_list(Args, _Ctx) ->
    run(<<"context_list">>, Args, fun() ->
        Opts = #{include_unlisted => maps:get(<<"unlisted">>, Args, false),
                 name_prefix => maps:get(<<"prefix">>, Args, <<>>)},
        {ok, barrel_server_contexts:list(Opts)}
    end).

context_discover(Args, _Ctx) ->
    run(<<"context_discover">>, Args, fun() ->
        {ok, barrel_server_contexts:discover(maps:get(<<"q">>, Args))}
    end).

context_inspect(Args, _Ctx) ->
    run(<<"context_inspect">>, Args, fun() ->
        barrel_ctx:inspect(maps:get(<<"context">>, Args))
    end).

%% Local members are checked against the caller's grant, as a `query'
%% tool call on that db would be. Nothing answered: an error result.
context_query(Args, Ctx) ->
    run(<<"context_query">>, Args, fun() ->
        Authorize = fun(Db) -> barrel_server_mcp_auth:allow(Ctx, Db, read) end,
        Global = barrel_server_mcp_auth:global(Ctx) =:= ok,
        case barrel_server_contexts:run_query(Args, Authorize, Global) of
            {ok, #{execution := failed} = Resp} -> {failed, Resp};
            Other -> Other
        end
    end).

%% Working-set tools need a global principal, as the REST routes do.
context_attach(Args, Ctx) ->
    global(<<"context_attach">>, Args, Ctx, fun() ->
        barrel_server_worksets:do_attach(ws_arg(Args), Args)
    end).

context_detach(Args, Ctx) ->
    global(<<"context_detach">>, Args, Ctx, fun() ->
        barrel_ctx:detach(maps:get(<<"working_set">>, Args),
                          maps:get(<<"context">>, Args))
    end).

context_materialize(Args, Ctx) ->
    global(<<"context_materialize">>, Args, Ctx, fun() ->
        barrel_server_worksets:do_materialize(ws_arg(Args), Args)
    end).

context_import(Args, Ctx) ->
    global(<<"context_import">>, Args, Ctx, fun() ->
        barrel_server_worksets:do_import(ws_arg(Args), Args)
    end).

context_working_sets(#{<<"working_set">> := WsId} = Args, Ctx) ->
    global(<<"context_working_sets">>, Args, Ctx,
           fun() -> barrel_ctx:get_ws(WsId) end);
context_working_sets(Args, Ctx) ->
    global(<<"context_working_sets">>, Args, Ctx,
           fun() -> {ok, #{working_sets => barrel_ctx:list_ws()}} end).

context_working_set_delete(Args, Ctx) ->
    global(<<"context_working_set_delete">>, Args, Ctx, fun() ->
        WsId = maps:get(<<"working_set">>, Args),
        case barrel_ctx:delete_ws(WsId) of
            ok -> {ok, #{ok => true, deleted => WsId}};
            {error, _} = Err -> Err
        end
    end).

context_offline(#{<<"offline">> := Flag} = Args, Ctx) ->
    global(<<"context_offline">>, Args, Ctx, fun() ->
        ok = barrel_ctx:set_offline(Flag),
        {ok, #{offline => Flag}}
    end);
context_offline(Args, _Ctx) ->
    run(<<"context_offline">>, Args,
        fun() -> {ok, #{offline => barrel_ctx:offline()}} end).

%%====================================================================
%% Internal
%%====================================================================

global(Tool, Args, Ctx, Fun) ->
    case barrel_server_mcp_auth:global(Ctx) of
        ok -> run(Tool, Args, Fun);
        {error, _} -> error_result({forbidden, #{operation => Tool}})
    end.

%% Check the arguments against the tool's schema, then run.
run(Tool, Args, Fun) ->
    {Tool, _, #{input_schema := Schema}} = lists:keyfind(Tool, 1, specs()),
    case check(Schema, Args) of
        ok ->
            case Fun() of
                {ok, Map} -> encode(Map);
                {failed, Map} -> {tool_error, encode(Map)};
                {error, Reason} -> error_result(Reason)
            end;
        {error, Reason} ->
            error_result(Reason)
    end.

error_result(Reason) ->
    {tool_error, encode(barrel_ctx_error:to_map(Reason))}.

%% Tool results are JSON text (a map with a `type' key would otherwise
%% be taken for a content block).
encode(Map) ->
    iolist_to_binary(json:encode(Map)).

ws_arg(Args) ->
    case maps:get(<<"working_set">>, Args, null) of
        WsId when is_binary(WsId) -> WsId;
        _ -> new
    end.

check(#{<<"properties">> := Props} = Schema, Args) when is_map(Args) ->
    Required = maps:get(<<"required">>, Schema, []),
    case {check_props(maps:to_list(Args), Props),
          [F || F <- Required, not is_map_key(F, Args)]} of
        {{error, _} = Err, _} ->
            Err;
        {ok, [Missing | _]} ->
            {error, {invalid_argument,
                     #{field => Missing,
                       expected => expected(maps:get(Missing, Props))}}};
        {ok, []} ->
            ok
    end;
check(_Schema, _Args) ->
    {error, {invalid_argument, #{}}}.

check_props([], _Props) ->
    ok;
check_props([{_K, null} | Rest], Props) ->
    check_props(Rest, Props);
check_props([{K, V} | Rest], Props) ->
    case maps:find(K, Props) of
        {ok, Spec} ->
            case valid(V, Spec) of
                true -> check_props(Rest, Props);
                false -> {error, {invalid_argument, bad_value(K, Spec)}}
            end;
        error ->
            {error, {invalid_argument,
                     #{field => K, accepted => lists:sort(maps:keys(Props))}}}
    end.

bad_value(K, #{<<"enum">> := Enum}) ->
    #{field => K, allowed => Enum};
bad_value(K, Spec) ->
    #{field => K, expected => expected(Spec)}.

%% A refused merge reaches barrel_ctx, which explains why it is refused.
valid(V, #{<<"enum">> := [<<"ordered">> | _]})
  when V =:= <<"rrf">>; V =:= <<"rerank">> -> true;
valid(V, #{<<"enum">> := Enum}) -> lists:member(V, Enum);
valid(V, #{<<"type">> := <<"string">>}) -> is_binary(V) andalso V =/= <<>>;
valid(V, #{<<"type">> := <<"integer">>}) -> is_integer(V) andalso V > 0;
valid(V, #{<<"type">> := <<"boolean">>}) -> is_boolean(V);
valid(V, #{<<"type">> := <<"object">>}) -> is_map(V);
valid(V, #{<<"type">> := <<"array">>}) ->
    is_list(V) andalso V =/= [] andalso lists:all(fun is_binary/1, V).

expected(#{<<"type">> := <<"string">>}) -> <<"a non-empty string">>;
expected(#{<<"type">> := <<"integer">>}) -> <<"a positive integer">>;
expected(#{<<"type">> := <<"boolean">>}) -> <<"true or false">>;
expected(#{<<"type">> := <<"object">>}) -> <<"an object">>;
expected(#{<<"type">> := <<"array">>}) -> <<"a non-empty array of strings">>.

schema(Props, Required) ->
    Base = #{<<"type">> => <<"object">>, <<"properties">> => Props,
             <<"additionalProperties">> => false},
    case Required of
        [] -> Base;
        _ -> Base#{<<"required">> => Required}
    end.

read_only() ->
    #{<<"readOnlyHint">> => true}.

ctx_ref() ->
    (str(<<"A context name (e.g. \"otp/sasl\") or id (ctx_...).">>))#{
      <<"examples">> => [<<"otp/sasl">>]}.

ws_ref() ->
    str(<<"Working-set id (ws_...). Omit to create a new working set.">>).

ws_id() ->
    str(<<"Working-set id (ws_...), from context_attach or "
          "context_working_sets.">>).

str(Desc) ->
    #{<<"type">> => <<"string">>, <<"description">> => Desc}.

obj(Desc) ->
    #{<<"type">> => <<"object">>, <<"description">> => Desc}.

int(Desc, undefined) ->
    #{<<"type">> => <<"integer">>, <<"minimum">> => 1,
      <<"description">> => Desc};
int(Desc, Default) ->
    (int(Desc, undefined))#{<<"default">> => Default}.

bool(Desc, undefined) ->
    #{<<"type">> => <<"boolean">>, <<"description">> => Desc};
bool(Desc, Default) ->
    (bool(Desc, undefined))#{<<"default">> => Default}.
