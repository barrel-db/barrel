%%%-------------------------------------------------------------------
%%% @doc The core MCP tools: databases, documents, BQL, search, the
%%% changes feed, and the timeline (branch/merge). Every handler is
%%% arity 2; the Ctx carries the authenticated principal, which
%%% barrel_server_mcp_auth:allow/3 checks against the database and
%%% right each tool touches. Denials come back as isError tool
%%% results, not protocol errors: agents recover better from content
%%% they can read.
%%%
%%% Writes thread provenance through write_opts/2 (actor = subject,
%%% session = MCP session or the `session' argument, source = mcp),
%%% the single seam every MCP write shares.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_tools).

-export([register_all/0, unregister_all/0, write_opts/2]).

%% Tool handlers (invoked by the barrel_mcp registry as M:F(Args, Ctx))
-export([
    db_create/2,
    db_list/2,
    db_info/2,
    doc_get/2,
    doc_put/2,
    doc_delete/2,
    'query'/2,
    search/2,
    changes/2,
    branch_create/2,
    branch_list/2,
    merge/2
]).

-define(DEFAULT_MAX_ROWS, 100).
-define(MAX_MAX_ROWS, 1000).

%%====================================================================
%% Registration
%%====================================================================

register_all() ->
    lists:foreach(
        fun({Name, Fun, Opts}) ->
            %% unreg first: registration must survive a barrel_server
            %% restart under a still-running barrel_mcp registry
            ok = barrel_mcp:unreg_tool(Name),
            ok = barrel_mcp:reg_tool(Name, ?MODULE, Fun,
                                     Opts#{validate_input => true})
        end, specs()).

unregister_all() ->
    lists:foreach(
        fun({Name, _Fun, _Opts}) -> barrel_mcp:unreg_tool(Name) end,
        specs()).

%% @doc The write options of an MCP write: provenance from the
%% authenticated principal.
-spec write_opts(map(), map()) -> map().
write_opts(Ctx, Args) ->
    #{provenance => barrel_server_mcp_auth:prov(Ctx, Args)}.

specs() ->
    [
        {<<"db_create">>, db_create, #{
            description => <<"Create (or open) a database by name.">>,
            input_schema => schema(#{<<"db">> => str()}, [<<"db">>]),
            annotations => #{<<"idempotentHint">> => true}
        }},
        {<<"db_list">>, db_list, #{
            description => <<"List the databases currently open on "
                             "this node (scoped to your grant).">>,
            input_schema => schema(#{}, []),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"db_info">>, db_info, #{
            description => <<"Info about a database: document count, "
                             "lineage, retention.">>,
            input_schema => schema(#{<<"db">> => str()}, [<<"db">>]),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"doc_get">>, doc_get, #{
            description => <<"Read a document by id.">>,
            input_schema => schema(#{<<"db">> => str(),
                                     <<"id">> => str()},
                                   [<<"db">>, <<"id">>]),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"doc_put">>, doc_put, #{
            description => <<"Write a document (the doc object must "
                             "carry its id). Optional session tags "
                             "the write's provenance.">>,
            input_schema => schema(#{<<"db">> => str(),
                                     <<"doc">> => obj(),
                                     <<"session">> => str()},
                                   [<<"db">>, <<"doc">>])
        }},
        {<<"doc_delete">>, doc_delete, #{
            description => <<"Delete a document by id.">>,
            input_schema => schema(#{<<"db">> => str(),
                                     <<"id">> => str(),
                                     <<"session">> => str()},
                                   [<<"db">>, <<"id">>]),
            annotations => #{<<"destructiveHint">> => true}
        }},
        {<<"query">>, 'query', #{
            description => <<"Run a BQL query. Rows are bounded by "
                             "max_rows (default 100, cap 1000); pass "
                             "the returned continuation to page. "
                             "SUBSCRIBE statements are rejected: use "
                             "query_subscribe.">>,
            input_schema => schema(#{<<"db">> => str(),
                                     <<"query">> => str(),
                                     <<"params">> => obj(),
                                     <<"max_rows">> => int(),
                                     <<"continuation">> => str()},
                                   [<<"db">>, <<"query">>]),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"search">>, search, #{
            description => <<"Search a database: mode bm25 or hybrid "
                             "with a query string, mode vector with "
                             "a query vector.">>,
            input_schema => schema(#{<<"db">> => str(),
                                     <<"mode">> => str(),
                                     <<"query">> => str(),
                                     <<"vector">> => arr(),
                                     <<"k">> => int()},
                                   [<<"db">>, <<"mode">>]),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"changes">>, changes, #{
            description => <<"Read the changes feed from a cursor "
                             "(absent = from the start). Returns the "
                             "changes and the next cursor.">>,
            input_schema => schema(#{<<"db">> => str(),
                                     <<"since">> => str()},
                                   [<<"db">>]),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"branch_create">>, branch_create, #{
            description => <<"Fork a database into a branch, "
                             "optionally at a changes cursor.">>,
            input_schema => schema(#{<<"db">> => str(),
                                     <<"name">> => str(),
                                     <<"at">> => str()},
                                   [<<"db">>, <<"name">>])
        }},
        {<<"branch_list">>, branch_list, #{
            description => <<"List a database's branches and its own "
                             "lineage.">>,
            input_schema => schema(#{<<"db">> => str()}, [<<"db">>]),
            annotations => #{<<"readOnlyHint">> => true}
        }},
        {<<"merge">>, merge, #{
            description => <<"Merge a branch back into its parent.">>,
            input_schema => schema(#{<<"db">> => str()}, [<<"db">>]),
            annotations => #{<<"destructiveHint">> => true}
        }}
    ].

%%====================================================================
%% Databases
%%====================================================================

db_create(#{<<"db">> := Name}, Ctx) ->
    with_db(Name, Ctx, write, fun(_Db) ->
        #{ok => true, db => Name}
    end).

db_list(_Args, Ctx) ->
    Dbs = [Db || Db <- barrel_server_dbs:list(),
                 barrel_server_mcp_auth:allow(Ctx, Db, read) =:= ok],
    #{dbs => lists:sort(Dbs)}.

db_info(#{<<"db">> := Name}, Ctx) ->
    with_db(Name, Ctx, read, fun(Db) ->
        case barrel:info(Db) of
            {ok, Info} ->
                barrel_server_http:jsonable(
                    barrel_server_http:encode_hlcs(Info));
            Err ->
                err(Err)
        end
    end).

%%====================================================================
%% Documents
%%====================================================================

doc_get(#{<<"db">> := Name, <<"id">> := Id}, Ctx) ->
    with_db(Name, Ctx, read, fun(Db) ->
        case barrel:get_doc(Db, Id) of
            {ok, Doc} -> Doc;
            Err -> err(Err)
        end
    end).

doc_put(#{<<"db">> := Name, <<"doc">> := Doc} = Args, Ctx) ->
    with_db(Name, Ctx, write, fun(Db) ->
        case Doc of
            #{<<"id">> := Id} when is_binary(Id) ->
                case barrel:put_doc(Db, Doc, write_opts(Ctx, Args)) of
                    {ok, Res} -> barrel_server_http:jsonable(Res);
                    Err -> err(Err)
                end;
            _ ->
                {tool_error, #{error => <<"missing_id">>,
                               hint => <<"the doc object needs an "
                                         "\"id\" field">>}}
        end
    end).

doc_delete(#{<<"db">> := Name, <<"id">> := Id} = Args, Ctx) ->
    with_db(Name, Ctx, write, fun(Db) ->
        case barrel:delete_doc(Db, Id, write_opts(Ctx, Args)) of
            {ok, Res} -> barrel_server_http:jsonable(Res);
            Err -> err(Err)
        end
    end).

%%====================================================================
%% BQL
%%====================================================================

'query'(#{<<"db">> := Name, <<"query">> := Bql} = Args, Ctx) ->
    Params = case maps:get(<<"params">>, Args, undefined) of
        P when is_map(P) -> P;
        _ -> #{}
    end,
    %% compile once for admission: parse errors and SUBSCRIBE are
    %% caught before the database is touched
    case barrel_bql:compile(Bql, #{params => Params}) of
        {ok, #{subscribe := true}} ->
            {tool_error, #{error => <<"subscribe_not_supported">>,
                           hint => <<"use query_subscribe for live "
                                     "queries">>}};
        {ok, _Plan} ->
            with_db(Name, Ctx, read, fun(Db) ->
                run_query(Db, Bql, Params, Args)
            end);
        {error, BqlError} ->
            {tool_error,
             #{error => <<"invalid_query">>,
               message => barrel_bql:format_error(BqlError)}}
    end.

run_query(Db, Bql, Params, Args) ->
    MaxRows = max_rows(Args),
    Opts0 = #{params => Params, chunk_size => MaxRows},
    case continuation_opt(Args, Opts0) of
        {ok, Opts} ->
            case barrel:'query'(Db, Bql, Opts) of
                {ok, Rows0, Meta} ->
                    Rows = lists:sublist(Rows0, MaxRows),
                    query_result(Rows, length(Rows0) > MaxRows, Meta);
                Err ->
                    err(Err)
            end;
        error ->
            {tool_error, #{error => <<"bad_continuation">>}}
    end.

max_rows(Args) ->
    case maps:get(<<"max_rows">>, Args, ?DEFAULT_MAX_ROWS) of
        N when is_integer(N), N > 0 -> min(N, ?MAX_MAX_ROWS);
        _ -> ?DEFAULT_MAX_ROWS
    end.

continuation_opt(Args, Opts) ->
    case maps:get(<<"continuation">>, Args, undefined) of
        undefined ->
            {ok, Opts};
        Token when is_binary(Token) ->
            try {ok, Opts#{continuation =>
                               base64:decode(Token, #{mode => urlsafe})}}
            catch _:_ -> error
            end;
        _ ->
            error
    end.

query_result(Rows, Truncated, Meta) ->
    Base = #{rows => Rows,
             count => length(Rows),
             has_more => maps:get(has_more, Meta, false) orelse Truncated},
    case maps:get(continuation, Meta, undefined) of
        undefined ->
            Base;
        Token ->
            Base#{continuation => base64:encode(Token, #{mode => urlsafe})}
    end.

%%====================================================================
%% Search
%%====================================================================

search(#{<<"db">> := Name, <<"mode">> := Mode} = Args, Ctx) ->
    with_db(Name, Ctx, read, fun(Db) ->
        K = case maps:get(<<"k">>, Args, 10) of
            N when is_integer(N), N > 0 -> N;
            _ -> 10
        end,
        Query = maps:get(<<"query">>, Args, <<>>),
        case Mode of
            <<"vector">> ->
                Vector = maps:get(<<"vector">>, Args, []),
                search_reply(barrel:search_vector(Db, Vector, #{k => K}));
            <<"bm25">> ->
                search_reply(barrel:search_bm25(Db, Query, #{k => K}));
            <<"hybrid">> ->
                search_reply(barrel:search_hybrid(Db, Query, #{k => K}));
            _ ->
                {tool_error, #{error => <<"bad_mode">>,
                               hint => <<"vector | bm25 | hybrid">>}}
        end
    end).

search_reply({ok, Hits}) when is_list(Hits) ->
    #{hits => [hit(H) || H <- Hits]};
search_reply(Hits) when is_list(Hits) ->
    #{hits => [hit(H) || H <- Hits]};
search_reply(Err) ->
    err(Err).

hit({Id, Score}) -> #{key => Id, score => Score};
hit(Map) when is_map(Map) -> barrel_server_http:jsonable(Map);
hit(Other) -> Other.

%%====================================================================
%% Changes
%%====================================================================

changes(#{<<"db">> := Name} = Args, Ctx) ->
    with_db(Name, Ctx, read, fun(Db) ->
        case since(Args) of
            {ok, Since} ->
                {ok, Changes, Last} = barrel:changes(Db, Since),
                #{changes => [barrel_server_http:sanitize_change(C)
                              || C <- Changes],
                  last => barrel:hlc_encode(Last)};
            error ->
                {tool_error, #{error => <<"bad_since">>}}
        end
    end).

since(Args) ->
    case maps:get(<<"since">>, Args, undefined) of
        undefined -> {ok, first};
        <<"first">> -> {ok, first};
        Cursor when is_binary(Cursor) ->
            try {ok, barrel:hlc_decode(Cursor)}
            catch _:_ -> error
            end;
        _ ->
            error
    end.

%%====================================================================
%% Timeline
%%====================================================================

branch_create(#{<<"db">> := Name, <<"name">> := Branch} = Args, Ctx) ->
    with_db(Name, Ctx, admin, fun(#{docdb := ParentBin}) ->
        case branch_at(Args) of
            {ok, At} ->
                case barrel_server_dbs:branch(ParentBin, Branch,
                                              #{at => At}) of
                    {ok, #{docdb := BranchBin} = BranchDb} ->
                        {ok, Info} = barrel:info(BranchDb),
                        #{ok => true,
                          branch => BranchBin,
                          parent => ParentBin,
                          fork_hlc => barrel:hlc_encode(
                              maps:get(fork_hlc, Info))};
                    Err ->
                        err(Err)
                end;
            error ->
                {tool_error, #{error => <<"bad_at">>}}
        end
    end).

branch_at(Args) ->
    case maps:get(<<"at">>, Args, undefined) of
        undefined ->
            {ok, now};
        Cursor when is_binary(Cursor) ->
            try {ok, barrel:hlc_decode(Cursor)}
            catch _:_ -> error
            end;
        _ ->
            error
    end.

branch_list(#{<<"db">> := Name}, Ctx) ->
    with_db(Name, Ctx, read, fun(#{docdb := DbBin} = Db) ->
        Base = #{db => DbBin,
                 branches => barrel_docdb:list_branches(DbBin)},
        case barrel:info(Db) of
            {ok, #{parent := Parent, fork_hlc := ForkHlc}} ->
                Base#{parent => Parent,
                      fork_hlc => barrel:hlc_encode(ForkHlc)};
            {ok, _} ->
                Base;
            Err ->
                err(Err)
        end
    end).

merge(#{<<"db">> := Name}, Ctx) ->
    with_db(Name, Ctx, admin, fun(Db) ->
        case barrel:merge(Db) of
            {ok, Report} ->
                barrel_server_http:jsonable(
                    barrel_server_http:merge_report(Report));
            Err ->
                err(Err)
        end
    end).

%%====================================================================
%% Internal
%%====================================================================

with_db(Name, Ctx, Right, Fun) ->
    case barrel_server_mcp_auth:allow(Ctx, Name, Right) of
        ok ->
            case barrel_server_dbs:ensure(Name) of
                {ok, Db} -> Fun(Db);
                Err -> err(Err)
            end;
        {error, forbidden} ->
            {tool_error, #{error => <<"forbidden">>, db => Name}}
    end.

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
