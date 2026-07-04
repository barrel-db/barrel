%%%-------------------------------------------------------------------
%%% @doc REST/JSON HTTP surface for the barrel edge database.
%%%
%%% One livery router served over HTTP/1.1 and HTTP/2. Every handler reads
%%% path bindings and the request body, calls the {@link barrel} facade through
%%% {@link barrel_server_dbs}, and renders JSON. Documents, attachments,
%%% vectors, search, and the changes feed (JSON or SSE) are exposed.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_http).

%% Service start (supervised child)
-export([start_link/0]).

%% Route handlers (invoked by the livery router as Module:Function/1)
-export([
    root/1,
    health/1,
    create_db/1,
    db_info/1,
    drop_db/1,
    put_doc/1,
    get_doc/1,
    delete_doc/1,
    bulk_docs/1,
    bulk_get/1,
    find/1,
    query/1,
    changes/1,
    put_att/1,
    get_att/1,
    delete_att/1,
    vector_add/1,
    search_vector/1,
    search_bm25/1,
    search_hybrid/1
]).

%%====================================================================
%% Service
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    Port = application:get_env(barrel_server, http_port, 8080),
    Router = livery_router:compile(routes()),
    livery:start_service(#{
        http => #{port => Port},
        router => Router,
        middleware => [
            {livery_request_id, undefined},
            {livery_access_log, #{}}
        ]
    }).

routes() ->
    [
        {<<"GET">>,    <<"/">>,                          {?MODULE, root}},
        {<<"GET">>,    <<"/health">>,                    {?MODULE, health}},

        {<<"PUT">>,    <<"/db/:db">>,                    {?MODULE, create_db}},
        {<<"GET">>,    <<"/db/:db">>,                    {?MODULE, db_info}},
        {<<"DELETE">>, <<"/db/:db">>,                    {?MODULE, drop_db}},

        {<<"PUT">>,    <<"/db/:db/doc/:id">>,            {?MODULE, put_doc}},
        {<<"GET">>,    <<"/db/:db/doc/:id">>,            {?MODULE, get_doc}},
        {<<"DELETE">>, <<"/db/:db/doc/:id">>,            {?MODULE, delete_doc}},
        {<<"POST">>,   <<"/db/:db/_bulk_docs">>,         {?MODULE, bulk_docs}},
        {<<"POST">>,   <<"/db/:db/_bulk_get">>,          {?MODULE, bulk_get}},
        {<<"POST">>,   <<"/db/:db/find">>,               {?MODULE, find}},
        %% GET exists for browser EventSource (SUBSCRIBE over SSE)
        {<<"POST">>,   <<"/db/:db/query">>,              {?MODULE, query}},
        {<<"GET">>,    <<"/db/:db/query">>,              {?MODULE, query}},
        {<<"GET">>,    <<"/db/:db/changes">>,            {?MODULE, changes}},

        %% Replication wire (barrel_rep_transport over HTTP)
        {<<"GET">>,    <<"/db/:db/_sync/info">>,         {barrel_server_sync, info}},
        {<<"POST">>,   <<"/db/:db/_sync/hlc">>,          {barrel_server_sync, sync_hlc}},
        {<<"POST">>,   <<"/db/:db/_sync/changes">>,      {barrel_server_sync, changes}},
        {<<"POST">>,   <<"/db/:db/_sync/diff">>,         {barrel_server_sync, diff}},
        {<<"GET">>,    <<"/db/:db/_sync/doc/:id">>,      {barrel_server_sync, get_doc}},
        {<<"PUT">>,    <<"/db/:db/_sync/doc/:id">>,      {barrel_server_sync, put_version}},
        {<<"GET">>,    <<"/db/:db/_sync/local/:id">>,    {barrel_server_sync, get_local}},
        {<<"PUT">>,    <<"/db/:db/_sync/local/:id">>,    {barrel_server_sync, put_local}},
        {<<"DELETE">>, <<"/db/:db/_sync/local/:id">>,    {barrel_server_sync, delete_local}},

        {<<"PUT">>,    <<"/db/:db/doc/:id/att/:name">>,  {?MODULE, put_att}},
        {<<"GET">>,    <<"/db/:db/doc/:id/att/:name">>,  {?MODULE, get_att}},
        {<<"DELETE">>, <<"/db/:db/doc/:id/att/:name">>,  {?MODULE, delete_att}},

        {<<"POST">>,   <<"/db/:db/vector">>,             {?MODULE, vector_add}},
        {<<"POST">>,   <<"/db/:db/search/vector">>,      {?MODULE, search_vector}},
        {<<"POST">>,   <<"/db/:db/search/bm25">>,        {?MODULE, search_bm25}},
        {<<"POST">>,   <<"/db/:db/search/hybrid">>,      {?MODULE, search_hybrid}}
    ].

%%====================================================================
%% Meta handlers
%%====================================================================

root(_Req) ->
    livery_resp:text(200, <<"barrel edge database\n">>).

health(_Req) ->
    json_resp(200, #{status => <<"ok">>}).

%%====================================================================
%% Database lifecycle
%%====================================================================

create_db(Req) ->
    Name = livery_req:binding(<<"db">>, Req),
    case barrel_server_dbs:ensure(Name) of
        {ok, _Db} -> json_resp(201, #{ok => true, db => Name});
        Err -> error_resp(Err)
    end.

db_info(Req) ->
    with_db(Req, fun(Db) ->
        case barrel:info(Db) of
            {ok, Info} -> json_resp(200, jsonable(Info));
            Err -> error_resp(Err)
        end
    end).

drop_db(Req) ->
    Name = livery_req:binding(<<"db">>, Req),
    ok = barrel_server_dbs:close(Name),
    json_resp(200, #{ok => true, db => Name}).

%%====================================================================
%% Documents
%%====================================================================

put_doc(Req) ->
    with_db(Req, fun(Db) ->
        with_json(Req, fun(Body) ->
            Id = livery_req:binding(<<"id">>, Req),
            Doc = Body#{<<"id">> => Id},
            case barrel:put_doc(Db, Doc) of
                {ok, Res} -> json_resp(201, jsonable(Res));
                Err -> error_resp(Err)
            end
        end)
    end).

get_doc(Req) ->
    with_db(Req, fun(Db) ->
        Id = livery_req:binding(<<"id">>, Req),
        case barrel:get_doc(Db, Id) of
            {ok, Doc} -> json_resp(200, Doc);
            Err -> error_resp(Err)
        end
    end).

delete_doc(Req) ->
    with_db(Req, fun(Db) ->
        Id = livery_req:binding(<<"id">>, Req),
        case barrel:delete_doc(Db, Id) of
            {ok, Res} -> json_resp(200, jsonable(Res));
            Err -> error_resp(Err)
        end
    end).

bulk_docs(Req) ->
    with_db(Req, fun(Db) ->
        with_json(Req, fun(Body) ->
            Docs = maps:get(<<"docs">>, Body, []),
            Results = barrel:put_docs(Db, Docs),
            json_resp(201, #{results => [batch_result(R) || R <- Results]})
        end)
    end).

bulk_get(Req) ->
    with_db(Req, fun(Db) ->
        with_json(Req, fun(Body) ->
            Ids = maps:get(<<"ids">>, Body, []),
            Results = barrel:get_docs(Db, Ids),
            json_resp(200, #{results => [batch_result(R) || R <- Results]})
        end)
    end).

find(Req) ->
    with_db(Req, fun(Db) ->
        with_json(Req, fun(Query) ->
            case barrel:find(Db, Query) of
                {ok, Rows, Meta} ->
                    json_resp(200, #{rows => Rows, meta => jsonable(Meta)});
                Err -> error_resp(Err)
            end
        end)
    end).

%%====================================================================
%% BQL query endpoint
%%
%% POST body: raw BQL text, or {"query","params","continuation"} as
%% JSON. GET takes ?q= (for browser EventSource). Plain statements
%% stream ndjson: one {"row":...} line per row, one final {"meta":...}
%% line; failures after the 200 is committed appear as an in-band
%% {"error":...} line. SUBSCRIBE statements need the SSE accept and
%% stream row / ready / change / error events with a 30s ping.
%% stream_deferred picks the status after compile, so bad BQL is a
%% clean 400 before the first byte.
%%====================================================================

query(Req) ->
    livery_resp:stream_deferred(fun() -> query_decision(Req) end).

query_decision(Req) ->
    JsonHs = [{<<"content-type">>, <<"application/json">>}],
    case prepare_query(Req) of
        {error, Status, Body} ->
            {full, Status, JsonHs, Body};
        {ok, Db, Bql, #{subscribe := true} = QOpts} ->
            case wants_sse(Req) of
                true ->
                    {sse, 200, [],
                     fun(Emit) -> run_subscribe(Db, Bql, QOpts, Emit) end};
                false ->
                    {full, 400, JsonHs,
                     json:encode(#{error => <<"subscribe_requires_sse">>})}
            end;
        {ok, Db, Bql, QOpts} ->
            {ndjson, 200, [],
             fun(Emit) -> run_query(Db, Bql, QOpts, Emit) end}
    end.

%% Resolve the database, read the statement, and compile it once for
%% admission (the producers re-drive the facade with the raw text).
prepare_query(Req) ->
    Name = livery_req:binding(<<"db">>, Req),
    case barrel_server_dbs:ensure(Name) of
        {ok, Db} ->
            case query_input(Req) of
                {ok, Bql, QOpts} ->
                    Params = maps:get(params, QOpts, #{}),
                    case barrel_bql:compile(Bql, #{params => Params}) of
                        {ok, #{subscribe := Subscribe}} ->
                            {ok, Db, Bql,
                             QOpts#{subscribe => Subscribe}};
                        {error, BqlError} ->
                            {error, 400, bql_error_body(BqlError)}
                    end;
                {error, Reason} ->
                    {error, 400,
                     json:encode(#{error => err_bin(Reason)})}
            end;
        {error, invalid_name} ->
            {error, 400, json:encode(#{error => <<"invalid_name">>})};
        {error, Reason} ->
            {error, 500, json:encode(#{error => err_bin(Reason)})}
    end.

query_input(Req) ->
    case livery_req:method(Req) of
        <<"GET">> ->
            case param(<<"q">>, Req) of
                undefined -> {error, missing_query};
                <<>> -> {error, missing_query};
                Bql -> {ok, Bql, #{}}
            end;
        _ ->
            ContentType = livery_req:header(<<"content-type">>, Req, <<>>),
            IsJson = binary:match(ContentType, <<"application/json">>)
                     =/= nomatch,
            case {IsJson, read_body(Req)} of
                {_, {error, Reason}} -> {error, Reason};
                {_, {ok, <<>>}} -> {error, missing_query};
                {false, {ok, Bql}} -> {ok, Bql, #{}};
                {true, {ok, Bin}} -> json_query_input(Bin)
            end
    end.

json_query_input(Bin) ->
    try json:decode(Bin) of
        #{<<"query">> := Bql} = Body when is_binary(Bql) ->
            QOpts0 = case maps:get(<<"params">>, Body, undefined) of
                Params when is_map(Params) -> #{params => Params};
                _ -> #{}
            end,
            QOpts = case maps:get(<<"continuation">>, Body, undefined) of
                Token when is_binary(Token) ->
                    QOpts0#{continuation =>
                                base64:decode(Token, #{mode => urlsafe})};
                _ ->
                    QOpts0
            end,
            {ok, Bql, QOpts};
        _ ->
            {error, missing_query}
    catch
        _:_ -> {error, bad_json}
    end.

bql_error_body(BqlError) ->
    Base = #{error => <<"invalid_query">>,
             message => barrel_bql:format_error(BqlError)},
    WithLoc = case BqlError of
        {_, {Line, Column}, _} ->
            Base#{line => Line, column => Column};
        _ ->
            Base
    end,
    json:encode(WithLoc).

run_query(Db, Bql, QOpts, Emit) ->
    FoldOpts = maps:without([subscribe], QOpts),
    Result = barrel:query_fold(Db, Bql, FoldOpts#{chunk_size => 100},
        fun(Row, ok) ->
            case Emit(#{row => Row}) of
                ok -> {ok, ok};
                {error, _} -> {stop, ok}
            end
        end,
        ok),
    case Result of
        {ok, _, Meta} ->
            _ = Emit(#{meta => query_meta(Meta)}),
            ok;
        {error, Reason} ->
            _ = Emit(#{error => err_bin(Reason)}),
            ok
    end.

query_meta(Meta) ->
    Base = #{has_more => maps:get(has_more, Meta, false)},
    Base1 = case maps:get(count, Meta, undefined) of
        undefined -> Base;
        Count -> Base#{count => Count}
    end,
    case maps:get(continuation, Meta, undefined) of
        undefined ->
            Base1;
        Token ->
            Base1#{continuation =>
                       base64:encode(Token, #{mode => urlsafe})}
    end.

run_subscribe(Db, Bql, QOpts, Emit) ->
    SubOpts = maps:with([params], QOpts),
    case barrel:subscribe_query(Db, Bql, SubOpts) of
        {ok, #{ref := Ref} = Sub} ->
            subscribe_loop(Ref, Sub, Emit);
        {error, Reason} ->
            _ = Emit(#{event => <<"error">>,
                       data => json:encode(#{error => err_bin(Reason)})}),
            ok
    end.

subscribe_loop(Ref, Sub, Emit) ->
    receive
        {bql_rows, Ref, Rows} ->
            case emit_rows(Rows, Emit) of
                ok -> subscribe_loop(Ref, Sub, Emit);
                stop -> barrel:unsubscribe_query(Sub)
            end;
        {bql_ready, Ref, Meta} ->
            Ready = #{count => maps:get(count, Meta, 0)},
            case Emit(#{event => <<"ready">>, data => json:encode(Ready)}) of
                ok -> subscribe_loop(Ref, Sub, Emit);
                {error, _} -> barrel:unsubscribe_query(Sub)
            end;
        {bql_change, Ref, Change} ->
            case Emit(#{event => <<"change">>,
                        data => json:encode(change_event(Change))}) of
                ok -> subscribe_loop(Ref, Sub, Emit);
                {error, _} -> barrel:unsubscribe_query(Sub)
            end;
        {bql_error, Ref, Reason} ->
            _ = Emit(#{event => <<"error">>,
                       data => json:encode(#{error => err_bin(Reason)})}),
            ok
    after 30000 ->
        %% heartbeat: keeps proxies from idling the stream out and
        %% detects gone clients
        case Emit(#{event => <<"ping">>, data => <<"{}">>}) of
            ok -> subscribe_loop(Ref, Sub, Emit);
            {error, _} -> barrel:unsubscribe_query(Sub)
        end
    end.

emit_rows([], _Emit) ->
    ok;
emit_rows([Row | Rest], Emit) ->
    case Emit(#{event => <<"row">>, data => json:encode(Row)}) of
        ok -> emit_rows(Rest, Emit);
        {error, _} -> stop
    end.

change_event(#{action := Action, id := Id} = Change) ->
    Base = #{action => atom_to_binary(Action, utf8), id => Id},
    Base1 = case maps:get(rev, Change, undefined) of
        undefined -> Base;
        Rev -> Base#{rev => Rev}
    end,
    case maps:get(row, Change, undefined) of
        undefined -> Base1;
        Row -> Base1#{row => Row}
    end.

%%====================================================================
%% Changes feed
%%====================================================================

changes(Req) ->
    with_db(Req, fun(Db) ->
        Since = since_from_query(Req),
        {ok, Changes, Last} = barrel:changes(Db, Since),
        case wants_sse(Req) of
            true ->
                livery_resp:sse(200, fun(Emit) ->
                    lists:foreach(
                        fun(C) ->
                            Emit(#{data => json:encode(sanitize_change(C))})
                        end, Changes),
                    Emit(#{event => <<"last">>, data => barrel:hlc_encode(Last)}),
                    ok
                end);
            false ->
                json_resp(200, #{
                    changes => [sanitize_change(C) || C <- Changes],
                    last => barrel:hlc_encode(Last)
                })
        end
    end).

%%====================================================================
%% Attachments
%%====================================================================

put_att(Req) ->
    with_db(Req, fun(Db) ->
        Id = livery_req:binding(<<"id">>, Req),
        Name = livery_req:binding(<<"name">>, Req),
        case read_body(Req) of
            {ok, Data} ->
                case barrel:put_attachment(Db, Id, Name, Data) of
                    {ok, Res} -> json_resp(201, jsonable(Res));
                    Err -> error_resp(Err)
                end;
            Err -> error_resp(Err)
        end
    end).

get_att(Req) ->
    with_db(Req, fun(Db) ->
        Id = livery_req:binding(<<"id">>, Req),
        Name = livery_req:binding(<<"name">>, Req),
        case barrel:get_attachment(Db, Id, Name) of
            {ok, Data} ->
                CT = att_content_type(Db, Id, Name),
                livery_resp:new(200, [{<<"content-type">>, CT}], {full, Data});
            Err -> error_resp(Err)
        end
    end).

delete_att(Req) ->
    with_db(Req, fun(Db) ->
        Id = livery_req:binding(<<"id">>, Req),
        Name = livery_req:binding(<<"name">>, Req),
        case barrel:delete_attachment(Db, Id, Name) of
            ok -> json_resp(200, #{ok => true});
            Err -> error_resp(Err)
        end
    end).

%%====================================================================
%% Vectors and search
%%====================================================================

vector_add(Req) ->
    with_db(Req, fun(Db) ->
        with_json(Req, fun(Body) ->
            Id = maps:get(<<"id">>, Body, undefined),
            Text = maps:get(<<"text">>, Body, <<>>),
            Meta = maps:get(<<"metadata">>, Body, #{}),
            Res = case maps:get(<<"vector">>, Body, undefined) of
                undefined -> barrel:vector_add(Db, Id, Text, Meta);
                Vector -> barrel:vector_add(Db, Id, Text, Meta, Vector)
            end,
            case Res of
                ok -> json_resp(201, #{ok => true, id => Id});
                {ok, R} -> json_resp(201, jsonable(R));
                Err -> error_resp(Err)
            end
        end)
    end).

search_vector(Req) ->
    with_db(Req, fun(Db) ->
        with_json(Req, fun(Body) ->
            Vector = maps:get(<<"vector">>, Body, []),
            search_reply(barrel:search_vector(Db, Vector, search_opts(Body)))
        end)
    end).

search_bm25(Req) ->
    with_db(Req, fun(Db) ->
        with_json(Req, fun(Body) ->
            Query = maps:get(<<"query">>, Body, <<>>),
            search_reply(barrel:search_bm25(Db, Query, search_opts(Body)))
        end)
    end).

search_hybrid(Req) ->
    with_db(Req, fun(Db) ->
        with_json(Req, fun(Body) ->
            Query = maps:get(<<"query">>, Body, <<>>),
            search_reply(barrel:search_hybrid(Db, Query, search_opts(Body)))
        end)
    end).

%%====================================================================
%% Internal
%%====================================================================

with_db(Req, Fun) ->
    Name = livery_req:binding(<<"db">>, Req),
    case barrel_server_dbs:ensure(Name) of
        {ok, Db} -> Fun(Db);
        Err -> error_resp(Err)
    end.

with_json(Req, Fun) ->
    case read_json(Req) of
        {ok, Body} -> Fun(Body);
        Err -> error_resp(Err)
    end.

read_json(Req) ->
    case read_body(Req) of
        {ok, <<>>} -> {ok, #{}};
        {ok, Bin} ->
            try {ok, json:decode(Bin)}
            catch _:_ -> {error, bad_json}
            end;
        {error, _} = E -> E
    end.

read_body(Req) ->
    case livery_req:body(Req) of
        empty -> {ok, <<>>};
        {buffered, Io} -> {ok, iolist_to_binary(Io)};
        {stream, Reader} ->
            case livery_body:read_all(Reader) of
                {ok, Bin, _R2} -> {ok, Bin};
                {error, Reason, _R2} -> {error, Reason}
            end
    end.

%% @private Render one batch element ({ok, Map} | {error, Reason}) as JSON.
batch_result({ok, Map}) when is_map(Map) -> jsonable(Map);
batch_result({ok, Other}) -> #{ok => true, result => Other};
batch_result({error, Reason}) -> #{error => err_bin(Reason)}.

err_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
err_bin(Other) -> iolist_to_binary(io_lib:format("~p", [Other])).

search_opts(Body) ->
    K = maps:get(<<"k">>, Body, 10),
    #{k => K}.

search_reply({ok, Hits}) when is_list(Hits) ->
    json_resp(200, #{hits => [hit(H) || H <- Hits]});
search_reply(Hits) when is_list(Hits) ->
    json_resp(200, #{hits => [hit(H) || H <- Hits]});
search_reply(Err) ->
    error_resp(Err).

%% @private Normalise a search hit to a JSON-safe map. Vector hits are already
%% maps; BM25 hits are {Id, Score} tuples.
hit({Id, Score}) -> #{key => Id, score => Score};
hit(Map) when is_map(Map) -> jsonable(Map);
hit(Other) -> Other.

att_content_type(Db, Id, Name) ->
    case barrel:attachment_info(Db, Id, Name) of
        {ok, Info} ->
            maps:get(<<"content_type">>, Info,
                maps:get(content_type, Info, <<"application/octet-stream">>));
        _ ->
            <<"application/octet-stream">>
    end.

%% @private Replace the HLC timestamp in a change with a JSON-safe cursor.
sanitize_change(Change) when is_map(Change) ->
    case maps:find(hlc, Change) of
        {ok, Hlc} -> Change#{hlc => barrel:hlc_encode(Hlc)};
        error -> Change
    end.

since_from_query(Req) ->
    case param(<<"since">>, Req) of
        undefined -> first;
        <<"first">> -> first;
        <<>> -> first;
        Cursor ->
            try barrel:hlc_decode(Cursor)
            catch _:_ -> first
            end
    end.

wants_sse(Req) ->
    Accept = livery_req:header(<<"accept">>, Req, <<>>),
    case binary:match(Accept, <<"text/event-stream">>) of
        nomatch -> param(<<"feed">>, Req) =:= <<"sse">>;
        _ -> true
    end.

param(Key, Req) ->
    case livery_req:query(Req) of
        undefined -> undefined;
        Raw ->
            find_param(Key, uri_string:dissect_query(to_bin(Raw)))
    end.

find_param(_Key, []) -> undefined;
find_param(Key, [{K, V} | T]) ->
    case to_bin(K) of
        Key -> to_bin(V);
        _ -> find_param(Key, T)
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> iolist_to_binary(L);
to_bin(true) -> <<>>.

json_resp(Status, Term) ->
    livery_resp:json(Status, json:encode(Term)).

error_resp({error, Reason}) -> error_resp(Reason);
error_resp(not_found) -> json_resp(404, #{error => <<"not_found">>});
error_resp(invalid_name) -> json_resp(400, #{error => <<"invalid_name">>});
error_resp(bad_json) -> json_resp(400, #{error => <<"bad_json">>});
error_resp(Reason) ->
    json_resp(500, #{error => iolist_to_binary(io_lib:format("~p", [Reason]))}).

%% @private Best-effort coercion of a result map to a JSON-encodable shape:
%% drop pairs whose value cannot be encoded (e.g. pids, refs, tuples).
jsonable(Map) when is_map(Map) ->
    maps:filter(fun(_K, V) -> is_jsonable(V) end, Map);
jsonable(Other) ->
    Other.

is_jsonable(V) when is_binary(V); is_number(V); is_boolean(V); is_atom(V) -> true;
is_jsonable(V) when is_list(V) -> lists:all(fun is_jsonable/1, V);
is_jsonable(V) when is_map(V) ->
    lists:all(fun is_jsonable/1, maps:values(V));
is_jsonable(_) -> false.
