%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_trace module
%%%
%%% Tests distributed tracing functionality including span creation,
%%% attribute generation, and context propagation.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_trace_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("instrument/include/instrument_otel.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases - span creation
-export([
    db_span_basic/1,
    db_span_with_attrs/1,
    http_span_basic/1,
    http_span_with_attrs/1,
    nested_spans/1
]).

%% Test cases - attributes
-export([
    db_attributes_basic/1,
    db_attributes_with_op/1,
    http_attributes_basic/1,
    http_attributes_with_status/1
]).

%% Test cases - context propagation
-export([
    inject_headers_basic/1,
    extract_headers_basic/1,
    inject_extract_roundtrip/1
]).

%% Test cases - error recording
-export([
    record_error_basic/1,
    record_error_with_attrs/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, spans}, {group, attributes}, {group, propagation}, {group, errors}].

groups() ->
    [
        {spans, [sequence], [
            db_span_basic,
            db_span_with_attrs,
            http_span_basic,
            http_span_with_attrs,
            nested_spans
        ]},
        {attributes, [sequence], [
            db_attributes_basic,
            db_attributes_with_op,
            http_attributes_basic,
            http_attributes_with_status
        ]},
        {propagation, [sequence], [
            inject_headers_basic,
            extract_headers_basic,
            inject_extract_roundtrip
        ]},
        {errors, [sequence], [
            record_error_basic,
            record_error_with_attrs
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    %% Use instrument_test for proper setup
    instrument_test:setup(),
    Config.

end_per_group(_Group, _Config) ->
    instrument_test:cleanup(),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Reset between tests
    instrument_test:reset(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Span Creation
%%====================================================================

db_span_basic(_Config) ->
    %% Test basic database span creation
    Result = barrel_trace:with_db_span(get, <<"testdb">>, fun() ->
        %% Verify we're in a span
        ?assertNotEqual(undefined, instrument_tracer:current_span()),
        42
    end),
    ?assertEqual(42, Result),

    %% Verify span completed
    Spans = instrument_test:get_spans(),
    ?assertEqual(1, length(Spans)),

    [Span] = Spans,
    %% Check span name follows convention: {op} {db}
    ?assertEqual(<<"get testdb">>, Span#span.name),
    %% Check attributes
    Attrs = Span#span.attributes,
    ?assertEqual(<<"barrel">>, maps:get(<<"db.system.name">>, Attrs)),
    ?assertEqual(<<"testdb">>, maps:get(<<"db.namespace">>, Attrs)),
    ?assertEqual(<<"get">>, maps:get(<<"db.operation.name">>, Attrs)),
    ok.

db_span_with_attrs(_Config) ->
    %% Test database span with extra attributes
    ExtraAttrs = #{
        <<"db.collection.name">> => <<"docs">>,
        <<"custom.doc_id">> => <<"doc123">>
    },
    barrel_trace:with_db_span(put, <<"mydb">>, ExtraAttrs, fun() ->
        ok
    end),

    Spans = instrument_test:get_spans(),
    ?assertEqual(1, length(Spans)),

    [Span] = Spans,
    Attrs = Span#span.attributes,
    %% Base attrs
    ?assertEqual(<<"barrel">>, maps:get(<<"db.system.name">>, Attrs)),
    ?assertEqual(<<"mydb">>, maps:get(<<"db.namespace">>, Attrs)),
    ?assertEqual(<<"put">>, maps:get(<<"db.operation.name">>, Attrs)),
    %% Extra attrs
    ?assertEqual(<<"docs">>, maps:get(<<"db.collection.name">>, Attrs)),
    ?assertEqual(<<"doc123">>, maps:get(<<"custom.doc_id">>, Attrs)),
    ok.

http_span_basic(_Config) ->
    %% Test basic HTTP span creation
    Result = barrel_trace:with_http_span(<<"GET">>, <<"/db/testdb/doc/123">>, fun() ->
        ?assertNotEqual(undefined, instrument_tracer:current_span()),
        {ok, response}
    end),
    ?assertEqual({ok, response}, Result),

    Spans = instrument_test:get_spans(),
    ?assertEqual(1, length(Spans)),

    [Span] = Spans,
    %% Check span name follows convention: {method} {path}
    ?assertEqual(<<"GET /db/testdb/doc/123">>, Span#span.name),
    %% Check kind is server
    ?assertEqual(server, Span#span.kind),
    %% Check attributes
    Attrs = Span#span.attributes,
    ?assertEqual(<<"GET">>, maps:get(<<"http.request.method">>, Attrs)),
    ?assertEqual(<<"/db/testdb/doc/123">>, maps:get(<<"url.path">>, Attrs)),
    ok.

http_span_with_attrs(_Config) ->
    %% Test HTTP span with extra attributes
    ExtraAttrs = #{
        <<"http.response.status_code">> => 201,
        <<"custom.request_id">> => <<"req-abc">>
    },
    barrel_trace:with_http_span(<<"POST">>, <<"/db/mydb/doc">>, ExtraAttrs, fun() ->
        ok
    end),

    Spans = instrument_test:get_spans(),
    ?assertEqual(1, length(Spans)),

    [Span] = Spans,
    Attrs = Span#span.attributes,
    ?assertEqual(<<"POST">>, maps:get(<<"http.request.method">>, Attrs)),
    ?assertEqual(201, maps:get(<<"http.response.status_code">>, Attrs)),
    ?assertEqual(<<"req-abc">>, maps:get(<<"custom.request_id">>, Attrs)),
    ok.

nested_spans(_Config) ->
    %% Test nested spans
    barrel_trace:with_http_span(<<"GET">>, <<"/db/mydb/find">>, fun() ->
        barrel_trace:with_db_span(query, <<"mydb">>, fun() ->
            %% Inner span should have parent
            ok
        end)
    end),

    Spans = instrument_test:get_spans(),
    ?assertEqual(2, length(Spans)),

    %% Find spans by name
    HttpSpan = find_span_by_name(<<"GET /db/mydb/find">>, Spans),
    DbSpan = find_span_by_name(<<"query mydb">>, Spans),

    ?assertNotEqual(undefined, HttpSpan),
    ?assertNotEqual(undefined, DbSpan),

    %% DB span should be child of HTTP span
    HttpTraceId = (HttpSpan#span.ctx)#span_ctx.trace_id,
    DbTraceId = (DbSpan#span.ctx)#span_ctx.trace_id,
    ?assertEqual(HttpTraceId, DbTraceId),

    %% Check parent relationship
    DbParentCtx = DbSpan#span.parent_ctx,
    HttpSpanId = (HttpSpan#span.ctx)#span_ctx.span_id,
    ?assertEqual(HttpSpanId, DbParentCtx#span_ctx.span_id),
    ok.

%%====================================================================
%% Test Cases - Attributes
%%====================================================================

db_attributes_basic(_Config) ->
    Attrs = barrel_trace:db_attributes(<<"testdb">>),
    ?assertEqual(<<"barrel">>, maps:get(<<"db.system.name">>, Attrs)),
    ?assertEqual(<<"testdb">>, maps:get(<<"db.namespace">>, Attrs)),
    ?assertNot(maps:is_key(<<"db.operation.name">>, Attrs)),
    ok.

db_attributes_with_op(_Config) ->
    Attrs = barrel_trace:db_attributes(<<"mydb">>, put),
    ?assertEqual(<<"barrel">>, maps:get(<<"db.system.name">>, Attrs)),
    ?assertEqual(<<"mydb">>, maps:get(<<"db.namespace">>, Attrs)),
    ?assertEqual(<<"put">>, maps:get(<<"db.operation.name">>, Attrs)),
    ok.

http_attributes_basic(_Config) ->
    Attrs = barrel_trace:http_attributes(<<"POST">>, <<"/api/doc">>),
    ?assertEqual(<<"POST">>, maps:get(<<"http.request.method">>, Attrs)),
    ?assertEqual(<<"/api/doc">>, maps:get(<<"url.path">>, Attrs)),
    ?assertNot(maps:is_key(<<"http.response.status_code">>, Attrs)),
    ok.

http_attributes_with_status(_Config) ->
    Attrs = barrel_trace:http_attributes(<<"GET">>, <<"/health">>, 200),
    ?assertEqual(<<"GET">>, maps:get(<<"http.request.method">>, Attrs)),
    ?assertEqual(<<"/health">>, maps:get(<<"url.path">>, Attrs)),
    ?assertEqual(200, maps:get(<<"http.response.status_code">>, Attrs)),
    ok.

%%====================================================================
%% Test Cases - Context Propagation
%%====================================================================

inject_headers_basic(_Config) ->
    %% Create a span to have trace context
    barrel_trace:with_db_span(get, <<"testdb">>, fun() ->
        Headers = barrel_trace:inject_headers([]),
        %% Should have traceparent header
        ?assertNotEqual(false, lists:keyfind(<<"traceparent">>, 1, Headers)),

        {<<"traceparent">>, TraceParent} = lists:keyfind(<<"traceparent">>, 1, Headers),
        %% Format: version-traceid-spanid-flags
        Parts = binary:split(TraceParent, <<"-">>, [global]),
        ?assertEqual(4, length(Parts)),
        [Version, TraceId, SpanId, Flags] = Parts,
        ?assertEqual(<<"00">>, Version),
        ?assertEqual(32, byte_size(TraceId)),  % 16 bytes hex encoded
        ?assertEqual(16, byte_size(SpanId)),   % 8 bytes hex encoded
        ?assertEqual(2, byte_size(Flags))
    end),
    ok.

extract_headers_basic(_Config) ->
    %% Simulate incoming headers with trace context
    TraceId = <<"0af7651916cd43dd8448eb211c80319c">>,
    SpanId = <<"b7ad6b7169203331">>,
    Headers = [
        {<<"traceparent">>, <<"00-", TraceId/binary, "-", SpanId/binary, "-01">>}
    ],

    %% Extract should attach context
    ok = barrel_trace:extract_headers(Headers),

    %% Create a span - it should continue the trace
    barrel_trace:with_db_span(get, <<"testdb">>, fun() ->
        %% Check trace ID matches
        CurrentTraceId = instrument_tracer:trace_id(),
        ?assertEqual(TraceId, CurrentTraceId)
    end),
    ok.

inject_extract_roundtrip(_Config) ->
    %% Create a span, inject headers, then extract and verify continuity
    barrel_trace:with_db_span(query, <<"db1">>, fun() ->
        OriginalTraceId = instrument_tracer:trace_id(),
        OriginalSpanId = instrument_tracer:span_id(),

        %% Inject headers
        Headers = barrel_trace:inject_headers([]),

        %% Simulate new process - extract headers
        spawn_link(fun() ->
            ok = barrel_trace:extract_headers(Headers),

            %% Create child span
            barrel_trace:with_db_span(get, <<"db1">>, fun() ->
                ChildTraceId = instrument_tracer:trace_id(),
                %% Trace ID should match
                ?assertEqual(OriginalTraceId, ChildTraceId)
            end)
        end),

        %% Wait for spawned process
        timer:sleep(100),

        %% Verify original context unchanged
        ?assertEqual(OriginalTraceId, instrument_tracer:trace_id()),
        ?assertEqual(OriginalSpanId, instrument_tracer:span_id())
    end),
    ok.

%%====================================================================
%% Test Cases - Error Recording
%%====================================================================

record_error_basic(_Config) ->
    %% Test error recording
    barrel_trace:with_db_span(put, <<"testdb">>, fun() ->
        barrel_trace:record_error(doc_conflict)
    end),

    Spans = instrument_test:get_spans(),
    ?assertEqual(1, length(Spans)),

    [Span] = Spans,
    %% Check status is error
    ?assertMatch({error, _}, Span#span.status),

    %% Check exception event was recorded
    Events = Span#span.events,
    ?assert(length(Events) > 0),
    [ExceptionEvent | _] = Events,
    ?assertEqual(<<"exception">>, ExceptionEvent#span_event.name),
    ok.

record_error_with_attrs(_Config) ->
    %% Test error recording with additional attributes
    barrel_trace:with_db_span(delete, <<"testdb">>, fun() ->
        barrel_trace:record_error(not_found, #{
            <<"doc_id">> => <<"doc123">>,
            <<"revision">> => <<"1-abc">>
        })
    end),

    Spans = instrument_test:get_spans(),
    ?assertEqual(1, length(Spans)),

    [Span] = Spans,
    ?assertMatch({error, _}, Span#span.status),

    %% Check exception event has custom attributes
    Events = Span#span.events,
    [ExceptionEvent | _] = Events,
    EventAttrs = ExceptionEvent#span_event.attributes,
    ?assertEqual(<<"doc123">>, maps:get(<<"doc_id">>, EventAttrs)),
    ?assertEqual(<<"1-abc">>, maps:get(<<"revision">>, EventAttrs)),
    ok.

%%====================================================================
%% Internal Helpers
%%====================================================================

find_span_by_name(Name, Spans) ->
    case lists:filter(fun(S) -> S#span.name == Name end, Spans) of
        [Span | _] -> Span;
        [] -> undefined
    end.
