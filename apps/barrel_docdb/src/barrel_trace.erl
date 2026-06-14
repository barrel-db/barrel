%%%-------------------------------------------------------------------
%%% @doc Distributed tracing helpers for barrel_docdb
%%%
%%% Provides OpenTelemetry-compatible tracing with semantic conventions
%%% for database operations following the OpenTelemetry Database Spans
%%% specification.
%%%
%%% Semantic conventions:
%%% - db.system.name: "barrel"
%%% - db.namespace: database name
%%% - db.collection.name: collection/table name
%%% - db.operation.name: operation type (get, put, delete, query)
%%% - db.query.text: query text (sanitized)
%%% - db.response.returned_rows: number of rows returned
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_trace).

%% Span creation with DB semantics
-export([
    with_db_span/3,
    with_db_span/4,
    with_http_span/3,
    with_http_span/4
]).

%% Attribute helpers
-export([
    db_attributes/1,
    db_attributes/2,
    http_attributes/2,
    http_attributes/3
]).

%% Context propagation for HTTP
-export([
    inject_headers/1,
    extract_headers/1,
    attach_context/1,
    with_extracted_context/2
]).

%% Recording check for expensive operations
-export([
    is_recording/0
]).

%% Error recording
-export([
    record_error/1,
    record_error/2
]).

%% Span access
-export([
    set_attribute/2,
    set_attributes/1,
    add_event/1,
    add_event/2
]).

%% Constants
-define(DB_SYSTEM, <<"barrel">>).

%%====================================================================
%% Span Creation
%%====================================================================

%% @doc Execute a function within a database span.
%% Creates a span with OpenTelemetry database semantic conventions.
%%
%% Example:
%% ```
%% barrel_trace:with_db_span(put, Db, fun() ->
%%     do_put(Db, DocId, Doc)
%% end).
%% '''
-spec with_db_span(atom(), binary() | undefined, fun(() -> Result)) -> Result
    when Result :: term().
with_db_span(Op, Db, Fun) ->
    with_db_span(Op, Db, #{}, Fun).

%% @doc Execute a function within a database span with extra attributes.
-spec with_db_span(atom(), binary() | undefined, map(), fun(() -> Result)) -> Result
    when Result :: term().
with_db_span(Op, Db, ExtraAttrs, Fun) ->
    SpanName = span_name(Op, Db),
    BaseAttrs = db_attributes(Db, Op),
    Attrs = maps:merge(BaseAttrs, ExtraAttrs),
    Opts = #{
        kind => internal,
        attributes => Attrs
    },
    instrument_tracer:with_span(SpanName, Opts, Fun).

%% @doc Execute a function within an HTTP server span.
%% Creates a span for incoming HTTP requests.
-spec with_http_span(binary(), binary(), fun(() -> Result)) -> Result
    when Result :: term().
with_http_span(Method, Path, Fun) ->
    with_http_span(Method, Path, #{}, Fun).

%% @doc Execute a function within an HTTP server span with extra attributes.
-spec with_http_span(binary(), binary(), map(), fun(() -> Result)) -> Result
    when Result :: term().
with_http_span(Method, Path, ExtraAttrs, Fun) ->
    SpanName = http_span_name(Method, Path),
    BaseAttrs = http_attributes(Method, Path),
    Attrs = maps:merge(BaseAttrs, ExtraAttrs),
    Opts = #{
        kind => server,
        attributes => Attrs
    },
    instrument_tracer:with_span(SpanName, Opts, Fun).

%%====================================================================
%% Attribute Helpers
%%====================================================================

%% @doc Generate database attributes for a span.
-spec db_attributes(binary() | undefined) -> map().
db_attributes(Db) ->
    db_attributes(Db, undefined).

%% @doc Generate database attributes with operation.
-spec db_attributes(binary() | undefined, atom() | undefined) -> map().
db_attributes(Db, Op) ->
    Attrs0 = #{<<"db.system.name">> => ?DB_SYSTEM},
    Attrs1 = case Db of
        undefined -> Attrs0;
        _ -> Attrs0#{<<"db.namespace">> => Db}
    end,
    case Op of
        undefined -> Attrs1;
        _ -> Attrs1#{<<"db.operation.name">> => atom_to_binary(Op, utf8)}
    end.

%% @doc Generate HTTP attributes for a span.
-spec http_attributes(binary(), binary()) -> map().
http_attributes(Method, Path) ->
    #{
        <<"http.request.method">> => Method,
        <<"url.path">> => Path
    }.

%% @doc Generate HTTP attributes with status code.
-spec http_attributes(binary(), binary(), integer()) -> map().
http_attributes(Method, Path, StatusCode) ->
    Attrs = http_attributes(Method, Path),
    Attrs#{<<"http.response.status_code">> => StatusCode}.

%%====================================================================
%% Context Propagation
%%====================================================================

%% @doc Inject trace context into HTTP headers.
%% Returns a list of {HeaderName, HeaderValue} tuples to add to outgoing requests.
-spec inject_headers(list()) -> list().
inject_headers(Headers) when is_list(Headers) ->
    Ctx = instrument_context:current(),
    TraceHeaders = instrument_propagation:inject_headers(Ctx),
    TraceHeaders ++ Headers.

%% @doc Extract trace context from HTTP headers.
%% Returns the extracted context (attached to current process).
%% @deprecated Use with_extracted_context/2 for proper context cleanup.
-spec extract_headers(list()) -> ok.
extract_headers(Headers) when is_list(Headers) ->
    Ctx = instrument_propagation:extract_headers(Headers),
    instrument_context:attach(Ctx),
    ok.

%% @doc Attach an extracted context to the current process.
%% @deprecated Use with_extracted_context/2 for proper context cleanup.
-spec attach_context(map()) -> ok.
attach_context(Ctx) when is_map(Ctx) ->
    instrument_context:attach(Ctx),
    ok.

%% @doc Execute function with trace context extracted from HTTP headers.
%% This properly scopes the context and ensures cleanup via detach.
-spec with_extracted_context(list(), fun(() -> Result)) -> Result
    when Result :: term().
with_extracted_context(Headers, Fun) when is_list(Headers), is_function(Fun, 0) ->
    Ctx = instrument_propagation:extract_headers(Headers),
    Token = instrument_context:attach(Ctx),
    try
        Fun()
    after
        instrument_context:detach(Token)
    end.

%% @doc Check if the current span is recording.
%% Use this to guard expensive attribute computation.
-spec is_recording() -> boolean().
is_recording() ->
    instrument_tracer:is_recording().

%%====================================================================
%% Error Recording
%%====================================================================

%% @doc Record an error on the current span.
-spec record_error(term()) -> ok.
record_error(Error) ->
    record_error(Error, #{}).

%% @doc Record an error with additional attributes.
-spec record_error(term(), map()) -> ok.
record_error(Error, Attrs) ->
    instrument_tracer:record_exception(Error, Attrs),
    ErrorMsg = format_error(Error),
    instrument_tracer:set_status(error, ErrorMsg),
    ok.

%%====================================================================
%% Span Modification
%%====================================================================

%% @doc Set a single attribute on the current span.
-spec set_attribute(term(), term()) -> ok.
set_attribute(Key, Value) ->
    instrument_tracer:set_attribute(Key, Value).

%% @doc Set multiple attributes on the current span.
-spec set_attributes(map()) -> ok.
set_attributes(Attrs) when is_map(Attrs) ->
    instrument_tracer:set_attributes(Attrs).

%% @doc Add an event to the current span.
-spec add_event(binary()) -> ok.
add_event(Name) ->
    instrument_tracer:add_event(Name).

%% @doc Add an event with attributes to the current span.
-spec add_event(binary(), map()) -> ok.
add_event(Name, Attrs) ->
    instrument_tracer:add_event(Name, Attrs).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Generate span name following OpenTelemetry conventions.
%% Format: {db.operation.name} {db.namespace} or {db.system.name}
span_name(Op, undefined) ->
    OpBin = atom_to_binary(Op, utf8),
    <<"barrel ", OpBin/binary>>;
span_name(Op, Db) ->
    OpBin = atom_to_binary(Op, utf8),
    <<OpBin/binary, " ", Db/binary>>.

%% @doc Generate HTTP span name.
%% Format: {http.method} {url.path}
http_span_name(Method, Path) ->
    <<Method/binary, " ", Path/binary>>.

%% @doc Format an error for the span status message.
format_error(Error) when is_binary(Error) ->
    Error;
format_error(Error) when is_atom(Error) ->
    atom_to_binary(Error, utf8);
format_error({error, Reason}) ->
    format_error(Reason);
format_error(Error) ->
    iolist_to_binary(io_lib:format("~p", [Error])).
