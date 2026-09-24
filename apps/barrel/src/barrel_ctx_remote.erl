%%%-------------------------------------------------------------------
%%% @doc Remote client for contexts: streamed BQL (`POST /db/:db/query')
%%% and id fetches (`POST /db/:db/_bulk_get'). One transport for both:
%%% hackney async, a deadline, a response byte cap, a node-wide slot
%%% counter, and bearer tokens from the `ctx_credentials' env.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_remote).

-export([query/3, bulk_get/3, observe/2, credential/2]).
-export([init_slots/0, acquire_slot/0, release_slot/0, slots_in_use/0]).

-define(DEFAULT_TIMEOUT, 4000).
-define(DEFAULT_MAX_BYTES, 16777216).
-define(DEFAULT_NODE_MAX, 32).
-define(MAX_ERROR_BODY, 65536).
-define(SLOTS_KEY, {?MODULE, slots}).

-type location() :: #{endpoint := binary(), db := binary(),
                      credential_ref => binary()}.
-type status() :: timeout | unreachable | unauthorized | error
                | skipped_budget.
-type failure() :: #{status := status(),
                     reason := term(),
                     rows_received := non_neg_integer(),
                     bytes := non_neg_integer()}.
-type meta() :: #{has_more := boolean(),
                  bound := binary() | undefined,
                  instance_id := binary() | undefined,
                  last_seq := binary() | undefined,
                  embedding => map(),
                  bytes := non_neg_integer()}.
-export_type([location/0, failure/0, meta/0]).

%% `mode': ndjson parses rows as they arrive; body buffers the whole
%% response (bounded by the byte cap).
-record(st, {conn :: term(),
             mode :: ndjson | body,
             deadline :: integer(),
             timeout :: non_neg_integer(),
             max_bytes :: pos_integer(),
             status = undefined :: undefined | integer(),
             buf = <<>> :: binary(),
             body = [] :: iolist(),
             bytes = 0 :: non_neg_integer(),
             rows = [] :: [map()],
             nrows = 0 :: non_neg_integer(),
             meta = undefined :: undefined | map()}).

%%====================================================================
%% API
%%====================================================================

%% @doc Run `Bql' against a remote location. Options: `timeout' (ms,
%% default 4000, also sent as the server `deadline_ms'), `max_bytes'
%% (default 16 MiB), `max_rows', `params', `credential' (a token) or
%% `credential_ref' (a key of the `ctx_credentials' env). Rows count
%% only when the final meta line arrives.
-spec query(location(), binary(), map()) ->
    {ok, [map()], meta()} | {error, failure()}.
query(#{db := Db} = Loc, Bql, Opts) ->
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    Body = iolist_to_binary(json:encode(request_body(Bql, Timeout, Opts))),
    request(ndjson, post, url(Loc, [<<"/db/">>, uri_string:quote(Db),
                                    <<"/query">>]),
            <<"application/x-ndjson">>, Body, Loc, Opts).

%% @doc Fetch `Ids' from a remote database, one entry per id in order:
%% `{ok, Doc}' (`<<"_embedding">>' vector decoded to floats when asked)
%% or `{error, Reason}'. Takes a node slot. Options: `include_embedding',
%% `timeout', `max_bytes', `credential', `credential_ref'.
-spec bulk_get(location(), [binary()], map()) ->
    {ok, [{ok, map()} | {error, term()}], non_neg_integer()} |
    {error, failure()}.
bulk_get(#{db := Db} = Loc, Ids, Opts) ->
    Body = iolist_to_binary(json:encode(
             #{<<"ids">> => Ids,
               <<"include_embedding">> =>
                   maps:get(include_embedding, Opts, false)})),
    with_slot(fun() ->
        case request(body, post, url(Loc, [<<"/db/">>, uri_string:quote(Db),
                                           <<"/_bulk_get">>]),
                     <<"application/json">>, Body, Loc, Opts) of
            {ok, RespBody} -> decode_results(RespBody, Ids);
            {error, _} = Err -> Err
        end
    end).

%% @doc Observed version of a remote database (instance id, last seq)
%% from the meta of a one-row query. Null values when unknown.
-spec observe(location(), map()) -> map().
observe(Loc, Opts) ->
    case query(Loc, <<"SELECT id FROM db LIMIT 1">>, Opts#{max_rows => 1}) of
        {ok, _Rows, #{instance_id := Iid, last_seq := Seq}} ->
            #{<<"instance_id">> => null_if_undefined(Iid),
              <<"last_seq">> => null_if_undefined(Seq)};
        {error, _} ->
            #{<<"instance_id">> => null, <<"last_seq">> => null}
    end.

%% @doc The token for a location: an explicit `credential' option, else
%% the `credential_ref' (option, then location) entry, else the endpoint
%% entry of the `ctx_credentials' env.
-spec credential(location() | binary(), map()) -> binary() | undefined.
credential(_Loc, #{credential := Token}) when is_binary(Token) ->
    Token;
credential(#{endpoint := Endpoint} = Loc, Opts) ->
    Ref = maps:get(credential_ref, Opts, maps:get(credential_ref, Loc, undefined)),
    credential_for(Ref, Endpoint);
credential(Endpoint, Opts) when is_binary(Endpoint) ->
    credential_for(maps:get(credential_ref, Opts, undefined), Endpoint).

%% @doc Create the node-wide remote request counter (barrel_app start).
-spec init_slots() -> ok.
init_slots() ->
    persistent_term:put(?SLOTS_KEY, atomics:new(1, [{signed, true}])).

%% @doc Take one node-wide remote slot (`ctx_node_remote_max', default
%% 32).
-spec acquire_slot() -> ok | {error, busy}.
acquire_slot() ->
    Ref = slots(),
    Max = application:get_env(barrel, ctx_node_remote_max, ?DEFAULT_NODE_MAX),
    case atomics:add_get(Ref, 1, 1) of
        N when N =< Max -> ok;
        _ ->
            atomics:sub(Ref, 1, 1),
            {error, busy}
    end.

-spec release_slot() -> ok.
release_slot() ->
    atomics:sub(slots(), 1, 1).

-spec slots_in_use() -> integer().
slots_in_use() ->
    atomics:get(slots(), 1).

%%====================================================================
%% Transport
%%====================================================================

%% The deadline starts before the connect: connecting and sending the
%% request spend the same budget as the answer.
request(Mode, Method, Url, Accept, Body, Loc, Opts) ->
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    Deadline = now_ms() + Timeout,
    Headers = [{<<"content-type">>, <<"application/json">>},
               {<<"accept">>, Accept}
               | auth_headers(credential(Loc, Opts))],
    HttpOpts = [{async, true},
                {connect_timeout, max(1, Timeout)},
                {recv_timeout, max(1, Timeout)},
                {send_timeout, max(1, Timeout)},
                {pool, false}],
    try hackney:request(Method, Url, Headers, Body, HttpOpts) of
        {ok, Conn} ->
            own(Conn),
            loop(#st{conn = Conn, mode = Mode,
                     deadline = Deadline, timeout = Timeout,
                     max_bytes = maps:get(max_bytes, Opts,
                                          ?DEFAULT_MAX_BYTES)});
        {error, Reason} ->
            {error, connect_failure(Reason, Timeout)}
    catch
        Class:Reason ->
            {error, failure(error, {Class, Reason}, 0, 0)}
    end.

%% hackney ties a connection to its supervisor, not to the caller: make
%% the caller the owner so the connection closes if the caller dies.
own(Conn) ->
    try hackney_conn:set_owner(Conn, self()) catch exit:_ -> ok end.

with_slot(Fun) ->
    case acquire_slot() of
        ok ->
            try Fun() after release_slot() end;
        {error, busy} ->
            {error, failure(skipped_budget, node_budget, 0, 0)}
    end.

loop(#st{conn = Conn, deadline = Deadline} = St) ->
    Remaining = max(0, Deadline - now_ms()),
    receive
        {hackney_response, Conn, {status, Status, _Reason}} ->
            loop(St#st{status = Status});
        {hackney_response, Conn, {headers, _Headers}} ->
            loop(St);
        {hackney_response, Conn, done} ->
            close(Conn),
            finish(St);
        %% hackney's receive timer can fire before ours: same deadline
        {hackney_response, Conn, {error, timeout}} ->
            close(Conn),
            {error, (failure(timeout, deadline, St))#{after_ms => St#st.timeout}};
        {hackney_response, Conn, {error, Reason}} ->
            close(Conn),
            {error, failure(transport_status(Reason), Reason, St)};
        {hackney_response, Conn, Chunk} when is_binary(Chunk) ->
            chunk(Chunk, St)
    after Remaining ->
        close(Conn),
        {error, (failure(timeout, deadline, St))#{after_ms => St#st.timeout}}
    end.

chunk(Chunk, #st{bytes = Bytes0, max_bytes = Max, conn = Conn} = St) ->
    Bytes = Bytes0 + byte_size(Chunk),
    case Bytes > Max of
        true ->
            close(Conn),
            {error, failure(error, response_too_large, St#st{bytes = Bytes})};
        false ->
            body_chunk(Chunk, St#st{bytes = Bytes})
    end.

body_chunk(Chunk, #st{status = 200, mode = body, body = Acc} = St) ->
    loop(St#st{body = [Acc, Chunk]});
body_chunk(Chunk, #st{status = 200, buf = Buf} = St) ->
    Lines = binary:split(<<Buf/binary, Chunk/binary>>, <<"\n">>, [global]),
    {Complete, [Rest]} = lists:split(length(Lines) - 1, Lines),
    case lines(Complete, St#st{buf = Rest}) of
        {ok, St1} -> loop(St1);
        {error, Reason, St1} ->
            close(St1#st.conn),
            {error, failure(error, Reason, St1)}
    end;
body_chunk(Chunk, #st{buf = Buf} = St) ->
    %% a non-200 body is an error document: keep a bounded prefix
    Kept = binary:part(<<Buf/binary, Chunk/binary>>, 0,
                       min(?MAX_ERROR_BODY, byte_size(Buf) + byte_size(Chunk))),
    loop(St#st{buf = Kept}).

lines([], St) ->
    {ok, St};
lines([<<>> | Rest], St) ->
    lines(Rest, St);
lines([_Line | _], #st{meta = Meta} = St) when Meta =/= undefined ->
    {error, data_after_meta, St};
lines([Line | Rest], St) ->
    try json:decode(Line) of
        #{<<"row">> := Row} when is_map(Row) ->
            lines(Rest, St#st{rows = [Row | St#st.rows],
                              nrows = St#st.nrows + 1});
        #{<<"meta">> := Meta} when is_map(Meta) ->
            lines(Rest, St#st{meta = Meta});
        #{<<"error">> := Error} ->
            {error, {remote_error, Error}, St};
        _Other ->
            {error, bad_line, St}
    catch
        _:_ -> {error, bad_line, St}
    end.

finish(#st{status = 200, mode = body, body = Body}) ->
    {ok, iolist_to_binary(Body)};
finish(#st{status = 200, buf = Buf} = St0) ->
    case lines([Buf], St0#st{buf = <<>>}) of
        {ok, #st{meta = undefined} = St} ->
            {error, failure(error, missing_meta, St)};
        {ok, #st{rows = Rows, meta = Meta, bytes = Bytes}} ->
            {ok, lists:reverse(Rows), meta(Meta, Bytes)};
        {error, Reason, St} ->
            {error, failure(error, Reason, St)}
    end;
finish(#st{status = Status, buf = Buf} = St) ->
    {error, failure(status_class(Status), {http_status, Status, error_body(Buf)},
                    St)}.

%%====================================================================
%% Internal
%%====================================================================

request_body(Bql, Timeout, Opts) ->
    Base = #{query => Bql, deadline_ms => max(1, Timeout)},
    Base1 = case maps:get(max_rows, Opts, undefined) of
        undefined -> Base;
        MaxRows -> Base#{max_rows => MaxRows}
    end,
    case maps:get(params, Opts, #{}) of
        Params when map_size(Params) > 0 -> Base1#{params => Params};
        _ -> Base1
    end.

meta(Meta, Bytes) ->
    Base = #{has_more => maps:get(<<"has_more">>, Meta, false) =:= true,
             bound => bin_or_undefined(maps:get(<<"bound">>, Meta, undefined)),
             instance_id =>
                 bin_or_undefined(maps:get(<<"instance_id">>, Meta, undefined)),
             last_seq =>
                 bin_or_undefined(maps:get(<<"last_seq">>, Meta, undefined)),
             bytes => Bytes},
    case maps:get(<<"embedding">>, Meta, undefined) of
        #{} = Emb -> Base#{embedding => Emb};
        _ -> Base
    end.

decode_results(Bin, Ids) ->
    try json:decode(Bin) of
        #{<<"results">> := Results} when length(Results) =:= length(Ids) ->
            {ok, [result(R) || R <- Results], byte_size(Bin)};
        _ ->
            {error, failure(error, bad_response, 0, byte_size(Bin))}
    catch
        _:_ -> {error, failure(error, bad_json, 0, byte_size(Bin))}
    end.

result(#{<<"error">> := Reason}) ->
    {error, Reason};
result(#{<<"_embedding">> := #{<<"vector">> := V} = E} = Doc)
        when is_binary(V) ->
    {ok, Doc#{<<"_embedding">> =>
                  E#{<<"vector">> =>
                         barrel_doc:decode_embedding(base64:decode(V))}}};
result(Doc) when is_map(Doc) ->
    {ok, Doc}.

bin_or_undefined(V) when is_binary(V) -> V;
bin_or_undefined(_) -> undefined.

null_if_undefined(undefined) -> null;
null_if_undefined(V) -> V.

failure(Status, Reason, #st{nrows = N, bytes = Bytes}) ->
    failure(Status, Reason, N, Bytes).

failure(Status, Reason, Rows, Bytes) ->
    #{status => Status, reason => Reason, rows_received => Rows,
      bytes => Bytes}.

connect_failure(Reason, Timeout) when Reason =:= timeout;
                                      Reason =:= connect_timeout ->
    (failure(timeout, connect_timeout, 0, 0))#{after_ms => Timeout};
connect_failure(Reason, _Timeout) ->
    failure(transport_status(Reason), Reason, 0, 0).

%% Anything that failed before or while reaching the server.
transport_status(econnrefused) -> unreachable;
transport_status(nxdomain) -> unreachable;
transport_status(ehostunreach) -> unreachable;
transport_status(enetunreach) -> unreachable;
transport_status(closed) -> unreachable;
transport_status(timeout) -> timeout;
transport_status(connect_timeout) -> timeout;
transport_status({_Tag, Reason}) -> transport_status(Reason);
transport_status(_Other) -> error.

status_class(401) -> unauthorized;
status_class(403) -> unauthorized;
status_class(_Status) -> error.

error_body(Buf) ->
    try json:decode(Buf) of
        #{<<"error">> := Error} -> Error;
        _ -> Buf
    catch
        _:_ -> Buf
    end.

auth_headers(undefined) -> [];
auth_headers(Token) -> [{<<"authorization">>, <<"Bearer ", Token/binary>>}].

url(#{endpoint := Endpoint}, Path) ->
    iolist_to_binary([trim_slash(Endpoint) | Path]).

%% A named reference wins over the endpoint entry.
credential_for(Ref, Endpoint) ->
    Creds = maps:to_list(application:get_env(barrel, ctx_credentials, #{})),
    case Ref of
        undefined -> lookup_credential(to_bin(Endpoint), Creds);
        _ ->
            case lookup_credential(to_bin(Ref), Creds) of
                undefined -> lookup_credential(to_bin(Endpoint), Creds);
                Token -> Token
            end
    end.

lookup_credential(_Key, []) ->
    undefined;
lookup_credential(Key, [{K, Token} | Rest]) ->
    case trim_slash(to_bin(K)) =:= trim_slash(Key) of
        true -> to_bin(Token);
        false -> lookup_credential(Key, Rest)
    end.

close(Conn) ->
    _ = try hackney:close(Conn) catch _:_ -> ok end,
    flush(Conn).

%% Drop messages the connection sent before it closed.
flush(Conn) ->
    receive
        {hackney_response, Conn, _} -> flush(Conn)
    after 0 ->
        ok
    end.

slots() ->
    case persistent_term:get(?SLOTS_KEY, undefined) of
        undefined ->
            ok = init_slots(),
            persistent_term:get(?SLOTS_KEY);
        Ref ->
            Ref
    end.

trim_slash(<<>>) ->
    <<>>;
trim_slash(Bin) ->
    case binary:last(Bin) of
        $/ -> trim_slash(binary:part(Bin, 0, byte_size(Bin) - 1));
        _ -> Bin
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> unicode:characters_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).

now_ms() ->
    erlang:monotonic_time(millisecond).
