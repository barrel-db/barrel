%% @doc The network replication transport: barrel_rep_transport over
%% HTTP (hackney), speaking the /db/:db/_sync/* wire served by
%% barrel_server_sync.
%%
%% The endpoint term is a normalized map built by {@link endpoint/1};
%% the normalized URL alone feeds the replication id (rep_id_term/1),
%% so credentials and tuning never invalidate checkpoints. Every
%% request carries an x-barrel-hlc header and folds the response
%% header back into the local clock (passive coupling on top of the
%% explicit sync_hlc exchange).
%%
%% Auth: `auth => #{token => Bin}' on the endpoint, else the app env
%% `{barrel_docdb, sync_auth, #{Key => Token}}'. Likewise `signing' and
%% `sync_signing', `ssl_options' and `sync_ssl'. `Key' is the full
%% database URL (normalized: lowercase scheme and host, no trailing
%% slash), looked up first, or the origin `<<"scheme://host:port">>'.
%% Persisted tasks only use the app env. A URL with user info is
%% rejected so credentials never reach logs or replication ids.
%%
%% Every request runs in a worker linked to the caller, killed at the
%% endpoint's `request_timeout' (whole request, attachment streams
%% included), which then returns `{error, request_timeout}'.
-module(barrel_rep_transport_http).
-behaviour(barrel_rep_transport).

-export([endpoint/1, has_userinfo/1]).

%% barrel_rep_transport callbacks
-export([
    get_doc/3,
    put_version/5,
    full_state/1,
    get_doc_state/2,
    put_doc_state/4,
    put_doc_state/5,
    repair_doc_bodies/3,
    diff_versions/2,
    get_changes/3,
    get_local_doc/2,
    put_local_doc/3,
    delete_local_doc/2,
    db_info/1,
    sync_hlc/2,
    rep_id_term/1
]).

%% Attachment sync (optional callbacks): raw octets streamed both
%% directions, digest and origin HLC in headers.
-export([att_changes/3, diff_attachments/2, get_attachment_stream/3,
         put_attachment/5, delete_attachment/4, touch_attachment/4,
         att_state/3, att_list/2, flush_attachments/1,
         att_capabilities/1]).

-export_type([endpoint/0]).

-ifdef(TEST).
-export([error_of/1]).
-endif.

-type endpoint() :: #{
    url := binary(),
    auth => #{token := binary()},
    %% Ed25519 signed-request auth (see barrel_sync_sig). Takes precedence
    %% over `auth' when set. priv_key is a 32-byte raw Ed25519 key.
    signing => #{key_id := binary(), priv_key := binary()},
    %% hackney ssl_options for mTLS (honored on https URLs).
    ssl_options => [term()],
    pool => atom(),
    connect_timeout => pos_integer(),
    recv_timeout => pos_integer(),
    headers => [{binary(), binary()}]
}.

%% Overall wall-clock budget for a single request. recv_timeout bounds
%% each receive but not the whole request, so a slow-trickle peer could
%% otherwise block a continuous task forever.
-define(REQUEST_TIMEOUT, 300000).

-define(HLC_HEADER, <<"x-barrel-hlc">>).
-define(DIGEST_HEADER, <<"x-barrel-digest">>).
-define(ORIGIN_HEADER, <<"x-barrel-att-origin">>).
-define(TOUCH_HEADER, <<"x-barrel-att-touch">>).

%%====================================================================
%% Endpoint
%%====================================================================

%% @doc Build a normalized endpoint from a URL (or a map carrying one).
%% Normalization matters: the URL is the replication identity, so
%% scheme/host lowercase and no trailing slash keep checkpoints stable
%% across config reformatting.
-spec endpoint(binary() | string() | map()) -> endpoint().
endpoint(#{url := Url} = Map) ->
    Map#{url := normalize_url(Url)};
endpoint(Url) when is_binary(Url); is_list(Url) ->
    #{url => normalize_url(Url)}.

normalize_url(Url0) ->
    Url = unicode:characters_to_binary(Url0),
    case uri_string:parse(Url) of
        #{scheme := Scheme0, host := Host0, path := Path0} = Parsed ->
            Scheme = string:lowercase(to_bin(Scheme0)),
            Host = string:lowercase(to_bin(Host0)),
            Path = strip_trailing_slash(to_bin(Path0)),
            SchemeOk = lists:member(Scheme, [<<"http">>, <<"https">>]),
            case maps:is_key(userinfo, Parsed) of
                true -> erlang:error({invalid_sync_url, userinfo});
                false -> ok
            end,
            Clean = not (maps:is_key(query, Parsed)
                         orelse maps:is_key(fragment, Parsed)),
            PathOk = case Path of
                <<"/db/", DbName/binary>> when DbName =/= <<>> -> true;
                _ -> false
            end,
            case SchemeOk andalso Clean andalso PathOk of
                true ->
                    PortPart = case maps:get(port, Parsed, undefined) of
                        undefined -> <<>>;
                        Port -> <<":", (integer_to_binary(Port))/binary>>
                    end,
                    <<Scheme/binary, "://", Host/binary, PortPart/binary,
                      Path/binary>>;
                false ->
                    erlang:error({invalid_sync_url, Url})
            end;
        _ ->
            erlang:error({invalid_sync_url, Url})
    end.

%% @doc Whether a URL carries user info (`user:pass@host').
-spec has_userinfo(term()) -> boolean().
has_userinfo(Url) ->
    try uri_string:parse(unicode:characters_to_binary(Url)) of
        #{userinfo := _} -> true;
        _ -> false
    catch
        _:_ -> false
    end.

strip_trailing_slash(Path) ->
    case binary:last(Path) of
        $/ -> strip_trailing_slash(binary:part(Path, 0,
                                               byte_size(Path) - 1));
        _ -> Path
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> unicode:characters_to_binary(L).

%% @doc The replication identity of this endpoint: the normalized URL
%% only (credentials and tuning excluded).
rep_id_term(#{url := Url}) -> Url.

%%====================================================================
%% Transport callbacks
%%====================================================================

%% A server from 1.11 answers in CBOR, which carries any binary; an
%% older one ignores the accept header and answers JSON.
get_doc(Endpoint, DocId, _Opts) ->
    case req_typed(Endpoint, get, [<<"/doc/">>, quote(DocId)], undefined,
                   [{<<"accept">>, <<"application/cbor, application/json">>}]) of
        {ok, 200, Encoding, #{<<"doc">> := Doc, <<"version">> := Token,
                              <<"vv">> := VV} = Body} ->
            {ok, Doc, #{version => Token,
                        vv => vv_from_wire(Encoding, VV),
                        deleted => maps:get(<<"deleted">>, Body, false)
                                       =:= true}};
        {ok, Status, _Encoding, Body} ->
            error_of({ok, Status, Body});
        Other ->
            error_of(Other)
    end.

%% JSON when it can carry the document (vv in base64), else CBOR with
%% the version vector as a byte string.
put_version(Endpoint, Doc, Token, VVBin, Deleted) ->
    DocId = maps:get(<<"id">>, Doc),
    Fields = #{doc => Doc, version => Token, deleted => Deleted},
    Body = try json:encode(Fields#{vv => base64:encode(VVBin)}) of
        Json -> {encoded, <<"application/json">>, iolist_to_binary(Json)}
    catch
        error:_ ->
            {encoded, <<"application/cbor">>,
             barrel_docdb_codec_cbor:encode_wire(Fields#{vv => VVBin})}
    end,
    case req(Endpoint, put, [<<"/doc/">>, quote(DocId)], Body) of
        {ok, 200, #{<<"id">> := Id, <<"winner">> := Winner}} ->
            {ok, Id, Winner};
        Other ->
            error_of(Other)
    end.

%% @doc Whether the server replicates whole document states (it says
%% so in /info). Asked once per endpoint per process.
full_state(Endpoint) ->
    Key = {?MODULE, full_state, maps:get(url, Endpoint)},
    case get(Key) of
        undefined ->
            Answer = case req(Endpoint, get, <<"/info">>, undefined) of
                {ok, 200, #{<<"full_state">> := true}} -> true;
                _ -> false
            end,
            put(Key, Answer),
            Answer;
        Answer ->
            Answer
    end.

get_doc_state(Endpoint, DocId) ->
    case req_typed(Endpoint, get, [<<"/state/">>, quote(DocId)], undefined,
                   [{<<"accept">>, <<"application/cbor">>}]) of
        {ok, 200, _Encoding, #{<<"state">> := State, <<"bodies">> := Bodies} = R} ->
            {ok, #{state => State, bodies => Bodies,
                   vectors => maps:get(<<"vectors">>, R, #{})}};
        {ok, Status, _Encoding, Body} ->
            error_of({ok, Status, Body});
        Other ->
            error_of(Other)
    end.

put_doc_state(Endpoint, DocId, State, Bodies) ->
    put_doc_state(Endpoint, DocId, State, Bodies, #{}).

%% An older server ignores `vectors': the target embeds the text.
put_doc_state(Endpoint, DocId, State, Bodies, Vectors) ->
    Fields = case map_size(Vectors) of
        0 -> #{state => State, bodies => Bodies};
        _ -> #{state => State, bodies => Bodies, vectors => Vectors}
    end,
    Body = {encoded, <<"application/cbor">>,
            barrel_docdb_codec_cbor:encode_wire(Fields)},
    case req(Endpoint, put, [<<"/state/">>, quote(DocId)], Body,
             [{<<"accept">>, <<"application/cbor">>}]) of
        {ok, 200, #{<<"id">> := Id, <<"winner">> := Winner,
                    <<"written">> := true}} ->
            {ok, Id, Winner};
        {ok, 200, #{<<"id">> := Id, <<"winner">> := Winner}} ->
            {unchanged, Id, Winner};
        {ok, 409, #{<<"error">> := <<"clock_skew">>}} ->
            {error, clock_skew};
        Other ->
            error_of(Other)
    end.

%% Rewrite stored bodies from a healthy copy (barrel_rep:reconcile/3).
repair_doc_bodies(Endpoint, DocId, Bodies) ->
    Body = {encoded, <<"application/cbor">>,
            barrel_docdb_codec_cbor:encode_wire(#{bodies => Bodies})},
    case req(Endpoint, put, [<<"/repair/">>, quote(DocId)], Body,
             [{<<"accept">>, <<"application/cbor">>}]) of
        {ok, 200, #{<<"id">> := Id, <<"repaired">> := Tokens}} ->
            {ok, Id, Tokens};
        Other ->
            error_of(Other)
    end.

diff_versions(Endpoint, TokenMap) ->
    case req(Endpoint, post, <<"/diff">>, #{versions => TokenMap}) of
        {ok, 200, #{<<"diff">> := Wire}} ->
            {ok, maps:map(fun(_Id, <<"missing">>) -> missing;
                             (_Id, <<"have">>) -> have
                          end, Wire)};
        Other ->
            error_of(Other)
    end.

get_changes(Endpoint, Since, Opts) ->
    Filter = barrel_rep_filter:to_wire(
        maps:with([paths, query, channel], Opts)),
    Body0 = #{since => seq_to_wire(Since),
              limit => maps:get(limit, Opts, 100)},
    Body = case map_size(Filter) of
        0 -> Body0;
        _ -> Body0#{filter => Filter}
    end,
    case req(Endpoint, post, <<"/changes">>, Body) of
        {ok, 200, #{<<"changes">> := Changes,
                    <<"last_seq">> := LastSeqWire}} ->
            case seq_from_wire(LastSeqWire) of
                {ok, LastSeq} ->
                    {ok, [change_from_wire(C) || C <- Changes], LastSeq};
                error ->
                    {error, {bad_response, last_seq}}
            end;
        Other ->
            error_of(Other)
    end.

get_local_doc(Endpoint, DocId) ->
    case req(Endpoint, get, [<<"/local/">>, quote(DocId)], undefined) of
        {ok, 200, Doc} -> {ok, Doc};
        Other -> error_of(Other)
    end.

put_local_doc(Endpoint, DocId, Doc) ->
    case req(Endpoint, put, [<<"/local/">>, quote(DocId)], Doc) of
        {ok, 200, _} -> ok;
        Other -> error_of(Other)
    end.

delete_local_doc(Endpoint, DocId) ->
    case req(Endpoint, delete, [<<"/local/">>, quote(DocId)], undefined) of
        {ok, 200, _} -> ok;
        Other -> error_of(Other)
    end.

db_info(Endpoint) ->
    case req(Endpoint, get, <<"/info">>, undefined) of
        {ok, 200, Body} ->
            Info0 = #{db => maps:get(<<"db">>, Body, undefined)},
            Info1 = maybe_floor(history_floor, <<"history_floor">>, Body,
                                Info0),
            Info2 = maybe_floor(att_floor, <<"att_floor">>, Body, Info1),
            Info = Info2#{att_flush => flag(<<"att_flush">>, Body),
                          att_filter => flag(<<"att_filter">>, Body),
                          read_only => flag(<<"read_only">>, Body)},
            {ok, Info};
        Other ->
            error_of(Other)
    end.

flag(WireKey, Body) ->
    maps:get(WireKey, Body, false) =:= true.

maybe_floor(Key, WireKey, Body, Info) ->
    case maps:get(WireKey, Body, undefined) of
        undefined -> Info;
        B64 -> Info#{Key => barrel_hlc:decode(base64:decode(B64))}
    end.

%% Explicit clock exchange: ship ours, adopt theirs. Both clocks end
%% at or beyond the other's, same coupling as the local transport plus
%% the return leg.
sync_hlc(Endpoint, Hlc) ->
    Body = #{hlc => base64:encode(barrel_hlc:encode(Hlc))},
    case req(Endpoint, post, <<"/hlc">>, Body) of
        {ok, 200, #{<<"hlc">> := TheirsB64}} ->
            Theirs = barrel_hlc:decode(base64:decode(TheirsB64)),
            barrel_hlc:sync_hlc(Theirs);
        {ok, 409, #{<<"error">> := <<"clock_skew">>}} ->
            logger:warning("sync_hlc rejected by ~s: clock skew",
                           [maps:get(url, Endpoint)]),
            {error, clock_skew};
        Other ->
            error_of(Other)
    end.

%%====================================================================
%% Attachment sync
%%====================================================================

att_changes(Endpoint, Since, Opts) ->
    Limit = maps:get(limit, Opts, 100),
    FilterParam = case barrel_rep_filter:to_wire(maps:get(filter, Opts, #{})) of
        Wire when map_size(Wire) =:= 0 ->
            [];
        Wire ->
            [<<"&filter=">>,
             base64:encode(iolist_to_binary(json:encode(Wire)),
                           #{mode => urlsafe, padding => false})]
    end,
    Path = [<<"/att_changes?since=">>, quote(seq_to_wire(Since)),
            <<"&limit=">>, integer_to_binary(Limit) | FilterParam],
    case req(Endpoint, get, Path, undefined) of
        {ok, 200, #{<<"changes">> := Changes,
                    <<"last_seq">> := LastSeqWire}} ->
            case seq_from_wire(LastSeqWire) of
                {ok, LastSeq} ->
                    {ok, [att_entry_from_wire(E) || E <- Changes],
                     LastSeq};
                error ->
                    {error, {bad_response, last_seq}}
            end;
        {ok, 501, _} ->
            %% the server speaks the protocol but its attachment
            %% backend has no feed: the rep degrades to skipped
            {error, att_sync_unsupported};
        Other ->
            error_of(Other)
    end.

%% Origins ride along: a server that knows them answers under
%% last-write-wins (and `touch'), an older one by digest.
diff_attachments(Endpoint, Entries) ->
    Wire = [maps:fold(fun(origin, O, Acc) -> Acc#{origin => hlc_to_wire(O)};
                         (K, V, Acc) -> Acc#{K => V}
                      end, #{}, maps:with([id, name, digest, origin], E))
            || E <- Entries],
    case req(Endpoint, post, <<"/att_diff">>,
             #{attachments => Wire, touch => true}) of
        {ok, 200, #{<<"diff">> := Diff}} ->
            {ok, [#{id => maps:get(<<"id">>, D),
                    name => maps:get(<<"name">>, D),
                    status => case maps:get(<<"status">>, D) of
                                  <<"have">> -> have;
                                  <<"touch">> -> touch;
                                  <<"missing">> -> missing
                              end} || D <- Diff]};
        Other ->
            error_of(Other)
    end.

%% An empty PUT on the attachment route marked as a touch. An older
%% server writes the empty body, fails its digest check (422) and so
%% asks for the bytes too.
touch_attachment(Endpoint, DocId, Name, #{origin := Origin,
                                          digest := Digest}) ->
    Extra = [{?TOUCH_HEADER, <<"1">>},
             {?DIGEST_HEADER, Digest},
             {?ORIGIN_HEADER, hlc_to_wire(Origin)}],
    case req_hashed(Endpoint, put,
                    [<<"/att/">>, quote(DocId), <<"/">>, quote(Name)],
                    Digest, Extra) of
        {ok, 200, #{<<"ok">> := <<"ignored">>}} -> {ok, ignored};
        {ok, 200, #{<<"ok">> := <<"touched">>}} -> {ok, touched};
        {ok, 200, _} -> {error, bytes_needed};
        {ok, 412, _} -> {error, bytes_needed};
        {ok, 422, _} -> {error, bytes_needed};
        Other -> error_of(Other)
    end.

att_state(Endpoint, DocId, Name) ->
    case req(Endpoint, get,
             [<<"/att_state/">>, quote(DocId), <<"/">>, quote(Name)],
             undefined) of
        {ok, 200, #{<<"origin">> := O, <<"op">> := Op} = B} ->
            {ok, #{id => DocId, name => Name, origin => hlc_from_wire(O),
                   op => case Op of
                             <<"put">> -> put;
                             <<"delete">> -> delete
                         end,
                   digest => maps:get(<<"digest">>, B, <<>>)}};
        Other ->
            error_of(Other)
    end.

att_list(Endpoint, DocIds) ->
    case req(Endpoint, post, <<"/att_list">>, #{ids => DocIds}) of
        {ok, 200, #{<<"attachments">> := Entries}} ->
            {ok, [att_entry_from_wire(E) || E <- Entries]};
        Other ->
            error_of(Other)
    end.

%% 200 only once the server synced its attachment writes.
flush_attachments(Endpoint) ->
    case req(Endpoint, post, <<"/att_flush">>, #{}) of
        {ok, 200, _} -> ok;
        {ok, 501, _} -> {error, unsupported};
        Other -> error_of(Other)
    end.

%% From _sync/info: an older server advertises neither.
att_capabilities(Endpoint) ->
    case db_info(Endpoint) of
        {ok, Info} ->
            {ok, #{flush => maps:get(att_flush, Info, false),
                   filter => maps:get(att_filter, Info, false)}};
        {error, _} = Error ->
            Error
    end.

hlc_to_wire(Hlc) ->
    base64:encode(barrel_hlc:encode(Hlc)).

%% hackney's sync request/5 always buffers the response body, so the
%% streamed read goes through connect + send_request, which leaves the
%% body on the connection for stream_body/1. A linked worker owns the
%% connection until EOF, an error, the deadline or close (Info's
%% `close' fun, for a consumer that stops early).
get_attachment_stream(Endpoint, DocId, Name) ->
    Deadline = deadline(Endpoint),
    Caller = self(),
    Tag = make_ref(),
    Pid = spawn_link(fun() ->
        download(Caller, Tag, Deadline, Endpoint, DocId, Name)
    end),
    receive
        {Tag, {ok, Info}} ->
            {ok, Info#{close => fun() -> close_download(Pid, Tag) end},
             download_read_fun(Pid, Tag, Deadline)};
        {Tag, {error, _} = Error} ->
            ok = forget_worker(Pid),
            Error
    after remaining(Deadline) ->
        ok = kill_worker(Pid),
        receive {Tag, _} -> ok after 0 -> ok end,
        {error, request_timeout}
    end.

download(Caller, Tag, Deadline, Endpoint, DocId, Name) ->
    CallerRef = erlang:monitor(process, Caller),
    case guarded(fun() -> open_download(Endpoint, DocId, Name) end) of
        {ok, Info, Conn} ->
            Caller ! {Tag, {ok, Info}},
            download_loop(Conn, Tag, CallerRef, Deadline);
        {error, _} = Error ->
            Caller ! {Tag, Error}
    end.

open_download(Endpoint, DocId, Name) ->
    Url = att_url(Endpoint, DocId, Name),
    case hackney:connect(Url, stream_opts(Endpoint)) of
        {ok, ConnPid} ->
            case hackney:send_request(
                     ConnPid,
                     {get, url_path(Url),
                      base_headers(Endpoint, <<"GET">>, url_target(Url),
                                   barrel_sync_sig:content_sha256(<<>>), []),
                      <<>>}) of
                {ok, 200, RespHeaders, ConnPid2} ->
                    ok = barrel_hlc:maybe_sync_from_header(
                        header_value(?HLC_HEADER, RespHeaders)),
                    Info = #{content_type =>
                                 header_value(<<"content-type">>,
                                              RespHeaders),
                             digest =>
                                 header_value(?DIGEST_HEADER,
                                              RespHeaders)},
                    {ok, Info, ConnPid2};
                {ok, Status, RespHeaders, ConnPid2} ->
                    Body = case hackney:body(ConnPid2) of
                        {ok, B} -> decode_body(
                              header_value(<<"content-type">>, RespHeaders),
                              B);
                        _ -> #{}
                    end,
                    _ = hackney:close(ConnPid2),
                    error_of({ok, Status, Body});
                {error, Reason} ->
                    _ = hackney:close(ConnPid),
                    {error, {transport, Reason}}
            end;
        {error, Reason} ->
            {error, {transport, Reason}}
    end.

%% Serves one chunk per request, to whichever process reads the stream.
download_loop(Conn, Tag, CallerRef, Deadline) ->
    receive
        {Tag, next, From, Ref} ->
            case guarded(fun() -> hackney:stream_body(Conn) end) of
                {ok, Chunk} ->
                    From ! {Ref, {ok, Chunk}},
                    download_loop(Conn, Tag, CallerRef, Deadline);
                done ->
                    From ! {Ref, eof},
                    close_conn(Conn);
                {error, Reason} ->
                    From ! {Ref, {error, {transport, Reason}}},
                    close_conn(Conn)
            end;
        {Tag, close} ->
            close_conn(Conn);
        {'DOWN', CallerRef, process, _, _} ->
            close_conn(Conn)
    after remaining(Deadline) ->
        close_conn(Conn)
    end.

download_read_fun(Pid, Tag, Deadline) ->
    fun() ->
        MRef = erlang:monitor(process, Pid),
        Pid ! {Tag, next, self(), MRef},
        receive
            {MRef, {ok, Chunk}} ->
                erlang:demonitor(MRef, [flush]),
                {ok, Chunk, download_read_fun(Pid, Tag, Deadline)};
            {MRef, Last} ->
                erlang:demonitor(MRef, [flush]),
                Last;
            {'DOWN', MRef, process, Pid, _} ->
                %% closed, or gone at the deadline
                case remaining(Deadline) of
                    0 -> {error, request_timeout};
                    _ -> {error, {transport, closed}}
                end
        after remaining(Deadline) ->
            erlang:demonitor(MRef, [flush]),
            ok = kill_worker(Pid),
            {error, request_timeout}
        end
    end.

close_download(Pid, Tag) ->
    Pid ! {Tag, close},
    forget_worker(Pid).

close_conn(Conn) ->
    _ = guarded(fun() -> hackney:close(Conn) end),
    ok.

url_path(Url) ->
    #{path := Path} = uri_string:parse(Url),
    Path.

%% The signed target: path plus the raw query (`<<>>' when none), what
%% the server signs on its side too.
url_target(Url) ->
    Parsed = uri_string:parse(Url),
    {maps:get(path, Parsed), maps:get(query, Parsed, <<>>)}.

put_attachment(Endpoint, DocId, Name, Meta, ReadFun) ->
    #{content_type := ContentType, digest := Digest,
      origin_hlc := Origin} = Meta,
    AttUrl = att_url(Endpoint, DocId, Name),
    %% The attachment body is streamed (never in memory here), so its
    %% content-address `Digest' is the signed content hash; body integrity
    %% rides the content-addressed store (422 digest_mismatch).
    Headers = base_headers(Endpoint, <<"PUT">>, url_target(AttUrl), Digest,
                           [{<<"content-type">>,
                             att_content_type(ContentType)},
                            {?DIGEST_HEADER, Digest},
                            {?ORIGIN_HEADER,
                             base64:encode(barrel_hlc:encode(Origin))}]),
    %% connect, every chunk and the final response share one deadline
    bounded(fun() ->
        case hackney:request(put, AttUrl, Headers,
                             stream, stream_opts(Endpoint)) of
            {ok, ClientRef} ->
                att_send_loop(ReadFun, ClientRef);
            {error, Reason} ->
                {error, {transport, Reason}}
        end
    end, deadline(Endpoint)).

att_content_type(<<>>) -> <<"application/octet-stream">>;
att_content_type(ContentType) -> ContentType.

att_send_loop(ReadFun, ClientRef) ->
    case ReadFun() of
        {ok, Chunk, NextReadFun} ->
            case hackney:send_body(ClientRef, Chunk) of
                ok ->
                    att_send_loop(NextReadFun, ClientRef);
                {error, Reason} ->
                    _ = hackney:close(ClientRef),
                    {error, {transport, Reason}}
            end;
        eof ->
            case hackney:finish_send_body(ClientRef) of
                ok ->
                    att_put_response(ClientRef);
                {error, Reason} ->
                    _ = hackney:close(ClientRef),
                    {error, {transport, Reason}}
            end;
        {error, _} = Error ->
            _ = hackney:close(ClientRef),
            Error
    end.

att_put_response(ClientRef) ->
    case hackney:start_response(ClientRef) of
        {ok, Status, RespHeaders, ClientRef2} ->
            ok = barrel_hlc:maybe_sync_from_header(
                header_value(?HLC_HEADER, RespHeaders)),
            Body = case hackney:body(ClientRef2) of
                {ok, B} -> decode_body(
                              header_value(<<"content-type">>, RespHeaders),
                              B);
                _ -> #{}
            end,
            case {Status, Body} of
                {200, #{<<"ok">> := <<"ignored">>}} -> {ok, ignored};
                {200, _} -> {ok, written};
                {422, _} -> {error, digest_mismatch};
                _ -> error_of({ok, Status, Body})
            end;
        {error, Reason} ->
            _ = hackney:close(ClientRef),
            {error, {transport, Reason}}
    end.

delete_attachment(Endpoint, DocId, Name, Meta) ->
    Extra = case Meta of
        #{origin_hlc := Origin} ->
            [{?ORIGIN_HEADER, base64:encode(barrel_hlc:encode(Origin))}];
        _ ->
            []
    end,
    case req(Endpoint, delete,
             [<<"/att/">>, quote(DocId), <<"/">>, quote(Name)],
             undefined, Extra) of
        {ok, 200, _} -> ok;
        Other -> error_of(Other)
    end.

att_url(#{url := BaseUrl}, DocId, Name) ->
    iolist_to_binary([BaseUrl, <<"/_sync/att/">>, quote(DocId),
                      <<"/">>, quote(Name)]).

att_entry_from_wire(#{<<"id">> := Id, <<"name">> := Name} = E) ->
    #{seq => hlc_from_wire(maps:get(<<"seq">>, E)),
      origin => hlc_from_wire(maps:get(<<"origin">>, E)),
      op => case maps:get(<<"op">>, E) of
                <<"put">> -> put;
                <<"delete">> -> delete
            end,
      id => Id,
      name => Name,
      digest => maps:get(<<"digest">>, E, <<>>),
      length => maps:get(<<"length">>, E, 0),
      content_type => maps:get(<<"content_type">>, E, <<>>)}.

hlc_from_wire(B64) ->
    barrel_hlc:decode(base64:decode(B64)).

%%====================================================================
%% HTTP plumbing
%%====================================================================

req(Endpoint, Method, PathSuffix, BodyTerm) ->
    req(Endpoint, Method, PathSuffix, BodyTerm, []).

req(Endpoint, Method, PathSuffix, BodyTerm, ExtraHeaders) ->
    case req_typed(Endpoint, Method, PathSuffix, BodyTerm, ExtraHeaders) of
        {ok, Status, _Encoding, Body} -> {ok, Status, Body};
        {error, _} = Err -> Err
    end.

%% Empty-bodied request signed over ContentHash (an attachment digest).
req_hashed(Endpoint, Method, PathSuffix, ContentHash, ExtraHeaders) ->
    case req_typed(Endpoint, Method, PathSuffix, {hash, ContentHash},
                   ExtraHeaders) of
        {ok, Status, _Encoding, Body} -> {ok, Status, Body};
        {error, _} = Err -> Err
    end.

%% Like req/5, with the response encoding (cbor | json).
req_typed(#{url := BaseUrl} = Endpoint, Method, PathSuffix, BodyTerm,
          ExtraHeaders) ->
    Url = iolist_to_binary([BaseUrl, <<"/_sync">>, PathSuffix]),
    {ContentType, Body, ContentHash} = case BodyTerm of
        {hash, Hash} ->
            {<<"application/octet-stream">>, <<>>, Hash};
        _ ->
            {CT, B} = encode_body(BodyTerm),
            {CT, B, barrel_sync_sig:content_sha256(B)}
    end,
    Headers = base_headers(Endpoint, method_bin(Method), url_target(Url),
                           ContentHash,
                           [{<<"content-type">>, ContentType}
                            | ExtraHeaders]),
    HttpOpts = [with_body | stream_opts(Endpoint)],
    case bounded(fun() ->
                     hackney:request(Method, Url, Headers, Body, HttpOpts)
                 end, deadline(Endpoint)) of
        {ok, Status, RespHeaders, RespBody} ->
            ok = barrel_hlc:maybe_sync_from_header(
                header_value(?HLC_HEADER, RespHeaders)),
            RespType = header_value(<<"content-type">>, RespHeaders),
            {ok, Status, encoding(RespType), decode_body(RespType, RespBody)};
        {error, request_timeout} = Timeout ->
            Timeout;
        {error, {transport, _}} = Err ->
            Err;
        {error, Reason} ->
            {error, {transport, Reason}}
    end.

%% Run `Fun' in a worker linked to the caller, so it dies with it. The
%% worker always exits normally; past the deadline the caller kills it
%% (its hackney connection closes with it).
bounded(Fun, Deadline) ->
    Caller = self(),
    Tag = make_ref(),
    Pid = spawn_link(fun() -> Caller ! {Tag, guarded(Fun)} end),
    receive
        {Tag, Result} ->
            ok = forget_worker(Pid),
            Result
    after remaining(Deadline) ->
        ok = kill_worker(Pid),
        receive {Tag, _} -> ok after 0 -> ok end,
        {error, request_timeout}
    end.

guarded(Fun) ->
    try Fun()
    catch C:R -> {error, {transport, {C, R}}}
    end.

%% Drop the link and its EXIT (a caller trapping exits gets one).
forget_worker(Pid) ->
    unlink(Pid),
    receive {'EXIT', Pid, _} -> ok after 0 -> ok end.

kill_worker(Pid) ->
    unlink(Pid),
    exit(Pid, kill),
    receive {'EXIT', Pid, _} -> ok after 0 -> ok end.

%% The request_timeout budget as a monotonic deadline.
deadline(Endpoint) ->
    case maps:get(request_timeout, Endpoint, ?REQUEST_TIMEOUT) of
        infinity -> infinity;
        Ms -> erlang:monotonic_time(millisecond) + Ms
    end.

remaining(infinity) ->
    infinity;
remaining(Deadline) ->
    max(0, Deadline - erlang:monotonic_time(millisecond)).

%% HLC + auth + endpoint headers. `Method' is the uppercase verb, `Path'
%% the request target `{Path, Query}' (matching what the server signs),
%% `ContentHash' the hex SHA-256 of the body (or the attachment digest),
%% `Headers' the request's own headers. The signature covers them all.
base_headers(Endpoint, Method, Target, ContentHash, Headers0) ->
    Headers = Headers0
              ++ [{?HLC_HEADER,
                   base64:encode(barrel_hlc:encode(barrel_hlc:get_hlc()))}]
              ++ maps:get(headers, Endpoint, []),
    Headers ++ auth_headers(Endpoint, Method, Target, ContentHash, Headers).

stream_opts(Endpoint) ->
    Base = [{pool, maps:get(pool, Endpoint, barrel_rep)},
            {connect_timeout, maps:get(connect_timeout, Endpoint, 5000)},
            {recv_timeout, maps:get(recv_timeout, Endpoint, 30000)}],
    case ssl_options(Endpoint) of
        [] -> Base;
        Opts -> [{ssl_options, Opts} | Base]
    end.

%% Signed requests take precedence over bearer when configured; otherwise
%% fall back to the (unchanged) bearer resolution.
auth_headers(Endpoint, Method, {Path, Query}, ContentHash, Headers) ->
    case signing_config(Endpoint) of
        #{key_id := KeyId, priv_key := PrivKey} ->
            TsMs = erlang:system_time(millisecond),
            %% v3: nonce, query and semantic headers signed (servers must
            %% accept v3: upgrade servers first, see the synchronization guide)
            Signed = case barrel_sync_sig:pick_headers(Headers) of
                {ok, Picked} -> Picked;
                %% an endpoint header repeats a signed one
                {error, duplicate_header} -> error(duplicate_signed_header)
            end,
            Auth = barrel_sync_sig:sign_v3(KeyId, PrivKey, Method, Path,
                                           Query, ContentHash, Signed, TsMs),
            [{<<"authorization">>, Auth},
             {<<"x-barrel-content-sha256">>, ContentHash}];
        undefined ->
            bearer_headers(Endpoint)
    end.

bearer_headers(#{auth := #{token := Token}}) ->
    [{<<"authorization">>, <<"Bearer ", Token/binary>>}];
bearer_headers(#{url := Url}) ->
    case env_setting(sync_auth, Url) of
        undefined -> [];
        Token -> [{<<"authorization">>, <<"Bearer ", Token/binary>>}]
    end.

%% Signing config from the endpoint, else the app env
%% `{barrel_docdb, sync_signing, #{UrlOrOrigin => #{key_id, priv_key}}}'
%% (mirrors sync_auth; private keys never persist in task configs).
signing_config(#{signing := Signing}) when is_map(Signing) ->
    Signing;
signing_config(#{url := Url}) ->
    env_setting(sync_signing, Url).

%% ssl_options from the endpoint, else app env
%% `{barrel_docdb, sync_ssl, #{UrlOrOrigin => SslOptions}}'.
ssl_options(#{ssl_options := Opts}) ->
    Opts;
ssl_options(#{url := Url}) ->
    case env_setting(sync_ssl, Url) of
        undefined -> [];
        Opts -> Opts
    end.

%% An app env credential map entry: the database URL, else its origin.
env_setting(Key, Url) ->
    Cfg = application:get_env(barrel_docdb, Key, #{}),
    case Cfg of
        #{Url := V} -> V;
        _ -> maps:get(origin(Url), Cfg, undefined)
    end.

method_bin(get) -> <<"GET">>;
method_bin(post) -> <<"POST">>;
method_bin(put) -> <<"PUT">>;
method_bin(delete) -> <<"DELETE">>.

origin(Url) ->
    case binary:match(Url, <<"/db/">>) of
        {Pos, _} -> binary:part(Url, 0, Pos);
        nomatch -> Url
    end.

header_value(Name, Headers) ->
    case lists:keyfind(Name, 1, [{string:lowercase(K), V}
                                 || {K, V} <- Headers]) of
        {_, V} -> V;
        false -> undefined
    end.

%% JSON when it can carry the body, else CBOR, whose byte strings carry
%% a binary that is not UTF-8 (a raw hash). The fields are the same.
encode_body(undefined) ->
    {<<"application/json">>, <<>>};
encode_body({encoded, ContentType, Bin}) ->
    {ContentType, Bin};
encode_body(Term) ->
    try json:encode(Term) of
        Json -> {<<"application/json">>, iolist_to_binary(Json)}
    catch
        error:_ ->
            {<<"application/cbor">>,
             barrel_docdb_codec_cbor:encode_wire(Term)}
    end.

encoding(<<"application/cbor", _/binary>>) -> cbor;
encoding(_ContentType) -> json.

vv_from_wire(cbor, VV) -> VV;
vv_from_wire(json, VV) -> base64:decode(VV).

decode_body(_ContentType, <<>>) -> #{};
decode_body(<<"application/cbor", _/binary>>, Bin) ->
    try barrel_docdb_codec_cbor:decode_cbor(Bin)
    catch _:_ -> Bin
    end;
decode_body(_ContentType, Bin) ->
    try json:decode(Bin)
    catch _:_ -> Bin
    end.

quote(Bin) ->
    uri_string:quote(Bin).

seq_to_wire(first) -> <<"first">>;
seq_to_wire(Hlc) -> base64:encode(barrel_hlc:encode(Hlc)).

seq_from_wire(<<"first">>) -> {ok, first};
seq_from_wire(B64) when is_binary(B64) ->
    try {ok, barrel_hlc:decode(base64:decode(B64))}
    catch _:_ -> error
    end;
seq_from_wire(_) -> error.

change_from_wire(Change) ->
    Base = #{
        id => maps:get(<<"id">>, Change),
        hlc => barrel_hlc:decode(
            base64:decode(maps:get(<<"hlc">>, Change))),
        rev => maps:get(<<"rev">>, Change),
        changes => [#{rev => maps:get(<<"rev">>, C)}
                    || C <- maps:get(<<"changes">>, Change, []),
                       is_map(C)],
        num_conflicts => maps:get(<<"num_conflicts">>, Change, 0)
    },
    case maps:get(<<"deleted">>, Change, false) of
        true -> Base#{deleted => true};
        false -> Base
    end.

%% 404 -> not_found; a 409 is clock skew only when the server says so
%% (other routes and proxies answer 409 for other reasons); anything else
%% keeps the status and body for the caller's logs.
error_of({ok, 404, _}) -> {error, not_found};
error_of({ok, 401, _}) -> {error, unauthorized};
error_of({ok, 409, #{<<"error">> := <<"clock_skew">>}}) -> {error, clock_skew};
error_of({ok, 413, _}) -> {error, too_large};
error_of({ok, Status, Body}) -> {error, {http, Status, Body}};
error_of({error, _} = Error) -> Error.
