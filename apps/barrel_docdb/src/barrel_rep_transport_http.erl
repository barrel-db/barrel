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
%% `{barrel_docdb, sync_auth, #{Origin => Token}}' keyed by
%% `<<"scheme://host:port">>' (no secrets in persisted task configs).
-module(barrel_rep_transport_http).
-behaviour(barrel_rep_transport).

-export([endpoint/1]).

%% barrel_rep_transport callbacks
-export([
    get_doc/3,
    put_version/5,
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
         put_attachment/5, delete_attachment/4]).

-export_type([endpoint/0]).

-type endpoint() :: #{
    url := binary(),
    auth => #{token := binary()},
    pool => atom(),
    connect_timeout => pos_integer(),
    recv_timeout => pos_integer(),
    headers => [{binary(), binary()}]
}.

-define(HLC_HEADER, <<"x-barrel-hlc">>).
-define(DIGEST_HEADER, <<"x-barrel-digest">>).
-define(ORIGIN_HEADER, <<"x-barrel-att-origin">>).

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

get_doc(Endpoint, DocId, _Opts) ->
    case req(Endpoint, get, [<<"/doc/">>, quote(DocId)], undefined) of
        {ok, 200, #{<<"doc">> := Doc, <<"version">> := Token,
                    <<"vv">> := VVB64} = Body} ->
            {ok, Doc, #{version => Token,
                        vv => base64:decode(VVB64),
                        deleted => maps:get(<<"deleted">>, Body, false)
                                       =:= true}};
        Other ->
            error_of(Other)
    end.

put_version(Endpoint, Doc, Token, VVBin, Deleted) ->
    DocId = maps:get(<<"id">>, Doc),
    Body = #{doc => Doc, version => Token,
             vv => base64:encode(VVBin), deleted => Deleted},
    case req(Endpoint, put, [<<"/doc/">>, quote(DocId)], Body) of
        {ok, 200, #{<<"id">> := Id, <<"winner">> := Winner}} ->
            {ok, Id, Winner};
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
            Info = maybe_floor(att_floor, <<"att_floor">>, Body, Info1),
            {ok, Info};
        Other ->
            error_of(Other)
    end.

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
        {ok, 409, _} ->
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
    Path = [<<"/att_changes?since=">>, quote(seq_to_wire(Since)),
            <<"&limit=">>, integer_to_binary(Limit)],
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

diff_attachments(Endpoint, Entries) ->
    case req(Endpoint, post, <<"/att_diff">>,
             #{attachments => Entries}) of
        {ok, 200, #{<<"diff">> := Wire}} ->
            {ok, [#{id => maps:get(<<"id">>, D),
                    name => maps:get(<<"name">>, D),
                    status => case maps:get(<<"status">>, D) of
                                  <<"have">> -> have;
                                  <<"missing">> -> missing
                              end} || D <- Wire]};
        Other ->
            error_of(Other)
    end.

%% hackney's sync request/5 always buffers the response body, so the
%% streamed read goes through connect + send_request, which leaves the
%% body on the connection for stream_body/1.
get_attachment_stream(Endpoint, DocId, Name) ->
    Url = att_url(Endpoint, DocId, Name),
    case hackney:connect(Url, stream_opts(Endpoint)) of
        {ok, ConnPid} ->
            case hackney:send_request(
                     ConnPid,
                     {get, url_path(Url), base_headers(Endpoint), <<>>}) of
                {ok, 200, RespHeaders, ConnPid2} ->
                    ok = barrel_hlc:maybe_sync_from_header(
                        header_value(?HLC_HEADER, RespHeaders)),
                    Info = #{content_type =>
                                 header_value(<<"content-type">>,
                                              RespHeaders),
                             digest =>
                                 header_value(?DIGEST_HEADER,
                                              RespHeaders)},
                    {ok, Info, att_read_fun(ConnPid2)};
                {ok, Status, _RespHeaders, ConnPid2} ->
                    %% drain so the pooled connection is reusable
                    Body = case hackney:body(ConnPid2) of
                        {ok, B} -> decode_body(B);
                        _ -> #{}
                    end,
                    error_of({ok, Status, Body});
                {error, Reason} ->
                    _ = hackney:close(ConnPid),
                    {error, {transport, Reason}}
            end;
        {error, Reason} ->
            {error, {transport, Reason}}
    end.

url_path(Url) ->
    #{path := Path} = uri_string:parse(Url),
    Path.

att_read_fun(ClientRef) ->
    fun() ->
        case hackney:stream_body(ClientRef) of
            {ok, Chunk} -> {ok, Chunk, att_read_fun(ClientRef)};
            done -> eof;
            {error, Reason} -> {error, {transport, Reason}}
        end
    end.

put_attachment(Endpoint, DocId, Name, Meta, ReadFun) ->
    #{content_type := ContentType, digest := Digest,
      origin_hlc := Origin} = Meta,
    Headers = [{<<"content-type">>, att_content_type(ContentType)},
               {?DIGEST_HEADER, Digest},
               {?ORIGIN_HEADER,
                base64:encode(barrel_hlc:encode(Origin))}
               | base_headers(Endpoint)],
    case hackney:request(put, att_url(Endpoint, DocId, Name), Headers,
                         stream, stream_opts(Endpoint)) of
        {ok, ClientRef} ->
            att_send_loop(ReadFun, ClientRef);
        {error, Reason} ->
            {error, {transport, Reason}}
    end.

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
                {ok, B} -> decode_body(B);
                _ -> #{}
            end,
            case {Status, Body} of
                {200, #{<<"ok">> := <<"ignored">>}} -> {ok, ignored};
                {200, _} -> {ok, written};
                {422, _} -> {error, digest_mismatch};
                _ -> error_of({ok, Status, Body})
            end;
        {error, Reason} ->
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

req(#{url := BaseUrl} = Endpoint, Method, PathSuffix, BodyTerm,
    ExtraHeaders) ->
    Url = iolist_to_binary([BaseUrl, <<"/_sync">>, PathSuffix]),
    Headers = [{<<"content-type">>, <<"application/json">>}
               | ExtraHeaders] ++ base_headers(Endpoint),
    Body = case BodyTerm of
        undefined -> <<>>;
        _ -> iolist_to_binary(json:encode(BodyTerm))
    end,
    HttpOpts = [with_body | stream_opts(Endpoint)],
    case hackney:request(Method, Url, Headers, Body, HttpOpts) of
        {ok, Status, RespHeaders, RespBody} ->
            ok = barrel_hlc:maybe_sync_from_header(
                header_value(?HLC_HEADER, RespHeaders)),
            {ok, Status, decode_body(RespBody)};
        {error, Reason} ->
            {error, {transport, Reason}}
    end.

base_headers(Endpoint) ->
    [{?HLC_HEADER,
      base64:encode(barrel_hlc:encode(barrel_hlc:get_hlc()))}]
    ++ auth_headers(Endpoint)
    ++ maps:get(headers, Endpoint, []).

stream_opts(Endpoint) ->
    [{pool, maps:get(pool, Endpoint, barrel_rep)},
     {connect_timeout, maps:get(connect_timeout, Endpoint, 5000)},
     {recv_timeout, maps:get(recv_timeout, Endpoint, 30000)}].

auth_headers(#{auth := #{token := Token}}) ->
    [{<<"authorization">>, <<"Bearer ", Token/binary>>}];
auth_headers(#{url := Url}) ->
    Tokens = application:get_env(barrel_docdb, sync_auth, #{}),
    case maps:get(origin(Url), Tokens, undefined) of
        undefined -> [];
        Token -> [{<<"authorization">>, <<"Bearer ", Token/binary>>}]
    end.

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

decode_body(<<>>) -> #{};
decode_body(Bin) ->
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

%% 404 -> not_found; anything else keeps the status and body for the
%% caller's logs.
error_of({ok, 404, _}) -> {error, not_found};
error_of({ok, 401, _}) -> {error, unauthorized};
error_of({ok, 409, _}) -> {error, clock_skew};
error_of({ok, 413, _}) -> {error, too_large};
error_of({ok, Status, Body}) -> {error, {http, Status, Body}};
error_of({error, _} = Error) -> Error.
