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
        #{scheme := Scheme0, host := Host0, path := Path0} = Parsed
          when Scheme0 =:= "http"; Scheme0 =:= "https";
               Scheme0 =:= <<"http">>; Scheme0 =:= <<"https">> ->
            case maps:is_key(query, Parsed) orelse
                 maps:is_key(fragment, Parsed) of
                true -> erlang:error({invalid_sync_url, Url});
                false -> ok
            end,
            Scheme = string:lowercase(to_bin(Scheme0)),
            Host = string:lowercase(to_bin(Host0)),
            Path = strip_trailing_slash(to_bin(Path0)),
            case Path of
                <<"/db/", DbName/binary>> when DbName =/= <<>> -> ok;
                _ -> erlang:error({invalid_sync_url, Url})
            end,
            PortPart = case maps:get(port, Parsed, undefined) of
                undefined -> <<>>;
                Port -> <<":", (integer_to_binary(Port))/binary>>
            end,
            <<Scheme/binary, "://", Host/binary, PortPart/binary,
              Path/binary>>;
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
%% HTTP plumbing
%%====================================================================

req(#{url := BaseUrl} = Endpoint, Method, PathSuffix, BodyTerm) ->
    Url = iolist_to_binary([BaseUrl, <<"/_sync">>, PathSuffix]),
    Headers = [{<<"content-type">>, <<"application/json">>},
               {?HLC_HEADER,
                base64:encode(barrel_hlc:encode(barrel_hlc:get_hlc()))}]
              ++ auth_headers(Endpoint)
              ++ maps:get(headers, Endpoint, []),
    Body = case BodyTerm of
        undefined -> <<>>;
        _ -> iolist_to_binary(json:encode(BodyTerm))
    end,
    HttpOpts = [with_body,
                {pool, maps:get(pool, Endpoint, barrel_rep)},
                {connect_timeout,
                 maps:get(connect_timeout, Endpoint, 5000)},
                {recv_timeout, maps:get(recv_timeout, Endpoint, 30000)}],
    case hackney:request(Method, Url, Headers, Body, HttpOpts) of
        {ok, Status, RespHeaders, RespBody} ->
            ok = barrel_hlc:maybe_sync_from_header(
                header_value(?HLC_HEADER, RespHeaders)),
            {ok, Status, decode_body(RespBody)};
        {error, Reason} ->
            {error, {transport, Reason}}
    end.

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
error_of({ok, Status, Body}) -> {error, {http, Status, Body}};
error_of({error, _} = Error) -> Error.
