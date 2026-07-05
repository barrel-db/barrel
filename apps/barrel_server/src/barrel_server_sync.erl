%%%-------------------------------------------------------------------
%%% @doc Replication endpoints: the barrel_rep_transport callbacks as
%%% HTTP, under /db/:db/_sync/*.
%%%
%%% These handlers call barrel_docdb directly (a documented layering
%%% exception: the replication machinery is deliberately absent from
%%% the barrel facade); databases still resolve through
%%% barrel_server_dbs. Wire stance: JSON, base64 for HLCs and version
%%% vectors, version tokens as-is. Every request/response carries an
%%% x-barrel-hlc header for passive clock coupling.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_sync).

%% Route handlers
-export([
    info/1,
    sync_hlc/1,
    changes/1,
    diff/1,
    get_doc/1,
    put_version/1,
    get_local/1,
    put_local/1,
    delete_local/1,
    att_changes/1,
    att_diff/1,
    get_att/1,
    put_att/1,
    delete_att/1
]).

-define(JSON_CT, {<<"content-type">>, <<"application/json">>}).
-define(HLC_HEADER, <<"x-barrel-hlc">>).
-define(DIGEST_HEADER, <<"x-barrel-digest">>).
-define(ORIGIN_HEADER, <<"x-barrel-att-origin">>).
-define(BODY_READ_TIMEOUT, 30000).

%%====================================================================
%% Handlers
%%====================================================================

info(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        case barrel_docdb:db_info(DbBin) of
            {ok, Info} ->
                Base = #{db => DbBin},
                WithHist = maybe_put(history_floor,
                                     maps:get(history_floor, Info,
                                              undefined), Base),
                WithAtt = maybe_put(att_floor,
                                    maps:get(att_floor, Info, undefined),
                                    WithHist),
                json_resp(Req, 200, WithAtt);
            {error, Reason} ->
                error_resp(Req, Reason)
        end
    end).

maybe_put(_Key, undefined, Map) -> Map;
maybe_put(Key, Hlc, Map) -> Map#{Key => hlc_to_wire(Hlc)}.

sync_hlc(Req) ->
    with_sync_db(Req, fun(_DbBin) ->
        with_json(Req, fun(Body) ->
            case decode_hlc_field(Body, <<"hlc">>) of
                {ok, Remote} ->
                    case barrel_docdb:sync_hlc(Remote) of
                        {ok, Merged} ->
                            json_resp(Req, 200,
                                      #{hlc => hlc_to_wire(Merged)});
                        {error, clock_skew} ->
                            json_resp(Req, 409,
                                      #{error => <<"clock_skew">>})
                    end;
                error ->
                    json_resp(Req, 400, #{error => <<"bad_hlc">>})
            end
        end)
    end).

changes(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        with_json(Req, fun(Body) ->
            case changes_opts(Body) of
                {ok, Since, Opts} ->
                    case barrel_docdb:get_changes(DbBin, Since, Opts) of
                        {ok, Changes, LastSeq} ->
                            json_resp(Req, 200, #{
                                changes =>
                                    [change_to_wire(C) || C <- Changes],
                                last_seq => seq_to_wire(LastSeq)
                            });
                        {error, Reason} ->
                            json_resp(Req, 400,
                                      #{error => err_bin(Reason)})
                    end;
                {error, Field} ->
                    json_resp(Req, 400, #{error => Field})
            end
        end)
    end).

diff(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        with_json(Req, fun(Body) ->
            case maps:get(<<"versions">>, Body, undefined) of
                Versions when is_map(Versions) ->
                    case barrel_docdb:diff_versions(DbBin, Versions) of
                        {ok, Diff} ->
                            Wire = maps:map(
                                fun(_Id, missing) -> <<"missing">>;
                                   (_Id, have) -> <<"have">>
                                end, Diff),
                            json_resp(Req, 200, #{diff => Wire});
                        {error, Reason} ->
                            error_resp(Req, Reason)
                    end;
                _ ->
                    json_resp(Req, 400, #{error => <<"bad_json">>})
            end
        end)
    end).

get_doc(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        case barrel_docdb:get_doc_for_replication(DbBin, DocId) of
            {ok, #{doc := Doc, version := Token, vv := VVBin,
                   deleted := Deleted}} ->
                json_resp(Req, 200, #{
                    doc => Doc,
                    version => Token,
                    vv => base64:encode(VVBin),
                    deleted => Deleted
                });
            {error, Reason} ->
                error_resp(Req, Reason)
        end
    end).

put_version(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        with_json(Req, fun(Body) ->
            case put_version_fields(Body) of
                {ok, Doc0, Token, VVBin, Deleted} ->
                    Doc = Doc0#{<<"id">> => DocId},
                    case barrel_docdb:put_version(DbBin, Doc, Token,
                                                  VVBin, Deleted) of
                        {ok, Id, Winner} ->
                            json_resp(Req, 200,
                                      #{id => Id, winner => Winner});
                        {error, Reason} ->
                            error_resp(Req, Reason)
                    end;
                error ->
                    json_resp(Req, 400, #{error => <<"bad_json">>})
            end
        end)
    end).

get_local(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        case barrel_docdb:get_local_doc(DbBin, DocId) of
            {ok, Doc} -> json_resp(Req, 200, Doc);
            {error, Reason} -> error_resp(Req, Reason)
        end
    end).

put_local(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        with_json(Req, fun(Body) ->
            case barrel_docdb:put_local_doc(DbBin, DocId, Body) of
                ok -> json_resp(Req, 200, #{ok => true});
                {error, Reason} -> error_resp(Req, Reason)
            end
        end)
    end).

delete_local(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        case barrel_docdb:delete_local_doc(DbBin, DocId) of
            ok -> json_resp(Req, 200, #{ok => true});
            {error, Reason} -> error_resp(Req, Reason)
        end
    end).

%%====================================================================
%% Attachment sync
%%====================================================================

att_changes(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        case att_changes_params(Req) of
            {ok, Since, Limit} ->
                case barrel_docdb:att_changes(DbBin, Since,
                                              #{limit => Limit}) of
                    {ok, Entries, LastSeq} ->
                        json_resp(Req, 200, #{
                            changes =>
                                [att_entry_to_wire(E) || E <- Entries],
                            last_seq => seq_to_wire(LastSeq)
                        });
                    {error, att_sync_unsupported} ->
                        json_resp(Req, 501,
                                  #{error => <<"att_sync_unsupported">>});
                    {error, Reason} ->
                        error_resp(Req, Reason)
                end;
            error ->
                json_resp(Req, 400, #{error => <<"bad_since">>})
        end
    end).

att_diff(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        with_json(Req, fun(Body) ->
            case att_diff_input(maps:get(<<"attachments">>, Body,
                                         undefined)) of
                {ok, Entries} ->
                    case barrel_docdb:diff_attachments(DbBin, Entries) of
                        {ok, Diff} ->
                            json_resp(Req, 200, #{diff =>
                                [#{id => Id, name => Name,
                                   status => atom_to_binary(St, utf8)}
                                 || #{id := Id, name := Name,
                                      status := St} <- Diff]});
                        {error, Reason} ->
                            error_resp(Req, Reason)
                    end;
                error ->
                    json_resp(Req, 400, #{error => <<"bad_json">>})
            end
        end)
    end).

%% Raw octets out, chunk by chunk; the status is decided before the
%% first byte, so a missing attachment is a clean 404.
get_att(Req) ->
    livery_resp:stream_deferred(fun() -> get_att_decision(Req) end).

get_att_decision(Req) ->
    ok = barrel_hlc:maybe_sync_from_header(
        livery_req:header(?HLC_HEADER, Req, undefined)),
    case barrel_server_dbs:ensure(livery_req:binding(<<"db">>, Req)) of
        {ok, Handle} ->
            DbBin = maps:get(docdb, Handle),
            DocId = binding(<<"id">>, Req),
            AttName = binding(<<"name">>, Req),
            case barrel_docdb:get_attachment_info(DbBin, DocId, AttName) of
                {ok, Info} ->
                    case barrel_docdb:open_attachment_stream(
                             DbBin, DocId, AttName) of
                        {ok, Stream} ->
                            {stream, 200, att_resp_headers(Info),
                             fun(Emit) -> pump_att_out(Stream, Emit) end};
                        {error, Reason} ->
                            att_error_decision(Reason)
                    end;
                {error, Reason} ->
                    att_error_decision(Reason)
            end;
        {error, invalid_name} ->
            {full, 400, att_json_headers(),
             json:encode(#{error => <<"invalid_name">>})};
        {error, Reason} ->
            att_error_decision(Reason)
    end.

att_resp_headers(Info) ->
    [{<<"content-type">>,
      maps:get(content_type, Info, <<"application/octet-stream">>)},
     {?DIGEST_HEADER, maps:get(digest, Info, <<>>)},
     %% not content-length: the body goes out chunked
     {<<"x-barrel-att-length">>,
      integer_to_binary(maps:get(length, Info, 0))},
     {?HLC_HEADER, hlc_to_wire(barrel_hlc:get_hlc())}].

att_json_headers() ->
    [?JSON_CT, {?HLC_HEADER, hlc_to_wire(barrel_hlc:get_hlc())}].

att_error_decision(not_found) ->
    {full, 404, att_json_headers(),
     json:encode(#{error => <<"not_found">>})};
att_error_decision(Reason) ->
    {full, 500, att_json_headers(),
     json:encode(#{error => err_bin(Reason)})}.

pump_att_out(Stream, Emit) ->
    case barrel_docdb:read_attachment_chunk(Stream) of
        {ok, Chunk, Stream2} ->
            case Emit(Chunk) of
                ok -> pump_att_out(Stream2, Emit);
                {error, _} = Error -> Error
            end;
        eof ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% Raw octets in, streamed into an attachment writer chunk by chunk:
%% a blob never sits in memory whole and read_all's 16 MiB cap does
%% not apply. The effective per-request ceiling is the listener's
%% max_body (and today the h1 engine's own 8 MiB parser cap, which
%% neither h1 nor livery expose yet). Digest and origin ride in
%% headers; both are checked at the writer's commit point.
put_att(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        AttName = binding(<<"name">>, Req),
        ContentType = livery_req:header(<<"content-type">>, Req,
                                        <<"application/octet-stream">>),
        case att_write_opts(Req) of
            {ok, Opts} ->
                case barrel_docdb:open_attachment_writer(
                         DbBin, DocId, AttName, ContentType, Opts) of
                    {ok, Writer} ->
                        pump_att_in(livery_req:body(Req), Writer, Req);
                    {error, Reason} ->
                        error_resp(Req, Reason)
                end;
            error ->
                json_resp(Req, 400, #{error => <<"bad_origin">>})
        end
    end).

att_write_opts(Req) ->
    Opts0 = case livery_req:header(?DIGEST_HEADER, Req, undefined) of
        undefined -> #{};
        Digest -> #{expected_digest => Digest}
    end,
    case livery_req:header(?ORIGIN_HEADER, Req, undefined) of
        undefined ->
            {ok, Opts0};
        OriginB64 ->
            try
                {ok, Opts0#{origin_hlc =>
                                barrel_hlc:decode(base64:decode(OriginB64))}}
            catch
                _:_ -> error
            end
    end.

pump_att_in(empty, Writer, Req) ->
    finish_att(Writer, Req);
pump_att_in({buffered, Io}, Writer, Req) ->
    case barrel_docdb:write_attachment_chunk(Writer,
                                             iolist_to_binary(Io)) of
        {ok, Writer2} -> finish_att(Writer2, Req);
        {error, Reason} -> abort_att(Writer, Req, Reason)
    end;
pump_att_in({stream, Reader}, Writer, Req) ->
    case livery_body:read(Reader, ?BODY_READ_TIMEOUT) of
        {ok, Chunk, Reader2} ->
            case barrel_docdb:write_attachment_chunk(
                     Writer, iolist_to_binary(Chunk)) of
                {ok, Writer2} ->
                    pump_att_in({stream, Reader2}, Writer2, Req);
                {error, Reason} ->
                    abort_att(Writer, Req, Reason)
            end;
        {done, _Reader2} ->
            finish_att(Writer, Req);
        {error, Reason, _Reader2} ->
            abort_att(Writer, Req, Reason)
    end.

finish_att(Writer, Req) ->
    case barrel_docdb:finish_attachment_writer(Writer) of
        {ok, ignored} ->
            json_resp(Req, 200, #{ok => <<"ignored">>});
        {ok, _Info} ->
            json_resp(Req, 200, #{ok => true});
        {error, digest_mismatch} ->
            json_resp(Req, 422, #{error => <<"digest_mismatch">>});
        {error, Reason} ->
            error_resp(Req, Reason)
    end.

abort_att(Writer, Req, Reason) ->
    _ = barrel_docdb:abort_attachment_writer(Writer),
    error_resp(Req, Reason).

delete_att(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        AttName = binding(<<"name">>, Req),
        case att_write_opts(Req) of
            {ok, Opts} ->
                case barrel_docdb:delete_attachment(
                         DbBin, DocId, AttName,
                         maps:with([origin_hlc], Opts)) of
                    ok -> json_resp(Req, 200, #{ok => true});
                    {error, Reason} -> error_resp(Req, Reason)
                end;
            error ->
                json_resp(Req, 400, #{error => <<"bad_origin">>})
        end
    end).

%%====================================================================
%% Wire codecs
%%====================================================================

hlc_to_wire(Hlc) ->
    base64:encode(barrel_hlc:encode(Hlc)).

seq_to_wire(first) -> <<"first">>;
seq_to_wire(Hlc) -> hlc_to_wire(Hlc).

seq_from_wire(<<"first">>) -> {ok, first};
seq_from_wire(B64) when is_binary(B64) ->
    try {ok, barrel_hlc:decode(base64:decode(B64))}
    catch _:_ -> error
    end;
seq_from_wire(_) -> error.

change_to_wire(Change) ->
    Base = #{
        id => maps:get(id, Change),
        hlc => hlc_to_wire(maps:get(hlc, Change)),
        rev => maps:get(rev, Change),
        changes => maps:get(changes, Change, []),
        num_conflicts => maps:get(num_conflicts, Change, 0)
    },
    case maps:get(deleted, Change, false) of
        true -> Base#{deleted => true};
        false -> Base
    end.

decode_hlc_field(Body, Field) ->
    case maps:get(Field, Body, undefined) of
        B64 when is_binary(B64) ->
            try {ok, barrel_hlc:decode(base64:decode(B64))}
            catch _:_ -> error
            end;
        _ ->
            error
    end.

changes_opts(Body) ->
    case seq_from_wire(maps:get(<<"since">>, Body, <<"first">>)) of
        {ok, Since} ->
            Opts0 = #{limit => maps:get(<<"limit">>, Body, 100)},
            case maps:get(<<"filter">>, Body, undefined) of
                undefined ->
                    {ok, Since, Opts0};
                FilterWire when is_map(FilterWire) ->
                    case barrel_rep_filter:from_wire(FilterWire) of
                        {ok, Filter} ->
                            {ok, Since, maps:merge(Opts0, Filter)};
                        {error, _} ->
                            {error, <<"bad_filter">>}
                    end;
                _ ->
                    {error, <<"bad_filter">>}
            end;
        error ->
            {error, <<"bad_since">>}
    end.

put_version_fields(#{<<"doc">> := Doc, <<"version">> := Token,
                     <<"vv">> := VVB64} = Body)
        when is_map(Doc), is_binary(Token), is_binary(VVB64) ->
    try
        {ok, Doc, Token, base64:decode(VVB64),
         maps:get(<<"deleted">>, Body, false) =:= true}
    catch
        _:_ -> error
    end;
put_version_fields(_) ->
    error.

att_entry_to_wire(#{seq := Seq, origin := Origin, op := Op, id := Id,
                    name := Name, digest := Digest, length := Length,
                    content_type := ContentType}) ->
    #{seq => hlc_to_wire(Seq),
      origin => hlc_to_wire(Origin),
      op => atom_to_binary(Op, utf8),
      id => Id,
      name => Name,
      digest => Digest,
      length => Length,
      content_type => ContentType}.

att_changes_params(Req) ->
    Params = query_params(Req),
    Limit = case lists:keyfind(<<"limit">>, 1, Params) of
        {_, LBin} ->
            try binary_to_integer(LBin) catch _:_ -> 100 end;
        false ->
            100
    end,
    SinceWire = case lists:keyfind(<<"since">>, 1, Params) of
        {_, S} -> S;
        false -> <<"first">>
    end,
    case seq_from_wire(SinceWire) of
        {ok, Since} -> {ok, Since, Limit};
        error -> error
    end.

query_params(Req) ->
    case uri_string:dissect_query(livery_req:query(Req)) of
        {error, _, _} -> [];
        Pairs -> Pairs
    end.

att_diff_input(List) when is_list(List) ->
    Decoded = [#{id => Id, name => Name, digest => Digest}
               || #{<<"id">> := Id, <<"name">> := Name,
                    <<"digest">> := Digest} <- List,
                  is_binary(Id), is_binary(Name), is_binary(Digest)],
    case length(Decoded) =:= length(List) of
        true -> {ok, Decoded};
        false -> error
    end;
att_diff_input(_) ->
    error.

%%====================================================================
%% Request plumbing
%%====================================================================

%% Resolve the db, couple clocks from the request header, and answer
%% with our current clock on the way out.
with_sync_db(Req, Fun) ->
    ok = barrel_hlc:maybe_sync_from_header(
        livery_req:header(?HLC_HEADER, Req, undefined)),
    Name = livery_req:binding(<<"db">>, Req),
    case barrel_server_dbs:ensure(Name) of
        {ok, Handle} -> Fun(maps:get(docdb, Handle));
        {error, invalid_name} ->
            json_resp(Req, 400, #{error => <<"invalid_name">>});
        {error, Reason} ->
            error_resp(Req, Reason)
    end.

binding(Name, Req) ->
    uri_string:percent_decode(livery_req:binding(Name, Req)).

with_json(Req, Fun) ->
    case read_json(Req) of
        {ok, Body} -> Fun(Body);
        {error, Reason} -> json_resp(Req, 400, #{error => err_bin(Reason)})
    end.

read_json(Req) ->
    case livery_req:body(Req) of
        empty ->
            {ok, #{}};
        {buffered, Io} ->
            decode_json(iolist_to_binary(Io));
        {stream, Reader} ->
            case livery_body:read_all(Reader) of
                {ok, Bin, _R2} -> decode_json(Bin);
                {error, Reason, _R2} -> {error, Reason}
            end
    end.

decode_json(<<>>) -> {ok, #{}};
decode_json(Bin) ->
    try {ok, json:decode(Bin)}
    catch _:_ -> {error, bad_json}
    end.

json_resp(_Req, Status, Term) ->
    Headers = [?JSON_CT,
               {?HLC_HEADER, hlc_to_wire(barrel_hlc:get_hlc())}],
    livery_resp:new(Status, Headers, {full, json:encode(Term)}).

error_resp(Req, {error, Reason}) -> error_resp(Req, Reason);
error_resp(Req, not_found) ->
    json_resp(Req, 404, #{error => <<"not_found">>});
error_resp(Req, Reason) ->
    json_resp(Req, 500, #{error => err_bin(Reason)}).

err_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
err_bin(Bin) when is_binary(Bin) -> Bin;
err_bin(Other) -> iolist_to_binary(io_lib:format("~p", [Other])).
