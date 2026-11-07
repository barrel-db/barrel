%%%-------------------------------------------------------------------
%%% @doc Replication endpoints: the barrel_rep_transport callbacks as
%%% HTTP, under /db/:db/_sync/*.
%%%
%%% These handlers call barrel_docdb directly (a documented layering
%%% exception: the replication machinery is deliberately absent from
%%% the barrel module); databases still resolve through
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
    get_state/1,
    put_state/1,
    get_local/1,
    put_local/1,
    delete_local/1,
    att_changes/1,
    att_diff/1,
    att_state/1,
    att_list/1,
    att_flush/1,
    get_att/1,
    put_att/1,
    delete_att/1
]).

-define(JSON_CT, {<<"content-type">>, <<"application/json">>}).
-define(HLC_HEADER, <<"x-barrel-hlc">>).
-define(DIGEST_HEADER, <<"x-barrel-digest">>).
-define(ORIGIN_HEADER, <<"x-barrel-att-origin">>).
-define(TOUCH_HEADER, <<"x-barrel-att-touch">>).
-define(BODY_READ_TIMEOUT, 30000).

%%====================================================================
%% Handlers
%%====================================================================

info(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        case barrel_docdb:db_info(DbBin) of
            {ok, Info} ->
                %% full_state: this server replicates whole document
                %% states (GET/PUT _sync/state/:id)
                Base = #{db => DbBin, full_state => true},
                WithHist = maybe_put(history_floor,
                                     maps:get(history_floor, Info,
                                              undefined), Base),
                WithAtt = maybe_put(att_floor,
                                    maps:get(att_floor, Info, undefined),
                                    WithHist),
                %% att_flush: POST _sync/att_flush syncs attachment
                %% writes; att_filter: att_changes applies a filter
                Caps = case barrel_docdb:att_capabilities(DbBin) of
                    {ok, #{flush := Flush, filter := Filter}} ->
                        #{att_flush => Flush, att_filter => Filter};
                    _ ->
                        #{}
                end,
                json_resp(Req, 200, maps:merge(WithAtt, Caps));
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
                    vv => vv_to_wire(accepts_cbor(Req), VVBin),
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
            case put_version_fields(request_cbor(Req), Body) of
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

%% A document's full replication state: the encoded state and digest are
%% byte strings in CBOR, standard base64 in JSON (as version vectors).
get_state(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        Cbor = accepts_cbor(Req),
        case barrel_docdb:get_doc_state(DbBin, DocId) of
            {ok, #{state := State, digest := Digest, bodies := Bodies}} ->
                json_resp(Req, 200, #{id => DocId,
                                      state => vv_to_wire(Cbor, State),
                                      digest => vv_to_wire(Cbor, Digest),
                                      bodies => Bodies});
            {error, Reason} ->
                error_resp(Req, Reason)
        end
    end).

put_state(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        with_json(Req, fun
            (#{<<"state">> := StateWire, <<"bodies">> := Bodies})
                    when is_binary(StateWire), is_map(Bodies) ->
                State = vv_from_wire(request_cbor(Req), StateWire),
                case barrel_docdb:put_doc_state(DbBin, DocId, State, Bodies) of
                    {Written, Id, Winner} when Written =:= ok;
                                               Written =:= unchanged ->
                        json_resp(Req, 200, #{id => Id, winner => Winner,
                                              written => Written =:= ok});
                    {error, clock_skew} ->
                        json_resp(Req, 409, #{error => <<"clock_skew">>});
                    {error, {missing_bodies, Tokens}} ->
                        json_resp(Req, 400, #{error => <<"missing_bodies">>,
                                              tokens => Tokens});
                    {error, Reason} ->
                        error_resp(Req, Reason)
                end;
            (_) ->
                json_resp(Req, 400, #{error => <<"bad_state">>})
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
            {ok, Since, Limit, Filter} ->
                case barrel_docdb:att_changes(DbBin, Since,
                                              #{limit => Limit,
                                                filter => Filter}) of
                    {ok, Entries, LastSeq} ->
                        json_resp(Req, 200, #{
                            changes =>
                                [att_entry_to_wire(E) || E <- Entries],
                            last_seq => seq_to_wire(LastSeq)
                        });
                    {error, att_sync_unsupported} ->
                        json_resp(Req, 501,
                                  #{error => <<"att_sync_unsupported">>});
                    {error, not_found} ->
                        error_resp(Req, not_found);
                    {error, Reason} ->
                        %% an unknown channel or a bad query: the
                        %% client stops instead of widening
                        json_resp(Req, 400, #{error => err_bin(Reason)})
                end;
            {error, Field} ->
                json_resp(Req, 400, #{error => Field})
        end
    end).

att_diff(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        with_json(Req, fun(Body) ->
            case att_diff_input(maps:get(<<"attachments">>, Body,
                                         undefined)) of
                {ok, Entries} ->
                    Touch = maps:get(<<"touch">>, Body, false) =:= true,
                    case barrel_docdb:diff_attachments(DbBin, Entries,
                                                       #{touch => Touch}) of
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

%% The feed index state of one attachment (deletes included).
att_state(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        AttName = binding(<<"name">>, Req),
        case barrel_docdb:att_state(DbBin, DocId, AttName) of
            {ok, #{origin := Origin, op := Op, digest := Digest}} ->
                json_resp(Req, 200, #{origin => hlc_to_wire(Origin),
                                      op => atom_to_binary(Op, utf8),
                                      digest => Digest});
            {error, unsupported} ->
                json_resp(Req, 501, #{error => <<"att_sync_unsupported">>});
            {error, Reason} ->
                error_resp(Req, Reason)
        end
    end).

%% Current attachment entries of the given documents.
att_list(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        with_json(Req, fun(Body) ->
            case maps:get(<<"ids">>, Body, undefined) of
                Ids when is_list(Ids) ->
                    case lists:all(fun is_binary/1, Ids) andalso
                         barrel_docdb:att_list(DbBin, Ids) of
                        false ->
                            json_resp(Req, 400, #{error => <<"bad_json">>});
                        {ok, Entries} ->
                            json_resp(Req, 200, #{attachments =>
                                [att_entry_to_wire(E) || E <- Entries]});
                        {error, unsupported} ->
                            json_resp(Req, 501,
                                      #{error => <<"att_sync_unsupported">>});
                        {error, Reason} ->
                            error_resp(Req, Reason)
                    end;
                _ ->
                    json_resp(Req, 400, #{error => <<"bad_json">>})
            end
        end)
    end).

%% Answers 200 only once the attachment writes are synced.
att_flush(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        case barrel_docdb:flush_attachments(DbBin) of
            ok ->
                json_resp(Req, 200, #{ok => true});
            {error, unsupported} ->
                json_resp(Req, 501, #{error => <<"att_flush_unsupported">>});
            {error, Reason} ->
                error_resp(Req, Reason)
        end
    end).

%% Raw octets out, chunk by chunk; the status is decided before the
%% first byte, so a missing attachment is a clean 404.
get_att(Req) ->
    livery_resp:stream_deferred(fun() -> get_att_decision(Req) end).

get_att_decision(Req) ->
    ok = barrel_hlc:maybe_sync_from_header(
        signed_header(?HLC_HEADER, Req)),
    case ensure_existing(livery_req:binding(<<"db">>, Req)) of
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
%% not apply. The per-request ceiling is the listener's max_body
%% (barrel_server env, default 1 GiB); past it the read errors with
%% body_too_large and the writer aborts with nothing committed.
%% Digest and origin ride in headers; both are checked at the
%% writer's commit point.
put_att(Req) ->
    with_sync_db(Req, fun(DbBin) ->
        DocId = binding(<<"id">>, Req),
        AttName = binding(<<"name">>, Req),
        ContentType = case signed_header(<<"content-type">>, Req) of
            undefined -> <<"application/octet-stream">>;
            CT -> CT
        end,
        Touch = signed_header(?TOUCH_HEADER, Req),
        case att_write_opts(Req) of
            {ok, #{origin_hlc := _, expected_digest := _} = Opts}
                    when Touch =/= undefined ->
                touch_att(DbBin, DocId, AttName, Opts, Req);
            {ok, _} when Touch =/= undefined ->
                json_resp(Req, 400, #{error => <<"bad_touch">>});
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

%% A touch: the stored bytes get the newer origin when their digest
%% matches; otherwise 412 asks for the bytes.
touch_att(DbBin, DocId, AttName, #{origin_hlc := Origin,
                                   expected_digest := Digest}, Req) ->
    case barrel_docdb:touch_attachment(DbBin, DocId, AttName,
                                       #{origin_hlc => Origin,
                                         digest => Digest}) of
        {ok, touched} ->
            json_resp(Req, 200, #{ok => <<"touched">>});
        {ok, ignored} ->
            json_resp(Req, 200, #{ok => <<"ignored">>});
        {error, bytes_needed} ->
            json_resp(Req, 412, #{error => <<"bytes_needed">>});
        {error, Reason} ->
            error_resp(Req, Reason)
    end.

att_write_opts(Req) ->
    %% Bind the body to the SIGNED content hash. x-barrel-content-sha256 is
    %% covered by the request signature (and equals the attachment digest on
    %% a signed upload); x-barrel-digest alone is unsigned, so an on-path
    %% attacker could rewrite it to match a forged body. Dropping or forging
    %% the signed header breaks the signature, so the x-barrel-digest
    %% fallback only applies to unsigned (bearer) requests.
    Digest = case livery_req:header(<<"x-barrel-content-sha256">>, Req,
                                    undefined) of
        undefined -> signed_header(?DIGEST_HEADER, Req);
        Signed -> Signed
    end,
    Opts0 = case Digest of
        undefined -> #{};
        _ -> #{expected_digest => Digest}
    end,
    case signed_header(?ORIGIN_HEADER, Req) of
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

abort_att(Writer, Req, body_too_large) ->
    _ = barrel_docdb:abort_attachment_writer(Writer),
    json_resp(Req, 413, #{error => <<"too_large">>});
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
            %% a replication feed: deletes pass every filter
            Opts0 = #{limit => maps:get(<<"limit">>, Body, 100),
                      replication => true},
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

put_version_fields(Cbor, #{<<"doc">> := Doc, <<"version">> := Token,
                           <<"vv">> := VV} = Body)
        when is_map(Doc), is_binary(Token), is_binary(VV) ->
    try
        {ok, Doc, Token, vv_from_wire(Cbor, VV),
         maps:get(<<"deleted">>, Body, false) =:= true}
    catch
        _:_ -> error
    end;
put_version_fields(_Cbor, _) ->
    error.

%% The version vector is a byte string in CBOR, base64 in JSON.
vv_to_wire(true, VV) -> VV;
vv_to_wire(false, VV) -> base64:encode(VV).

vv_from_wire(true, VV) -> VV;
vv_from_wire(false, VV) -> base64:decode(VV).

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
    case {seq_from_wire(SinceWire), att_filter(Params)} of
        {{ok, Since}, {ok, Filter}} -> {ok, Since, Limit, Filter};
        {error, _} -> {error, <<"bad_since">>};
        {_, error} -> {error, <<"bad_filter">>}
    end.

%% base64url JSON of barrel_rep_filter:to_wire/1.
att_filter(Params) ->
    case lists:keyfind(<<"filter">>, 1, Params) of
        false ->
            {ok, #{}};
        {_, B64} ->
            try json:decode(base64:decode(B64, #{mode => urlsafe,
                                                 padding => false})) of
                Wire when is_map(Wire) ->
                    case barrel_rep_filter:from_wire(Wire) of
                        {ok, Filter} -> {ok, Filter};
                        {error, _} -> error
                    end;
                _ ->
                    error
            catch
                _:_ -> error
            end
    end.

query_params(Req) ->
    case uri_string:dissect_query(livery_req:query(Req)) of
        {error, _, _} -> [];
        Pairs -> Pairs
    end.

att_diff_input(List) when is_list(List) ->
    Decoded = [with_origin(E, #{id => Id, name => Name, digest => Digest})
               || #{<<"id">> := Id, <<"name">> := Name,
                    <<"digest">> := Digest} = E <- List,
                  is_binary(Id), is_binary(Name), is_binary(Digest)],
    case length(Decoded) =:= length(List)
         andalso not lists:member(error, Decoded) of
        true -> {ok, Decoded};
        false -> error
    end;
att_diff_input(_) ->
    error.

%% An older client sends no origin: the diff is then by digest.
with_origin(#{<<"origin">> := B64}, Entry) ->
    try Entry#{origin => barrel_hlc:decode(base64:decode(B64))}
    catch _:_ -> error
    end;
with_origin(_, Entry) ->
    Entry.

%%====================================================================
%% Request plumbing
%%====================================================================

%% Resolve the db, couple clocks from the request header, and answer
%% with our current clock on the way out.
with_sync_db(Req, Fun) ->
    ok = barrel_hlc:maybe_sync_from_header(
        signed_header(?HLC_HEADER, Req)),
    Name = livery_req:binding(<<"db">>, Req),
    case ensure_existing(Name) of
        {ok, Handle} -> Fun(maps:get(docdb, Handle));
        {error, invalid_name} ->
            json_resp(Req, 400, #{error => <<"invalid_name">>});
        {error, Reason} ->
            error_resp(Req, Reason)
    end.

%% Replication never creates a database: an unknown one answers 404.
ensure_existing(Name) ->
    barrel_server_dbs:ensure(Name, #{must_exist => true}).

binding(Name, Req) ->
    uri_string:percent_decode(livery_req:binding(Name, Req)).

with_json(Req, Fun) ->
    case read_json(Req) of
        {ok, Body} -> Fun(Body);
        {error, Reason} -> json_resp(Req, 400, #{error => err_bin(Reason)})
    end.

read_json(Req) ->
    case read_raw(Req) of
        {ok, Bin} ->
            case content_hash_ok(Req, Bin) of
                ok -> decode_body(content_type(Req), Bin);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

read_raw(Req) ->
    case livery_req:body(Req) of
        empty ->
            {ok, <<>>};
        {buffered, Io} ->
            {ok, iolist_to_binary(Io)};
        {stream, Reader} ->
            case livery_body:read_all(Reader) of
                {ok, Bin, _R2} -> {ok, Bin};
                {error, Reason, _R2} -> {error, Reason}
            end
    end.

%% End-to-end body binding for signed requests: when the client signed an
%% `x-barrel-content-sha256' (verified by barrel_server_auth), the body
%% must hash to it. Absent header = not a signed request; no check.
content_hash_ok(Req, Bin) ->
    case livery_req:header(<<"x-barrel-content-sha256">>, Req, undefined) of
        undefined ->
            ok;
        Declared ->
            case barrel_sync_sig:content_sha256(Bin) of
                Declared -> ok;
                _ -> {error, content_hash_mismatch}
            end
    end.

%% A replication client from barrel_docdb 1.8 sends CBOR when a document
%% holds a binary JSON cannot carry; anything else is JSON.
decode_body(_ContentType, <<>>) -> {ok, #{}};
decode_body(<<"application/cbor", _/binary>>, Bin) ->
    try barrel_docdb_codec_cbor:decode_cbor(Bin) of
        #{} = Body -> {ok, Body};
        _ -> {error, bad_cbor}
    catch _:_ -> {error, bad_cbor}
    end;
decode_body(_ContentType, Bin) ->
    try {ok, json:decode(Bin)}
    catch _:_ -> {error, bad_json}
    end.

content_type(Req) ->
    case signed_header(<<"content-type">>, Req) of
        undefined -> <<>>;
        CT -> string:lowercase(CT)
    end.

%% A request header as authenticated (the signed value on a signed request).
signed_header(Name, Req) ->
    barrel_server_auth:header(Name, Req).

request_cbor(Req) ->
    case content_type(Req) of
        <<"application/cbor", _/binary>> -> true;
        _ -> false
    end.

%% CBOR to a client that accepts it, JSON otherwise; a body JSON cannot
%% carry (a binary that is not UTF-8) to a JSON-only client is a 406.
json_resp(Req, Status, Term) ->
    case encode_resp(accepts_cbor(Req), Term) of
        {ContentType, Body} ->
            resp(Status, ContentType, Body);
        not_acceptable ->
            resp(406, <<"application/json">>,
                 json:encode(#{error => <<"not_acceptable">>}))
    end.

resp(Status, ContentType, Body) ->
    Headers = [{<<"content-type">>, ContentType},
               {?HLC_HEADER, hlc_to_wire(barrel_hlc:get_hlc())}],
    livery_resp:new(Status, Headers, {full, Body}).

encode_resp(true, Term) ->
    {<<"application/cbor">>, barrel_docdb_codec_cbor:encode_wire(Term)};
encode_resp(false, Term) ->
    try json:encode(Term) of
        Json -> {<<"application/json">>, Json}
    catch
        error:_ -> not_acceptable
    end.

accepts_cbor(Req) ->
    Accept = string:lowercase(livery_req:header(<<"accept">>, Req, <<>>)),
    binary:match(Accept, <<"application/cbor">>) =/= nomatch.

error_resp(Req, {error, Reason}) -> error_resp(Req, Reason);
error_resp(Req, not_found) ->
    json_resp(Req, 404, #{error => <<"not_found">>});
error_resp(Req, attachments_disabled) ->
    json_resp(Req, 501, #{error => <<"attachments_disabled">>});
error_resp(Req, Reason) ->
    json_resp(Req, 500, #{error => err_bin(Reason)}).

err_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
err_bin(Bin) when is_binary(Bin) -> Bin;
err_bin(Other) -> iolist_to_binary(io_lib:format("~p", [Other])).
