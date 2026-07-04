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
    delete_local/1
]).

-define(JSON_CT, {<<"content-type">>, <<"application/json">>}).
-define(HLC_HEADER, <<"x-barrel-hlc">>).

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
