%%%-------------------------------------------------------------------
%%% @doc Attachment sync over the wire: blobs streamed both directions
%%% through /db/:db/_sync/att/*, digest-verified at the commit point,
%%% LWW-converged on origin HLC.
%%%
%%% Size bounds: neither side ever buffers a blob whole, response
%%% bodies are unbounded, and uploads are bounded only by the
%%% server's max_body knob (this suite runs with 32 MiB): the 20 MiB
%%% cases prove both directions clear the old engine and read_all
%%% caps, and the oversized case pins the graceful per-attachment
%%% failure past the knob.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_att_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_pull_large_blob/1,
    t_push_large_blob/1,
    t_oversized_push_degrades/1,
    t_pull_attachments/1,
    t_digest_reject/1,
    t_delete_propagation/1,
    t_lww_convergence/1,
    t_att_wire_shapes/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% 1 MiB building block for the multi-chunk blobs
-define(MIB, binary:copy(<<"0123456789abcdef">>, 65536)).

all() ->
    [t_pull_large_blob, t_push_large_blob, t_oversized_push_degrades,
     t_pull_attachments, t_digest_reject, t_delete_propagation,
     t_lww_convergence, t_att_wire_shapes].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    %% a small ceiling so the oversized case stays cheap
    application:set_env(barrel_server, max_body, 32 * 1024 * 1024),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, max_body),
    ok.

init_per_testcase(TC, Config) ->
    Local = <<(atom_to_binary(TC, utf8))/binary, "_local">>,
    Served = atom_to_list(TC) ++ "_served",
    {ok, _} = barrel_docdb:create_db(Local, #{
        data_dir => filename:join(?config(priv_dir, Config), "local")
    }),
    Endpoint = barrel_rep_transport_http:endpoint(
        list_to_binary(?config(base, Config) ++ "/db/" ++ Served)),
    [{local, Local}, {served, list_to_binary(Served)},
     {endpoint, Endpoint} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(local, Config)) catch _:_ -> ok end,
    ok.

push_opts() ->
    #{target_transport => barrel_rep_transport_http}.

pull_opts() ->
    #{source_transport => barrel_rep_transport_http}.

%%====================================================================
%% Cases
%%====================================================================

%% The GET leg has no size ceiling: 20 MiB (past the 16 MiB read_all
%% cap) pulls fine.
t_pull_large_blob(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"big">>}),
    {Digest, Length} = write_chunked(Served, <<"big">>, <<"blob.bin">>,
                                     20),
    {ok, R} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    ?assertMatch(#{atts_written := 1, att_bytes_written := Length},
                 maps:get(att_sync, R)),
    {ok, #{digest := Digest}} =
        barrel_docdb:get_attachment_info(Local, <<"big">>,
                                         <<"blob.bin">>),
    %% and read it once more over the raw wire stream
    {ok, Info, ReadFun} = barrel_rep_transport_http:get_attachment_stream(
        Endpoint, <<"big">>, <<"blob.bin">>),
    ?assertEqual(Digest, maps:get(digest, Info)),
    {Bytes, Hash} = drain(ReadFun, 0, crypto:hash_init(sha256)),
    ?assertEqual(Length, Bytes),
    Hex = string:lowercase(binary:encode_hex(crypto:hash_final(Hash))),
    ?assertEqual(Digest, <<"sha256-", Hex/binary>>),
    ok.

%% The PUT leg streams chunk by chunk: 20 MiB clears the old 8 MiB
%% engine cap (lifted with livery 0.5.1 / h1 0.7.1) and the 16 MiB
%% read_all cap.
t_push_large_blob(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"mid">>}),
    {Digest, Length} = write_chunked(Local, <<"mid">>, <<"blob.bin">>,
                                     20),
    {ok, R} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertMatch(#{atts_written := 1, att_bytes_written := Length},
                 maps:get(att_sync, R)),
    {ok, #{digest := Digest}} =
        barrel_docdb:get_attachment_info(Served, <<"mid">>,
                                         <<"blob.bin">>),
    ok.

%% A push past the server's max_body fails for that attachment only
%% (413 on the wire): the rep completes, smaller blobs still
%% transfer, and nothing partial lands on the target.
t_oversized_push_degrades(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"o">>}),
    {_BigDigest, _} = write_chunked(Local, <<"o">>, <<"too-big">>, 40),
    {ok, _} = barrel_docdb:put_attachment(Local, <<"o">>, <<"small">>,
                                          <<"fits">>),
    {ok, R} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertMatch(#{atts_written := 1, att_write_failures := 1},
                 maps:get(att_sync, R)),
    {ok, <<"fits">>} = barrel_docdb:get_attachment(Served, <<"o">>,
                                                   <<"small">>),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment_info(Served, <<"o">>,
                                                  <<"too-big">>)),
    ok.

write_chunked(Db, DocId, Name, Mibs) ->
    {ok, W0} = barrel_docdb:open_attachment_writer(
        Db, DocId, Name, <<"application/octet-stream">>),
    WN = lists:foldl(
        fun(_, W) ->
            {ok, W2} = barrel_docdb:write_attachment_chunk(W, ?MIB),
            W2
        end, W0, lists:seq(1, Mibs)),
    {ok, #{digest := Digest, length := Length}} =
        barrel_docdb:finish_attachment_writer(WN),
    ?assertEqual(Mibs * 1024 * 1024, Length),
    {Digest, Length}.

drain(ReadFun, Bytes, Hash) ->
    case ReadFun() of
        {ok, Chunk, Next} ->
            drain(Next, Bytes + byte_size(Chunk),
                  crypto:hash_update(Hash, Chunk));
        eof ->
            {Bytes, Hash}
    end.

t_pull_attachments(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"p">>}),
    {ok, _} = barrel_docdb:put_attachment(Served, <<"p">>, <<"f.txt">>,
                                          <<"pulled bytes">>),
    {ok, R} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    ?assertMatch(#{atts_written := 1}, maps:get(att_sync, R)),
    {ok, <<"pulled bytes">>} =
        barrel_docdb:get_attachment(Local, <<"p">>, <<"f.txt">>),
    %% second run: digest diff answers have, nothing re-transfers
    {ok, R2} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    ?assertMatch(#{atts_written := 0}, maps:get(att_sync, R2)),
    ok.

t_digest_reject(Config) ->
    Local = ?config(local, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:put_attachment(Local, <<"d">>, <<"f">>,
                                          <<"honest bytes">>),
    {ok, [Entry], _} = barrel_docdb:att_changes(Local, first),
    {ok, _Info, ReadFun} = get_local_stream(Local, <<"d">>, <<"f">>),
    Meta = #{content_type => maps:get(content_type, Entry),
             digest => <<"sha256-bogus">>,
             length => maps:get(length, Entry),
             origin_hlc => maps:get(origin, Entry)},
    ?assertEqual({error, digest_mismatch},
                 barrel_rep_transport_http:put_attachment(
                     Endpoint, <<"d">>, <<"f">>, Meta, ReadFun)),
    %% nothing committed at the target
    ?assertEqual({error, not_found},
                 barrel_rep_transport_http:get_attachment_stream(
                     Endpoint, <<"d">>, <<"f">>)),
    ok.

get_local_stream(Db, DocId, Name) ->
    {ok, Info} = barrel_docdb:get_attachment_info(Db, DocId, Name),
    {ok, Stream} = barrel_docdb:open_attachment_stream(Db, DocId, Name),
    {ok, Info, local_read_fun(Stream)}.

local_read_fun(Stream) ->
    fun() ->
        case barrel_docdb:read_attachment_chunk(Stream) of
            {ok, Chunk, Stream2} -> {ok, Chunk, local_read_fun(Stream2)};
            eof -> eof;
            {error, _} = Error -> Error
        end
    end.

t_delete_propagation(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:put_attachment(Local, <<"d">>, <<"f">>,
                                          <<"doomed">>),
    {ok, R1} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertMatch(#{atts_written := 1}, maps:get(att_sync, R1)),
    ok = barrel_docdb:delete_attachment(Local, <<"d">>, <<"f">>),
    {ok, R2} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertMatch(#{atts_deleted := 1}, maps:get(att_sync, R2)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment_info(Served, <<"d">>,
                                                  <<"f">>)),
    ok.

t_lww_convergence(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"x">>}),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"x">>}),
    %% the same key written on both sides with different bytes
    {ok, _} = barrel_docdb:put_attachment(Local, <<"x">>, <<"f">>,
                                          <<"local bytes">>),
    {ok, _} = barrel_docdb:put_attachment(Served, <<"x">>, <<"f">>,
                                          <<"served bytes">>),
    %% two sync rounds each way: LWW on origin HLC must converge
    lists:foreach(
        fun(_) ->
            {ok, _} = barrel_rep:replicate(Local, Endpoint, push_opts()),
            {ok, _} = barrel_rep:replicate(Endpoint, Local, pull_opts())
        end, [1, 2]),
    {ok, #{digest := DL}} =
        barrel_docdb:get_attachment_info(Local, <<"x">>, <<"f">>),
    {ok, #{digest := DS}} =
        barrel_docdb:get_attachment_info(Served, <<"x">>, <<"f">>),
    ?assertEqual(DL, DS),
    ok.

t_att_wire_shapes(Config) ->
    Local = ?config(local, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"w">>}),
    {ok, _} = barrel_docdb:put_attachment(Local, <<"w">>, <<"f">>,
                                          <<"wire bytes">>),
    {ok, _} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    %% att_changes over the wire mirrors the local feed entry
    {ok, [LocalEntry], _} = barrel_docdb:att_changes(Local, first),
    {ok, Entries, LastSeq} =
        barrel_rep_transport_http:att_changes(Endpoint, first, #{}),
    ?assertMatch([#{op := put, id := <<"w">>, name := <<"f">>}],
                 Entries),
    [WireEntry] = Entries,
    ?assertEqual(maps:get(digest, LocalEntry),
                 maps:get(digest, WireEntry)),
    ?assertEqual(maps:get(length, LocalEntry),
                 maps:get(length, WireEntry)),
    %% the replicated entry keeps the source's origin HLC
    ?assertEqual(maps:get(origin, LocalEntry),
                 maps:get(origin, WireEntry)),
    ?assertEqual(LastSeq, maps:get(seq, WireEntry)),
    %% since-exclusive: nothing after the last seq
    {ok, [], LastSeq} =
        barrel_rep_transport_http:att_changes(Endpoint, LastSeq, #{}),
    %% digest diff both ways
    {ok, [#{status := have}]} =
        barrel_rep_transport_http:diff_attachments(
            Endpoint, [maps:with([id, name, digest], WireEntry)]),
    {ok, [#{status := missing}]} =
        barrel_rep_transport_http:diff_attachments(
            Endpoint, [#{id => <<"w">>, name => <<"f">>,
                         digest => <<"sha256-other">>}]),
    ok.
