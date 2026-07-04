%%%-------------------------------------------------------------------
%%% @doc Test suite for caller-side point reads (barrel_docdb_reader).
%%%
%%% Covers the persistent_term fast path used by barrel_docdb:get_doc,
%%% get_docs, find, and get_changes when the database is addressed by
%%% name: results identical to the server path, snapshot consistency
%%% under a concurrent writer, and the close race mapping to
%%% {error, not_found}.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_reader_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([get_doc_by_name/1,
         get_doc_via_pid/1,
         get_doc_raw_body/1,
         get_doc_after_close/1,
         get_docs_batch/1,
         get_docs_include_deleted/1,
         find_by_name/1,
         changes_by_name/1,
         snapshot_consistency/1]).

all() ->
    [get_doc_by_name,
     get_doc_via_pid,
     get_doc_raw_body,
     get_doc_after_close,
     get_docs_batch,
     get_docs_include_deleted,
     find_by_name,
     changes_by_name,
     snapshot_consistency].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_reader_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC, utf8),
    {ok, Pid} = barrel_docdb:create_db(Db, #{data_dir => ?config(dir, Config)}),
    [{db, Db}, {pid, Pid} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% Name-addressed get_doc takes the caller-side fast path and returns
%% the same result as the server path.
get_doc_by_name(Config) ->
    Db = ?config(db, Config),
    Pid = ?config(pid, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    {ok, Doc} = barrel_docdb:get_doc(Db, <<"a">>),
    ?assertEqual(1, maps:get(<<"v">>, Doc)),
    ?assert(maps:is_key(<<"_rev">>, Doc)),
    %% Identical to the server (pid) path
    {ok, DocViaPid} = barrel_docdb:get_doc(Pid, <<"a">>),
    ?assertEqual(Doc, DocViaPid),
    %% Missing doc still not_found on the fast path
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"nope">>)).

%% Pid-addressed get_doc keeps working through the server.
get_doc_via_pid(Config) ->
    Db = ?config(db, Config),
    Pid = ?config(pid, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"p">>, <<"v">> => 2}),
    {ok, Doc} = barrel_docdb:get_doc(Pid, <<"p">>),
    ?assertEqual(2, maps:get(<<"v">>, Doc)).

%% raw_body reads return {ok, CborBin, Meta} on the fast path.
get_doc_raw_body(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"raw">>, <<"v">> => 3}),
    {ok, CborBin, Meta} = barrel_docdb:get_doc(Db, <<"raw">>, #{raw_body => true}),
    ?assert(is_binary(CborBin)),
    ?assertEqual(<<"raw">>, maps:get(id, Meta)),
    ?assertEqual(false, maps:get(deleted, Meta)),
    Decoded = barrel_docdb_codec_cbor:decode_any(CborBin),
    ?assertEqual(3, maps:get(<<"v">>, Decoded)).

%% After close the persistent_term is gone: reads map to not_found.
get_doc_after_close(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"c">>, <<"v">> => 1}),
    {ok, _Doc} = barrel_docdb:get_doc(Db, <<"c">>),
    ok = barrel_docdb:close_db(Db),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"c">>)),
    %% get_docs on a closed/missing db returns the scalar with_db error,
    %% not a per-id list (pre-existing API shape, preserved).
    ?assertEqual({error, not_found}, barrel_docdb:get_docs(Db, [<<"c">>])),
    ok.

%% Batch reads preserve input order and report missing ids.
get_docs_batch(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b1">>, <<"v">> => 1}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b2">>, <<"v">> => 2}),
    Results = barrel_docdb:get_docs(Db, [<<"b2">>, <<"missing">>, <<"b1">>]),
    [{ok, D2}, {error, not_found}, {ok, D1}] = Results,
    ?assertEqual(2, maps:get(<<"v">>, D2)),
    ?assertEqual(1, maps:get(<<"v">>, D1)).

%% include_deleted is honored on the fast path.
get_docs_include_deleted(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>, <<"v">> => 1}),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"d">>, #{rev => Rev}),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"d">>)),
    [{error, not_found}] = barrel_docdb:get_docs(Db, [<<"d">>]),
    {ok, Deleted} = barrel_docdb:get_doc(Db, <<"d">>, #{include_deleted => true}),
    ?assertEqual(true, maps:get(<<"_deleted">>, Deleted)),
    [{ok, _}] = barrel_docdb:get_docs(Db, [<<"d">>], #{include_deleted => true}).

%% find works through the store-ref fast path (no writer round-trip).
find_by_name(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"f1">>, <<"type">> => <<"user">>}),
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"f2">>, <<"type">> => <<"admin">>}),
    {ok, Rows, _Meta} = barrel_docdb:find(
        Db, #{where => [{path, [<<"type">>], <<"user">>}]}),
    ?assertEqual(1, length(Rows)),
    [Row] = Rows,
    ?assertEqual(<<"f1">>, maps:get(<<"id">>, Row)).

%% get_changes works through the store-ref fast path.
changes_by_name(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"ch1">>, <<"v">> => 1}),
    {ok, Changes, _Last} = barrel_docdb:get_changes(Db, first),
    ?assertEqual(1, length(Changes)),
    [Change] = Changes,
    ?assertEqual(<<"ch1">>, maps:get(id, Change)).

%% Caller-side reads must never observe a torn entity/body pair while a
%% writer is updating the same document. Version tokens sort causally,
%% so across successive reads (Rev, N) must advance together: a torn
%% read shows a newer token with a stale body counter (or vice versa).
snapshot_consistency(Config) ->
    Db = ?config(db, Config),
    N = 200,
    Self = self(),
    Writer = spawn_link(fun() ->
        write_loop(Db, 1, N, undefined),
        Self ! writer_done
    end),
    read_until_done(Db, Writer),
    %% Final state is the last write
    {ok, Doc} = barrel_docdb:get_doc(Db, <<"snap">>),
    ?assertEqual(N, maps:get(<<"n">>, Doc)).

%%====================================================================
%% Helpers
%%====================================================================

write_loop(_Db, I, N, _PrevRev) when I > N ->
    ok;
write_loop(Db, I, N, PrevRev) ->
    Doc0 = #{<<"id">> => <<"snap">>, <<"n">> => I},
    Doc = case PrevRev of
        undefined -> Doc0;
        _ -> Doc0#{<<"_rev">> => PrevRev}
    end,
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Db, Doc),
    write_loop(Db, I + 1, N, Rev).

read_until_done(Db, Writer) ->
    read_until_done(Db, Writer, {<<>>, 0}).

read_until_done(Db, Writer, {LastRev, LastN} = Last) ->
    receive
        writer_done -> ok
    after 0 ->
        Next = case barrel_docdb:get_doc(Db, <<"snap">>) of
            {error, not_found} ->
                Last; %% writer has not created the doc yet
            {ok, Doc} ->
                Rev = maps:get(<<"_rev">>, Doc),
                BodyN = maps:get(<<"n">>, Doc),
                %% monotone together: same rev -> same body; newer body
                %% -> newer rev (tokens sort causally). A torn pair
                %% breaks one of these.
                case BodyN of
                    LastN -> ?assertEqual(LastRev, Rev);
                    _ -> ?assert(BodyN > LastN andalso Rev > LastRev)
                end,
                {Rev, BodyN}
        end,
        case is_process_alive(Writer) of
            true -> read_until_done(Db, Writer, Next);
            false ->
                %% drain the done message if it raced the alive check
                receive writer_done -> ok after 1000 -> ok end
        end
    end.
