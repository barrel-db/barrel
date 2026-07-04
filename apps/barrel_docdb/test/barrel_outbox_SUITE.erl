%%%-------------------------------------------------------------------
%%% @doc Test suite for the generic tagged outbox (barrel_outbox).
%%%
%%% Covers: atomic entry creation with tagged writes, replace-on-rewrite,
%%% the documented stale-entry case for untagged rewrites, tagged deletes,
%%% exact-key acks (including the superseded-entry race), multi-tag
%%% writes, batch writes, fold limits, and the entry codec.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_outbox_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([tagged_put_creates_entry/1,
         rewrite_replaces_entry/1,
         untagged_rewrite_leaves_stale_entry/1,
         tagged_delete_marks_deleted/1,
         ack_removes_exact_keys/1,
         ack_superseded_is_noop/1,
         multi_tag_writes/1,
         batch_put_docs_tagged/1,
         fold_limit/1,
         entry_codec_roundtrip/1,
         replication_create_tagged/1,
         replication_update_replaces_entry/1,
         replication_delete_tagged/1,
         replication_loser_untagged/1,
         replication_merge_tagged/1,
         replication_untagged_without_config/1]).

-define(TAG, <<"embed">>).

all() ->
    [tagged_put_creates_entry,
     rewrite_replaces_entry,
     untagged_rewrite_leaves_stale_entry,
     tagged_delete_marks_deleted,
     ack_removes_exact_keys,
     ack_superseded_is_noop,
     multi_tag_writes,
     batch_put_docs_tagged,
     fold_limit,
     entry_codec_roundtrip,
     replication_create_tagged,
     replication_update_replaces_entry,
     replication_delete_tagged,
     replication_loser_untagged,
     replication_merge_tagged,
     replication_untagged_without_config].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_outbox_test_"
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

%% A tagged put creates exactly one pending entry carrying id/rev.
tagged_put_creates_entry(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, #{outbox => [?TAG]}),
    [Entry] = pending(Db, ?TAG),
    ?assertEqual(<<"a">>, maps:get(id, Entry)),
    ?assertEqual(Rev, maps:get(rev, Entry)),
    ?assertEqual(false, maps:get(deleted, Entry)),
    ?assert(maps:is_key(hlc, Entry)),
    %% An untagged write creates no entry
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b">>, <<"v">> => 1}),
    ?assertEqual(1, length(pending(Db, ?TAG))).

%% A tagged rewrite replaces the pending entry (one per doc per tag).
rewrite_replaces_entry(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, #{outbox => [?TAG]}),
    [#{hlc := Hlc1}] = pending(Db, ?TAG),
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 2, <<"_rev">> => Rev1},
        #{outbox => [?TAG]}),
    [Entry] = pending(Db, ?TAG),
    ?assertEqual(Rev2, maps:get(rev, Entry)),
    ?assertNotEqual(Hlc1, maps:get(hlc, Entry)).

%% An untagged rewrite of a tagged doc leaves the old entry in place
%% (documented: the producer decides tagging per write; record mode
%% always tags).
untagged_rewrite_leaves_stale_entry(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, #{outbox => [?TAG]}),
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 2, <<"_rev">> => Rev1}),
    [Entry] = pending(Db, ?TAG),
    %% Entry still points at the first write
    ?assertEqual(Rev1, maps:get(rev, Entry)).

%% A tagged delete replaces the entry with a deleted one.
tagged_delete_marks_deleted(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, #{outbox => [?TAG]}),
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:delete_doc(
        Db, <<"a">>, #{rev => Rev1, outbox => [?TAG]}),
    [Entry] = pending(Db, ?TAG),
    ?assertEqual(true, maps:get(deleted, Entry)),
    ?assertEqual(Rev2, maps:get(rev, Entry)).

%% Ack removes exactly the given HLC keys.
ack_removes_exact_keys(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, #{outbox => [?TAG]}),
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"b">>, <<"v">> => 1}, #{outbox => [?TAG]}),
    [#{hlc := HlcA, id := <<"a">>}, EntryB] = pending(Db, ?TAG),
    ok = barrel_docdb:outbox_ack(Db, ?TAG, [HlcA]),
    ?assertEqual([EntryB], pending(Db, ?TAG)),
    %% Empty ack is a no-op
    ok = barrel_docdb:outbox_ack(Db, ?TAG, []),
    ?assertEqual([EntryB], pending(Db, ?TAG)).

%% Acking a superseded (already replaced) entry never removes the newer
%% one: the rewrite moved the entry to a different HLC key.
ack_superseded_is_noop(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, #{outbox => [?TAG]}),
    [#{hlc := Hlc1}] = pending(Db, ?TAG),
    %% Consumer holds Hlc1; doc is rewritten meanwhile
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 2, <<"_rev">> => Rev1},
        #{outbox => [?TAG]}),
    ok = barrel_docdb:outbox_ack(Db, ?TAG, [Hlc1]),
    %% The newer entry survives and re-drives the consumer
    [Entry] = pending(Db, ?TAG),
    ?assertNotEqual(Hlc1, maps:get(hlc, Entry)).

%% Multiple tags on one write produce independent entries.
multi_tag_writes(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1},
        #{outbox => [?TAG, <<"audit">>]}),
    [#{id := <<"a">>, hlc := EmbedHlc}] = pending(Db, ?TAG),
    [#{id := <<"a">>}] = pending(Db, <<"audit">>),
    %% Acking one tag leaves the other untouched
    ok = barrel_docdb:outbox_ack(Db, ?TAG, [EmbedHlc]),
    ?assertEqual([], pending(Db, ?TAG)),
    ?assertEqual(1, length(pending(Db, <<"audit">>))).

%% Batch writes tag every doc in the batch.
batch_put_docs_tagged(Config) ->
    Db = ?config(db, Config),
    Docs = [#{<<"id">> => <<"a">>}, #{<<"id">> => <<"b">>},
            #{<<"id">> => <<"c">>}],
    Results = barrel_docdb:put_docs(Db, Docs, #{outbox => [?TAG]}),
    ?assertEqual(3, length([ok || {ok, _} <- Results])),
    Ids = [maps:get(id, E) || E <- pending(Db, ?TAG)],
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], lists:sort(Ids)).

%% The fold limit bounds how many entries are visited.
fold_limit(Config) ->
    Db = ?config(db, Config),
    [ {ok, _} = barrel_docdb:put_doc(
          Db, #{<<"id">> => integer_to_binary(I)}, #{outbox => [?TAG]})
      || I <- lists:seq(1, 3) ],
    Collect = fun(E, Acc) -> {ok, [E | Acc]} end,
    Limited = barrel_docdb:outbox_fold(Db, ?TAG, Collect, [], #{limit => 2}),
    ?assertEqual(2, length(Limited)),
    All = barrel_docdb:outbox_fold(Db, ?TAG, Collect, []),
    ?assertEqual(3, length(All)).

%% Entry codec round-trips.
entry_codec_roundtrip(_Config) ->
    Bin = barrel_outbox:encode_entry(<<"doc-1">>, <<"3-abc">>, true),
    ?assertEqual(#{id => <<"doc-1">>, rev => <<"3-abc">>, deleted => true},
                 barrel_outbox:decode_entry(Bin)),
    Bin2 = barrel_outbox:encode_entry(<<>>, <<>>, false),
    ?assertEqual(#{id => <<>>, rev => <<>>, deleted => false},
                 barrel_outbox:decode_entry(Bin2)).

%%====================================================================
%% Replication tagging (outbox_tags_on_replication db config)
%%====================================================================

%% A replicated create on a tagged db produces a pending entry.
replication_create_tagged(Config) ->
    Db = tagged_db(Config),
    {V1, VV1} = remote_version(<<"peer_a">>),
    {ok, _, _} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, V1, VV1, false),
    [Entry] = pending(Db, ?TAG),
    ?assertEqual(<<"a">>, maps:get(id, Entry)),
    ?assertEqual(V1, maps:get(rev, Entry)),
    ?assertEqual(false, maps:get(deleted, Entry)).

%% A dominating replicated update replaces the pending entry: exactly
%% one entry per doc, pointing at the newest arrival.
replication_update_replaces_entry(Config) ->
    Db = tagged_db(Config),
    {V1, VV1Bin, VV1} = remote_chain_start(<<"peer_a">>),
    {ok, _, _} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, V1, VV1Bin, false),
    {V2, VV2Bin} = remote_chain_next(<<"peer_a">>, VV1),
    {ok, _, _} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 2}, V2, VV2Bin, false),
    [Entry] = pending(Db, ?TAG),
    ?assertEqual(V2, maps:get(rev, Entry)),
    ?assertEqual(false, maps:get(deleted, Entry)).

%% A replicated delete lands a deleted entry.
replication_delete_tagged(Config) ->
    Db = tagged_db(Config),
    {V1, VV1Bin, VV1} = remote_chain_start(<<"peer_a">>),
    {ok, _, _} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, V1, VV1Bin, false),
    {V2, VV2Bin} = remote_chain_next(<<"peer_a">>, VV1),
    {ok, _, _} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"a">>}, V2, VV2Bin, true),
    [Entry] = pending(Db, ?TAG),
    ?assertEqual(V2, maps:get(rev, Entry)),
    ?assertEqual(true, maps:get(deleted, Entry)).

%% A concurrent loser arrival keeps the local winner: the current body
%% did not change, so no entry is written (re-embedding identical text
%% would be wasted compute).
replication_loser_untagged(Config) ->
    Db = tagged_db(Config),
    %% remote version issued BEFORE the local write = concurrent loser
    {VLoser, VVLoser} = remote_version(<<"peer_a">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    {ok, _, Winner} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 0}, VLoser, VVLoser, false),
    ?assertNotEqual(VLoser, Winner),
    ?assertEqual([], pending(Db, ?TAG)).

%% A merger resolution is a new current body: the resolving write is
%% tagged (the superseded remote arrival is not).
replication_merge_tagged(Config) ->
    Db = tagged_db(Config, #{
        conflict_merger =>
            fun(_Id, Local, Remote) -> {merge, maps:merge(Remote, Local)} end
    }),
    {VLoser, VVLoser} = remote_version(<<"peer_a">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    {ok, _, ResolvedToken} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"a">>, <<"w">> => 2}, VLoser, VVLoser, false),
    [Entry] = pending(Db, ?TAG),
    ?assertEqual(ResolvedToken, maps:get(rev, Entry)),
    ?assertEqual(false, maps:get(deleted, Entry)).

%% Without the config key, replicated writes stay untagged.
replication_untagged_without_config(Config) ->
    Db = ?config(db, Config),
    {V1, VV1} = remote_version(<<"peer_a">>),
    {ok, _, _} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 1}, V1, VV1, false),
    ?assertEqual([], pending(Db, ?TAG)).

%%====================================================================
%% Helpers
%%====================================================================

%% Collect pending entries for a tag, in HLC order.
pending(Db, Tag) ->
    lists:reverse(
        barrel_docdb:outbox_fold(Db, Tag, fun(E, Acc) -> {ok, [E | Acc]} end, [])).

%% Recreate the per-case db configured to tag replication-applied
%% writes (so end_per_testcase cleans it up normally).
tagged_db(Config) ->
    tagged_db(Config, #{}).

tagged_db(Config, Extra) ->
    Db = ?config(db, Config),
    ok = barrel_docdb:delete_db(Db),
    DbConfig = Extra#{data_dir => ?config(dir, Config),
                      outbox_tags_on_replication => [?TAG]},
    {ok, _} = barrel_docdb:create_db(Db, DbConfig),
    Db.

%% A fabricated remote version + its vector, as a network peer ships it.
remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.

%% First link of a remote chain: also returns the decoded VV so a later
%% version by the same author can dominate it.
remote_chain_start(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV), VV}.

%% Next link: a newer version whose vector contains the previous one,
%% so it dominates (fast-forward on arrival).
remote_chain_next(Author, PrevVV) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(PrevVV, V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.
