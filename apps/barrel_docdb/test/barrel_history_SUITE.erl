%%%-------------------------------------------------------------------
%%% @doc Test suite for the retained history log (barrel_history)
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_history_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, log},
        {group, versions}
    ].

groups() ->
    [
        {log, [sequence], [
            history_records_every_write,
            history_keeps_live_feed_invariant,
            history_records_delete,
            history_records_batch,
            history_replicated_cause,
            history_resolve_cause,
            history_fold_range_and_limit,
            history_floor_undefined
        ]},
        {versions, [sequence], [
            doc_versions_after_updates,
            doc_versions_with_conflict,
            version_body_current_and_archived,
            version_body_unknown
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_group(Group, Config) ->
    case barrel_docdb:open_db(<<"history_test_db">>) of
        {ok, _} -> barrel_docdb:delete_db(<<"history_test_db">>);
        _ -> ok
    end,
    DataDir = "/tmp/barrel_test_history_" ++ atom_to_list(Group),
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(<<"history_test_db">>, #{data_dir => DataDir}),
    [{data_dir, DataDir} | Config].

end_per_group(_Group, Config) ->
    barrel_docdb:delete_db(<<"history_test_db">>),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% All retained history entries, oldest first.
all_history(Db) ->
    {ok, Entries} = barrel_docdb:fold_history(
        Db, fun(E, Acc) -> {ok, [E | Acc]} end, []),
    lists:reverse(Entries).

doc_history(Db, DocId) ->
    [E || #{id := Id} = E <- all_history(Db), Id =:= DocId].

%% A version authored by a fake remote peer with its fresh-write vector.
remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.

%%====================================================================
%% Log Tests
%%====================================================================

history_records_every_write(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"hist_every_write">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev1,
                                   <<"v">> => 2}),
    {ok, #{<<"rev">> := Rev3}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev2,
                                   <<"v">> => 3}),

    Entries = doc_history(Db, DocId),
    ?assertEqual(3, length(Entries)),
    %% One entry per write, in write order, all local, versions matching
    ?assertEqual([Rev1, Rev2, Rev3],
                 [maps:get(version, E) || E <- Entries]),
    ?assert(lists:all(fun(#{cause := C}) -> C =:= local end, Entries)),
    ?assert(lists:all(fun(#{deleted := D}) -> D =:= false end, Entries)),
    %% HLCs strictly increase
    Hlcs = [maps:get(hlc, E) || E <- Entries],
    ?assertEqual(Hlcs, lists:usort(Hlcs)),
    %% Each entry's vector covers its version
    lists:foreach(
        fun(#{version := Tok, vv := VV}) ->
            ?assert(barrel_vv:contains(VV, barrel_version:from_token(Tok)))
        end, Entries),
    ok.

history_keeps_live_feed_invariant(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"hist_feed_invariant">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev1,
                                   <<"v">> => 2}),

    %% The live feed keeps ONE row per doc (its invariant), the history
    %% keeps every write
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    FeedRows = [C || C <- Changes, maps:get(id, C) =:= DocId],
    ?assertEqual(1, length(FeedRows)),
    [#{rev := FeedRev}] = FeedRows,
    ?assertEqual(Rev2, FeedRev),
    ?assertEqual(2, length(doc_history(Db, DocId))),
    ok.

history_records_delete(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"hist_delete">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:delete_doc(Db, DocId, #{rev => Rev1}),

    [First, Last] = doc_history(Db, DocId),
    ?assertEqual({Rev1, false}, {maps:get(version, First), maps:get(deleted, First)}),
    ?assertEqual({Rev2, true}, {maps:get(version, Last), maps:get(deleted, Last)}),
    ?assertEqual(local, maps:get(cause, Last)),
    ok.

history_records_batch(_Config) ->
    Db = <<"history_test_db">>,
    Ids = [<<"hist_batch_a">>, <<"hist_batch_b">>, <<"hist_batch_c">>],

    Results = barrel_docdb:put_docs(
        Db, [#{<<"id">> => Id, <<"n">> => 1} || Id <- Ids]),
    ?assertEqual(3, length([ok || {ok, _} <- Results])),

    lists:foreach(
        fun(Id) ->
            [Entry] = doc_history(Db, Id),
            ?assertEqual(local, maps:get(cause, Entry))
        end, Ids),
    ok.

history_replicated_cause(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"hist_replicated">>,

    %% A fresh replicated doc
    {Tok, VVBin} = remote_version(<<"peer_a">>),
    {ok, DocId, Tok} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>}, Tok, VVBin, false),
    [Entry1] = doc_history(Db, DocId),
    ?assertEqual(replicated, maps:get(cause, Entry1)),
    ?assertEqual(Tok, maps:get(version, Entry1)),

    %% A losing concurrent sibling still lands in the history
    DocId2 = <<"hist_replicated_loser">>,
    {LoserTok, LoserVV} = remote_version(<<"peer_b">>),
    {ok, #{<<"rev">> := LocalRev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId2, <<"v">> => <<"local">>}),
    {ok, DocId2, LocalRev} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId2, <<"v">> => <<"remote">>},
        LoserTok, LoserVV, false),
    [E1, E2] = doc_history(Db, DocId2),
    ?assertEqual({LocalRev, local}, {maps:get(version, E1), maps:get(cause, E1)}),
    ?assertEqual({LoserTok, replicated}, {maps:get(version, E2), maps:get(cause, E2)}),
    ok.

history_resolve_cause(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"hist_resolve">>,

    {RemoteTok, RemoteVV} = remote_version(<<"peer_a">>),
    {ok, #{<<"rev">> := LocalRev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
    {ok, DocId, LocalRev} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>},
        RemoteTok, RemoteVV, false),
    {ok, #{rev := ResolvedRev}} = barrel_docdb:resolve_conflict(
        Db, DocId, LocalRev, {merge, #{<<"v">> => <<"merged">>}}),

    Entries = doc_history(Db, DocId),
    ?assertEqual([local, replicated, resolve],
                 [maps:get(cause, E) || E <- Entries]),
    ?assertEqual(ResolvedRev, maps:get(version, lists:last(Entries))),
    ok.

history_fold_range_and_limit(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"hist_range">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev1,
                                   <<"v">> => 2}),
    {ok, #{<<"rev">> := _Rev3}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev2,
                                   <<"v">> => 3}),

    [#{hlc := H1}, #{hlc := H2}, #{hlc := H3}] = doc_history(Db, DocId),

    %% from is inclusive
    {ok, FromH2} = barrel_docdb:fold_history(
        Db, fun(E, Acc) -> {ok, [E | Acc]} end, [], #{from => H2}),
    ?assertEqual([H2, H3], [maps:get(hlc, E) || E <- lists:reverse(FromH2),
                                                maps:get(id, E) =:= DocId]),

    %% to is inclusive
    {ok, ToH2} = barrel_docdb:fold_history(
        Db, fun(E, Acc) -> {ok, [E | Acc]} end, [], #{to => H2}),
    HlcsToH2 = [maps:get(hlc, E) || E <- lists:reverse(ToH2),
                                    maps:get(id, E) =:= DocId],
    ?assertEqual([H1, H2], HlcsToH2),

    %% limit caps the scan
    {ok, Limited} = barrel_docdb:fold_history(
        Db, fun(E, Acc) -> {ok, [E | Acc]} end, [],
        #{from => H1, limit => 1}),
    ?assertEqual(1, length(Limited)),
    ok.

history_floor_undefined(_Config) ->
    %% No sweep has run: history complete since creation
    ?assertEqual(undefined, barrel_docdb:history_floor(<<"history_test_db">>)),
    ok.

%%====================================================================
%% Version Listing / Body Tests
%%====================================================================

doc_versions_after_updates(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"vers_updates">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev1,
                                   <<"v">> => 2}),

    {ok, Versions} = barrel_docdb:get_doc_versions(Db, DocId),
    ?assertEqual(2, length(Versions)),
    [Current | Rest] = Versions,
    ?assertEqual({Rev2, current},
                 {maps:get(version, Current), maps:get(status, Current)}),
    ?assertEqual([{Rev1, superseded}],
                 [{maps:get(version, V), maps:get(status, V)} || V <- Rest]),

    ?assertEqual({error, not_found},
                 barrel_docdb:get_doc_versions(Db, <<"vers_missing">>)),
    ok.

doc_versions_with_conflict(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"vers_conflict">>,

    {RemoteTok, RemoteVV} = remote_version(<<"peer_a">>),
    {ok, #{<<"rev">> := LocalRev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
    {ok, DocId, LocalRev} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>},
        RemoteTok, RemoteVV, false),

    {ok, Versions} = barrel_docdb:get_doc_versions(Db, DocId),
    ByStatus = maps:groups_from_list(
        fun(V) -> maps:get(status, V) end,
        fun(V) -> maps:get(version, V) end,
        Versions),
    ?assertEqual([LocalRev], maps:get(current, ByStatus)),
    ?assertEqual([RemoteTok], maps:get(conflict, ByStatus)),
    ok.

version_body_current_and_archived(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"vbody">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev1,
                                   <<"v">> => 2}),

    {ok, Body2} = barrel_docdb:get_version_body(Db, DocId, Rev2),
    ?assertEqual(2, maps:get(<<"v">>, Body2)),
    {ok, Body1} = barrel_docdb:get_version_body(Db, DocId, Rev1),
    ?assertEqual(1, maps:get(<<"v">>, Body1)),
    ok.

version_body_unknown(_Config) ->
    Db = <<"history_test_db">>,
    DocId = <<"vbody_unknown">>,

    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {UnknownTok, _} = remote_version(<<"peer_z">>),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_version_body(Db, DocId, UnknownTok)),
    ok.
