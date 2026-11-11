%%% @doc Every version keeps its client vector: a version that stops being
%%% current archives it next to its body, replication carries the vectors
%%% of all live versions, a version that becomes current again gets its
%%% vector back, and cleanup removes the vector with the body.
-module(barrel_version_vector_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(VA, [0.5, 0.25]).
-define(VB, [0.75, 1.0]).
-define(VC, [0.125, 2.0]).

all() ->
    [{group, versions}, {group, resolution}, {group, cleanup},
     {group, timeline}].

groups() ->
    [{versions, [], [local_edit_archives_vector,
                     local_delete_archives_vector,
                     losing_version_archives_vector,
                     sibling_vectors_cross_state_sync,
                     promoted_sibling_vector_replicates,
                     superseded_vector_readable,
                     current_version_read_with_vector]},
     {resolution, [], [later_resolution_keeps_archived_vector,
                       resolve_choose_sibling_keeps_vector,
                       resolve_choose_current_keeps_vector,
                       resolve_merge_with_embedding,
                       resolve_merge_without_embedding]},
     {cleanup, [], [retention_removes_vector,
                    stub_removes_vectors,
                    purge_removes_vectors]},
     {timeline, [], [branch_reads_archived_vector,
                     pitr_branch_restores_vector,
                     pitr_branch_forgets_vectors,
                     merge_branch_carries_vectors]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_version_vector",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Base = atom_to_binary(TC),
    Opts0 = #{data_dir => ?config(data_dir, Config)},
    Opts = case TC of
        later_resolution_keeps_archived_vector ->
            Opts0#{conflict_resolver => {?MODULE, choose_keep_of_three}};
        retention_removes_vector -> Opts0#{retention_period => 1};
        stub_removes_vectors -> Opts0#{retention_period => 1};
        purge_removes_vectors ->
            Opts0#{retention_period => 1, tombstone_purge_after => 1};
        _ -> Opts0
    end,
    Dbs = [<<Base/binary, "_", S/binary>> || S <- [<<"a">>, <<"b">>, <<"c">>]],
    [begin
         _ = barrel_docdb:delete_db(D),
         {ok, _} = barrel_docdb:create_db(D, Opts)
     end || D <- Dbs],
    [{dbs, Dbs}, {branch, <<Base/binary, "_br">>} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_docdb:delete_db(?config(branch, Config),
                               #{data_dir => ?config(data_dir, Config)}),
    [_ = barrel_docdb:delete_db(D) || D <- ?config(dbs, Config)],
    ok.

%%====================================================================
%% Versions
%%====================================================================

local_edit_archives_vector(Config) ->
    [A | _] = ?config(dbs, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    _ = put(A, <<"m">>, #{<<"v">> => 2, <<"_rev">> => R1}, ?VB),
    ?assertEqual(?VA, archived(A, <<"m">>, R1)),
    ?assertEqual({client, ?VB}, vector(A, <<"m">>)).

local_delete_archives_vector(Config) ->
    [A | _] = ?config(dbs, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    {ok, _} = barrel_docdb:delete_doc(A, <<"m">>, #{rev => R1}),
    ?assertEqual(?VA, archived(A, <<"m">>, R1)).

%% A's version loses to a later concurrent write from B.
losing_version_archives_vector(Config) ->
    {A, B, _} = dbs(Config),
    {RA, _RB} = concurrent(A, B),
    {ok, _} = replicate(B, A),
    ?assertEqual({client, ?VB}, vector(A, <<"m">>)),
    ?assertEqual(?VA, archived(A, <<"m">>, RA)).

sibling_vectors_cross_state_sync(Config) ->
    {A, B, _} = dbs(Config),
    {RA, RB} = concurrent(A, B),
    {ok, _} = replicate(B, A),
    {ok, #{vectors := Vectors}} = barrel_docdb:get_doc_state(A, <<"m">>),
    ?assertEqual(#{RA => ?VA, RB => ?VB}, Vectors),
    %% B receives A's losing version with its vector
    {ok, _} = replicate(A, B),
    ?assertEqual(?VA, archived(B, <<"m">>, RA)).

%% B never held A's version as current: it got it as a sibling over
%% replication. Resolving to it on B, then replicating to C, gives both
%% A's vector.
promoted_sibling_vector_replicates(Config) ->
    {A, B, C} = dbs(Config),
    {RA, RB} = concurrent(A, B),
    {ok, _} = replicate(A, B),
    {ok, _} = barrel_docdb:resolve_conflict(B, <<"m">>, RB, {choose, RA}),
    {ok, #{<<"text">> := <<"a">>}} = barrel_docdb:get_doc(B, <<"m">>),
    ?assertEqual({client, ?VA}, vector(B, <<"m">>)),
    {ok, _} = replicate(B, C),
    ?assertEqual({client, ?VA}, vector(C, <<"m">>)).

superseded_vector_readable(Config) ->
    [A | _] = ?config(dbs, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    R2 = put(A, <<"m">>, #{<<"v">> => 2, <<"_rev">> => R1}, ?VB),
    _ = put(A, <<"m">>, #{<<"v">> => 3, <<"_rev">> => R2}, ?VC),
    {ok, B1} = barrel_docdb:get_version_body(A, <<"m">>, R1,
                                             #{include_embedding => true}),
    ?assertEqual(1, maps:get(<<"v">>, B1)),
    ?assertEqual(#{<<"vector">> => ?VA, <<"source">> => <<"client">>},
                 maps:get(<<"_embedding">>, B1)),
    {ok, B2} = barrel_docdb:get_version_body(A, <<"m">>, R2, #{}),
    ?assertNot(maps:is_key(<<"_embedding">>, B2)).

current_version_read_with_vector(Config) ->
    [A | _] = ?config(dbs, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    {ok, B1} = barrel_docdb:get_version_body(A, <<"m">>, R1,
                                             #{include_embedding => true}),
    ?assertEqual(#{<<"vector">> => ?VA, <<"source">> => <<"client">>},
                 maps:get(<<"_embedding">>, B1)).

%%====================================================================
%% Resolution
%%====================================================================

%% The resolver declines two versions and picks `keep' once a third
%% arrives, in a later write than the one where A's version lost.
later_resolution_keeps_archived_vector(Config) ->
    {A, B, C} = dbs(Config),
    _ = put(A, <<"m">>, #{<<"keep">> => true}, ?VA),
    timer:sleep(2),
    _ = put(B, <<"m">>, #{<<"v">> => <<"b">>}, ?VB),
    timer:sleep(2),
    _ = put(C, <<"m">>, #{<<"v">> => <<"c">>}, ?VC),
    {ok, _} = replicate(B, A),
    {ok, [_]} = barrel_docdb:get_conflicts(A, <<"m">>),
    {ok, _} = replicate(C, A),
    {ok, Doc} = barrel_docdb:get_doc(A, <<"m">>),
    ?assertEqual(true, maps:get(<<"keep">>, Doc)),
    {ok, []} = barrel_docdb:get_conflicts(A, <<"m">>),
    ?assertEqual({client, ?VA}, vector(A, <<"m">>)).

resolve_choose_sibling_keeps_vector(Config) ->
    {A, B, _} = dbs(Config),
    {RA, RB} = concurrent(A, B),
    {ok, _} = replicate(B, A),
    {ok, _} = barrel_docdb:resolve_conflict(A, <<"m">>, RB, {choose, RA}),
    {ok, []} = barrel_docdb:get_conflicts(A, <<"m">>),
    ?assertEqual({client, ?VA}, vector(A, <<"m">>)).

resolve_choose_current_keeps_vector(Config) ->
    {A, B, _} = dbs(Config),
    {_RA, RB} = concurrent(A, B),
    {ok, _} = replicate(B, A),
    {ok, _} = barrel_docdb:resolve_conflict(A, <<"m">>, RB, {choose, RB}),
    ?assertEqual({client, ?VB}, vector(A, <<"m">>)).

resolve_merge_with_embedding(Config) ->
    {A, B, C} = dbs(Config),
    {_RA, RB} = concurrent(A, B),
    {ok, _} = replicate(B, A),
    Merged = #{<<"text">> => <<"merged">>, <<"_embedding">> => ?VC},
    {ok, _} = barrel_docdb:resolve_conflict(A, <<"m">>, RB, {merge, Merged}),
    {ok, Doc} = barrel_docdb:get_doc(A, <<"m">>),
    ?assertEqual(<<"merged">>, maps:get(<<"text">>, Doc)),
    ?assertNot(maps:is_key(<<"_embedding">>, Doc)),
    ?assertEqual({client, ?VC}, vector(A, <<"m">>)),
    {ok, _} = replicate(A, C),
    ?assertEqual({client, ?VC}, vector(C, <<"m">>)).

resolve_merge_without_embedding(Config) ->
    {A, B, _} = dbs(Config),
    {_RA, RB} = concurrent(A, B),
    {ok, _} = replicate(B, A),
    {ok, _} = barrel_docdb:resolve_conflict(A, <<"m">>, RB,
                                            {merge, #{<<"text">> => <<"x">>}}),
    ?assertEqual(none, vector(A, <<"m">>)).

choose_keep_of_three(Versions) when length(Versions) >= 3 ->
    [#{token := T}] = [V || #{body := #{<<"keep">> := true}} = V <- Versions],
    {choose, T};
choose_keep_of_three(_Versions) ->
    keep.

%%====================================================================
%% Cleanup
%%====================================================================

retention_removes_vector(Config) ->
    [A | _] = ?config(dbs, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    _ = put(A, <<"m">>, #{<<"v">> => 2, <<"_rev">> => R1}, ?VB),
    ?assertEqual(?VA, archived(A, <<"m">>, R1)),
    timer:sleep(1200),
    {ok, #{superseded_removed := 1}} = barrel_docdb:sweep_retention(A),
    ?assertEqual(none, archived(A, <<"m">>, R1)),
    ?assertEqual({client, ?VB}, vector(A, <<"m">>)).

stub_removes_vectors(Config) ->
    [A | _] = ?config(dbs, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    {ok, _} = barrel_docdb:delete_doc(A, <<"m">>, #{rev => R1}),
    timer:sleep(1200),
    {ok, #{docs_stubbed := 1}} = barrel_docdb:sweep_retention(A),
    ?assertEqual(none, archived(A, <<"m">>, R1)).

purge_removes_vectors(Config) ->
    [A | _] = ?config(dbs, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    {ok, _} = barrel_docdb:delete_doc(A, <<"m">>, #{rev => R1}),
    timer:sleep(1200),
    {ok, #{docs_purged := 1}} = barrel_docdb:sweep_retention(A),
    ?assertEqual(none, archived(A, <<"m">>, R1)).

%%====================================================================
%% Timeline
%%====================================================================

branch_reads_archived_vector(Config) ->
    [A | _] = ?config(dbs, Config),
    Branch = ?config(branch, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    _ = put(A, <<"m">>, #{<<"v">> => 2, <<"_rev">> => R1}, ?VB),
    {ok, _} = barrel_docdb:branch_db(A, Branch, #{}),
    ?assertEqual(?VA, archived(Branch, <<"m">>, R1)),
    ?assertEqual({client, ?VB}, vector(Branch, <<"m">>)).

pitr_branch_restores_vector(Config) ->
    [A | _] = ?config(dbs, Config),
    Branch = ?config(branch, Config),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    T = cursor(A),
    _ = put(A, <<"m">>, #{<<"v">> => 2, <<"_rev">> => R1}, ?VB),
    {ok, _} = barrel_docdb:branch_db(A, Branch, #{at => T}),
    {ok, #{<<"v">> := 1, <<"_rev">> := R1}} = barrel_docdb:get_doc(Branch, <<"m">>),
    ?assertEqual({client, ?VA}, vector(Branch, <<"m">>)).

pitr_branch_forgets_vectors(Config) ->
    [A | _] = ?config(dbs, Config),
    Branch = ?config(branch, Config),
    _ = put(A, <<"x">>, #{}, undefined),
    T = cursor(A),
    R1 = put(A, <<"m">>, #{<<"v">> => 1}, ?VA),
    _ = put(A, <<"m">>, #{<<"v">> => 2, <<"_rev">> => R1}, ?VB),
    {ok, _} = barrel_docdb:branch_db(A, Branch, #{at => T}),
    {error, not_found} = barrel_docdb:get_doc(Branch, <<"m">>),
    ?assertEqual(none, archived(Branch, <<"m">>, R1)).

merge_branch_carries_vectors(Config) ->
    [A | _] = ?config(dbs, Config),
    Branch = ?config(branch, Config),
    R0 = put(A, <<"m">>, #{<<"v">> => 0}, undefined),
    {ok, _} = barrel_docdb:branch_db(A, Branch, #{}),
    RA = put(A, <<"m">>, #{<<"v">> => <<"parent">>, <<"_rev">> => R0}, ?VA),
    timer:sleep(2),
    _ = put(Branch, <<"m">>, #{<<"v">> => <<"branch">>, <<"_rev">> => R0}, ?VB),
    {ok, _} = barrel_docdb:merge_branch(Branch, #{attachments => false}),
    ?assertEqual({client, ?VB}, vector(A, <<"m">>)),
    ?assertEqual(?VA, archived(A, <<"m">>, RA)).

%%====================================================================
%% Helpers
%%====================================================================

dbs(Config) ->
    [A, B, C] = ?config(dbs, Config),
    {A, B, C}.

put(Db, Id, Fields, Vec) ->
    Doc0 = Fields#{<<"id">> => Id},
    Doc = case Vec of
        undefined -> Doc0;
        _ -> Doc0#{<<"_embedding">> => Vec}
    end,
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Db, Doc),
    Rev.

%% Concurrent versions of `m': A's first (with ?VA), then B's (with ?VB),
%% which wins.
concurrent(A, B) ->
    RA = put(A, <<"m">>, #{<<"text">> => <<"a">>}, ?VA),
    timer:sleep(2),
    RB = put(B, <<"m">>, #{<<"text">> => <<"b">>}, ?VB),
    {RA, RB}.

replicate(Src, Tgt) ->
    barrel_rep:replicate(Src, Tgt, #{attachments => false}).

cursor(Db) ->
    {ok, _Changes, Last} = barrel_docdb:get_changes(Db, first),
    Last.

vector(Db, Id) ->
    {ok, Doc} = barrel_docdb:get_doc(Db, Id, #{include_embedding => true}),
    case maps:get(<<"_embedding">>, Doc, undefined) of
        undefined -> none;
        #{<<"vector">> := V, <<"source">> := <<"client">>} -> {client, V};
        #{<<"vector">> := V, <<"source">> := <<"computed">>} -> {computed, V}
    end.

%% The archived vector of a version, read from the store.
archived(Db, Id, Token) ->
    StoreRef = persistent_term:get({barrel_store, Db}),
    Ks = barrel_keyspace:resolve(Db),
    VEnc = barrel_version:encode(barrel_version:from_token(Token)),
    case barrel_store_rocksdb:body_get(
           StoreRef, barrel_store_keys:doc_vector_rev(Ks, Id, VEnc)) of
        {ok, Bin} -> barrel_doc:decode_embedding(Bin);
        not_found -> none
    end.
