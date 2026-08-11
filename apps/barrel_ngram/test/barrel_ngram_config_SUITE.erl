%%%-------------------------------------------------------------------
%%% @doc `open/2' option-validation lifecycle, end to end.
%%%
%%% `barrel_ngram_manifest_tests.erl' proves `reconcile_config/2' and the
%%% manifest version check are correct in isolation; this suite proves the
%%% same guarantees survive the real path -- `barrel_ngram:open/2' ->
%%% shard supervisor -> `barrel_ngram_shard:init/1' -- including the
%%% failure-propagates-cleanly and no-partial-state properties that only
%%% show up when the whole stack runs.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_config_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([missing_db_option/1, selector_option_rejected/1,
         reopen_same_config_ok/1, reopen_different_phase2_opts_rejected/1,
         reopen_different_fields_rejected/1,
         reopen_after_stale_segment_version_rejected/1,
         failed_reopen_leaves_corpus_closed/1]).

all() ->
    [missing_db_option, selector_option_rejected, reopen_same_config_ok,
     reopen_different_phase2_opts_rejected, reopen_different_fields_rejected,
     reopen_after_stale_segment_version_rejected,
     failed_reopen_leaves_corpus_closed].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_cfg_">>, atom_to_binary(TC, utf8)]),
    Corpus = Db,
    DataDir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    [{db, Db}, {corpus, Corpus}, {data_dir, DataDir} | Config].

end_per_testcase(_TC, Config) ->
    Corpus = ?config(corpus, Config),
    Db = ?config(db, Config),
    _ = barrel_ngram:close(Corpus),
    _ = barrel_docdb:delete_db(Db),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

missing_db_option(_Config) ->
    ?assertEqual({error, {missing_option, db}}, barrel_ngram:open(<<"whatever">>, #{})).

selector_option_rejected(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    ?assertEqual({error, {unsupported_option, selector}},
                 barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                             selector => barrel_ngram_selector_sparse})),
    %% a rejected open leaves nothing open
    ?assertEqual(false, barrel_ngram:is_open(Corpus)).

%% Config is persisted in the manifest only once something is actually
%% frozen to a segment (an empty, never-indexed corpus has no manifest on
%% disk at all, so there is nothing yet to conflict with -- see
%% seed_and_freeze/2). Every reopen test below seeds a document and
%% forces a freeze first, so it exercises the real protected invariant:
%% a corpus that has already indexed something under one config rejects
%% a later open under a different one.
reopen_same_config_ok(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Opts = #{db => Db, data_dir => DataDir,
             phase2_selector_opts => #{radius => 5, sample_rate => 8},
             fields => [<<"body">>]},
    ok = barrel_ngram:open(Corpus, Opts),
    seed_and_freeze(Db, Corpus),
    ok = barrel_ngram:close(Corpus),
    %% same options, same data_dir: re-attaches cleanly
    ?assertEqual(ok, barrel_ngram:open(Corpus, Opts)),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)).

reopen_different_phase2_opts_rejected(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => #{radius => 3}}),
    seed_and_freeze(Db, Corpus),
    ok = barrel_ngram:close(Corpus),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                         phase2_selector_opts => #{radius => 9}}),
    ?assertMatch({error, {config_mismatch, phase2_selector_opts, _, _}}, Result),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)).

reopen_different_fields_rejected(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir, fields => all}),
    seed_and_freeze(Db, Corpus),
    ok = barrel_ngram:close(Corpus),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                         fields => [<<"title">>]}),
    ?assertEqual({error, {config_mismatch, fields, all, [<<"title">>]}}, Result),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)).

%% A segment written by a future/older format must be rejected through the
%% real open/2 path, not just at the segment/manifest module level.
reopen_after_stale_segment_version_rejected(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Opts = #{db => Db, data_dir => DataDir},
    ok = barrel_ngram:open(Corpus, Opts),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>,
                                         <<"body">> => <<"connect_timeout">>}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    {ok, [{_Gen, Path}]} = barrel_ngram_shard:get_manifest(Corpus),
    ok = barrel_ngram:close(Corpus),
    corrupt_segment_version(Path, 3),
    Result = barrel_ngram:open(Corpus, Opts),
    ?assertEqual({error, {unsupported_segment_version, Path, 3, 4}}, Result),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)).

%% After a rejected reopen, the corpus is fully closed -- a caller can
%% retry with corrected options rather than being left in limbo.
failed_reopen_leaves_corpus_closed(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     fields => all}),
    seed_and_freeze(Db, Corpus),
    ok = barrel_ngram:close(Corpus),
    ?assertMatch({error, {config_mismatch, fields, _, _}},
                 barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                             fields => [<<"body">>]})),
    %% retry with the original (correct) options succeeds
    ?assertEqual(ok, barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                                 fields => all})),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)).

%%====================================================================
%% Helpers
%%====================================================================

%% Put one document and refresh so the buffer actually freezes to a real
%% segment -- config is only persisted in the manifest once a save
%% happens (see barrel_ngram_manifest:reconcile_config/2), so a reopen
%% test that never indexes anything would vacuously "pass" without ever
%% exercising the persisted-config comparison at all.
seed_and_freeze(Db, Corpus) ->
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"seed">>,
                                         <<"body">> => <<"connect_timeout">>}),
    {ok, _} = barrel_ngram:refresh(Corpus).

%% Force the on-disk segment version field (bytes 8..11, little-endian,
%% right after the 8-byte magic) back to an unsupported value.
corrupt_segment_version(Path, Version) ->
    {ok, Bin} = file:read_file(Path),
    <<Magic:8/binary, _OldVersion:32/little, Rest/binary>> = Bin,
    ok = file:write_file(Path, <<Magic/binary, Version:32/little, Rest/binary>>).
