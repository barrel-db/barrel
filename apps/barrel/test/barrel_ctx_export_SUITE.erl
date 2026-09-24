%%%-------------------------------------------------------------------
%%% @doc Quiesced export and read-only import of composed databases
%%% (B14, B15, spike S3): same answers under a new name, loaded ANN
%%% graph, refused writes, resumable and verified imports, and the
%%% breakages of a naive copy under a new name.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_export_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([plain_same_answers/1,
         record_same_answers/1,
         encrypted_same_answers/1,
         imported_writes_refused/1,
         interrupted_import_resumes/1,
         corrupt_artifact_rejected/1,
         export_refusals/1,
         closed_db_export/1,
         same_name_refused/1,
         s3_naive_copy_breaks/1,
         s3_same_name_reuses_source_id/1,
         imported_files_untouched/1,
         legacy_source_upgraded/1]).

-define(APPS, [<<"tools">>, <<"sasl">>, <<"eunit">>]).
-define(VQ, ["cover analysis of modules", "release handling upgrade",
             "unit testing framework", "profiling function calls",
             "system alarms"]).
-define(BQ, ["cover", "release", "test", "module", "alarm", "server"]).
-define(ENC, #{provider => barrel_facade_test_keyprovider}).

all() ->
    [plain_same_answers, record_same_answers, encrypted_same_answers,
     imported_writes_refused, interrupted_import_resumes,
     corrupt_artifact_rejected, export_refusals, closed_db_export,
     same_name_refused, s3_naive_copy_breaks,
     s3_same_name_reuses_source_id, imported_files_untouched,
     legacy_source_upgraded].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    Priv = ?config(priv_dir, Config),
    ok = application:set_env(barrel, ctx_dir, filename:join(Priv, "ctx")),
    ok = application:set_env(barrel, test_encryption_master,
                             crypto:strong_rand_bytes(32)),
    Docs = barrel_ctx_test_corpus:docs(?APPS),
    ct:pal("corpus: ~p docs from ~s", [length(Docs),
                                      barrel_ctx_test_corpus:corpus_file()]),
    [{docs, Docs} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    ok = barrel_ctx_test_corpus:mock_embed(),
    Dir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    [{dir, Dir}, {src, <<"src_", (atom_to_binary(TC))/binary>>} | Config].

end_per_testcase(_TC, Config) ->
    [ok = barrel_ctx_export:remove_import(N)
     || N <- barrel_ctx_export:list_imports()],
    _ = barrel_dbs:unhold(?config(src, Config)),
    _ = barrel_dbs:destroy(?config(src, Config)),
    ok = barrel_ctx_test_corpus:unmock_embed(),
    ok.

%%====================================================================
%% Cases
%%====================================================================

plain_same_answers(Config) ->
    {Src, Before} = seed_plain(Config, #{}),
    {ok, Exp} = barrel_ctx_export:export(Src, dest(Config), #{}),
    ct:pal("export: ~p artifacts, ~p bytes, ~p ms",
           [maps:get(artifacts, Exp), maps:get(bytes, Exp),
            maps:get(elapsed_ms, Exp)]),
    %% the source was open in barrel_dbs, so it is open again
    ?assert(lists:member(Src, barrel_dbs:list())),
    {ok, #{db := Imp, version := #{kind := generation, generation := 1},
           copied := Copied}} =
        barrel_ctx_export:import(dest(Config), #{name => <<"imp_plain">>}),
    ?assertEqual(maps:get(artifacts, Exp), Copied),
    ?assertEqual(Before, answers(Imp)),
    {ok, Stats} = barrel:vector_stats(Imp),
    ?assertEqual(loaded, maps:get(index_origin, Stats)),
    %% the copy reads the source keyspace under a fresh identity
    {ok, Info} = barrel_docdb:db_info(<<"imp_plain">>),
    ?assertEqual(Src, maps:get(keyspace, Info)),
    ?assertNot(maps:is_key(parent, Info)),
    ?assertNotEqual(barrel_docdb:db_instance_id(Src),
                    barrel_docdb:db_instance_id(<<"imp_plain">>)).

record_same_answers(Config) ->
    {Src, Before} = seed_record(Config, #{}),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config), #{}),
    {ok, #{db := Imp}} =
        barrel_ctx_export:import(dest(Config), #{name => <<"imp_record">>}),
    ?assert(maps:is_key(embedding, Imp)),
    ?assertEqual(true, maps:get(read_only, Imp)),
    {ok, Stats} = barrel:vector_stats(Imp),
    ?assertEqual(loaded, maps:get(index_origin, Stats)),
    After = answers(Imp),
    %% the disk BM25 index is copied as written: same hits, same scores
    ?assertEqual(Before, After).

encrypted_same_answers(Config) ->
    {Src, Before} = seed_plain(Config, #{encryption => ?ENC}),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config), #{}),
    %% without the key the copy does not open
    {error, _} = barrel_ctx_export:import(dest(Config),
                                          #{name => <<"imp_nokey">>}),
    {ok, #{db := Imp}} =
        barrel_ctx_export:import(dest(Config), #{name => <<"imp_enc">>,
                                                 encryption => ?ENC}),
    ?assertEqual(Before, answers(Imp)),
    %% ciphertext on disk: a known body string is not in any file
    Needle = <<"-module(">>,
    Dir = filename:join(barrel_ctx_export:imports_dir(), "imp_enc"),
    Hits = filelib:fold_files(Dir, ".*", true, fun(F, Acc) ->
        {ok, B} = file:read_file(F),
        case binary:match(B, Needle) of nomatch -> Acc; _ -> [F | Acc] end
    end, []),
    ?assertEqual([], [F || F <- Hits, filename:basename(F) =/= "IMPORT"]).

imported_writes_refused(Config) ->
    {Src, _} = seed_plain(Config, #{}),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config), #{}),
    {ok, #{db := Imp}} =
        barrel_ctx_export:import(dest(Config), #{name => <<"imp_ro">>}),
    [#{<<"id">> := Id} | _] = ?config(docs, Config),
    ?assertEqual({error, read_only},
                 barrel:put_doc(Imp, #{<<"id">> => <<"new">>})),
    ?assertEqual({error, read_only}, barrel:delete_doc(Imp, Id)),
    ?assertEqual({error, read_only},
                 barrel:put_attachment(Imp, Id, <<"a">>, <<"x">>)),
    ?assertEqual({error, read_only},
                 barrel:vector_add(Imp, <<"new">>, <<"t">>, #{},
                                   barrel_ctx_test_corpus:vec(<<"t">>))),
    ?assertEqual({error, read_only}, barrel:vector_delete(Imp, Id)),
    {ok, _} = barrel:get_doc(Imp, Id).

interrupted_import_resumes(Config) ->
    {Src, Before} = seed_plain(Config, #{}),
    {ok, #{artifacts := N}} =
        barrel_ctx_export:export(Src, dest(Config), #{}),
    Name = <<"imp_resume">>,
    {error, {interrupted, {3, 0}}} =
        barrel_ctx_export:import(dest(Config), #{name => Name,
                                                 stop_after => 3}),
    %% nothing half copied is ever openable
    ?assertEqual({error, not_imported}, barrel_ctx_export:open(Name)),
    ?assertEqual([], barrel_ctx_export:list_imports()),
    ?assertNot(lists:member(Name, barrel_dbs:list())),
    %% a second interruption keeps the verified files
    {error, {interrupted, {2, 3}}} =
        barrel_ctx_export:import(dest(Config), #{name => Name,
                                                 stop_after => 2}),
    {ok, #{db := Imp, copied := Copied, reused := 5}} =
        barrel_ctx_export:import(dest(Config), #{name => Name}),
    ?assertEqual(N - 5, Copied),
    ?assertEqual(Before, answers(Imp)),
    %% importing the same generation again is a no-op
    {ok, #{copied := 0}} = barrel_ctx_export:import(dest(Config),
                                                    #{name => Name}).

corrupt_artifact_rejected(Config) ->
    {Src, _} = seed_plain(Config, #{}),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config), #{}),
    {ok, _, Arts} = barrel_ctx_manifest:read(dest(Config)),
    [#{path := P} | _] = [A || #{path := <<"docdb/docs/", _/binary>>} = A
                               <- Arts],
    File = filename:join(dest(Config), P),
    {ok, Bin} = file:read_file(File),
    ok = file:write_file(File, <<Bin/binary, 0>>),
    ?assertEqual({error, {checksum_mismatch, P}},
                 barrel_ctx_export:import(dest(Config),
                                          #{name => <<"imp_bad">>})),
    ?assertEqual({error, not_imported}, barrel_ctx_export:open(<<"imp_bad">>)),
    %% a tampered part is refused before any copy
    [PartRef] = [R || #{<<"ref">> := R} <- manifest_parts(dest(Config))],
    ok = file:write_file(filename:join(dest(Config), PartRef), <<"{}">>),
    ?assertEqual({error, {part_checksum_mismatch, PartRef}},
                 barrel_ctx_export:import(dest(Config),
                                          #{name => <<"imp_bad2">>})),
    %% the verified files of the failed import stay for a resume until
    %% removed
    ok = barrel_ctx_export:remove_import(<<"imp_bad">>).

export_refusals(Config) ->
    {Src, _} = seed_plain(Config, #{}),
    ok = barrel_dbs:pin(Src),
    ?assertEqual({error, pinned},
                 barrel_ctx_export:export(Src, dest(Config), #{})),
    ok = barrel_dbs:unpin(Src),
    %% tagged by another owner
    ok = barrel_dbs:close(Src),
    {ok, _} = barrel_dbs:ensure(Src, (open_opts_plain(Config, #{}))#{
                                        owner => someone}),
    ?assertEqual({error, {owned_by, someone}},
                 barrel_ctx_export:export(Src, dest(Config), #{})),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config),
                                       #{owner => someone}),
    %% opened outside the manager
    ok = barrel_dbs:close(Src),
    {ok, Db} = barrel:open(Src, open_opts_plain(Config, #{})),
    ?assertEqual({error, open_outside_manager},
                 barrel_ctx_export:export(Src, dest(Config) ++ "2", #{})),
    ok = barrel:close(Db),
    %% a non-empty destination
    ?assertMatch({error, {dest_not_empty, _}},
                 barrel_ctx_export:export(Src, dest(Config), #{})).

closed_db_export(Config) ->
    {Src, Before} = seed_plain(Config, #{}),
    ok = barrel_dbs:close(Src),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config),
                                       #{open_opts =>
                                             open_opts_plain(Config, #{})}),
    %% closed before, closed after
    ?assertNot(lists:member(Src, barrel_dbs:list())),
    {ok, #{db := Imp}} =
        barrel_ctx_export:import(dest(Config), #{name => <<"imp_closed">>}),
    ?assertEqual(Before, answers(Imp)).

same_name_refused(Config) ->
    {Src, _} = seed_plain(Config, #{}),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config), #{}),
    ?assertEqual({error, {same_name_as_source, Src}},
                 barrel_ctx_export:import(dest(Config), #{name => Src})).

%% Spike S3, naive import: the docdb directory copied under a new name
%% and opened normally. Every assertion records a breakage.
s3_naive_copy_breaks(Config) ->
    {Src, Before} = seed_record(Config, #{}),
    {ok, SrcInfo} = barrel_docdb:db_info(Src),
    SrcPath = maps:get(db_path, SrcInfo),
    ok = barrel_dbs:close(Src),
    Naive = <<"naive_copy">>,
    DataDir = filename:join(?config(dir, Config), "naive"),
    ok = copy_tree(SrcPath, filename:join(DataDir, binary_to_list(Naive))),
    {ok, N} = barrel:open(Naive, #{docdb => #{data_dir => DataDir},
                                   vectordb => #{dimension => 64,
                                                 db_path => filename:join(
                                                     DataDir, "vec")}}),
    try
        [#{<<"id">> := Id} | _] = ?config(docs, Config),
        %% 1. keys embed the source name: every document is invisible
        ?assertEqual({error, not_found}, barrel:get_doc(N, Id)),
        {ok, [], _} = barrel:find(N, #{where => [{path, [<<"app">>],
                                                  <<"tools">>}]}),
        %% 2. the record-mode policy (a local doc) is invisible too, so
        %%    the copy cannot reopen in record mode from what it carries
        ?assertEqual({error, not_found},
                     barrel_docdb:get_local_doc(Naive,
                                                <<"_barrel/embedding">>)),
        %% 3. the vector store is not in the docdb directory: a fresh,
        %%    empty store opens at whatever path the caller names
        {ok, #{count := 0}} = barrel:vector_stats(N),
        %% 4. identity: a new name mints a new source id (fresh author)
        ?assertNotEqual(barrel_docdb:db_instance_id(Src),
                        barrel_docdb:db_instance_id(Naive)),
        ?assert(maps:get(find, Before) =/= [])
    after
        barrel:close(N)
    end.

%% Spike S3, hazard: the same directory restored under the SAME name on
%% another data dir reuses the source id (two live authors, one id).
s3_same_name_reuses_source_id(Config) ->
    {Src, _} = seed_plain(Config, #{}),
    {ok, SrcId} = barrel_docdb:db_instance_id(Src),
    {ok, SrcInfo} = barrel_docdb:db_info(Src),
    ok = barrel_dbs:close(Src),
    Other = filename:join(?config(dir, Config), "other_node"),
    ok = copy_tree(maps:get(db_path, SrcInfo),
                   filename:join(Other, binary_to_list(Src))),
    {ok, _} = barrel_docdb:create_db(Src, #{data_dir => Other}),
    ?assertEqual({ok, SrcId}, barrel_docdb:db_instance_id(Src)),
    ok = barrel_docdb:close_db(Src).

%% A read-only open, queries and close leave every copied file as the
%% manifest lists it and add none (RocksDB opens with OpenForReadOnly).
imported_files_untouched(Config) ->
    {Src, _} = seed_plain(Config, #{}),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config), #{}),
    Name = <<"imp_untouched">>,
    {ok, #{db := Imp}} = barrel_ctx_export:import(dest(Config),
                                                  #{name => Name}),
    Dir = filename:join(barrel_ctx_export:imports_dir(), "imp_untouched"),
    Before = tree(Dir),
    _ = non_trivial(answers(Imp)),
    ok = barrel_dbs:close(Name),
    ?assertEqual(Before, tree(Dir)),
    ?assertEqual([], manifest_mismatches(Config, Name, Dir)),
    %% the copy's source id comes from the import sidecar
    {ok, #{source_id := Sid}} = barrel_keyspace:read_meta(
                                  filename:join([Dir, "docdb", Name])),
    {ok, _} = barrel_ctx_export:open(Name),
    ?assertEqual({ok, Sid}, barrel_docdb:db_instance_id(Name)),
    ok = barrel_dbs:close(Name).

%% A source written before bm25.ids had its 2.4.1 column families: the
%% export upgrades it under the hold, the import opens read only and
%% writes nothing.
legacy_source_upgraded(Config) ->
    {Src, Before} = seed_record(Config, #{}),
    ok = barrel_dbs:close(Src),
    Opts = record_opts(Config, #{}),
    make_legacy(filename:join([?config(dir, Config), "vec", "bm25",
                               "bm25.ids"])),
    ?assertMatch({error, _},
                 barrel:open(Src, Opts#{read_only => true,
                                        store_supervised => true})),
    {ok, _} = barrel_ctx_export:export(Src, dest(Config),
                                       #{open_opts => Opts}),
    Name = <<"imp_legacy">>,
    {ok, #{db := Imp}} = barrel_ctx_export:import(dest(Config),
                                                  #{name => Name}),
    Dir = filename:join(barrel_ctx_export:imports_dir(), "imp_legacy"),
    Files = tree(Dir),
    ?assertEqual(Before, answers(Imp)),
    ok = barrel_dbs:close(Name),
    ?assertEqual(Files, tree(Dir)),
    ?assertEqual([], manifest_mismatches(Config, Name, Dir)).

%%====================================================================
%% Helpers
%%====================================================================

tree(Dir) ->
    lists:sort(filelib:fold_files(
                 Dir, ".*", true,
                 fun(F, Acc) ->
                         {ok, Sha} = barrel_ctx_manifest:sha256_file(F),
                         [{F, Sha} | Acc]
                 end, [])).

manifest_mismatches(Config, Name, Dir) ->
    {ok, _Root, Arts} = barrel_ctx_manifest:read(dest(Config)),
    Local = fun(<<"docdb/", Rest/binary>>) ->
                    filename:join([Dir, "docdb", Name, Rest]);
               (P) -> filename:join(Dir, P)
            end,
    [P || #{path := P} = A <- Arts,
          not barrel_ctx_manifest:verify_file(Local(P), A)].

%% What 2.4.1 added to bm25.ids is dropped (as the #47 tests do).
make_legacy(IdsPath) ->
    Cfs = ["default", "terms_fwd", "terms_rev", "docs_fwd", "docs_rev",
           "doc_terms", "term_df", "pending"],
    {ok, Db, [CfD, _, _, _, _ | New]} =
        rocksdb:open(IdsPath, [], [{N, []} || N <- Cfs]),
    [ok = rocksdb:drop_column_family(Db, Cf) || Cf <- New],
    [ok = rocksdb:delete(Db, CfD, K, [])
     || K <- [<<"format">>, <<"stats">>, <<"segment">>]],
    ok = rocksdb:close(Db).

dest(Config) ->
    filename:join(?config(dir, Config), "export").

open_opts_plain(Config, Extra) ->
    Dir = ?config(dir, Config),
    maps:merge(#{docdb => #{data_dir => filename:join(Dir, "docdb")},
                 vectordb => #{dimension => barrel_ctx_test_corpus:dim(),
                               db_path => filename:join(Dir, "vec"),
                               bm25_backend => memory}},
               Extra).

seed_plain(Config, Extra) ->
    Src = ?config(src, Config),
    Docs = ?config(docs, Config),
    {ok, Db} = barrel_dbs:ensure(Src, open_opts_plain(Config, Extra)),
    [{ok, _} = R || R <- barrel:put_docs(Db, Docs)],
    Batch = [{Id, T, #{}, barrel_ctx_test_corpus:vec(T)}
             || #{<<"id">> := Id} = D <- Docs,
                T <- [barrel_ctx_test_corpus:text(D)]],
    {ok, _} = barrel:vector_add_batch(Db, Batch),
    {Src, non_trivial(answers(Db))}.

record_opts(Config, Extra) ->
    Dir = ?config(dir, Config),
    maps:merge(#{embedding => #{fields => [<<"moduledoc">>, <<"path">>],
                                mode => sync},
                 docdb => #{data_dir => filename:join(Dir, "docdb")},
                 vectordb => #{dimension => barrel_ctx_test_corpus:dim(),
                               db_path => filename:join(Dir, "vec")}},
               Extra).

seed_record(Config, Extra) ->
    Src = ?config(src, Config),
    Docs = ?config(docs, Config),
    {ok, Db} = barrel_dbs:ensure(Src, record_opts(Config, Extra)),
    [{ok, _} = R || R <- barrel:put_docs(Db, Docs)],
    {Src, non_trivial(answers(Db))}.

%% Every query answers something, so equality checks are not vacuous.
non_trivial(#{find := F, vector := V, bm25 := B} = A) ->
    [?assertNotEqual({App, []}, {App, Ids}) || {App, Ids} <- F],
    [?assertEqual({Q, 10}, {Q, length(Rows)}) || {Q, Rows} <- V],
    [?assertNotEqual({Q, []}, {Q, Rows}) || {Q, Rows} <- B],
    A.

%% Query answers normalized for comparison: rows as {Id, Score} with
%% ties ordered by id (a rebuilt index may break ties differently).
answers(Db) ->
    #{find => lists:sort([{App, find_ids(Db, App)} || App <- ?APPS]),
      vector => [{Q, top(Db, "vector_top_k", Q)} || Q <- ?VQ],
      bm25 => [{Q, top(Db, "bm25_top_k", Q)} || Q <- ?BQ]}.

find_ids(Db, App) ->
    {ok, Rows, _} = barrel:find(Db, #{where => [{path, [<<"app">>], App}]}),
    lists:sort([maps:get(<<"id">>, R) || R <- Rows]).

top(Db, Fn, Q) ->
    Bql = io_lib:format("SELECT * FROM ~s('~s', k => 10) AS h", [Fn, Q]),
    {ok, Rows, _} = barrel:query(Db, lists:flatten(Bql)),
    lists:sort(fun({I1, S1}, {I2, S2}) -> {-S1, I1} =< {-S2, I2} end,
               [{maps:get(<<"id">>, R), maps:get(<<"_score">>, R)}
                || R <- Rows]).

manifest_parts(Dir) ->
    {ok, Bin} = file:read_file(filename:join(Dir, "manifest.json")),
    maps:get(<<"parts">>, json:decode(Bin)).

copy_tree(Src, Dst) ->
    case filelib:is_dir(Src) of
        true ->
            ok = filelib:ensure_path(Dst),
            {ok, Names} = file:list_dir(Src),
            lists:foreach(fun(N) ->
                              ok = copy_tree(filename:join(Src, N),
                                             filename:join(Dst, N))
                          end, Names);
        false ->
            {ok, _} = file:copy(Src, Dst),
            ok
    end.
