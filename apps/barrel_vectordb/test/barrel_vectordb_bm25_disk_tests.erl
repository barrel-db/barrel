%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_vectordb_bm25_disk
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_disk_tests).

-include_lib("eunit/include/eunit.hrl").

%% Test fixtures
-define(TEST_DIR, "/tmp/bm25_disk_tests").

setup() ->
    %% Clean up any existing test directory
    os:cmd("rm -rf " ++ ?TEST_DIR),
    ok = filelib:ensure_dir(?TEST_DIR ++ "/dummy"),
    ?TEST_DIR.

cleanup(_Dir) ->
    os:cmd("rm -rf " ++ ?TEST_DIR),
    ok.

%%====================================================================
%% Initialization Tests
%%====================================================================

new_default_config_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"create with defaults", fun() ->
             Path = filename:join(?TEST_DIR, "new_default"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             Stats = barrel_vectordb_bm25_disk:stats(Index),
             ?assertEqual(0, maps:get(total_docs, Stats)),
             ?assertEqual(0, maps:get(vocab_size, Stats)),
             ?assertEqual(1.2, maps:get(k1, maps:get(config, Stats))),
             ?assertEqual(0.75, maps:get(b, maps:get(config, Stats))),
             ok = barrel_vectordb_bm25_disk:close(Index)
         end}]
     end}.

new_custom_config_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"create with custom config", fun() ->
             Path = filename:join(?TEST_DIR, "new_custom"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{
                 base_path => Path,
                 k1 => 1.5,
                 b => 0.8,
                 block_size => 64
             }),
             Stats = barrel_vectordb_bm25_disk:stats(Index),
             ?assertEqual(1.5, maps:get(k1, maps:get(config, Stats))),
             ?assertEqual(0.8, maps:get(b, maps:get(config, Stats))),
             ?assertEqual(64, maps:get(block_size, maps:get(config, Stats))),
             ok = barrel_vectordb_bm25_disk:close(Index)
         end}]
     end}.

new_invalid_config_test() ->
    ?assertEqual({error, base_path_required}, barrel_vectordb_bm25_disk:new(#{})).

%%====================================================================
%% Add Document Tests
%%====================================================================

add_single_doc_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"add single document", fun() ->
             Path = filename:join(?TEST_DIR, "add_single"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             Stats = barrel_vectordb_bm25_disk:stats(Index2),
             ?assertEqual(1, maps:get(total_docs, Stats)),
             ?assertEqual(2, maps:get(vocab_size, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

add_multiple_docs_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"add multiple documents", fun() ->
             Path = filename:join(?TEST_DIR, "add_multiple"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             {ok, Index3} = barrel_vectordb_bm25_disk:add(Index2, <<"doc2">>, <<"hello there">>),
             {ok, Index4} = barrel_vectordb_bm25_disk:add(Index3, <<"doc3">>, <<"world peace">>),
             Stats = barrel_vectordb_bm25_disk:stats(Index4),
             ?assertEqual(3, maps:get(total_docs, Stats)),
             ?assertEqual(3, maps:get(hot_docs, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index4)
         end}]
     end}.

add_duplicate_id_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"update existing document", fun() ->
             Path = filename:join(?TEST_DIR, "add_dup"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             {ok, Index3} = barrel_vectordb_bm25_disk:add(Index2, <<"doc1">>, <<"goodbye world">>),
             Stats = barrel_vectordb_bm25_disk:stats(Index3),
             %% Should still be 1 doc (updated, not added)
             ?assertEqual(1, maps:get(total_docs, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index3)
         end}]
     end}.

add_empty_text_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"add empty document", fun() ->
             Path = filename:join(?TEST_DIR, "add_empty"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"">>),
             Stats = barrel_vectordb_bm25_disk:stats(Index2),
             ?assertEqual(1, maps:get(total_docs, Stats)),
             ?assertEqual(0, maps:get(vocab_size, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

add_unicode_text_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"add unicode document", fun() ->
             Path = filename:join(?TEST_DIR, "add_unicode"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             %% Note: tokenization only keeps alphanumeric
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>,
                 <<"hello world café résumé">>),
             Stats = barrel_vectordb_bm25_disk:stats(Index2),
             ?assertEqual(1, maps:get(total_docs, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

%%====================================================================
%% Build Tests
%%====================================================================

build_empty_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"build with no docs", fun() ->
             Path = filename:join(?TEST_DIR, "build_empty"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:build(Index, []),
             Stats = barrel_vectordb_bm25_disk:stats(Index2),
             ?assertEqual(0, maps:get(total_docs, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

build_single_doc_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"build with one doc", fun() ->
             Path = filename:join(?TEST_DIR, "build_single"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:build(Index, [
                 {<<"doc1">>, <<"hello world">>}
             ]),
             Stats = barrel_vectordb_bm25_disk:stats(Index2),
             ?assertEqual(1, maps:get(total_docs, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

build_many_docs_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"build with many docs", fun() ->
             Path = filename:join(?TEST_DIR, "build_many"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             Docs = [{list_to_binary("doc" ++ integer_to_list(I)),
                      list_to_binary("hello world document " ++ integer_to_list(I))}
                     || I <- lists:seq(1, 100)],
             {ok, Index2} = barrel_vectordb_bm25_disk:build(Index, Docs),
             Stats = barrel_vectordb_bm25_disk:stats(Index2),
             ?assertEqual(100, maps:get(total_docs, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

%%====================================================================
%% Search Tests
%%====================================================================

search_empty_index_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"search empty index", fun() ->
             Path = filename:join(?TEST_DIR, "search_empty"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             Results = barrel_vectordb_bm25_disk:search(Index, <<"hello">>, 10),
             ?assertEqual([], Results),
             ok = barrel_vectordb_bm25_disk:close(Index)
         end}]
     end}.

search_single_term_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"search single term", fun() ->
             Path = filename:join(?TEST_DIR, "search_single"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             {ok, Index3} = barrel_vectordb_bm25_disk:add(Index2, <<"doc2">>, <<"goodbye world">>),
             Results = barrel_vectordb_bm25_disk:search(Index3, <<"hello">>, 10),
             ?assertEqual(1, length(Results)),
             [{DocId, _Score}] = Results,
             ?assertEqual(<<"doc1">>, DocId),
             ok = barrel_vectordb_bm25_disk:close(Index3)
         end}]
     end}.

search_multiple_terms_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"search multiple terms", fun() ->
             Path = filename:join(?TEST_DIR, "search_multi"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             {ok, Index3} = barrel_vectordb_bm25_disk:add(Index2, <<"doc2">>, <<"hello there">>),
             {ok, Index4} = barrel_vectordb_bm25_disk:add(Index3, <<"doc3">>, <<"world peace">>),
             Results = barrel_vectordb_bm25_disk:search(Index4, <<"hello world">>, 10),
             %% doc1 should rank highest (has both terms)
             ?assert(length(Results) >= 1),
             [{TopDocId, _} | _] = Results,
             ?assertEqual(<<"doc1">>, TopDocId),
             ok = barrel_vectordb_bm25_disk:close(Index4)
         end}]
     end}.

search_no_match_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"search no match", fun() ->
             Path = filename:join(?TEST_DIR, "search_nomatch"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             Results = barrel_vectordb_bm25_disk:search(Index2, <<"xyz123">>, 10),
             ?assertEqual([], Results),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

search_ranking_order_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"search ranking order", fun() ->
             Path = filename:join(?TEST_DIR, "search_rank"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             %% doc1: erlang appears 3 times
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>,
                 <<"erlang erlang erlang programming">>),
             %% doc2: erlang appears 1 time
             {ok, Index3} = barrel_vectordb_bm25_disk:add(Index2, <<"doc2">>,
                 <<"erlang is a language">>),
             %% doc3: no erlang
             {ok, Index4} = barrel_vectordb_bm25_disk:add(Index3, <<"doc3">>,
                 <<"python is also a language">>),
             Results = barrel_vectordb_bm25_disk:search(Index4, <<"erlang">>, 10),
             %% Should return docs sorted by score
             ?assertEqual(2, length(Results)),
             [{Doc1, Score1}, {Doc2, Score2}] = Results,
             ?assertEqual(<<"doc1">>, Doc1),
             ?assertEqual(<<"doc2">>, Doc2),
             ?assert(Score1 > Score2),
             ok = barrel_vectordb_bm25_disk:close(Index4)
         end}]
     end}.

%%====================================================================
%% Sparse Vector Tests
%%====================================================================

get_vector_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"get sparse vector", fun() ->
             Path = filename:join(?TEST_DIR, "get_vector"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             {ok, Vector} = barrel_vectordb_bm25_disk:get_vector(Index2, <<"doc1">>),
             ?assert(is_map(Vector)),
             ?assert(maps:is_key(<<"hello">>, Vector)),
             ?assert(maps:is_key(<<"world">>, Vector)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

get_vector_not_found_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"get vector not found", fun() ->
             Path = filename:join(?TEST_DIR, "get_vector_404"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             ?assertEqual({error, not_found}, barrel_vectordb_bm25_disk:get_vector(Index, <<"doc1">>)),
             ok = barrel_vectordb_bm25_disk:close(Index)
         end}]
     end}.

encode_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"encode text to vector", fun() ->
             Path = filename:join(?TEST_DIR, "encode"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             Vector = barrel_vectordb_bm25_disk:encode(Index2, <<"hello there">>),
             ?assert(is_map(Vector)),
             %% "hello" is known term, should have weight
             ?assert(maps:is_key(<<"hello">>, Vector)),
             %% "there" is unknown term, should not appear
             ?assertNot(maps:is_key(<<"there">>, Vector)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

%%====================================================================
%% Stats Tests
%%====================================================================

stats_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"get stats", fun() ->
             Path = filename:join(?TEST_DIR, "stats"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>,
                 <<"hello hello world">>),
             Stats = barrel_vectordb_bm25_disk:stats(Index2),
             ?assertEqual(1, maps:get(total_docs, Stats)),
             ?assertEqual(3, maps:get(total_tokens, Stats)),
             ?assertEqual(2, maps:get(vocab_size, Stats)),
             ?assertMatch(AvgDL when AvgDL > 0, maps:get(avg_doc_length, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

info_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"get info", fun() ->
             Path = filename:join(?TEST_DIR, "info"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             Info = barrel_vectordb_bm25_disk:info(Index),
             ?assert(maps:is_key(base_path, Info)),
             ?assert(maps:is_key(hot_enabled, Info)),
             ?assert(maps:is_key(hot_max_size, Info)),
             ok = barrel_vectordb_bm25_disk:close(Index)
         end}]
     end}.

%%====================================================================
%% Persistence Tests
%%====================================================================

open_close_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"open and close", fun() ->
             Path = filename:join(?TEST_DIR, "open_close"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             ok = barrel_vectordb_bm25_disk:close(Index2),

             %% Reopen
             {ok, Index3} = barrel_vectordb_bm25_disk:open(Path),
             %% Note: hot layer data is not persisted, so doc count might be 0
             %% until we implement full persistence
             ok = barrel_vectordb_bm25_disk:close(Index3)
         end}]
     end}.

%%====================================================================
%% Compaction Tests
%%====================================================================

compact_empty_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"compact empty hot layer", fun() ->
             Path = filename:join(?TEST_DIR, "compact_empty"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:compact(Index),
             Stats = barrel_vectordb_bm25_disk:stats(Index2),
             ?assertEqual(0, maps:get(total_docs, Stats)),
             ok = barrel_vectordb_bm25_disk:close(Index2)
         end}]
     end}.

compact_with_data_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"compact hot layer with data", fun() ->
             Path = filename:join(?TEST_DIR, "compact_data"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             {ok, Index3} = barrel_vectordb_bm25_disk:add(Index2, <<"doc2">>, <<"hello there">>),

             Stats1 = barrel_vectordb_bm25_disk:stats(Index3),
             ?assertEqual(2, maps:get(hot_docs, Stats1)),

             {ok, Index4} = barrel_vectordb_bm25_disk:compact(Index3),

             Stats2 = barrel_vectordb_bm25_disk:stats(Index4),
             %% After compaction, hot layer should be empty
             ?assertEqual(0, maps:get(hot_docs, Stats2)),
             %% But total docs should be preserved
             ?assertEqual(2, maps:get(total_docs, Stats2)),

             ok = barrel_vectordb_bm25_disk:close(Index4)
         end}]
     end}.

search_after_compact_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"search works after compaction", fun() ->
             Path = filename:join(?TEST_DIR, "search_compact"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             {ok, Index3} = barrel_vectordb_bm25_disk:add(Index2, <<"doc2">>, <<"hello there">>),

             %% Compact
             {ok, Index4} = barrel_vectordb_bm25_disk:compact(Index3),

             %% Search should still work (disk layer)
             Results = barrel_vectordb_bm25_disk:search(Index4, <<"hello">>, 10),
             ?assertEqual(2, length(Results)),

             ok = barrel_vectordb_bm25_disk:close(Index4)
         end}]
     end}.

%%====================================================================
%% Remove Tests
%%====================================================================

remove_doc_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"remove document from hot layer", fun() ->
             Path = filename:join(?TEST_DIR, "remove"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             {ok, Index2} = barrel_vectordb_bm25_disk:add(Index, <<"doc1">>, <<"hello world">>),
             {ok, Index3} = barrel_vectordb_bm25_disk:add(Index2, <<"doc2">>, <<"hello there">>),

             Stats1 = barrel_vectordb_bm25_disk:stats(Index3),
             ?assertEqual(2, maps:get(total_docs, Stats1)),

             {ok, Index4} = barrel_vectordb_bm25_disk:remove(Index3, <<"doc1">>),

             Stats2 = barrel_vectordb_bm25_disk:stats(Index4),
             ?assertEqual(1, maps:get(total_docs, Stats2)),

             %% Search should not find removed doc
             Results = barrel_vectordb_bm25_disk:search(Index4, <<"world">>, 10),
             ?assertEqual(0, length(Results)),

             ok = barrel_vectordb_bm25_disk:close(Index4)
         end}]
     end}.

remove_not_found_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"remove non-existent document", fun() ->
             Path = filename:join(?TEST_DIR, "remove_404"),
             {ok, Index} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
             ?assertEqual({error, not_found}, barrel_vectordb_bm25_disk:remove(Index, <<"doc1">>)),
             ok = barrel_vectordb_bm25_disk:close(Index)
         end}]
     end}.
