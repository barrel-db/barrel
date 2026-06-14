%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_vectordb_bm25_disk_file
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_disk_file_tests).

-include_lib("eunit/include/eunit.hrl").

%% Test fixtures
-define(TEST_DIR, "/tmp/bm25_disk_file_tests").

setup() ->
    %% Clean up any existing test directory
    os:cmd("rm -rf " ++ ?TEST_DIR),
    ok = filelib:ensure_dir(?TEST_DIR ++ "/dummy"),
    ?TEST_DIR.

cleanup(_Dir) ->
    os:cmd("rm -rf " ++ ?TEST_DIR),
    ok.

%%====================================================================
%% Varint Encoding Tests
%%====================================================================

varint_encode_decode_test() ->
    %% Test round-trip for single values
    lists:foreach(fun(N) ->
        Encoded = barrel_vectordb_bm25_disk_file:varint_encode(N),
        {Decoded, <<>>} = barrel_vectordb_bm25_disk_file:varint_decode(Encoded),
        ?assertEqual(N, Decoded)
    end, [0, 1, 127, 128, 255, 256, 16383, 16384, 2097151, 2097152]).

varint_encode_large_values_test() ->
    %% Test values > 127 (require multiple bytes)
    ?assertEqual(<<1>>, barrel_vectordb_bm25_disk_file:varint_encode(1)),
    ?assertEqual(<<127>>, barrel_vectordb_bm25_disk_file:varint_encode(127)),
    %% 128 = 0x80 encoded as: (128 & 0x7F) | 0x80, 128 >> 7 = 0x80, 0x01
    ?assertEqual(<<128, 1>>, barrel_vectordb_bm25_disk_file:varint_encode(128)),
    %% 300 = 0x12C encoded as: (300 & 0x7F) | 0x80 = 0xAC, 300 >> 7 = 2
    ?assertEqual(<<172, 2>>, barrel_vectordb_bm25_disk_file:varint_encode(300)),
    %% 16384 = 0x4000
    {16384, <<>>} = barrel_vectordb_bm25_disk_file:varint_decode(
        barrel_vectordb_bm25_disk_file:varint_encode(16384)).

varint_decode_list_test() ->
    %% Test decoding sequence of varints
    Values = [1, 100, 1000, 10000],
    Encoded = barrel_vectordb_bm25_disk_file:varint_encode_list(Values),
    {Decoded, <<>>} = barrel_vectordb_bm25_disk_file:varint_decode_list(Encoded, 4),
    ?assertEqual(Values, Decoded).

varint_edge_cases_test() ->
    %% 0 encodes to single byte
    ?assertEqual(<<0>>, barrel_vectordb_bm25_disk_file:varint_encode(0)),
    {0, <<>>} = barrel_vectordb_bm25_disk_file:varint_decode(<<0>>),

    %% Max reasonable value
    MaxVal = 16#FFFFFFFF,  %% 32-bit max
    Encoded = barrel_vectordb_bm25_disk_file:varint_encode(MaxVal),
    {MaxVal, <<>>} = barrel_vectordb_bm25_disk_file:varint_decode(Encoded),

    %% Incomplete varint
    {error, incomplete} = barrel_vectordb_bm25_disk_file:varint_decode(<<>>),
    {error, incomplete} = barrel_vectordb_bm25_disk_file:varint_decode(<<128>>).

varint_with_remainder_test() ->
    %% Decode varint with remaining data
    Bin = <<127, 42, 99>>,
    {127, <<42, 99>>} = barrel_vectordb_bm25_disk_file:varint_decode(Bin).

%%====================================================================
%% Header Tests
%%====================================================================

header_write_read_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"write and read header", fun() ->
             Path = filename:join(?TEST_DIR, "header_test"),
             Config = #{k1 => 1.5, b => 0.8, block_size => 256},
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, Config),

             Header = barrel_vectordb_bm25_disk_file:read_header(File),
             ?assertEqual(1.5, maps:get(k1, Header)),
             ?assertEqual(0.8, maps:get(b, Header)),
             ?assertEqual(256, maps:get(block_size, Header)),
             ?assertEqual(0, maps:get(term_count, Header)),
             ?assertEqual(0, maps:get(doc_count, Header)),

             ok = barrel_vectordb_bm25_disk_file:close(File)
         end}]
     end}.

header_update_stats_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"update stats in header", fun() ->
             Path = filename:join(?TEST_DIR, "stats_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             {ok, File2} = barrel_vectordb_bm25_disk_file:update_stats(File, #{
                 term_count => 1000,
                 doc_count => 500,
                 total_tokens => 50000,
                 avgdl => 100.0
             }),

             Header = barrel_vectordb_bm25_disk_file:read_header(File2),
             ?assertEqual(1000, maps:get(term_count, Header)),
             ?assertEqual(500, maps:get(doc_count, Header)),
             ?assertEqual(50000, maps:get(total_tokens, Header)),
             ?assertMatch(100.0, maps:get(avgdl, Header)),

             ok = barrel_vectordb_bm25_disk_file:close(File2)
         end}]
     end}.

header_persistence_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"header persists across open/close", fun() ->
             Path = filename:join(?TEST_DIR, "persist_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{k1 => 2.0}),
             {ok, File2} = barrel_vectordb_bm25_disk_file:update_stats(File, #{
                 doc_count => 42
             }),
             ok = barrel_vectordb_bm25_disk_file:close(File2),

             %% Reopen and verify
             {ok, File3} = barrel_vectordb_bm25_disk_file:open(Path),
             Header = barrel_vectordb_bm25_disk_file:read_header(File3),
             ?assertEqual(2.0, maps:get(k1, Header)),
             ?assertEqual(42, maps:get(doc_count, Header)),
             ok = barrel_vectordb_bm25_disk_file:close(File3)
         end}]
     end}.

header_magic_validation_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"reject invalid magic", fun() ->
             Path = filename:join(?TEST_DIR, "bad_magic"),
             ok = filelib:ensure_dir(filename:join(Path, "dummy")),
             MetaPath = filename:join(Path, "bm25.meta"),
             PostingsPath = filename:join(Path, "bm25.postings"),
             BlockmaxPath = filename:join(Path, "bm25.blockmax"),

             %% Create files with invalid magic
             ok = file:write_file(MetaPath, <<"INVALID!", 0:4000/unit:8>>),
             ok = file:write_file(PostingsPath, <<>>),
             ok = file:write_file(BlockmaxPath, <<>>),

             ?assertMatch({error, invalid_magic}, barrel_vectordb_bm25_disk_file:open(Path))
         end}]
     end}.

%%====================================================================
%% Block I/O Tests
%%====================================================================

block_write_read_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"write and read block", fun() ->
             Path = filename:join(?TEST_DIR, "block_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             TestData = <<"Hello, World! This is test data for block I/O.">>,
             ok = barrel_vectordb_bm25_disk_file:write_block(File, 4096, TestData),

             {ok, ReadData} = barrel_vectordb_bm25_disk_file:read_block(File, 4096),
             %% Read data is padded to 4KB
             ?assertEqual(4096, byte_size(ReadData)),
             %% But starts with our data
             ?assertEqual(TestData, binary:part(ReadData, 0, byte_size(TestData))),

             ok = barrel_vectordb_bm25_disk_file:close(File)
         end}]
     end}.

block_padding_test() ->
    %% Test padding function
    ?assertEqual(4096, byte_size(barrel_vectordb_bm25_disk_file:pad_to_sector(<<1, 2, 3>>))),
    %% Empty binary pads to 0 (no allocation for empty data)
    ?assertEqual(0, byte_size(barrel_vectordb_bm25_disk_file:pad_to_sector(<<>>))),
    %% Exact 4KB doesn't need padding
    FourKB = binary:copy(<<0>>, 4096),
    ?assertEqual(4096, byte_size(barrel_vectordb_bm25_disk_file:pad_to_sector(FourKB))),
    %% 4097 bytes rounds up to 8KB
    ?assertEqual(8192, byte_size(barrel_vectordb_bm25_disk_file:pad_to_sector(<<0:4097/unit:8>>))).

block_batch_read_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"batch read multiple blocks", fun() ->
             Path = filename:join(?TEST_DIR, "batch_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             %% Write multiple blocks
             ok = barrel_vectordb_bm25_disk_file:write_block(File, 4096, <<"Block1">>),
             ok = barrel_vectordb_bm25_disk_file:write_block(File, 8192, <<"Block2">>),
             ok = barrel_vectordb_bm25_disk_file:write_block(File, 12288, <<"Block3">>),

             %% Batch read
             {ok, Blocks} = barrel_vectordb_bm25_disk_file:read_blocks(File, [
                 {4096, 4096},
                 {8192, 4096},
                 {12288, 4096}
             ]),

             ?assertEqual(3, length(Blocks)),
             [B1, B2, B3] = Blocks,
             ?assertEqual(<<"Block1">>, binary:part(B1, 0, 6)),
             ?assertEqual(<<"Block2">>, binary:part(B2, 0, 6)),
             ?assertEqual(<<"Block3">>, binary:part(B3, 0, 6)),

             ok = barrel_vectordb_bm25_disk_file:close(File)
         end}]
     end}.

%%====================================================================
%% Posting Tests
%%====================================================================

posting_encode_decode_test() ->
    %% Empty postings
    EmptyEncoded = barrel_vectordb_bm25_disk_file:encode_posting_block([]),
    {ok, []} = barrel_vectordb_bm25_disk_file:decode_posting_block(EmptyEncoded),

    %% Single posting
    Single = [{10, 3}],
    SingleEncoded = barrel_vectordb_bm25_disk_file:encode_posting_block(Single),
    {ok, Single} = barrel_vectordb_bm25_disk_file:decode_posting_block(SingleEncoded),

    %% Multiple postings (delta encoded)
    Multiple = [{5, 1}, {10, 2}, {20, 3}, {100, 1}],
    MultipleEncoded = barrel_vectordb_bm25_disk_file:encode_posting_block(Multiple),
    {ok, Multiple} = barrel_vectordb_bm25_disk_file:decode_posting_block(MultipleEncoded).

posting_delta_encoding_test() ->
    %% Verify delta encoding produces smaller output for sequential IDs
    Sequential = [{I, 1} || I <- lists:seq(1, 100)],
    SeqEncoded = barrel_vectordb_bm25_disk_file:encode_posting_block(Sequential),

    %% Each posting should be: delta (1) + TF (1) = 2 varints
    %% With count header (1 varint), should be small
    ?assert(byte_size(SeqEncoded) < 300),  %% Much smaller than 100 * (4 + 4) bytes

    %% Round-trip
    {ok, Sequential} = barrel_vectordb_bm25_disk_file:decode_posting_block(SeqEncoded).

posting_write_read_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"write and read postings", fun() ->
             Path = filename:join(?TEST_DIR, "posting_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             Postings = [{1, 5}, {10, 3}, {100, 1}, {1000, 2}],
             {ok, Size} = barrel_vectordb_bm25_disk_file:write_postings(File, 4096, Postings),
             ?assert(Size > 0),

             {ok, ReadPostings} = barrel_vectordb_bm25_disk_file:read_postings(File, 4096, Size),
             ?assertEqual(Postings, ReadPostings),

             ok = barrel_vectordb_bm25_disk_file:close(File)
         end}]
     end}.

%%====================================================================
%% Block-Max Index Tests
%%====================================================================

blockmax_encode_decode_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"write and read block-max index", fun() ->
             Path = filename:join(?TEST_DIR, "blockmax_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             %% Create test block-max index
             TermBlocks = #{
                 1 => [
                     #{max_impact => 5.5, doc_start => 0, doc_end => 127,
                       offset => 4096, size => 4096},
                     #{max_impact => 3.2, doc_start => 128, doc_end => 200,
                       offset => 8192, size => 4096}
                 ],
                 2 => [
                     #{max_impact => 10.0, doc_start => 0, doc_end => 50,
                       offset => 12288, size => 4096}
                 ]
             },

             {ok, File2} = barrel_vectordb_bm25_disk_file:write_blockmax_index(File, TermBlocks),
             ok = barrel_vectordb_bm25_disk_file:sync(File2),

             %% Read back
             {ok, ReadIndex} = barrel_vectordb_bm25_disk_file:read_blockmax_index(File2),

             %% Verify term 1
             Term1Blocks = maps:get(1, ReadIndex),
             ?assertEqual(2, length(Term1Blocks)),
             [Block1_1, Block1_2] = Term1Blocks,
             %% Use tolerance for float comparison (32-bit float precision)
             ?assert(abs(5.5 - maps:get(max_impact, Block1_1)) < 0.001),
             ?assertEqual(0, maps:get(doc_start, Block1_1)),
             ?assertEqual(127, maps:get(doc_end, Block1_1)),
             ?assert(abs(3.2 - maps:get(max_impact, Block1_2)) < 0.001),

             %% Verify term 2
             Term2Blocks = maps:get(2, ReadIndex),
             ?assertEqual(1, length(Term2Blocks)),
             [Block2_1] = Term2Blocks,
             ?assert(abs(10.0 - maps:get(max_impact, Block2_1)) < 0.001),

             ok = barrel_vectordb_bm25_disk_file:close(File2)
         end}]
     end}.

blockmax_lookup_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"lookup blocks for specific term", fun() ->
             Path = filename:join(?TEST_DIR, "lookup_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             TermBlocks = #{
                 42 => [#{max_impact => 1.0, doc_start => 0, doc_end => 127,
                          offset => 4096, size => 4096}]
             },
             {ok, File2} = barrel_vectordb_bm25_disk_file:write_blockmax_index(File, TermBlocks),

             %% Found term
             {ok, Blocks} = barrel_vectordb_bm25_disk_file:lookup_blocks_for_term(File2, 42),
             ?assertEqual(1, length(Blocks)),

             %% Not found term
             ?assertEqual({error, not_found},
                          barrel_vectordb_bm25_disk_file:lookup_blocks_for_term(File2, 999)),

             ok = barrel_vectordb_bm25_disk_file:close(File2)
         end}]
     end}.

blockmax_empty_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"empty block-max index", fun() ->
             Path = filename:join(?TEST_DIR, "empty_blockmax"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             {ok, File2} = barrel_vectordb_bm25_disk_file:write_blockmax_index(File, #{}),
             {ok, ReadIndex} = barrel_vectordb_bm25_disk_file:read_blockmax_index(File2),
             ?assertEqual(#{}, ReadIndex),

             ok = barrel_vectordb_bm25_disk_file:close(File2)
         end}]
     end}.

%%====================================================================
%% File Open/Close Tests
%%====================================================================

file_create_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"create new file", fun() ->
             Path = filename:join(?TEST_DIR, "create_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),
             ?assertEqual(list_to_binary(Path),
                          barrel_vectordb_bm25_disk_file:get_path(File)),
             ok = barrel_vectordb_bm25_disk_file:close(File),

             %% Verify files exist
             ?assert(filelib:is_file(filename:join(Path, "bm25.meta"))),
             ?assert(filelib:is_file(filename:join(Path, "bm25.postings"))),
             ?assert(filelib:is_file(filename:join(Path, "bm25.blockmax")))
         end}]
     end}.

file_reopen_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"reopen after write", fun() ->
             Path = filename:join(?TEST_DIR, "reopen_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             Postings = [{1, 1}, {2, 2}, {3, 3}],
             {ok, _} = barrel_vectordb_bm25_disk_file:write_postings(File, 4096, Postings),
             ok = barrel_vectordb_bm25_disk_file:close(File),

             %% Reopen
             {ok, File2} = barrel_vectordb_bm25_disk_file:open(Path),
             {ok, ReadPostings} = barrel_vectordb_bm25_disk_file:read_postings(File2, 4096, 4096),
             ?assertEqual(Postings, ReadPostings),

             ok = barrel_vectordb_bm25_disk_file:close(File2)
         end}]
     end}.

%%====================================================================
%% mmap Tests
%%====================================================================

mmap_availability_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Dir) ->
         [{"check mmap availability", fun() ->
             Path = filename:join(?TEST_DIR, "mmap_test"),
             {ok, File} = barrel_vectordb_bm25_disk_file:create(Path, #{}),

             %% Write some data to blockmax
             TermBlocks = #{1 => [#{max_impact => 1.0, doc_start => 0,
                                    doc_end => 127, offset => 4096, size => 4096}]},
             {ok, File2} = barrel_vectordb_bm25_disk_file:write_blockmax_index(File, TermBlocks),
             ok = barrel_vectordb_bm25_disk_file:close(File2),

             %% Reopen - mmap should be available if iommap works
             {ok, File3} = barrel_vectordb_bm25_disk_file:open(Path),
             %% has_mmap depends on iommap availability
             _HasMmap = barrel_vectordb_bm25_disk_file:has_mmap(File3),

             ok = barrel_vectordb_bm25_disk_file:close(File3)
         end}]
     end}.
