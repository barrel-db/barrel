%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_diskann_file module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_diskann_file_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

diskann_file_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"create creates files", fun test_create/0},
        {"open reads existing", fun test_open/0},
        {"header roundtrip", fun test_header_roundtrip/0},
        {"node write and read", fun test_node_roundtrip/0},
        {"sector alignment", fun test_sector_alignment/0},
        {"batch read", fun test_batch_read/0},
        {"vector write and read", fun test_vector_roundtrip/0},
        {"pq codes write and read", fun test_pq_codes_roundtrip/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    %% Create temp directory
    TempDir = temp_path(),
    ok = filelib:ensure_dir(filename:join(TempDir, "dummy")),
    TempDir.

cleanup(TempDir) ->
    %% Clean up temp files
    Files = filelib:wildcard(filename:join(TempDir, "*")),
    lists:foreach(fun(F) -> file:delete(F) end, Files),
    file:del_dir(TempDir),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_create() ->
    Path = temp_path(),
    try
        Config = #{dimension => 128, r => 64},
        {ok, F} = barrel_vectordb_diskann_file:create(Path, Config),
        ok = barrel_vectordb_diskann_file:close(F),

        %% Verify files exist
        ?assert(filelib:is_file(filename:join(Path, "diskann.graph"))),
        ?assert(filelib:is_file(filename:join(Path, "diskann.vectors"))),
        ?assert(filelib:is_file(filename:join(Path, "diskann.pq"))),
        ?assert(filelib:is_file(filename:join(Path, "diskann.meta")))
    after
        cleanup_path(Path)
    end.

test_open() ->
    Path = temp_path(),
    try
        Config = #{dimension => 256, r => 32},
        {ok, F1} = barrel_vectordb_diskann_file:create(Path, Config),
        ok = barrel_vectordb_diskann_file:close(F1),

        %% Reopen
        {ok, F2} = barrel_vectordb_diskann_file:open(Path),
        Header = barrel_vectordb_diskann_file:read_header(F2),
        ok = barrel_vectordb_diskann_file:close(F2),

        ?assertEqual(256, maps:get(dimension, Header)),
        ?assertEqual(32, maps:get(r, Header))
    after
        cleanup_path(Path)
    end.

test_header_roundtrip() ->
    Path = temp_path(),
    try
        {ok, F1} = barrel_vectordb_diskann_file:create(Path, #{dimension => 128, r => 64}),

        %% Update header
        NewHeader = #{
            dimension => 128,
            r => 64,
            node_count => 100,
            entry_point => <<"medoid_1">>,
            distance_fn => cosine
        },
        {ok, F2} = barrel_vectordb_diskann_file:write_header(F1, NewHeader),
        ok = barrel_vectordb_diskann_file:close(F2),

        %% Reopen and verify
        {ok, F3} = barrel_vectordb_diskann_file:open(Path),
        Header = barrel_vectordb_diskann_file:read_header(F3),
        ok = barrel_vectordb_diskann_file:close(F3),

        ?assertEqual(100, maps:get(node_count, Header)),
        ?assertEqual(<<"medoid_1">>, maps:get(entry_point, Header))
    after
        cleanup_path(Path)
    end.

test_node_roundtrip() ->
    Path = temp_path(),
    try
        {ok, F1} = barrel_vectordb_diskann_file:create(Path, #{dimension => 128, r => 8}),

        %% Write a node
        Node = #{neighbors => [4096, 8192, 12288]},  %% Offsets
        {ok, Offset, F2} = barrel_vectordb_diskann_file:write_node(F1, <<"test_node_1">>, Node),

        %% Read it back
        {ok, {Id, ReadNode}} = barrel_vectordb_diskann_file:read_node(F2, Offset),
        ok = barrel_vectordb_diskann_file:close(F2),

        ?assertEqual(<<"test_node_1">>, Id),
        ?assertEqual([4096, 8192, 12288], maps:get(neighbors, ReadNode))
    after
        cleanup_path(Path)
    end.

test_sector_alignment() ->
    Path = temp_path(),
    try
        {ok, F1} = barrel_vectordb_diskann_file:create(Path, #{dimension => 128, r => 64}),

        %% Write multiple nodes
        {ok, Offset1, F2} = barrel_vectordb_diskann_file:write_node(F1, <<"n1">>, #{neighbors => []}),
        {ok, Offset2, F3} = barrel_vectordb_diskann_file:write_node(F2, <<"n2">>, #{neighbors => [Offset1]}),
        {ok, Offset3, F4} = barrel_vectordb_diskann_file:write_node(F3, <<"n3">>, #{neighbors => [Offset1, Offset2]}),
        ok = barrel_vectordb_diskann_file:close(F4),

        %% All offsets should be 4KB aligned
        ?assertEqual(0, Offset1 rem 4096),
        ?assertEqual(0, Offset2 rem 4096),
        ?assertEqual(0, Offset3 rem 4096),

        %% Offsets should be sequential sectors (after header sector)
        ?assertEqual(4096, Offset1),
        ?assertEqual(8192, Offset2),
        ?assertEqual(12288, Offset3)
    after
        cleanup_path(Path)
    end.

test_batch_read() ->
    Path = temp_path(),
    try
        {ok, F1} = barrel_vectordb_diskann_file:create(Path, #{dimension => 128, r => 8}),

        %% Write 10 nodes
        {Offsets, FN} = lists:foldl(
            fun(I, {AccOffsets, AccF}) ->
                Id = list_to_binary("node_" ++ integer_to_list(I)),
                Node = #{neighbors => AccOffsets},  %% Link to previous
                {ok, Offset, NewF} = barrel_vectordb_diskann_file:write_node(AccF, Id, Node),
                {[Offset | AccOffsets], NewF}
            end,
            {[], F1},
            lists:seq(1, 10)
        ),

        %% Batch read all nodes
        Results = barrel_vectordb_diskann_file:read_nodes_batch(FN, lists:reverse(Offsets)),
        ok = barrel_vectordb_diskann_file:close(FN),

        ?assertEqual(10, length(Results)),
        ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, Results)),

        %% Verify IDs
        Ids = [Id || {ok, {Id, _}} <- Results],
        ExpectedIds = [list_to_binary("node_" ++ integer_to_list(I)) || I <- lists:seq(1, 10)],
        ?assertEqual(ExpectedIds, Ids)
    after
        cleanup_path(Path)
    end.

test_vector_roundtrip() ->
    Path = temp_path(),
    try
        {ok, F1} = barrel_vectordb_diskann_file:create(Path, #{dimension => 8, r => 4}),

        %% Write vectors
        Vec1 = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        Vec2 = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        ok = barrel_vectordb_diskann_file:write_vector(F1, 0, Vec1),
        ok = barrel_vectordb_diskann_file:write_vector(F1, 1, Vec2),

        %% Read back
        {ok, ReadVec1} = barrel_vectordb_diskann_file:read_vector(F1, 0),
        {ok, ReadVec2} = barrel_vectordb_diskann_file:read_vector(F1, 1),
        ok = barrel_vectordb_diskann_file:close(F1),

        %% Compare (float comparison with tolerance)
        ?assert(vectors_equal(Vec1, ReadVec1, 0.0001)),
        ?assert(vectors_equal(Vec2, ReadVec2, 0.0001))
    after
        cleanup_path(Path)
    end.

test_pq_codes_roundtrip() ->
    Path = temp_path(),
    try
        {ok, F1} = barrel_vectordb_diskann_file:create(Path, #{dimension => 128, r => 64}),

        %% Write PQ codes (8 bytes each)
        Codes = [<<I:8, (I+1):8, (I+2):8, (I+3):8, (I+4):8, (I+5):8, (I+6):8, (I+7):8>>
                 || I <- lists:seq(0, 9)],
        ok = barrel_vectordb_diskann_file:write_pq_codes(F1, 0, Codes),

        %% Read back
        {ok, ReadCodes} = barrel_vectordb_diskann_file:read_pq_codes(F1, {0, 10}),
        ok = barrel_vectordb_diskann_file:close(F1),

        ?assertEqual(Codes, ReadCodes)
    after
        cleanup_path(Path)
    end.

%%====================================================================
%% Helpers
%%====================================================================

temp_path() ->
    TmpDir = filename:basedir(user_cache, "barrel_vectordb_test"),
    Unique = integer_to_list(erlang:unique_integer([positive])),
    Path = filename:join([TmpDir, "diskann_test_" ++ Unique]),
    ok = filelib:ensure_dir(filename:join(Path, "dummy")),
    Path.

cleanup_path(Path) ->
    Files = filelib:wildcard(filename:join(Path, "*")),
    lists:foreach(fun(F) -> file:delete(F) end, Files),
    file:del_dir(Path),
    ok.

vectors_equal(V1, V2, Tolerance) ->
    lists:all(
        fun({A, B}) -> abs(A - B) < Tolerance end,
        lists:zip(V1, V2)
    ).
