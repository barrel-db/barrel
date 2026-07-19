%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the segment manifest.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_manifest_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_manifest).

manifest_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun missing_is_empty/1,
      fun save_load_roundtrip/1,
      fun atomic_replace/1,
      fun add_segment_advances_gen/1,
      fun orphan_cleanup/1
     ]}.

setup() ->
    Dir = filename:join(["/tmp",
                         "barrel_ngram_man_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    Dir.

cleanup(Dir) ->
    os:cmd("rm -rf " ++ Dir),
    ok.

missing_is_empty(Dir) ->
    fun() ->
        {ok, M} = ?M:load(Dir),
        ?assertEqual(first, ?M:watermark(M)),
        ?assertEqual(0, ?M:next_gen(M)),
        ?assertEqual([], ?M:list_segments(M))
    end.

save_load_roundtrip(Dir) ->
    fun() ->
        M0 = ?M:empty(),
        M1 = ?M:add_segment(M0, #{gen => 0, file => <<"segment-000000.ngseg">>,
                                  doc_count => 3}),
        M2 = ?M:set_watermark(M1, <<1,2,3,4,5,6,7,8,9,10,11,12>>),
        ok = ?M:save(Dir, M2),
        {ok, Loaded} = ?M:load(Dir),
        ?assertEqual(<<1,2,3,4,5,6,7,8,9,10,11,12>>, ?M:watermark(Loaded)),
        ?assertEqual(1, ?M:next_gen(Loaded)),
        ?assertEqual([#{gen => 0, file => <<"segment-000000.ngseg">>,
                        doc_count => 3}], ?M:list_segments(Loaded))
    end.

atomic_replace(Dir) ->
    fun() ->
        ok = ?M:save(Dir, ?M:set_watermark(?M:empty(), <<0:96>>)),
        M2 = ?M:set_watermark(?M:empty(), <<1:96>>),
        ok = ?M:save(Dir, M2),
        {ok, Loaded} = ?M:load(Dir),
        ?assertEqual(<<1:96>>, ?M:watermark(Loaded)),
        %% no temp file left behind
        ?assertEqual(false, filelib:is_file(filename:join(Dir, "manifest.tmp")))
    end.

add_segment_advances_gen(_Dir) ->
    fun() ->
        M0 = ?M:empty(),
        M1 = ?M:add_segment(M0, #{gen => 0, file => <<"a">>, doc_count => 1}),
        M2 = ?M:add_segment(M1, #{gen => 1, file => <<"b">>, doc_count => 2}),
        ?assertEqual(2, ?M:next_gen(M2)),
        ?assertEqual([0, 1], [maps:get(gen, S) || S <- ?M:list_segments(M2)])
    end.

orphan_cleanup(Dir) ->
    fun() ->
        %% one live segment, one orphan segment, one stray temp file
        ok = file:write_file(filename:join(Dir, "segment-000000.ngseg"), <<"live">>),
        ok = file:write_file(filename:join(Dir, "segment-000001.ngseg"), <<"orphan">>),
        ok = file:write_file(filename:join(Dir, "manifest.tmp"), <<"stray">>),
        M = ?M:add_segment(?M:empty(),
                           #{gen => 0, file => <<"segment-000000.ngseg">>,
                             doc_count => 1}),
        ok = ?M:cleanup_orphans(Dir, M),
        ?assert(filelib:is_file(filename:join(Dir, "segment-000000.ngseg"))),
        ?assertNot(filelib:is_file(filename:join(Dir, "segment-000001.ngseg"))),
        ?assertNot(filelib:is_file(filename:join(Dir, "manifest.tmp")))
    end.
