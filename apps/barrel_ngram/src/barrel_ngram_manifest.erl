%%%-------------------------------------------------------------------
%%% @doc Per-corpus segment manifest.
%%%
%%% The manifest is the single source of truth for which segments are
%%% live and how far the index has consumed the changes feed. It lists
%%% the live `segment-<gen>.ngseg' files, the next generation number, and
%%% the applied HLC watermark (12-byte encoded, or the `first' sentinel).
%%%
%%% It is written atomically (temp file + `file:rename'), so the rename
%%% is the commit point: a crash between writing a new segment and
%%% committing the manifest leaves an orphan segment that the reader
%%% never sees and that {@link cleanup_orphans/2} removes at startup. The
%%% tail since the committed watermark is replayed from the feed.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_manifest).

-export([empty/0, load/1, save/2]).
-export([watermark/1, set_watermark/2, list_segments/1, next_gen/1,
         add_segment/2, remove_segments/2]).
-export([cleanup_orphans/2]).

-define(FILENAME, "manifest").
-define(VERSION, 1).

-type segment() :: #{gen := non_neg_integer(), file := binary(),
                     doc_count := non_neg_integer()}.
-type manifest() :: #{version := pos_integer(),
                      watermark := binary() | first,
                      next_gen := non_neg_integer(),
                      segments := [segment()]}.
-export_type([manifest/0, segment/0]).

%% @doc An empty manifest for a fresh corpus.
-spec empty() -> manifest().
empty() ->
    #{version => ?VERSION, watermark => first, next_gen => 0, segments => []}.

%% @doc Load the manifest from a corpus directory. A missing manifest is
%% an empty corpus.
-spec load(file:name_all()) -> {ok, manifest()} | {error, term()}.
load(Dir) ->
    Path = filename:join(Dir, ?FILENAME),
    case file:read_file(Path) of
        {ok, Bin} ->
            try binary_to_term(Bin) of
                #{version := _, segments := _} = M -> {ok, M};
                _ -> {error, corrupt_manifest}
            catch
                _:_ -> {error, corrupt_manifest}
            end;
        {error, enoent} ->
            {ok, empty()};
        {error, _} = Err ->
            Err
    end.

%% @doc Write the manifest atomically (temp + rename).
-spec save(file:name_all(), manifest()) -> ok | {error, term()}.
save(Dir, M) ->
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            Path = filename:join(Dir, ?FILENAME),
            Tmp = iolist_to_binary([to_binary(Path), <<".tmp">>]),
            case file:write_file(Tmp, term_to_binary(M)) of
                ok -> file:rename(Tmp, Path);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc The applied HLC watermark (12-byte encoded, or `first').
-spec watermark(manifest()) -> binary() | first.
watermark(M) -> maps:get(watermark, M, first).

%% @doc Set the applied watermark.
-spec set_watermark(manifest(), binary() | first) -> manifest().
set_watermark(M, Wm) -> M#{watermark => Wm}.

%% @doc The live segments, ascending by generation.
-spec list_segments(manifest()) -> [segment()].
list_segments(M) ->
    lists:sort(fun(#{gen := A}, #{gen := B}) -> A =< B end,
               maps:get(segments, M, [])).

%% @doc The next generation number to assign.
-spec next_gen(manifest()) -> non_neg_integer().
next_gen(M) -> maps:get(next_gen, M, 0).

%% @doc Append a segment and advance the generation counter.
-spec add_segment(manifest(), segment()) -> manifest().
add_segment(M, #{gen := Gen} = Seg) ->
    M#{segments => maps:get(segments, M, []) ++ [Seg],
       next_gen => max(maps:get(next_gen, M, 0), Gen + 1)}.

%% @doc Drop the segments whose file name is in `Files' (merge inputs).
-spec remove_segments(manifest(), [binary()]) -> manifest().
remove_segments(M, Files) ->
    Keep = [S || S <- maps:get(segments, M, []),
                 not lists:member(maps:get(file, S), Files)],
    M#{segments => Keep}.

%% @doc Delete `segment-*.ngseg' files (and stray `*.tmp') in Dir that
%% the manifest does not list. Call at startup to clear orphans left by a
%% crash before the manifest commit.
-spec cleanup_orphans(file:name_all(), manifest()) -> ok.
cleanup_orphans(Dir, M) ->
    Live = [maps:get(file, S) || S <- maps:get(segments, M, [])],
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:foreach(
                fun(F) ->
                    FB = list_to_binary(F),
                    IsOrphanSeg = is_segment_file(F) andalso not lists:member(FB, Live),
                    IsTmp = filename:extension(F) =:= ".tmp",
                    case IsOrphanSeg orelse IsTmp of
                        true -> _ = file:delete(filename:join(Dir, F)), ok;
                        false -> ok
                    end
                end, Files),
            ok;
        {error, _} ->
            ok
    end.

%%====================================================================
%% Internal
%%====================================================================

is_segment_file(F) ->
    filename:extension(F) =:= ".ngseg".

to_binary(P) when is_binary(P) -> P;
to_binary(P) when is_list(P) -> list_to_binary(P).
