%%%-------------------------------------------------------------------
%%% @doc Per-corpus segment manifest.
%%%
%%% The manifest is the single source of truth for which segments are
%%% live and how far the index has consumed the changes feed. It lists
%%% the live `segment-<gen>.ngseg' files, the next generation number, and
%%% the applied HLC watermark (12-byte encoded, or the `first' sentinel).
%%% It also carries the corpus's index-critical config
%%% (`phase2_selector_opts', `fields'): reopening with a different value
%%% would silently desync the query planner from what was actually
%%% indexed, so {@link reconcile_config/2} rejects the mismatch instead.
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
-export([config/1, reconcile_config/2]).

-define(FILENAME, "manifest").
-define(VERSION, 2).

-type segment() :: #{gen := non_neg_integer(), file := binary(),
                     doc_count := non_neg_integer()}.
-type config() :: #{phase2_selector_opts := map(), fields := all | [binary()]}.
-type manifest() :: #{version := pos_integer(),
                      watermark := binary() | first,
                      next_gen := non_neg_integer(),
                      segments := [segment()],
                      config := config() | undefined}.
-export_type([manifest/0, segment/0, config/0]).

%% @doc An empty manifest for a fresh corpus. `config' is `undefined'
%% until {@link reconcile_config/2} persists the first requested config.
-spec empty() -> manifest().
empty() ->
    #{version => ?VERSION, watermark => first, next_gen => 0, segments => [],
      config => undefined}.

%% @doc Load the manifest from a corpus directory. A missing manifest is
%% an empty corpus. A manifest written by a different (older or newer)
%% version is rejected -- there is no migration path, a version bump
%% means reindex.
-spec load(file:name_all()) -> {ok, manifest()} | {error, term()}.
load(Dir) ->
    Path = filename:join(Dir, ?FILENAME),
    case file:read_file(Path) of
        {ok, Bin} ->
            try binary_to_term(Bin) of
                #{version := V, segments := _} = M when V =:= ?VERSION ->
                    {ok, M};
                #{version := V, segments := _} ->
                    {error, {unsupported_manifest_version, V, ?VERSION}};
                _ ->
                    {error, corrupt_manifest}
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

%% @doc The persisted corpus config, or `undefined' for a corpus never
%% reconciled (a fresh manifest before its first
%% {@link reconcile_config/2}).
-spec config(manifest()) -> config() | undefined.
config(M) -> maps:get(config, M, undefined).

%% @doc Reconcile `Requested' (the caller's, already-defaulted, `open/2'
%% config) against the manifest's persisted config. A never-yet-persisted
%% manifest (`config =:= undefined', a fresh corpus) adopts `Requested' as
%% what gets persisted at the next {@link save/2}. A manifest that already
%% has a persisted config must match `Requested' exactly in every
%% index-critical field, or the corpus was indexed under different
%% assumptions than this open is making -- rejected rather than silently
%% reindexed or silently queried under the wrong assumption.
-spec reconcile_config(manifest(), config()) ->
    {ok, manifest()} | {error, {config_mismatch, atom(), term(), term()}}.
reconcile_config(M, Requested) ->
    case config(M) of
        undefined ->
            {ok, M#{config => Requested}};
        Persisted ->
            case first_mismatch(Persisted, Requested) of
                none -> {ok, M};
                {Field, Got, Want} -> {error, {config_mismatch, Field, Got, Want}}
            end
    end.

%% @private First index-critical field that differs between the persisted
%% and requested config, checked in a fixed order so the error is
%% deterministic.
first_mismatch(#{phase2_selector_opts := Got}, #{phase2_selector_opts := Want})
        when Got =/= Want ->
    {phase2_selector_opts, Got, Want};
first_mismatch(#{fields := Got}, #{fields := Want}) when Got =/= Want ->
    {fields, Got, Want};
first_mismatch(_Persisted, _Requested) ->
    none.

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
