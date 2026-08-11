%%%-------------------------------------------------------------------
%%% @doc Per-corpus shard: changes-feed subscriber, segment writer, and
%%% compaction coordinator.
%%%
%%% The shard keeps a corpus in sync with its barrel_docdb database. It
%%% subscribes to the changes feed in push mode and applies each batch to
%%% an in-memory buffer keyed by document id (an update replaces, a delete
%%% becomes a tombstone). The buffer holds each live key's corpus TEXT, not
%%% pre-computed grams: gram selection (both phase-1 dense and phase-2
%%% positional) happens once, at freeze time, from that text, rather than
%%% once per change -- a document updated several times before a freeze
%%% only ever has its final version's grams computed once, and the buffer
%%% is not searched directly (every buffered key is always a candidate),
%%% so nothing needs its grams before freeze. When the buffer passes a
%%% threshold it freezes to a new immutable segment and commits the
%%% manifest, advancing the persisted watermark.
%%%
%%% Segments only ever accumulate on their own, so when the live count
%%% crosses a threshold the shard compacts: an offloaded worker merges the
%%% segments, collapsing each key to its newest version by HLC and
%%% dropping superseded and deleted ordinals, and the shard swaps the
%%% manifest to the merged segment. `compact/1' does this synchronously.
%%%
%%% Recovery is the watermark: on start the shard loads the manifest and
%%% resubscribes from its watermark, so only the feed tail is replayed
%%% (idempotently). Correctness of updates/deletes never depends on
%%% compaction, the query confirm pass re-fetches the current document and
%%% drops a stale or deleted candidate.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_shard).
-behaviour(gen_server).

-export([start_link/2]).
-export([refresh/1, compact/1, get_manifest/1, buffer_keys/1, snapshot/1,
         get_config/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_FREEZE_THRESHOLD, 1000).
-define(DEFAULT_COMPACT_THRESHOLD, 16).
-define(REFRESH_BATCH, 1000).
-define(SUBSCRIBE_RETRY_MS, 1000).

%% Buffer content: live corpus text (grams are derived at freeze time,
%% from both phases -- see the moduledoc), or a tombstone.
-type content() :: {live, binary()} | deleted.

-record(state, {
    ref :: term(),
    corpus :: term(),
    shard_index :: non_neg_integer(),
    shards :: pos_integer(),
    config :: map(),
    db :: binary(),
    dir :: binary(),
    manifest :: barrel_ngram_manifest:manifest(),
    watermark :: binary() | first,   %% applied HLC (12-byte) or first
    buffer = #{} :: #{binary() => {binary(), content()}},
    freeze_threshold :: pos_integer(),
    compact_threshold :: pos_integer() | infinity,
    merge_worker :: undefined | {pid(), reference()},
    stream_pid :: pid() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(barrel_ngram_shards:ref(), map()) -> {ok, pid()} | {error, term()}.
start_link(Ref, Config) ->
    gen_server:start_link(via(Ref), ?MODULE, {Ref, Config}, []).

%% @doc Synchronously drain the feed up to now and freeze the buffer.
%% The deterministic catch-up point for tests and ops.
-spec refresh(term()) -> {ok, map()} | {error, term()}.
refresh(Corpus) ->
    gen_server:call(via(Corpus), refresh, infinity).

%% @doc Synchronously compact every live segment into one, evicting
%% superseded and deleted ordinals. Returns `{error, busy}' if a
%% background compaction is in flight.
-spec compact(term()) -> {ok, map()} | {error, term()}.
compact(Corpus) ->
    gen_server:call(via(Corpus), compact, infinity).

%% @doc The live segments as `{Gen, Path}', ascending by generation.
-spec get_manifest(term()) -> {ok, [{non_neg_integer(), binary()}]}.
get_manifest(Corpus) ->
    gen_server:call(via(Corpus), get_manifest, infinity).

%% @doc Ids currently buffered (not yet frozen into a segment).
-spec buffer_keys(term()) -> [binary()].
buffer_keys(Corpus) ->
    gen_server:call(via(Corpus), buffer_keys, infinity).

%% @doc The live segments and an immutable copy of the buffer in one
%% atomic read, so a query never straddles a freeze (which could move a
%% doc out of the buffer into a segment the query did not see). The
%% buffer snapshot carries each key's change HLC and whether it is live
%% or a tombstone -- not just the key -- because the query layer's
%% buffer/segment precedence rule needs to tell a live buffered update
%% from a buffered delete (see barrel_ngram_query's confirm pass).
-spec snapshot(term()) ->
    {ok, [{non_neg_integer(), binary()}], #{binary() => {binary(), live | deleted}}}.
snapshot(Corpus) ->
    gen_server:call(via(Corpus), snapshot, infinity).

%% @doc The corpus config held by the shard.
-spec get_config(term()) -> {ok, map()}.
get_config(Corpus) ->
    gen_server:call(via(Corpus), get_config, infinity).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({Ref, Config}) ->
    process_flag(trap_exit, true),
    Corpus = maps:get(corpus, Config),
    ShardIndex = maps:get(shard_index, Config, 0),
    Shards = maps:get(shards, Config, 1),
    Db = maps:get(db, Config),
    Dir = shard_dir(Corpus, ShardIndex, Shards, Config),
    case open_manifest(Dir, Config) of
        {ok, Manifest0} ->
            State0 = #state{
                ref = Ref,
                corpus = Corpus,
                shard_index = ShardIndex,
                shards = Shards,
                config = Config,
                db = Db,
                dir = Dir,
                manifest = Manifest0,
                watermark = barrel_ngram_manifest:watermark(Manifest0),
                freeze_threshold = maps:get(freeze_threshold, Config, ?DEFAULT_FREEZE_THRESHOLD),
                compact_threshold = maps:get(compact_threshold, Config, ?DEFAULT_COMPACT_THRESHOLD),
                merge_worker = undefined
            },
            {ok, subscribe(State0)};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private Load the manifest, eagerly validate every segment it lists
%% (fail closed on any pre-v4 segment rather than surfacing it lazily on
%% first query), and reconcile the corpus's persisted config against this
%% open's request. Runs before `cleanup_orphans/2' so a rejected open
%% never deletes anything.
open_manifest(Dir, Config) ->
    case barrel_ngram_manifest:load(Dir) of
        {ok, Manifest0} ->
            case validate_segments(Dir, Manifest0) of
                ok ->
                    Requested = #{
                        phase2_selector_opts => maps:get(phase2_selector_opts, Config, #{}),
                        fields => maps:get(fields, Config, all)
                    },
                    case barrel_ngram_manifest:reconcile_config(Manifest0, Requested) of
                        {ok, Manifest1} ->
                            ok = barrel_ngram_manifest:cleanup_orphans(Dir, Manifest1),
                            {ok, Manifest1};
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

validate_segments(Dir, Manifest) ->
    validate_segment_files(Dir, barrel_ngram_manifest:list_segments(Manifest)).

validate_segment_files(_Dir, []) ->
    ok;
validate_segment_files(Dir, [#{file := File} | Rest]) ->
    Path = filename:join(Dir, File),
    case barrel_ngram_segment:open(Path) of
        {ok, H} ->
            barrel_ngram_segment:close(H),
            validate_segment_files(Dir, Rest);
        {error, {unsupported_segment_version, Got, Expected}} ->
            {error, {unsupported_segment_version, Path, Got, Expected}};
        {error, _} = Err ->
            Err
    end.

handle_call(refresh, _From, State) ->
    State1 = maybe_compact(do_freeze(drain(State))),
    Reply = {ok, #{
        segments => length(barrel_ngram_manifest:list_segments(State1#state.manifest)),
        watermark => State1#state.watermark
    }},
    {reply, Reply, State1};

handle_call(compact, _From, #state{merge_worker = undefined} = State0) ->
    State1 = do_freeze(State0),
    case barrel_ngram_manifest:list_segments(State1#state.manifest) of
        [] ->
            {reply, {ok, #{segments => 0}}, State1};
        Segs ->
            InputFiles = [maps:get(file, S) || S <- Segs],
            InputPaths = [filename:join(State1#state.dir, F) || F <- InputFiles],
            case barrel_ngram_merge:merge(InputPaths, true) of
                {ok, TempPath, DocCount, _Wm} ->
                    State2 = apply_merge_result(TempPath, DocCount, InputFiles, State1),
                    N = length(barrel_ngram_manifest:list_segments(State2#state.manifest)),
                    {reply, {ok, #{segments => N, doc_count => DocCount}}, State2};
                {error, Reason} ->
                    {reply, {error, Reason}, State1}
            end
    end;
handle_call(compact, _From, State) ->
    {reply, {error, busy}, State};

handle_call(get_manifest, _From, #state{dir = Dir, manifest = M} = State) ->
    Segs = [{maps:get(gen, S), filename:join(Dir, maps:get(file, S))}
            || S <- barrel_ngram_manifest:list_segments(M)],
    {reply, {ok, Segs}, State};

handle_call(buffer_keys, _From, #state{buffer = Buffer} = State) ->
    {reply, maps:keys(Buffer), State};

handle_call(snapshot, _From, #state{dir = Dir, manifest = M, buffer = Buffer} = State) ->
    Segs = [{maps:get(gen, S), filename:join(Dir, maps:get(file, S))}
            || S <- barrel_ngram_manifest:list_segments(M)],
    BufferSnapshot = maps:map(fun(_K, {Hlc, Content}) -> {Hlc, content_kind(Content)} end,
                              Buffer),
    {reply, {ok, Segs, BufferSnapshot}, State};

handle_call(get_config, _From, #state{config = Config} = State) ->
    {reply, {ok, Config}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({changes, ReqId, Changes}, #state{stream_pid = Pid} = State) ->
    State1 = apply_changes(Changes, State),
    _ = case Pid of
        undefined -> ok;
        _ -> barrel_changes_stream:ack(Pid, ReqId)
    end,
    {noreply, maybe_compact(maybe_freeze(State1))};

handle_info({merge_done, Result, InputFiles},
            #state{merge_worker = {_Pid, MRef}} = State) ->
    erlang:demonitor(MRef, [flush]),
    State1 = case Result of
        {ok, TempPath, DocCount, _Wm} ->
            apply_merge_result(TempPath, DocCount, InputFiles, State);
        {error, Reason} ->
            logger:warning("barrel_ngram compaction failed for ~p: ~p",
                           [State#state.corpus, Reason]),
            State
    end,
    {noreply, State1#state{merge_worker = undefined}};

handle_info({'DOWN', MRef, process, _Pid, Reason},
            #state{merge_worker = {_WPid, MRef}} = State) ->
    logger:warning("barrel_ngram compaction worker died for ~p: ~p",
                   [State#state.corpus, Reason]),
    {noreply, State#state{merge_worker = undefined}};

handle_info({'EXIT', Pid, _Reason}, #state{stream_pid = Pid} = State) ->
    {noreply, subscribe(State#state{stream_pid = undefined})};

handle_info(subscribe_retry, #state{stream_pid = undefined} = State) ->
    {noreply, subscribe(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{stream_pid = Pid}) ->
    _ = case Pid of
        undefined -> ok;
        _ -> (try barrel_changes_stream:stop(Pid) catch _:_ -> ok end)
    end,
    ok.

%%====================================================================
%% Subscription
%%====================================================================

subscribe(#state{db = Db, watermark = Wm} = State) ->
    Opts = #{mode => push, owner => self(), include_docs => true},
    case barrel_docdb:subscribe_changes(Db, since(Wm), Opts) of
        {ok, Pid} ->
            State#state{stream_pid = Pid};
        {error, _} ->
            erlang:send_after(?SUBSCRIBE_RETRY_MS, self(), subscribe_retry),
            State#state{stream_pid = undefined}
    end.

%%====================================================================
%% Applying changes
%%====================================================================

%% Phase-1 (dense, non-positional) is the always-on selector; there is no
%% longer a corpus-wide selector choice (see the app moduledoc).
-define(PHASE1_SELECTOR, barrel_ngram_selector_dense).

%% @private Apply a batch to the buffer, advancing the watermark. Changes
%% at or below the current watermark are skipped (idempotent replay).
apply_changes(Changes, #state{buffer = Buffer, watermark = Wm, config = Cfg,
                              shard_index = I, shards = N} = State) ->
    {Buffer1, Wm1} = lists:foldl(
        fun(Change, Acc) -> apply_change(Change, Acc, Cfg, I, N) end,
        {Buffer, Wm}, Changes),
    State#state{buffer = Buffer1, watermark = Wm1}.

%% Advance the watermark for every change above it (owned or not, so the
%% shard never reprocesses the feed), but only buffer a change this shard
%% owns by rendezvous.
apply_change(Change, {Buffer, Wm}, Cfg, I, N) ->
    EncHlc = barrel_hlc:encode(maps:get(hlc, Change)),
    case Wm =/= first andalso EncHlc =< Wm of
        true ->
            {Buffer, Wm};
        false ->
            Id = maps:get(id, Change),
            Buffer1 = case barrel_ngram_shards:shard_for(Id, N) =:= I of
                false ->
                    Buffer;
                true ->
                    case maps:get(deleted, Change, false) of
                        true ->
                            Buffer#{Id => {EncHlc, deleted}};
                        false ->
                            case maps:get(doc, Change, undefined) of
                                Doc when is_map(Doc) ->
                                    Text = barrel_ngram_corpus:doc_text(Doc, Cfg),
                                    Buffer#{Id => {EncHlc, {live, Text}}};
                                _ ->
                                    Buffer
                            end
                    end
            end,
            {Buffer1, EncHlc}
    end.

%%====================================================================
%% Refresh (synchronous drain + freeze)
%%====================================================================

drain(#state{db = Db, watermark = Wm} = State) ->
    case barrel_docdb:get_changes(Db, since(Wm), #{include_docs => true,
                                                   limit => ?REFRESH_BATCH}) of
        {ok, [], _Last} ->
            State;
        {ok, Changes, _Last} ->
            #state{watermark = Wm1} = State1 = apply_changes(Changes, State),
            %% continue only on a full batch that made progress; a full batch
            %% that does not advance the watermark would otherwise loop forever
            case length(Changes) >= ?REFRESH_BATCH andalso Wm1 =/= Wm of
                true -> drain(State1);
                false -> State1
            end;
        {error, _} ->
            State
    end.

%%====================================================================
%% Freeze
%%====================================================================

maybe_freeze(#state{buffer = Buffer, freeze_threshold = T} = State)
        when map_size(Buffer) >= T ->
    do_freeze(State);
maybe_freeze(State) ->
    State.

do_freeze(#state{buffer = Buffer} = State) when map_size(Buffer) =:= 0 ->
    State;
do_freeze(#state{buffer = Buffer, manifest = M, dir = Dir,
                 watermark = Wm, config = Config} = State) ->
    Keys = maps:keys(Buffer),
    PositionalOpts = maps:get(phase2_selector_opts, Config, #{}),
    {Entries, Postings, PositionalPostings} = build_segment(Buffer, Keys, PositionalOpts),
    Gen = barrel_ngram_manifest:next_gen(M),
    File = segment_file(Gen),
    Path = filename:join(Dir, File),
    WmBin = wm_bin(Wm),
    Spec = #{doc_count => length(Keys), watermark => WmBin,
             postings => Postings, positional_postings => PositionalPostings,
             entries => Entries,
             codec => maps:get(postings, Config, varint)},
    case barrel_ngram_segment:write(Path, Spec) of
        ok ->
            M1 = barrel_ngram_manifest:add_segment(
                   M, #{gen => Gen, file => File, doc_count => length(Keys)}),
            M2 = barrel_ngram_manifest:set_watermark(M1, WmBin),
            case barrel_ngram_manifest:save(Dir, M2) of
                ok ->
                    State#state{manifest = M2, buffer = #{}};
                {error, SReason} ->
                    %% manifest not committed: drop the orphan segment and
                    %% keep the buffer + old watermark (the tail replays)
                    logger:error("barrel_ngram freeze manifest save failed for ~p: ~p",
                                 [State#state.corpus, SReason]),
                    _ = file:delete(Path),
                    State
            end;
        {error, Reason} ->
            logger:error("barrel_ngram freeze failed for ~p: ~p",
                         [State#state.corpus, Reason]),
            State
    end.

%% @private Build ordinal-ordered entries and both phases' postings from
%% the keyed buffer. Ordinal i is Keys!!i (the freeze order); a tombstone
%% contributes an entry but no grams. Both phase-1 (dense) grams and
%% phase-2 (positional) grams are selected from the same buffered text,
%% here at freeze time -- see the moduledoc for why.
build_segment(Buffer, Keys, PositionalOpts) ->
    KeyToOrd = maps:from_list(
        lists:zip(Keys, lists:seq(0, length(Keys) - 1))),
    Entries = [begin
                   {Hlc, Content} = maps:get(K, Buffer),
                   #{key => K, hlc => Hlc, deleted => Content =:= deleted}
               end || K <- Keys],
    {GramMap, PosMap} = maps:fold(
        fun(Key, {_Hlc, {live, Text}}, {GM, PM}) ->
                Ord = maps:get(Key, KeyToOrd),
                Grams = barrel_ngram_selector:select_grams(?PHASE1_SELECTOR, #{}, Text),
                GM1 = lists:foldl(
                    fun(G, A) -> maps:update_with(G, fun(L) -> [Ord | L] end, [Ord], A) end,
                    GM, Grams),
                PosGrams = barrel_ngram_selector_sparse:select_grams_positional(
                             Text, PositionalOpts),
                PM1 = lists:foldl(
                    fun({G, Off}, A) ->
                        maps:update_with(G, fun(L) -> [{Ord, Off} | L] end, [{Ord, Off}], A)
                    end, PM, PosGrams),
                {GM1, PM1};
           (_Key, {_Hlc, deleted}, Acc) ->
                Acc
        end, {#{}, #{}}, Buffer),
    Postings = [{G, lists:usort(Os)} || {G, Os} <- maps:to_list(GramMap)],
    PositionalPostings = [{G, group_offsets(Pairs)} || {G, Pairs} <- maps:to_list(PosMap)],
    {Entries, Postings, PositionalPostings}.

%% @private [{Ordinal, Offset}] (one entry per sampled position, possibly
%% several per ordinal for a repeated gram) -> [{Ordinal, [Offset]}]
%% (barrel_ngram_postings_positional's entry shape).
group_offsets(Pairs) ->
    Grouped = lists:foldl(
        fun({Ord, Off}, Acc) ->
            maps:update_with(Ord, fun(L) -> [Off | L] end, [Off], Acc)
        end, #{}, Pairs),
    [{Ord, lists:usort(Offs)} || {Ord, Offs} <- maps:to_list(Grouped)].

%%====================================================================
%% Compaction
%%====================================================================

%% @private Trigger a background compaction when the live segment count
%% crosses the threshold and none is running.
maybe_compact(#state{merge_worker = undefined, compact_threshold = T,
                     manifest = M, dir = Dir} = State) when is_integer(T) ->
    Segs = barrel_ngram_manifest:list_segments(M),
    case length(Segs) >= T of
        true ->
            InputFiles = [maps:get(file, S) || S <- Segs],
            InputPaths = [filename:join(Dir, F) || F <- InputFiles],
            Self = self(),
            {Pid, MRef} = spawn_monitor(
                fun() ->
                    Result = barrel_ngram_merge:merge(InputPaths, true),
                    Self ! {merge_done, Result, InputFiles}
                end),
            State#state{merge_worker = {Pid, MRef}};
        false ->
            State
    end;
maybe_compact(State) ->
    State.

%% @private Swap in a merged segment: rename it to the next gen, drop the
%% inputs from the manifest, commit, then delete the input files. The
%% manifest save is the atomic commit; a crash before it leaves the merged
%% file as an orphan and keeps the inputs.
apply_merge_result(TempPath, DocCount, InputFiles,
                   #state{dir = Dir, manifest = M} = State) ->
    Gen = barrel_ngram_manifest:next_gen(M),
    FinalFile = segment_file(Gen),
    FinalPath = filename:join(Dir, FinalFile),
    case file:rename(TempPath, FinalPath) of
        ok ->
            M1 = barrel_ngram_manifest:remove_segments(M, InputFiles),
            M2 = barrel_ngram_manifest:add_segment(
                   M1, #{gen => Gen, file => FinalFile, doc_count => DocCount}),
            case barrel_ngram_manifest:save(Dir, M2) of
                ok ->
                    [file:delete(filename:join(Dir, F))
                     || F <- InputFiles, F =/= FinalFile],
                    State#state{manifest = M2};
                {error, SReason} ->
                    logger:error("barrel_ngram manifest save failed for ~p: ~p",
                                 [State#state.corpus, SReason]),
                    _ = file:delete(FinalPath),
                    State
            end;
        {error, RReason} ->
            logger:error("barrel_ngram merge rename failed for ~p: ~p",
                         [State#state.corpus, RReason]),
            _ = file:delete(TempPath),
            State
    end.

%%====================================================================
%% Helpers
%%====================================================================

via(Ref) ->
    {via, barrel_ngram_registry, {shard, Ref}}.

content_kind({live, _Text}) -> live;
content_kind(deleted) -> deleted.

segment_file(Gen) ->
    iolist_to_binary(io_lib:format("segment-~6..0b.ngseg", [Gen])).

since(first) -> first;
since(Bin) when is_binary(Bin) -> barrel_hlc:decode(Bin).

wm_bin(first) -> <<0:96>>;
wm_bin(Bin) when is_binary(Bin) -> Bin.

%% Single shard keeps the corpus dir unchanged; multi-shard nests a
%% shard-<I> subdir under it.
shard_dir(Corpus, _I, 1, Config) ->
    iolist_to_binary(filename:join([data_dir(Config), corpus_name(Corpus)]));
shard_dir(Corpus, I, _N, Config) ->
    Sub = io_lib:format("shard-~6..0b", [I]),
    iolist_to_binary(filename:join([data_dir(Config), corpus_name(Corpus), Sub])).

data_dir(Config) ->
    maps:get(data_dir, Config,
             application:get_env(barrel_ngram, data_dir, "data/barrel_ngram")).

corpus_name(Corpus) when is_binary(Corpus) -> Corpus;
corpus_name(Corpus) when is_atom(Corpus) -> atom_to_binary(Corpus, utf8);
corpus_name(Corpus) -> iolist_to_binary(io_lib:format("~p", [Corpus])).
