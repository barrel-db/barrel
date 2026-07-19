%%%-------------------------------------------------------------------
%%% @doc Per-corpus shard: the changes-feed subscriber and segment writer.
%%%
%%% The shard keeps a corpus in sync with its barrel_docdb database. It
%%% subscribes to the changes feed in push mode and applies each batch to
%%% an in-memory buffer keyed by document id, so an update or delete
%%% inside the unfrozen window cleanly replaces or removes an entry. When
%%% the buffer passes a threshold it freezes to a new immutable segment
%%% and commits the manifest, advancing the persisted watermark.
%%%
%%% Recovery is the watermark: on start the shard loads the manifest and
%%% resubscribes from its watermark, so only the feed tail is replayed
%%% (idempotently). Durability of the index is the segments + manifest;
%%% the buffer is always reconstructable from the feed.
%%%
%%% Correctness of updates/deletes does not depend on any liveness
%%% structure here: a stale or deleted id that still lives in an older
%%% segment is dropped by the query confirm pass (it re-fetches the
%%% current document and drops `not_found'). Merge and a live-docs bitmap
%%% are a later milestone's growth control.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_shard).
-behaviour(gen_server).

-export([start_link/2]).
-export([refresh/1, get_manifest/1, buffer_keys/1, get_config/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_FREEZE_THRESHOLD, 1000).
-define(REFRESH_BATCH, 1000).
-define(SUBSCRIBE_RETRY_MS, 1000).

-record(state, {
    corpus :: term(),
    config :: map(),
    selector :: module(),
    db :: binary(),
    dir :: binary(),
    manifest :: barrel_ngram_manifest:manifest(),
    watermark :: binary() | first,   %% applied HLC (12-byte) or first
    buffer = #{} :: #{binary() => [barrel_ngram_selector:gram()]},
    freeze_threshold :: pos_integer(),
    stream_pid :: pid() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(term(), map()) -> {ok, pid()} | {error, term()}.
start_link(Corpus, Config) ->
    gen_server:start_link(via(Corpus), ?MODULE, {Corpus, Config}, []).

%% @doc Synchronously drain the feed up to now and freeze the buffer.
%% The deterministic catch-up point for tests and ops.
-spec refresh(term()) -> {ok, map()} | {error, term()}.
refresh(Corpus) ->
    gen_server:call(via(Corpus), refresh, infinity).

%% @doc The live segments as `{Gen, Path}', ascending by generation.
-spec get_manifest(term()) -> {ok, [{non_neg_integer(), binary()}]}.
get_manifest(Corpus) ->
    gen_server:call(via(Corpus), get_manifest, infinity).

%% @doc Ids currently buffered (not yet frozen into a segment).
-spec buffer_keys(term()) -> [binary()].
buffer_keys(Corpus) ->
    gen_server:call(via(Corpus), buffer_keys, infinity).

%% @doc The corpus config held by the shard.
-spec get_config(term()) -> {ok, map()}.
get_config(Corpus) ->
    gen_server:call(via(Corpus), get_config, infinity).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({Corpus, Config}) ->
    process_flag(trap_exit, true),
    Selector = maps:get(selector, Config, barrel_ngram_selector_dense),
    Db = maps:get(db, Config),
    Dir = corpus_dir(Corpus, Config),
    {ok, Manifest0} = barrel_ngram_manifest:load(Dir),
    ok = barrel_ngram_manifest:cleanup_orphans(Dir, Manifest0),
    State0 = #state{
        corpus = Corpus,
        config = Config,
        selector = Selector,
        db = Db,
        dir = Dir,
        manifest = Manifest0,
        watermark = barrel_ngram_manifest:watermark(Manifest0),
        freeze_threshold = maps:get(freeze_threshold, Config, ?DEFAULT_FREEZE_THRESHOLD)
    },
    {ok, subscribe(State0)}.

handle_call(refresh, _From, State) ->
    State1 = drain(State),
    State2 = do_freeze(State1),
    Reply = {ok, #{
        segments => length(barrel_ngram_manifest:list_segments(State2#state.manifest)),
        watermark => State2#state.watermark
    }},
    {reply, Reply, State2};

handle_call(get_manifest, _From, #state{dir = Dir, manifest = M} = State) ->
    Segs = [{maps:get(gen, S), filename:join(Dir, maps:get(file, S))}
            || S <- barrel_ngram_manifest:list_segments(M)],
    {reply, {ok, Segs}, State};

handle_call(buffer_keys, _From, #state{buffer = Buffer} = State) ->
    {reply, maps:keys(Buffer), State};

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
    {noreply, maybe_freeze(State1)};

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

%% @private Apply a batch to the buffer, advancing the watermark. Changes
%% at or below the current watermark are skipped (idempotent replay).
apply_changes(Changes, #state{buffer = Buffer, watermark = Wm,
                              selector = Sel, config = Cfg} = State) ->
    {Buffer1, Wm1} = lists:foldl(
        fun(Change, Acc) -> apply_change(Change, Acc, Sel, Cfg) end,
        {Buffer, Wm}, Changes),
    State#state{buffer = Buffer1, watermark = Wm1}.

apply_change(Change, {Buffer, Wm}, Sel, Cfg) ->
    EncHlc = barrel_hlc:encode(maps:get(hlc, Change)),
    case Wm =/= first andalso EncHlc =< Wm of
        true ->
            {Buffer, Wm};
        false ->
            Id = maps:get(id, Change),
            Buffer1 = case maps:get(deleted, Change, false) of
                true ->
                    maps:remove(Id, Buffer);
                false ->
                    case maps:get(doc, Change, undefined) of
                        Doc when is_map(Doc) ->
                            Text = barrel_ngram_corpus:doc_text(Doc, Cfg),
                            Buffer#{Id => barrel_ngram_selector:select_grams(Sel, Text)};
                        _ ->
                            maps:remove(Id, Buffer)
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
            State1 = apply_changes(Changes, State),
            case length(Changes) < ?REFRESH_BATCH of
                true -> State1;
                false -> drain(State1)
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
                 watermark = Wm} = State) ->
    Keys = maps:keys(Buffer),
    Postings = build_postings(Buffer, Keys),
    Gen = barrel_ngram_manifest:next_gen(M),
    File = iolist_to_binary(io_lib:format("segment-~6..0b.ngseg", [Gen])),
    Path = filename:join(Dir, File),
    WmBin = wm_bin(Wm),
    Spec = #{doc_count => length(Keys), watermark => WmBin,
             postings => Postings, keys => Keys},
    case barrel_ngram_segment:write(Path, Spec) of
        ok ->
            M1 = barrel_ngram_manifest:add_segment(
                   M, #{gen => Gen, file => File, doc_count => length(Keys)}),
            M2 = barrel_ngram_manifest:set_watermark(M1, WmBin),
            ok = barrel_ngram_manifest:save(Dir, M2),
            State#state{manifest = M2, buffer = #{}};
        {error, Reason} ->
            logger:error("barrel_ngram freeze failed for ~p: ~p",
                         [State#state.corpus, Reason]),
            State
    end.

%% @private Group the keyed buffer into {Gram, AscendingOrdinals}. Ordinal
%% i is Keys!!i (the freeze order).
build_postings(Buffer, Keys) ->
    KeyToOrd = maps:from_list(
        lists:zip(Keys, lists:seq(0, length(Keys) - 1))),
    GramMap = maps:fold(
        fun(Key, Grams, Acc) ->
            Ord = maps:get(Key, KeyToOrd),
            lists:foldl(
                fun(G, A) -> maps:update_with(G, fun(L) -> [Ord | L] end, [Ord], A) end,
                Acc, Grams)
        end, #{}, Buffer),
    [{G, lists:usort(Os)} || {G, Os} <- maps:to_list(GramMap)].

%%====================================================================
%% Helpers
%%====================================================================

via(Corpus) ->
    {via, barrel_ngram_registry, {shard, Corpus}}.

since(first) -> first;
since(Bin) when is_binary(Bin) -> barrel_hlc:decode(Bin).

wm_bin(first) -> <<0:96>>;
wm_bin(Bin) when is_binary(Bin) -> Bin.

corpus_dir(Corpus, Config) ->
    DataDir = maps:get(data_dir, Config,
                       application:get_env(barrel_ngram, data_dir, "data/barrel_ngram")),
    iolist_to_binary(filename:join([DataDir, corpus_name(Corpus)])).

corpus_name(Corpus) when is_binary(Corpus) -> Corpus;
corpus_name(Corpus) when is_atom(Corpus) -> atom_to_binary(Corpus, utf8);
corpus_name(Corpus) -> iolist_to_binary(io_lib:format("~p", [Corpus])).
