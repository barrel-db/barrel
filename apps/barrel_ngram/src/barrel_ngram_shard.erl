%%%-------------------------------------------------------------------
%%% @doc Per-corpus shard: the active index buffer and its freeze.
%%%
%%% Holds the write-side state for a corpus: an ETS buffer accumulating
%%% `{Gram, Ordinal}' pairs, the local `key <-> ordinal' maps, and the
%%% path of the frozen segment once written. M1 runs one shard per corpus
%%% and freezes to a single immutable segment.
%%%
%%% The shard serialises writes and the freeze. Queries do NOT run here:
%%% they open their own read handle on the immutable segment, so a slow
%%% query never blocks indexing (see {@link barrel_ngram_query}).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_shard).
-behaviour(gen_server).

-export([start_link/2]).
-export([index_docs/2, freeze/2, get_segment/1, get_config/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    corpus :: term(),
    config :: map(),
    selector :: module(),
    segment_path :: binary(),
    buf :: ets:tid(),
    key2ord = #{} :: #{binary() => non_neg_integer()},
    ord2key = #{} :: #{non_neg_integer() => binary()},
    next_ord = 0 :: non_neg_integer(),
    frozen = false :: boolean()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(term(), map()) -> {ok, pid()} | {error, term()}.
start_link(Corpus, Config) ->
    gen_server:start_link(via(Corpus), ?MODULE, {Corpus, Config}, []).

%% @doc Add documents to the active buffer. Docs are `{Key, Text}' where
%% Text is the corpus bytes for the document.
-spec index_docs(term(), [{binary(), binary()}]) -> ok.
index_docs(Corpus, Docs) ->
    gen_server:call(via(Corpus), {index_docs, Docs}, infinity).

%% @doc Freeze the active buffer to a single immutable segment stamped
%% with the given (12-byte encoded) HLC watermark.
-spec freeze(term(), binary()) -> {ok, binary()} | {error, term()}.
freeze(Corpus, Watermark) ->
    gen_server:call(via(Corpus), {freeze, Watermark}, infinity).

%% @doc Path of the frozen segment, or `none' if nothing is frozen yet.
-spec get_segment(term()) -> {ok, binary()} | none.
get_segment(Corpus) ->
    gen_server:call(via(Corpus), get_segment, infinity).

%% @doc The corpus config held by the shard.
-spec get_config(term()) -> {ok, map()}.
get_config(Corpus) ->
    gen_server:call(via(Corpus), get_config, infinity).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({Corpus, Config}) ->
    Selector = maps:get(selector, Config, barrel_ngram_selector_dense),
    Buf = ets:new(ngram_buf, [bag, private]),
    {ok, #state{
        corpus = Corpus,
        config = Config,
        selector = Selector,
        segment_path = segment_path(Corpus, Config),
        buf = Buf
    }}.

handle_call({index_docs, Docs}, _From, State) ->
    State1 = lists:foldl(fun index_one/2, State, Docs),
    {reply, ok, State1};

handle_call({freeze, Watermark}, _From, State) ->
    case do_freeze(Watermark, State) of
        {ok, Path, State1} -> {reply, {ok, Path}, State1};
        {error, _} = Err -> {reply, Err, State}
    end;

handle_call(get_segment, _From, #state{frozen = true, segment_path = Path} = State) ->
    {reply, {ok, Path}, State};
handle_call(get_segment, _From, #state{frozen = false} = State) ->
    {reply, none, State};

handle_call(get_config, _From, #state{config = Config} = State) ->
    {reply, {ok, Config}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{buf = Buf}) ->
    _ = (try ets:delete(Buf) catch _:_ -> ok end),
    ok.

%%====================================================================
%% Internal
%%====================================================================

via(Corpus) ->
    {via, barrel_ngram_registry, {shard, Corpus}}.

%% @private Assign/lookup an ordinal for the key and buffer its grams.
index_one({Key, Text}, #state{selector = Selector, buf = Buf} = State) ->
    {Ord, State1} = ensure_ordinal(Key, State),
    Grams = barrel_ngram_selector:select_grams(Selector, Text),
    ets:insert(Buf, [{G, Ord} || G <- Grams]),
    State1.

ensure_ordinal(Key, #state{key2ord = K2O, ord2key = O2K, next_ord = N} = State) ->
    case maps:find(Key, K2O) of
        {ok, Ord} ->
            {Ord, State};
        error ->
            {N, State#state{
                key2ord = K2O#{Key => N},
                ord2key = O2K#{N => Key},
                next_ord = N + 1
            }}
    end.

do_freeze(Watermark, #state{buf = Buf, ord2key = O2K, next_ord = N,
                            segment_path = Path} = State) ->
    Postings = group_postings(ets:tab2list(Buf)),
    Keys = [maps:get(O, O2K) || O <- lists:seq(0, N - 1)],
    Spec = #{doc_count => N, watermark => Watermark,
             postings => Postings, keys => Keys},
    case barrel_ngram_segment:write(Path, Spec) of
        ok ->
            ets:delete_all_objects(Buf),
            {ok, Path, State#state{
                key2ord = #{}, ord2key = #{}, next_ord = 0, frozen = true}};
        {error, _} = Err ->
            Err
    end.

%% @private Group buffered {Gram, Ord} pairs into {Gram, AscendingOrds}.
group_postings(Pairs) ->
    Map = lists:foldl(
        fun({G, O}, Acc) ->
            maps:update_with(G, fun(L) -> [O | L] end, [O], Acc)
        end, #{}, Pairs),
    [{G, lists:usort(Os)} || {G, Os} <- maps:to_list(Map)].

%% @private Filesystem path for the corpus's single segment.
segment_path(Corpus, Config) ->
    DataDir = maps:get(data_dir, Config,
                       application:get_env(barrel_ngram, data_dir, "data/barrel_ngram")),
    iolist_to_binary(
        filename:join([DataDir, corpus_dir(Corpus), "segment-0000.ngseg"])).

corpus_dir(Corpus) when is_binary(Corpus) -> Corpus;
corpus_dir(Corpus) when is_atom(Corpus) -> atom_to_binary(Corpus, utf8);
corpus_dir(Corpus) -> iolist_to_binary(io_lib:format("~p", [Corpus])).
