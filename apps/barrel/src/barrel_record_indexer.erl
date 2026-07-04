%%%-------------------------------------------------------------------
%%% @doc Record indexer: drives vector indexing from the embed outbox.
%%%
%%% One indexer runs per record-mode database. Each round it folds a
%%% batch of pending outbox entries, re-reads the CURRENT state of each
%%% document (never the state captured in the entry), applies the
%%% embedding policy, embeds through the facade's barrel_embed state,
%%% writes vectors with the index-only vectordb path, deletes vectors of
%%% deleted or non-matching documents, and acks the processed entries by
%%% exact HLC key.
%%%
%%% Exactly-once effect without a checkpoint: un-acked entries ARE the
%%% checkpoint, processing is an idempotent upsert, and a rewrite during
%%% processing lands at a new HLC key that re-drives the consumer.
%%%
%%% Failure handling:
%%% <ul>
%%% <li>Embed failures count per document id; after ?MAX_FAILURES the id
%%%     is skipped in later rounds (its entry stays pending and visible)
%%%     and logged. A batch embed failure falls back to per-item embeds
%%%     so one poison document cannot fail the batch.</li>
%%% <li>vectordb call failures are transient (store restarting): the
%%%     entries stay un-acked and the round retries later.</li>
%%% </ul>
%%%
%%% Scheduling: a `nudge' cast (sent by the facade after tagged writes)
%%% triggers an immediate round; otherwise the indexer polls with
%%% backoff (?POLL_MIN up to ?POLL_MAX when idle).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_record_indexer).
-behaviour(gen_server).

-export([start_link/1, name/1, nudge/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(EMBED_TAG, <<"embed">>).
-define(BATCH_SIZE, 100).
-define(POLL_MIN, 100).
-define(POLL_MAX, 1000).
-define(MAX_FAILURES, 5).

-record(state, {
    name :: atom(),
    db :: binary(),
    vstore :: atom(),
    policy :: barrel_embedding_policy:policy(),
    embed :: term(),
    batch_size :: pos_integer(),
    poll :: pos_integer(),
    timer :: reference() | undefined,
    failures = #{} :: #{binary() => pos_integer()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start an indexer.
%% Config: #{name := atom(), db := binary(), vstore := atom(),
%%           policy := policy(), embed := term(), batch_size => pos_integer()}.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(#{name := Name} = Config) ->
    gen_server:start_link({local, name(Name)}, ?MODULE, Config, []).

%% @doc Registered name of a database's indexer.
-spec name(atom()) -> atom().
name(Name) ->
    list_to_atom("barrel_record_indexer_" ++ atom_to_list(Name)).

%% @doc Trigger an immediate indexing round.
-spec nudge(atom()) -> ok.
nudge(Name) ->
    case erlang:whereis(name(Name)) of
        undefined -> ok;
        Pid -> gen_server:cast(Pid, nudge)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init(#{name := Name, db := DbBin, vstore := VStore,
       policy := Policy, embed := Embed} = Config) ->
    State = #state{
        name = Name,
        db = DbBin,
        vstore = VStore,
        policy = Policy,
        embed = Embed,
        batch_size = maps:get(batch_size, Config, ?BATCH_SIZE),
        poll = ?POLL_MIN
    },
    %% First round soon after start (also picks up entries left by a
    %% previous incarnation).
    {ok, schedule(State#state{poll = ?POLL_MIN})}.

%% @private
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(nudge, State) ->
    {noreply, run_round(cancel_timer(State))};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(poll, State) ->
    {noreply, run_round(State#state{timer = undefined})};
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal
%%====================================================================

schedule(#state{poll = Poll} = State) ->
    Ref = erlang:send_after(Poll, self(), poll),
    State#state{timer = Ref}.

cancel_timer(#state{timer = undefined} = State) ->
    State;
cancel_timer(#state{timer = Ref} = State) ->
    _ = erlang:cancel_timer(Ref),
    State#state{timer = undefined}.

%% @private One indexing round; backoff when idle, stay hot otherwise.
run_round(State0) ->
    {Processed, State} = drain(State0),
    Poll = case Processed of
        0 -> min(State#state.poll * 2, ?POLL_MAX);
        _ -> ?POLL_MIN
    end,
    schedule(State#state{poll = Poll}).

drain(#state{db = DbBin, batch_size = BatchSize} = State) ->
    Collect = fun(Entry, Acc) -> {ok, [Entry | Acc]} end,
    case barrel_docdb:outbox_fold(DbBin, ?EMBED_TAG, Collect, [],
                                  #{limit => BatchSize}) of
        {error, _} ->
            %% Database closed or closing; retry later
            {0, State};
        Entries0 ->
            Entries = lists:reverse(Entries0),
            process_entries(Entries, State)
    end.

process_entries([], State) ->
    {0, State};
process_entries(Entries, #state{failures = Failures} = State) ->
    %% Park poisoned ids: their entries stay pending and visible.
    Workable = [E || #{id := Id} = E <- Entries,
                     maps:get(Id, Failures, 0) < ?MAX_FAILURES],
    {ToDelete, ToIndex, State1} = classify(Workable, State),
    {DelAcks, State2} = delete_vectors(ToDelete, State1),
    {IdxAcks, State3} = index_docs(ToIndex, State2),
    Acks = DelAcks ++ IdxAcks,
    ok = ack(State3, Acks),
    {length(Acks), State3}.

%% @private Split entries by the CURRENT document state: deleted or
%% missing or non-matching docs lose their vector; matching docs embed.
classify(Entries, #state{db = DbBin, policy = Policy} = State) ->
    {Deleted, Live} = lists:partition(
        fun(#{deleted := D}) -> D end, Entries),
    LiveIds = [Id || #{id := Id} <- Live],
    Docs = case LiveIds of
        [] -> [];
        _ -> barrel_docdb:get_docs(DbBin, LiveIds)
    end,
    {ToDelete0, ToIndex} = case Docs of
        {error, _} ->
            %% Database going away; process nothing this round
            {[], []};
        _ ->
            lists:foldl(
                fun({#{id := Id, hlc := Hlc}, DocRes}, {DelAcc, IdxAcc}) ->
                    case DocRes of
                        {ok, Doc} ->
                            case barrel_embedding_policy:matches(Policy, Doc) of
                                true ->
                                    Text = barrel_embedding_policy:text(Policy, Doc),
                                    {DelAcc, [{Id, Hlc, Text} | IdxAcc]};
                                false ->
                                    %% Stale vector (if any) must go
                                    {[{Id, Hlc} | DelAcc], IdxAcc}
                            end;
                        {error, not_found} ->
                            %% Deleted between fold and read
                            {[{Id, Hlc} | DelAcc], IdxAcc};
                        {error, _} ->
                            %% Transient read error: leave un-acked
                            {DelAcc, IdxAcc}
                    end
                end,
                {[], []},
                lists:zip(Live, Docs))
    end,
    DeletedPairs = [{Id, Hlc} || #{id := Id, hlc := Hlc} <- Deleted],
    {DeletedPairs ++ ToDelete0, lists:reverse(ToIndex), State}.

%% @private Remove vectors; vectordb errors leave entries un-acked.
delete_vectors(Pairs, #state{vstore = VStore} = State) ->
    Acks = lists:filtermap(
        fun({Id, Hlc}) ->
            case safe(fun() -> barrel_vectordb:delete(VStore, Id) end) of
                ok -> {true, Hlc};
                {error, not_found} -> {true, Hlc};
                _Other -> false
            end
        end,
        Pairs),
    {Acks, State}.

%% @private Embed and index matching documents. A batch embed failure
%% falls back to per-item embeds; per-item failures bump the poison
%% counter and stay un-acked.
index_docs([], State) ->
    {[], State};
index_docs(Items, #state{embed = Embed} = State) ->
    Texts = [Text || {_Id, _Hlc, Text} <- Items],
    case safe(fun() -> barrel_embed:embed_batch(Texts, Embed) end) of
        {ok, Vectors} when length(Vectors) =:= length(Items) ->
            Entries = [{Id, Text, Vector}
                       || {{Id, _Hlc, Text}, Vector} <- lists:zip(Items, Vectors)],
            write_index(Entries, [Hlc || {_Id, Hlc, _} <- Items], Items, State);
        _BatchFailure ->
            index_docs_one_by_one(Items, State)
    end.

index_docs_one_by_one(Items, #state{embed = Embed} = State0) ->
    {Entries, Hlcs, OkItems, State} = lists:foldl(
        fun({Id, Hlc, Text} = Item, {EntAcc, HlcAcc, ItemAcc, StAcc}) ->
            case safe(fun() -> barrel_embed:embed(Text, Embed) end) of
                {ok, Vector} ->
                    {[{Id, Text, Vector} | EntAcc], [Hlc | HlcAcc],
                     [Item | ItemAcc], StAcc};
                Failure ->
                    {EntAcc, HlcAcc, ItemAcc, bump_failure(Id, Failure, StAcc)}
            end
        end,
        {[], [], [], State0},
        Items),
    write_index(lists:reverse(Entries), lists:reverse(Hlcs),
                lists:reverse(OkItems), State).

%% @private Index embedded entries; success clears poison counters and
%% acks, vectordb failure leaves everything un-acked for retry.
write_index([], _Hlcs, _Items, State) ->
    {[], State};
write_index(Entries, Hlcs, _Items, #state{vstore = VStore,
                                          failures = Failures} = State) ->
    case safe(fun() -> barrel_vectordb:add_index_only_batch(VStore, Entries) end) of
        {ok, _Stats} ->
            Cleared = maps:without([Id || {Id, _, _} <- Entries], Failures),
            {Hlcs, State#state{failures = Cleared}};
        _Failure ->
            {[], State}
    end.

bump_failure(Id, Failure, #state{name = Name, failures = Failures} = State) ->
    Count = maps:get(Id, Failures, 0) + 1,
    case Count >= ?MAX_FAILURES of
        true ->
            logger:warning(
                "barrel record indexer ~s: parking doc ~s after ~b embed "
                "failures (last: ~p); entry stays pending",
                [Name, Id, Count, Failure]);
        false ->
            ok
    end,
    State#state{failures = Failures#{Id => Count}}.

ack(_State, []) ->
    ok;
ack(#state{db = DbBin}, Hlcs) ->
    case barrel_docdb:outbox_ack(DbBin, ?EMBED_TAG, Hlcs) of
        ok -> ok;
        {error, _} -> ok %% db closing; entries re-drive on reopen
    end.

%% @private Call a fun, mapping crashes (dead store, timeouts) to an
%% error tuple so callers can treat them as transient failures.
safe(Fun) ->
    try
        Fun()
    catch
        Class:Reason ->
            {error, {Class, Reason}}
    end.
