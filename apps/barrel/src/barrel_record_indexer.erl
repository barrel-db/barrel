%%%-------------------------------------------------------------------
%%% @doc Record indexer: drives vector indexing from the embed outbox.
%%%
%%% One indexer runs per record-mode database. Each round it folds a
%%% batch of pending outbox entries, re-reads the CURRENT state of each
%%% document (never the state captured in the entry), applies the
%%% embedding policy, embeds through barrel's barrel_embed state,
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
%%% Scheduling: a `nudge' cast (sent by barrel after tagged writes)
%%% triggers an immediate round; otherwise the indexer polls with
%%% backoff (?POLL_MIN up to ?POLL_MAX when idle).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_record_indexer).
-behaviour(gen_server).

-export([start_link/1, whereis_pid/1, nudge/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(EMBED_TAG, <<"embed">>).
-define(BATCH_SIZE, 100).
-define(POLL_MIN, 100).
-define(POLL_MAX, 1000).
-define(MAX_FAILURES, 5).

-record(state, {
    name :: binary(),
    db :: binary(),
    vstore :: binary(),
    policy :: barrel_embedding_policy:policy(),
    embed :: term(),
    dimensions :: pos_integer(),
    batch_size :: pos_integer(),
    poll :: pos_integer(),
    timer :: reference() | undefined,
    failures = #{} :: #{binary() => pos_integer()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start an indexer.
%% Config: #{name := binary(), db := binary(), vstore := binary(),
%%           policy := policy(), embed := term(), batch_size => pos_integer()}.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(#{name := Name} = Config) ->
    %% Registered in the vectordb name registry: binary names, no
    %% atoms minted for dynamically named databases.
    ok = barrel_vectordb_registry:ensure(),
    gen_server:start_link({via, barrel_vectordb_registry, reg_name(Name)},
                          ?MODULE, Config, []).

%% @doc The indexer's pid for a database, or undefined.
-spec whereis_pid(atom() | binary()) -> pid() | undefined.
whereis_pid(Name) ->
    barrel_vectordb_registry:whereis_name(reg_name(Name)).

%% @doc Trigger an immediate indexing round.
-spec nudge(atom() | binary()) -> ok.
nudge(Name) ->
    case whereis_pid(Name) of
        undefined -> ok;
        Pid -> gen_server:cast(Pid, nudge)
    end.

reg_name(Name) when is_binary(Name) ->
    {record_indexer, Name};
reg_name(Name) when is_atom(Name) ->
    {record_indexer, atom_to_binary(Name, utf8)}.

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
        dimensions = maps:get(dimensions, Config, 768),
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
%% missing or non-matching docs lose their vector; docs carrying an
%% `_embedding' index it directly; matching docs embed their policy
%% text. A carried vector of the wrong dimension counts as a failure
%% (parks after ?MAX_FAILURES) and stays un-acked.
classify(Entries, #state{db = DbBin, policy = Policy,
                         dimensions = Dim} = State0) ->
    {Deleted, Live} = lists:partition(
        fun(#{deleted := D}) -> D end, Entries),
    LiveIds = [Id || #{id := Id} <- Live],
    Docs = case LiveIds of
        [] -> [];
        _ -> barrel_docdb:get_docs(DbBin, LiveIds,
                                   #{include_embedding => true})
    end,
    {ToDelete0, ToIndex, State} = case Docs of
        {error, _} ->
            %% Database going away; process nothing this round
            {[], [], State0};
        _ ->
            lists:foldl(
                fun({#{id := Id, hlc := Hlc}, DocRes}, {DelAcc, IdxAcc, StAcc}) ->
                    case DocRes of
                        {ok, Doc} ->
                            case doc_plan(Policy, Dim, Doc) of
                                {index, Plan} ->
                                    {DelAcc, [{Id, Hlc, Plan} | IdxAcc], StAcc};
                                deindex ->
                                    %% Stale vector (if any) must go
                                    {[{Id, Hlc} | DelAcc], IdxAcc, StAcc};
                                {bad_dimension, Len} ->
                                    {DelAcc, IdxAcc,
                                     bump_failure(Id, {dimension_mismatch, Dim, Len},
                                                  StAcc)}
                            end;
                        {error, not_found} ->
                            %% Deleted between fold and read
                            {[{Id, Hlc} | DelAcc], IdxAcc, StAcc};
                        {error, _} ->
                            %% Transient read error: leave un-acked
                            {DelAcc, IdxAcc, StAcc}
                    end
                end,
                {[], [], State0},
                lists:zip(Live, Docs))
    end,
    DeletedPairs = [{Id, Hlc} || #{id := Id, hlc := Hlc} <- Deleted],
    {DeletedPairs ++ ToDelete0, lists:reverse(ToIndex), State}.

%% @private How the current document indexes: a stored client-supplied
%% vector is authoritative; computed vectors are derived data, so the
%% policy re-embeds (text may have changed); no vector and no matching
%% fields means no index entry.
doc_plan(Policy, Dim, Doc) ->
    case maps:get(<<"_embedding">>, Doc, undefined) of
        #{<<"vector">> := Vector, <<"source">> := <<"client">>}
          when is_list(Vector), length(Vector) =:= Dim ->
            {index, {given, barrel_embedding_policy:text(Policy, Doc), Vector}};
        #{<<"vector">> := Vector, <<"source">> := <<"client">>}
          when is_list(Vector) ->
            {bad_dimension, length(Vector)};
        _ ->
            case barrel_embedding_policy:matches(Policy, Doc) of
                true ->
                    {index, {embed, barrel_embedding_policy:text(Policy, Doc),
                             maps:get(<<"_rev">>, Doc, undefined)}};
                false ->
                    deindex
            end
    end.

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

%% @private Index planned documents: carried vectors go straight to the
%% store; policy texts embed first (batch, with per-item fallback so one
%% poison document cannot fail the batch). Computed vectors are written
%% back into the document's embedding column after indexing, so
%% _embedding always holds the current vector.
index_docs([], State) ->
    {[], State};
index_docs(Items, State0) ->
    {Given, ToEmbed} = lists:partition(
        fun({_Id, _Hlc, {given, _Text, _Vector}}) -> true;
           ({_Id, _Hlc, {embed, _Text, _Rev}}) -> false
        end,
        Items),
    GivenEntries = [{Id, Text, Vector}
                    || {Id, _Hlc, {given, Text, Vector}} <- Given],
    GivenHlcs = [Hlc || {_Id, Hlc, _Plan} <- Given],
    {EmbedEntries, EmbedHlcs, WriteBacks, State} = embed_items(ToEmbed, State0),
    case write_index(GivenEntries ++ EmbedEntries, GivenHlcs ++ EmbedHlcs,
                     State) of
        {[], State1} ->
            {[], State1};
        {Acks, State1} ->
            ok = write_back_embeddings(State1, WriteBacks),
            {Acks, State1}
    end.

embed_items([], State) ->
    {[], [], [], State};
embed_items(Items, #state{embed = Embed} = State) ->
    Texts = [Text || {_Id, _Hlc, {embed, Text, _Rev}} <- Items],
    case safe(fun() -> barrel_embed:embed_batch(Texts, Embed) end) of
        {ok, Vectors} when length(Vectors) =:= length(Items) ->
            Entries = [{Id, Text, Vector}
                       || {{Id, _Hlc, {embed, Text, _Rev}}, Vector}
                          <- lists:zip(Items, Vectors)],
            WriteBacks = [{Id, Rev, Vector}
                          || {{Id, _Hlc, {embed, _Text, Rev}}, Vector}
                             <- lists:zip(Items, Vectors)],
            {Entries, [Hlc || {_Id, Hlc, _Plan} <- Items], WriteBacks, State};
        _BatchFailure ->
            embed_items_one_by_one(Items, State)
    end.

embed_items_one_by_one(Items, #state{embed = Embed} = State0) ->
    {Entries, Hlcs, WriteBacks, State} = lists:foldl(
        fun({Id, Hlc, {embed, Text, Rev}}, {EntAcc, HlcAcc, WbAcc, StAcc}) ->
            case safe(fun() -> barrel_embed:embed(Text, Embed) end) of
                {ok, Vector} ->
                    {[{Id, Text, Vector} | EntAcc], [Hlc | HlcAcc],
                     [{Id, Rev, Vector} | WbAcc], StAcc};
                Failure ->
                    {EntAcc, HlcAcc, WbAcc, bump_failure(Id, Failure, StAcc)}
            end
        end,
        {[], [], [], State0},
        Items),
    {lists:reverse(Entries), lists:reverse(Hlcs),
     lists:reverse(WriteBacks), State}.

%% @private Best-effort write-back of computed vectors into the doc
%% embedding column (CAS on revision). A conflict means a newer write
%% exists, whose own outbox entry re-drives everything.
write_back_embeddings(_State, []) ->
    ok;
write_back_embeddings(#state{db = DbBin} = State, [{Id, Rev, Vector} | Rest]) ->
    _ = case Rev of
        undefined -> ok;
        _ -> safe(fun() ->
                 barrel_docdb:set_doc_embedding(DbBin, Id, Rev, Vector)
             end)
    end,
    write_back_embeddings(State, Rest).

%% @private Index embedded entries; success clears poison counters and
%% acks, vectordb failure leaves everything un-acked for retry.
write_index([], _Hlcs, State) ->
    {[], State};
write_index(Entries, Hlcs, #state{vstore = VStore,
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
