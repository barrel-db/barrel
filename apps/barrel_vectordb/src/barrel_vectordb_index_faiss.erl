%%%-------------------------------------------------------------------
%%% @doc FAISS backend wrapper for barrel_vectordb_index behaviour
%%%
%%% Wraps barrel_faiss (Facebook FAISS NIF bindings) with:
%%% - ID mapping: binary IDs <-> sequential integer labels
%%% - Soft delete: deleted IDs filtered from search results
%%% - Metric normalization: cosine via normalized inner product
%%% - Combined serialization: FAISS binary + Erlang state
%%%
%%% == Delete Strategy ==
%%% FAISS doesn't support deletion natively. This backend uses soft delete:
%%% deleted IDs are added to a set and filtered from search results.
%%% Use `compact/1` to rebuild the index without deleted vectors.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_index_faiss).
-behaviour(barrel_vectordb_index).

%% barrel_vectordb_index callbacks
-export([
    new/1,
    insert/3,
    search/3,
    search/4,
    delete/2,
    size/1,
    info/1,
    serialize/1,
    deserialize/1,
    close/1
]).

%% Additional API
-export([
    compact/1,
    deleted_count/1
]).

%% Serialization version
-define(FAISS_STATE_VERSION, 1).

%% Default index type
-define(DEFAULT_INDEX_TYPE, <<"HNSW32">>).

%%====================================================================
%% Internal state
%%====================================================================

-record(faiss_state, {
    index :: reference(),                              %% FAISS NIF reference
    dimension :: pos_integer(),                        %% Vector dimension
    metric :: l2 | inner_product,                      %% FAISS metric type
    distance_fn :: cosine | euclidean,                 %% User-facing distance
    id_to_label :: #{binary() => non_neg_integer()},   %% Binary ID -> FAISS label
    label_to_id :: #{non_neg_integer() => binary()},   %% FAISS label -> Binary ID
    vectors :: #{binary() => [float()]},               %% Original vectors for rebuild
    next_label :: non_neg_integer(),                   %% Next label to assign
    deleted_ids :: sets:set(binary()),                 %% Soft-deleted IDs
    config :: map()                                    %% Original config
}).

%%====================================================================
%% barrel_vectordb_index callbacks
%%====================================================================

%% @doc Create a new FAISS index.
%% Config options:
%% - dimension: Vector dimension (required)
%% - distance_fn: cosine | euclidean (default: cosine)
%% - index_type: FAISS factory string (default: "HNSW32")
-spec new(map()) -> {ok, #faiss_state{}} | {error, term()}.
new(Config) ->
    case barrel_vectordb_index:is_available(faiss) of
        false ->
            {error, faiss_not_available};
        true ->
            try
                create_index(Config)
            catch
                error:Reason -> {error, Reason};
                throw:Reason -> {error, Reason}
            end
    end.

%% @doc Insert a vector with the given ID.
-spec insert(#faiss_state{}, binary(), [float()]) ->
    {ok, #faiss_state{}} | {error, term()}.
insert(#faiss_state{dimension = Dim} = State, Id, Vector)
  when is_binary(Id), is_list(Vector), length(Vector) =:= Dim ->
    try
        do_insert(State, Id, Vector)
    catch
        error:Reason -> {error, Reason}
    end;
insert(#faiss_state{dimension = Dim}, _Id, Vector) when is_list(Vector) ->
    {error, {dimension_mismatch, Dim, length(Vector)}};
insert(_, Id, _Vector) when not is_binary(Id) ->
    {error, {invalid_id, Id}}.

%% @doc Search for K nearest neighbors.
-spec search(#faiss_state{}, [float()], pos_integer()) -> [{binary(), float()}].
search(State, Query, K) ->
    search(State, Query, K, #{}).

%% @doc Search for K nearest neighbors with options.
-spec search(#faiss_state{}, [float()], pos_integer(), map()) -> [{binary(), float()}].
search(#faiss_state{index = Index, dimension = Dim, distance_fn = DistFn,
                    label_to_id = LabelToId, deleted_ids = DeletedIds} = _State,
       Query, K, _Opts) when length(Query) =:= Dim ->
    %% Normalize for cosine similarity if needed
    NormQuery = maybe_normalize(Query, DistFn),
    QueryBin = vector_to_binary(NormQuery),

    %% Request extra results to account for deleted entries
    DeletedCount = sets:size(DeletedIds),
    SearchK = min(K + DeletedCount, maps:size(LabelToId)),

    case SearchK of
        0 -> [];
        _ ->
            case barrel_faiss:search(Index, QueryBin, SearchK) of
                {ok, DistancesBin, LabelsBin} ->
                    Results = parse_search_results(DistancesBin, LabelsBin,
                                                   LabelToId, DeletedIds, DistFn),
                    lists:sublist(Results, K);
                {error, _} ->
                    []
            end
    end;
search(_, _, _, _) ->
    [].

%% @doc Delete a vector by ID (soft delete).
-spec delete(#faiss_state{}, binary()) -> {ok, #faiss_state{}} | {error, term()}.
delete(#faiss_state{id_to_label = IdToLabel, deleted_ids = DeletedIds} = State, Id) ->
    case maps:is_key(Id, IdToLabel) of
        true ->
            %% Soft delete: add to deleted set
            NewDeletedIds = sets:add_element(Id, DeletedIds),
            {ok, State#faiss_state{deleted_ids = NewDeletedIds}};
        false ->
            %% Not found, no-op
            {ok, State}
    end.

%% @doc Get the number of active (non-deleted) vectors.
-spec size(#faiss_state{}) -> non_neg_integer().
size(#faiss_state{id_to_label = IdToLabel, deleted_ids = DeletedIds}) ->
    maps:size(IdToLabel) - sets:size(DeletedIds).

%% @doc Get index information and statistics.
-spec info(#faiss_state{}) -> map().
info(#faiss_state{dimension = Dim, distance_fn = DistFn, metric = Metric,
                  id_to_label = IdToLabel, deleted_ids = DeletedIds, config = Config}) ->
    #{
        backend => faiss,
        dimension => Dim,
        distance_fn => DistFn,
        metric => Metric,
        size => maps:size(IdToLabel) - sets:size(DeletedIds),
        total_vectors => maps:size(IdToLabel),
        deleted_count => sets:size(DeletedIds),
        index_type => maps:get(index_type, Config, ?DEFAULT_INDEX_TYPE),
        config => Config
    }.

%% @doc Serialize index to binary.
%% Format: <<Version, FAISSLen, FAISSBin, StateLen, StateBin>>
-spec serialize(#faiss_state{}) -> binary().
serialize(#faiss_state{index = Index} = State) ->
    {ok, IndexBin} = barrel_faiss:serialize(Index),

    %% Serialize Erlang state (ID mappings, vectors, deleted set)
    StateMap = #{
        dimension => State#faiss_state.dimension,
        metric => State#faiss_state.metric,
        distance_fn => State#faiss_state.distance_fn,
        id_to_label => State#faiss_state.id_to_label,
        label_to_id => State#faiss_state.label_to_id,
        vectors => State#faiss_state.vectors,
        next_label => State#faiss_state.next_label,
        deleted_ids => sets:to_list(State#faiss_state.deleted_ids),
        config => State#faiss_state.config
    },
    StateBin = term_to_binary(StateMap),

    <<?FAISS_STATE_VERSION:8,
      (byte_size(IndexBin)):32, IndexBin/binary,
      (byte_size(StateBin)):32, StateBin/binary>>.

%% @doc Deserialize index from binary.
-spec deserialize(binary()) -> {ok, #faiss_state{}} | {error, term()}.
deserialize(<<?FAISS_STATE_VERSION:8,
              IndexLen:32, IndexBin:IndexLen/binary,
              StateLen:32, StateBin:StateLen/binary>>) ->
    case barrel_vectordb_index:is_available(faiss) of
        false ->
            {error, faiss_not_available};
        true ->
            try
                {ok, Index} = barrel_faiss:deserialize(IndexBin),
                StateMap = binary_to_term(StateBin),

                State = #faiss_state{
                    index = Index,
                    dimension = maps:get(dimension, StateMap),
                    metric = maps:get(metric, StateMap),
                    distance_fn = maps:get(distance_fn, StateMap),
                    id_to_label = maps:get(id_to_label, StateMap),
                    label_to_id = maps:get(label_to_id, StateMap),
                    vectors = maps:get(vectors, StateMap, #{}),
                    next_label = maps:get(next_label, StateMap),
                    deleted_ids = sets:from_list(maps:get(deleted_ids, StateMap)),
                    config = maps:get(config, StateMap)
                },
                {ok, State}
            catch
                error:Reason -> {error, {deserialization_failed, Reason}}
            end
    end;
deserialize(_) ->
    {error, invalid_format}.

%% @doc Close and release FAISS index resources.
-spec close(#faiss_state{}) -> ok.
close(#faiss_state{index = Index}) ->
    barrel_faiss:close(Index).

%%====================================================================
%% Additional API
%%====================================================================

%% @doc Compact the index by rebuilding without deleted vectors.
%% This reclaims space used by soft-deleted vectors.
-spec compact(#faiss_state{}) -> {ok, #faiss_state{}} | {error, term()}.
compact(#faiss_state{deleted_ids = DeletedIds} = State) ->
    case sets:size(DeletedIds) of
        0 ->
            %% Nothing to compact
            {ok, State};
        _ ->
            rebuild_without_deleted(State)
    end.

%% @doc Get the number of soft-deleted vectors.
-spec deleted_count(#faiss_state{}) -> non_neg_integer().
deleted_count(#faiss_state{deleted_ids = DeletedIds}) ->
    sets:size(DeletedIds).

%%====================================================================
%% Internal functions
%%====================================================================

create_index(Config) ->
    Dim = maps:get(dimension, Config, 768),
    DistFn = maps:get(distance_fn, Config, cosine),
    IndexType = maps:get(index_type, Config, ?DEFAULT_INDEX_TYPE),

    %% Map distance function to FAISS metric
    %% For cosine: use inner_product with normalized vectors
    Metric = case DistFn of
        cosine -> inner_product;
        euclidean -> l2;
        l2 -> l2;
        inner_product -> inner_product
    end,

    {ok, Index} = barrel_faiss:index_factory(Dim, IndexType, Metric),

    State = #faiss_state{
        index = Index,
        dimension = Dim,
        metric = Metric,
        distance_fn = DistFn,
        id_to_label = #{},
        label_to_id = #{},
        vectors = #{},
        next_label = 0,
        deleted_ids = sets:new(),
        config = Config
    },
    {ok, State}.

do_insert(#faiss_state{index = Index, distance_fn = DistFn,
                       id_to_label = IdToLabel, label_to_id = LabelToId,
                       vectors = Vectors,
                       next_label = NextLabel, deleted_ids = DeletedIds} = State,
          Id, Vector) ->
    %% Check if ID already exists
    case maps:is_key(Id, IdToLabel) of
        true ->
            %% Update: soft-delete old label, insert with new label
            %% Remove from deleted set if was deleted
            NewDeletedIds = sets:del_element(Id, DeletedIds),

            %% Normalize if cosine
            NormVector = maybe_normalize(Vector, DistFn),
            VectorBin = vector_to_binary(NormVector),

            %% Add to FAISS (new label)
            ok = barrel_faiss:add(Index, VectorBin),

            %% Update mappings - old label becomes orphaned in FAISS but we don't track it
            OldLabel = maps:get(Id, IdToLabel),
            NewIdToLabel = IdToLabel#{Id => NextLabel},
            NewLabelToId = maps:remove(OldLabel, LabelToId),
            NewLabelToId2 = NewLabelToId#{NextLabel => Id},

            {ok, State#faiss_state{
                id_to_label = NewIdToLabel,
                label_to_id = NewLabelToId2,
                vectors = Vectors#{Id => Vector},
                next_label = NextLabel + 1,
                deleted_ids = NewDeletedIds
            }};
        false ->
            %% New insertion
            NormVector = maybe_normalize(Vector, DistFn),
            VectorBin = vector_to_binary(NormVector),

            ok = barrel_faiss:add(Index, VectorBin),

            NewIdToLabel = IdToLabel#{Id => NextLabel},
            NewLabelToId = LabelToId#{NextLabel => Id},

            {ok, State#faiss_state{
                id_to_label = NewIdToLabel,
                label_to_id = NewLabelToId,
                vectors = Vectors#{Id => Vector},
                next_label = NextLabel + 1
            }}
    end.

maybe_normalize(Vector, cosine) ->
    %% Normalize for cosine similarity via inner product
    Norm = math:sqrt(lists:sum([V * V || V <- Vector])),
    case Norm < 1.0e-10 of
        true -> Vector;
        false -> [V / Norm || V <- Vector]
    end;
maybe_normalize(Vector, _) ->
    Vector.

vector_to_binary(Vector) ->
    << <<F:32/float-native>> || F <- Vector >>.

parse_search_results(DistancesBin, LabelsBin, LabelToId, DeletedIds, DistFn) ->
    Distances = [D || <<D:32/float-native>> <= DistancesBin],
    Labels = [L || <<L:64/signed-native>> <= LabelsBin],

    Results = lists:filtermap(
        fun({Label, Dist}) when Label >= 0 ->
            case maps:get(Label, LabelToId, undefined) of
                undefined ->
                    false;
                Id ->
                    case sets:is_element(Id, DeletedIds) of
                        true -> false;
                        false -> {true, {Id, convert_distance(Dist, DistFn)}}
                    end
            end;
           ({-1, _}) ->
            false  %% FAISS returns -1 for empty slots
        end,
        lists:zip(Labels, Distances)
    ),

    %% Sort by distance ascending
    lists:sort(fun({_, D1}, {_, D2}) -> D1 =< D2 end, Results).

convert_distance(Dist, cosine) ->
    %% Inner product of normalized vectors = cosine similarity
    %% Convert to distance: 1 - similarity
    1.0 - Dist;
convert_distance(Dist, _) ->
    Dist.

rebuild_without_deleted(#faiss_state{dimension = Dim, distance_fn = DistFn,
                                      metric = Metric, vectors = Vectors,
                                      deleted_ids = DeletedIds, config = Config} = OldState) ->
    %% Close old index
    close(OldState),

    %% Create new FAISS index
    IndexType = maps:get(index_type, Config, ?DEFAULT_INDEX_TYPE),
    {ok, NewIndex} = barrel_faiss:index_factory(Dim, IndexType, Metric),

    %% Filter out deleted vectors
    ActiveVectors = maps:filter(
        fun(Id, _Vector) -> not sets:is_element(Id, DeletedIds) end,
        Vectors
    ),

    %% Re-insert all active vectors
    {NewIdToLabel, NewLabelToId, NextLabel} = maps:fold(
        fun(Id, Vector, {IdToLabel, LabelToId, Label}) ->
            NormVector = maybe_normalize(Vector, DistFn),
            VectorBin = vector_to_binary(NormVector),
            ok = barrel_faiss:add(NewIndex, VectorBin),
            {
                IdToLabel#{Id => Label},
                LabelToId#{Label => Id},
                Label + 1
            }
        end,
        {#{}, #{}, 0},
        ActiveVectors
    ),

    NewState = #faiss_state{
        index = NewIndex,
        dimension = Dim,
        metric = Metric,
        distance_fn = DistFn,
        id_to_label = NewIdToLabel,
        label_to_id = NewLabelToId,
        vectors = ActiveVectors,
        next_label = NextLabel,
        deleted_ids = sets:new(),
        config = Config
    },
    {ok, NewState}.
