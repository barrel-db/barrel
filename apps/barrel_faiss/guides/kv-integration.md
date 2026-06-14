# K/V Database Integration

FAISS indexes can be serialized to binary for storage in any key-value database.

## Serialize and Deserialize

```erlang
%% Create and populate an index
{ok, Index} = barrel_faiss:new(128),
ok = barrel_faiss:add(Index, Vectors),

%% Serialize to binary
{ok, Binary} = barrel_faiss:serialize(Index),

%% Deserialize back to index
{ok, RestoredIndex} = barrel_faiss:deserialize(Binary).
```

## RocksDB Integration

Store FAISS indexes in RocksDB:

```erlang
-module(vector_store).
-export([save_index/3, load_index/2, search/4]).

save_index(Db, Name, Index) ->
    {ok, Binary} = barrel_faiss:serialize(Index),
    Key = <<"barrel_faiss:", Name/binary>>,
    rocksdb:put(Db, Key, Binary, []).

load_index(Db, Name) ->
    Key = <<"barrel_faiss:", Name/binary>>,
    case rocksdb:get(Db, Key, []) of
        {ok, Binary} -> barrel_faiss:deserialize(Binary);
        not_found -> {error, not_found}
    end.

search(Db, Name, Query, K) ->
    case load_index(Db, Name) of
        {ok, Index} ->
            Result = barrel_faiss:search(Index, Query, K),
            barrel_faiss:close(Index),
            Result;
        Error ->
            Error
    end.
```

## ETS Cache with Persistence

Cache indexes in ETS, persist to disk:

```erlang
-module(vector_cache).
-behaviour(gen_server).

-export([start_link/1, get_or_load/2, save/2]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

start_link(Dir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Dir, []).

init(Dir) ->
    Tab = ets:new(?MODULE, [set, protected, {keypos, 1}]),
    {ok, #{table => Tab, dir => Dir}}.

get_or_load(Name, Dimension) ->
    gen_server:call(?MODULE, {get_or_load, Name, Dimension}).

save(Name, Index) ->
    gen_server:call(?MODULE, {save, Name, Index}).

handle_call({get_or_load, Name, Dimension}, _From, State) ->
    #{table := Tab, dir := Dir} = State,
    case ets:lookup(Tab, Name) of
        [{Name, Index}] ->
            {reply, {ok, Index}, State};
        [] ->
            Path = filename:join(Dir, <<Name/binary, ".faiss">>),
            case barrel_faiss:read_index(Path) of
                {ok, Index} ->
                    ets:insert(Tab, {Name, Index}),
                    {reply, {ok, Index}, State};
                {error, _} ->
                    {ok, Index} = barrel_faiss:new(Dimension),
                    ets:insert(Tab, {Name, Index}),
                    {reply, {ok, Index}, State}
            end
    end;

handle_call({save, Name, Index}, _From, State) ->
    #{dir := Dir} = State,
    Path = filename:join(Dir, <<Name/binary, ".faiss">>),
    Result = barrel_faiss:write_index(Index, Path),
    {reply, Result, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    #{table := Tab} = State,
    ets:foldl(fun({_Name, Index}, _) -> barrel_faiss:close(Index) end, ok, Tab),
    ok.
```

## Sharded Index Pattern

For large vector sets, shard across multiple indexes:

```erlang
-module(sharded_index).
-export([new/2, add/3, search/3]).

-record(sharded, {
    dimension :: pos_integer(),
    shards :: #{non_neg_integer() => barrel_faiss:index()},
    num_shards :: pos_integer()
}).

new(Dimension, NumShards) ->
    Shards = maps:from_list([
        {I, element(2, barrel_faiss:new(Dimension))}
        || I <- lists:seq(0, NumShards - 1)
    ]),
    #sharded{dimension = Dimension, shards = Shards, num_shards = NumShards}.

add(#sharded{shards = Shards, num_shards = N} = S, VectorId, Vector) ->
    ShardId = VectorId rem N,
    Index = maps:get(ShardId, Shards),
    ok = barrel_faiss:add(Index, Vector),
    S.

search(#sharded{shards = Shards}, Query, K) ->
    %% Search all shards in parallel
    Results = pmap(
        fun({_Id, Index}) ->
            barrel_faiss:search(Index, Query, K)
        end,
        maps:to_list(Shards)
    ),
    %% Merge and sort results
    merge_results(Results, K).

pmap(Fun, List) ->
    Parent = self(),
    Pids = [spawn_link(fun() -> Parent ! {self(), Fun(X)} end) || X <- List],
    [receive {Pid, R} -> R end || Pid <- Pids].

merge_results(Results, K) ->
    %% Combine all distances and labels, sort by distance, take top K
    AllPairs = lists:flatmap(
        fun({ok, Distances, Labels}) ->
            Ds = [D || <<D:32/float-native>> <= Distances],
            Ls = [L || <<L:64/signed-native>> <= Labels],
            lists:zip(Ds, Ls)
        end,
        Results
    ),
    Sorted = lists:sort(fun({D1, _}, {D2, _}) -> D1 =< D2 end, AllPairs),
    TopK = lists:sublist(Sorted, K),
    {Ds, Ls} = lists:unzip(TopK),
    DistBin = << <<D:32/float-native>> || D <- Ds >>,
    LabelBin = << <<L:64/signed-native>> || L <- Ls >>,
    {ok, DistBin, LabelBin}.
```

## Best Practices

1. **Batch operations** - Add vectors in batches for better performance
2. **Use HNSW for search-heavy workloads** - Fast approximate search
3. **Use IVF for large indexes** - Better memory efficiency
4. **Cache deserialized indexes** - Avoid repeated deserialization
5. **Close indexes when done** - Release native resources
