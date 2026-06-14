# Changes Feed and Subscriptions

barrel_docdb tracks all document modifications with a changes feed. Changes are ordered using HLC (Hybrid Logical Clock) timestamps for consistent ordering across distributed systems.

## Changes Feed

### Getting Changes

Retrieve document changes since a timestamp:

```erlang
%% Get all changes from the beginning
{ok, Changes, LastHlc} = barrel_docdb:get_changes(<<"mydb">>, first).

%% Get changes since a specific HLC timestamp
{ok, NewChanges, NewHlc} = barrel_docdb:get_changes(<<"mydb">>, LastHlc).
```

### Change Structure

Each change is a map with these fields:

```erlang
#{
    id => <<"doc123">>,           %% Document ID
    hlc => #timestamp{...},       %% HLC timestamp
    rev => <<"2-abc123">>,        %% Current revision
    deleted => false,             %% Deletion flag
    changes => [<<"2-abc123">>]   %% List of new revisions
}
```

### Options

```erlang
{ok, Changes, _} = barrel_docdb:get_changes(<<"mydb">>, first, #{
    limit => 100,              %% Maximum changes to return
    include_docs => true,      %% Include full document body
    descending => true,        %% Reverse order (newest first)
    doc_ids => [<<"doc1">>]    %% Filter to specific documents
}).
```

### Filtered Changes

Filter changes by path patterns or queries:

```erlang
%% Filter by path pattern (MQTT-style)
{ok, Changes, _} = barrel_docdb:get_changes(<<"mydb">>, first, #{
    paths => [<<"users/#">>]
}).

%% Filter by query
{ok, Changes, _} = barrel_docdb:get_changes(<<"mydb">>, first, #{
    query => #{
        where => [{path, [<<"type">>], <<"user">>}]
    }
}).
```

**Wildcard Performance:** Path patterns with `#` wildcards (e.g., `<<"users/#">>`) use time-bucketed posting list indexes for efficient HLC-ordered iteration. Changes are pre-indexed by path prefix and time bucket, enabling ~50x faster queries compared to full scan with deduplication.

## HLC (Hybrid Logical Clock)

HLC provides distributed event ordering without synchronized clocks.

### Why HLC?

- **Causality**: Events are ordered by cause-effect relationships
- **Distributed**: No central clock coordinator needed
- **Monotonic**: Timestamps always increase within a node

### Using HLC

```erlang
%% Get current HLC timestamp
Ts = barrel_docdb:get_hlc().

%% Generate a new timestamp (advances clock)
NewTs = barrel_docdb:new_hlc().

%% Synchronize with remote node's clock
RemoteTs = receive_from_remote(),
{ok, SyncedTs} = barrel_docdb:sync_hlc(RemoteTs).
```

### Cross-Node Synchronization

When receiving data from another node:

```erlang
handle_remote_message(#{hlc := RemoteHlc, data := Data}) ->
    %% Sync local clock with remote
    {ok, _} = barrel_docdb:sync_hlc(RemoteHlc),

    %% Process data - subsequent writes will be ordered after remote events
    process_data(Data).
```

## Path Subscriptions

Subscribe to real-time notifications for documents matching path patterns.

### Basic Subscription

```erlang
%% Subscribe to all user document changes
{ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"users/#">>).

%% Receive notifications
receive
    {barrel_change, <<"mydb">>, Change} ->
        #{id := DocId, rev := Rev, deleted := Deleted} = Change,
        io:format("Document ~s changed (rev: ~s)~n", [DocId, Rev])
end.

%% Unsubscribe when done
ok = barrel_docdb:unsubscribe(SubRef).
```

### Pattern Syntax

Patterns use MQTT-style wildcards:

| Pattern | Matches |
|---------|---------|
| `users/alice` | Exact path |
| `users/+` | Any single segment: `users/alice`, `users/bob` |
| `users/#` | Any suffix: `users/alice`, `users/alice/profile` |
| `users/+/orders` | Single wildcard: `users/alice/orders`, `users/bob/orders` |

### How Paths Work

Document structure is converted to paths:

```erlang
%% This document:
#{
    <<"id">> => <<"user:alice">>,
    <<"type">> => <<"user">>,
    <<"profile">> => #{
        <<"name">> => <<"Alice">>,
        <<"city">> => <<"Paris">>
    }
}

%% Generates these paths:
%% - type/user
%% - profile/name/Alice
%% - profile/city/Paris
```

### Notification Format

```erlang
{barrel_change, DbName, #{
    id => DocId,           %% Document ID
    rev => RevId,          %% New revision
    hlc => HlcTimestamp,   %% HLC timestamp
    deleted => boolean(),  %% Deletion flag
    paths => [binary()]    %% Changed paths
}}
```

## Query Subscriptions

Subscribe to changes for documents matching a query.

### Basic Query Subscription

```erlang
%% Subscribe to active user changes
Query = #{
    where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"active">>], true}
    ]
},
{ok, SubRef} = barrel_docdb:subscribe_query(<<"mydb">>, Query).

%% Receive notifications
receive
    {barrel_query_change, <<"mydb">>, Change} ->
        io:format("Matching document changed: ~p~n", [Change])
end.

%% Unsubscribe
ok = barrel_docdb:unsubscribe_query(SubRef).
```

### Path-Optimized Evaluation

Query subscriptions are optimized using path extraction:

1. Paths referenced in the query are extracted
2. Changes are filtered by path intersection
3. Full query evaluation only runs when paths overlap

This avoids evaluating the query for every document change.

## Complete Examples

### Real-Time Dashboard

```erlang
-module(dashboard).
-export([start/1]).

start(DbName) ->
    %% Subscribe to order changes
    {ok, SubRef} = barrel_docdb:subscribe(DbName, <<"orders/#">>),
    loop(SubRef).

loop(SubRef) ->
    receive
        {barrel_change, _Db, #{id := Id, deleted := false}} ->
            update_dashboard(Id),
            loop(SubRef);
        {barrel_change, _Db, #{id := Id, deleted := true}} ->
            remove_from_dashboard(Id),
            loop(SubRef);
        stop ->
            barrel_docdb:unsubscribe(SubRef)
    end.
```

### Incremental Processing

```erlang
process_changes(DbName, Since) ->
    {ok, Changes, LastHlc} = barrel_docdb:get_changes(DbName, Since, #{
        limit => 100,
        include_docs => true
    }),

    %% Process batch
    lists:foreach(fun process_change/1, Changes),

    %% Continue if more changes exist
    case length(Changes) of
        100 -> process_changes(DbName, LastHlc);
        _ -> {ok, LastHlc}
    end.
```

### Reactive Query Cache

```erlang
-module(user_cache).
-behaviour(gen_server).

init([DbName]) ->
    %% Subscribe to user changes
    Query = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, SubRef} = barrel_docdb:subscribe_query(DbName, Query),

    %% Build initial cache
    {ok, Users, _} = barrel_docdb:find(DbName, Query),
    Cache = maps:from_list([{maps:get(<<"id">>, U), U} || U <- Users]),

    {ok, #{sub => SubRef, cache => Cache}}.

handle_info({barrel_query_change, _, #{id := Id, deleted := true}}, State) ->
    {noreply, State#{cache := maps:remove(Id, maps:get(cache, State))}};

handle_info({barrel_query_change, _, #{id := Id}}, State = #{cache := Cache}) ->
    %% Fetch updated document
    {ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, Id),
    {noreply, State#{cache := Cache#{Id => Doc}}}.
```

## API Reference

See the [Erlang API Reference](api/erlang.md) for complete function documentation.

### Changes Feed
- `barrel_docdb:get_changes/2,3` - Get changes since timestamp

### HLC
- `barrel_docdb:get_hlc/0` - Get current HLC
- `barrel_docdb:new_hlc/0` - Generate new HLC
- `barrel_docdb:sync_hlc/1` - Sync with remote HLC

### Path Subscriptions
- `barrel_docdb:subscribe/2,3` - Subscribe to path pattern
- `barrel_docdb:unsubscribe/1` - Unsubscribe

### Query Subscriptions
- `barrel_docdb:subscribe_query/2,3` - Subscribe to query
- `barrel_docdb:unsubscribe_query/1` - Unsubscribe
