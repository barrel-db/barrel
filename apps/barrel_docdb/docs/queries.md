# Query Guide

barrel_docdb provides a declarative query system for finding documents without predefined views. All document paths are automatically indexed, enabling ad-hoc queries.

## Basic Queries

### Finding Documents

Use `find/2` to query documents by field values:

```erlang
%% Find all users
{ok, Users, _} = barrel_docdb:find(<<"mydb">>, #{
    where => [{path, [<<"type">>], <<"user">>}]
}).

%% Find users in a specific organization
{ok, OrgUsers, _} = barrel_docdb:find(<<"mydb">>, #{
    where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"org_id">>], <<"org123">>}
    ]
}).
```

### Query Structure

A query specification is a map with these keys:

| Key | Type | Description |
|-----|------|-------------|
| `where` | list | **Required** (unless an id scan is used). List of conditions to match |
| `select` | list | Fields or variables to return |
| `order_by` | term | Field or variable for sorting |
| `limit` | integer | Maximum results |
| `offset` | integer | Skip first N results |
| `include_docs` | boolean | Include full documents (default: true) |
| `flat` | boolean | Return flat docs instead of `{id, doc}` wrappers (default: false) |
| `id_prefix` | binary | Standalone id prefix scan (no `where`) |
| `id_range` | `{Start, End}` | Standalone id range scan `Start =< id < End` (no `where`) |

### Result shape

With `include_docs => true` (the default), each result is a wrapper
`#{<<"id">> => Id, <<"doc">> => Doc}`. Pass `flat => true` to get the flat
document `Doc#{<<"id">>}` instead (matching `get_doc/2`'s id; flat docs carry
`<<"id">>` but not `<<"_rev">>` — use `get_doc/2` if you need the rev). With
`include_docs => false`, each result is `#{<<"id">> => Id}`.

```erlang
%% Wrapper (default)
{ok, [#{<<"id">> := <<"u1">>, <<"doc">> := Doc}], _} =
    barrel_docdb:find(Db, #{where => [{path, [<<"type">>], <<"user">>}]}).

%% Flat
{ok, [#{<<"id">> := <<"u1">>, <<"type">> := <<"user">>}], _} =
    barrel_docdb:find(Db, #{where => [{path, [<<"type">>], <<"user">>}],
                            flat => true}).
```

### Id scans (primary key)

The document id is not in the path index. To scan by id, use the standalone,
ordered `id_prefix` / `id_range` options (no `where` clause). These run as a
range scan over the entity keyspace — O(matches), tombstones skipped, and
cursor-friendly. Model hierarchical/scannable keys in the id itself.

```erlang
%% All ids starting with "user:"
{ok, Rows, _} = barrel_docdb:find(Db, #{id_prefix => <<"user:">>}).

%% Ids in [a, n)
{ok, Rows, _} = barrel_docdb:find(Db, #{id_range => {<<"a">>, <<"n">>}}).
```

### Reserved fields

Top-level fields whose key begins with `_` (e.g. `<<"_meta">>`) are reserved
metadata. They are stripped before storage and are **neither persisted nor
indexed**, so they cannot be queried. Put application data under a non-`_`
top-level namespace.

## Conditions

### Path Equality

Match documents where a path equals a value:

```erlang
%% Match type = "user"
{path, [<<"type">>], <<"user">>}

%% Match nested path: address.city = "Paris"
{path, [<<"address">>, <<"city">>], <<"Paris">>}

%% Match array element: tags[0] = "important"
{path, [<<"tags">>, 0], <<"important">>}
```

### Comparisons

Use `compare` for range queries:

```erlang
%% Age greater than 18
{compare, [<<"age">>], '>', 18}

%% Price less than or equal to 100
{compare, [<<"price">>], '=<', 100}

%% Supported operators: '>', '<', '>=', '=<', '==', '=/='
```

### Logic Variables

Bind values to variables for use in projections or joins:

```erlang
%% Bind org_id to ?Org variable
{path, [<<"org_id">>], '?Org'}

%% Use in select
#{
    where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"org_id">>], '?Org'},
        {path, [<<"name">>], '?Name'}
    ],
    select => ['?Org', '?Name']
}
```

### Boolean Logic

Combine conditions with `and`, `or`, `not`:

```erlang
%% All conditions must match (AND is implicit for top-level)
#{
    where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"active">>], true}
    ]
}

%% Explicit AND
{'and', [
    {path, [<<"type">>], <<"user">>},
    {compare, [<<"age">>], '>=', 18}
]}

%% OR: match either condition
{'or', [
    {path, [<<"status">>], <<"active">>},
    {path, [<<"status">>], <<"pending">>}
]}

%% NOT: negate a condition
{'not', {path, [<<"deleted">>], true}}
```

### Collection Operators

```erlang
%% IN: value must be in list
{in, [<<"status">>], [<<"active">>, <<"pending">>, <<"review">>]}

%% CONTAINS: array must contain value
{contains, [<<"tags">>], <<"important">>}
```

### Existence Checks

```erlang
%% Path must exist
{exists, [<<"email">>]}

%% Path must not exist
{missing, [<<"deleted_at">>]}
```

### Pattern Matching

```erlang
%% Regex match
{regex, [<<"email">>], <<".*@example\\.com$">>}

%% Prefix match (more efficient than regex)
{prefix, [<<"name">>], <<"John">>}
```

## Complete Examples

### Find Active Users Over 18

```erlang
{ok, Results, _} = barrel_docdb:find(<<"mydb">>, #{
    where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"active">>], true},
        {compare, [<<"age">>], '>=', 18}
    ],
    limit => 100
}).
```

### Find Orders by Status with Pagination

```erlang
{ok, Page1, Meta1} = barrel_docdb:find(<<"mydb">>, #{
    where => [
        {path, [<<"type">>], <<"order">>},
        {in, [<<"status">>], [<<"pending">>, <<"processing">>]}
    ],
    order_by => [<<"created_at">>],
    limit => 20
}),

%% Next page using continuation token
case maps:get(has_more, Meta1) of
    true ->
        Token = maps:get(continuation, Meta1),
        {ok, Page2, _} = barrel_docdb:find(<<"mydb">>, #{
            where => [...]
        }, #{continuation => Token});
    false ->
        done
end.
```

### Complex Filter with Nested Conditions

```erlang
{ok, Results, _} = barrel_docdb:find(<<"mydb">>, #{
    where => [
        {path, [<<"type">>], <<"product">>},
        {'or', [
            {'and', [
                {compare, [<<"price">>], '<', 50},
                {path, [<<"category">>], <<"electronics">>}
            ]},
            {'and', [
                {compare, [<<"price">>], '<', 100},
                {path, [<<"on_sale">>], true}
            ]}
        ]}
    ]
}).
```

## Query Optimization

### Explain Query Plan

Use `explain/2` to understand how a query will execute:

```erlang
{ok, Plan} = barrel_docdb:explain(<<"mydb">>, #{
    where => [{path, [<<"type">>], <<"user">>}]
}),

%% Plan contains:
#{
    strategy => index_seek,  %% or: index_scan, multi_index, full_scan
    conditions => [...],
    bindings => #{...}
}
```

### Execution Strategies

| Strategy | Description | Performance |
|----------|-------------|-------------|
| `index_seek` | Direct lookup by indexed path | Best |
| `index_scan` | Range scan on index | Good |
| `multi_index` | Intersect multiple indexes | Good |
| `full_scan` | Scan all documents | Slowest |

### Optimization Tips

1. **Put selective conditions first**: More specific conditions reduce the search space
2. **Use prefix over regex**: `{prefix, Path, Value}` is faster than regex
3. **Avoid NOT on large sets**: Negation requires scanning excluded documents
4. **Limit results**: Always use `limit` for large datasets

## Chunked Query Execution

For large result sets, use chunked execution with continuation tokens to iterate through all matching documents without loading everything into memory.

### Basic Usage

```erlang
%% Get store reference and database name
{ok, StoreRef} = barrel_db_server:get_store_ref(Db),
{ok, Info} = barrel_db_server:info(Db),
DbName = maps:get(name, Info),

%% Compile query once
{ok, Plan} = barrel_query:compile(#{
    where => [{compare, [<<"age">>], '>', 50}],
    include_docs => false
}).

%% First chunk
{ok, Results1, Meta1} = barrel_query:execute(StoreRef, DbName, Plan, #{chunk_size => 100}).
%% Meta1 = #{has_more => true, continuation => Token, last_seq => Seq}

%% Next chunk
Token = maps:get(continuation, Meta1),
{ok, Results2, Meta2} = barrel_query:execute(StoreRef, DbName, Plan, #{continuation => Token}).
%% Continue until has_more => false
```

### Chunk Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `chunk_size` | integer | 1000 | Max results per chunk |
| `continuation` | binary | - | Token from previous chunk |
| `eventual_consistency` | boolean | false | Skip snapshot for faster reads |

### Iterating All Results

```erlang
%% Helper to iterate through all matching documents
iterate_all(StoreRef, DbName, Plan) ->
    iterate_loop(StoreRef, DbName, Plan, #{chunk_size => 500}, []).

iterate_loop(StoreRef, DbName, Plan, Opts, Acc) ->
    case barrel_query:execute(StoreRef, DbName, Plan, Opts) of
        {ok, Results, #{has_more := false}} ->
            lists:flatten(lists:reverse([Results | Acc]));
        {ok, Results, #{has_more := true, continuation := Token}} ->
            iterate_loop(StoreRef, DbName, Plan,
                        #{chunk_size => 500, continuation => Token},
                        [Results | Acc])
    end.
```

### Cursor Management

- Cursors expire after 60 seconds of inactivity
- Each access extends TTL by 60 seconds
- Cursors hold RocksDB snapshots for consistent reads
- Snapshots are released when `has_more => false` or cursor expires

### When to Use Chunked Execution

| Scenario | API | Reason |
|----------|-----|--------|
| Small result sets (<1000 docs) | `barrel_docdb:find/2` | Simpler, returns all at once |
| Large result sets | `barrel_query:execute/4` | Memory efficient, streaming |
| Pagination in HTTP API | `barrel_query:execute/4` | Natural page boundaries |
| Background processing | `barrel_query:execute/4` | Process in batches |

## API Reference

See the [Erlang API Reference](api/erlang.md) for complete function documentation.

- `barrel_docdb:find/2,3` - Execute a query
- `barrel_docdb:explain/2` - Explain query plan
