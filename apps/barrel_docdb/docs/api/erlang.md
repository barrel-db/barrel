# Erlang API Reference

The Erlang API provides direct access to Barrel DocDB when embedded in your OTP application.

## Getting Started

```erlang
%% Start the application
application:ensure_all_started(barrel_docdb).
```

---

## Database Operations

### barrel_docdb:create_db/1,2

Create a new database.

```erlang
-spec create_db(DbName :: binary()) -> {ok, pid()} | {error, term()}.
-spec create_db(DbName :: binary(), Opts :: map()) -> {ok, pid()} | {error, term()}.
```

**Example:**
```erlang
{ok, _Pid} = barrel_docdb:create_db(<<"mydb">>).

%% With options
{ok, _Pid} = barrel_docdb:create_db(<<"mydb">>, #{
    data_dir => "/custom/path"
}).
```

### barrel_docdb:delete_db/1

Delete a database.

```erlang
-spec delete_db(DbName :: binary()) -> ok | {error, term()}.
```

**Example:**
```erlang
ok = barrel_docdb:delete_db(<<"mydb">>).
```

### barrel_docdb:list_dbs/0

List all databases.

```erlang
-spec list_dbs() -> [binary()].
```

**Example:**
```erlang
Dbs = barrel_docdb:list_dbs().
%% [<<"mydb">>, <<"otherdb">>]
```

### barrel_docdb:db_info/1

Get database information.

```erlang
-spec db_info(DbName :: binary()) -> {ok, map()} | {error, term()}.
```

**Example:**
```erlang
{ok, Info} = barrel_docdb:db_info(<<"mydb">>).
%% #{name => <<"mydb">>, keyspace => ..., config => ..., db_path => ...,
%%   retention_period => ..., history_floor => Hlc, att_floor => ...}
%% Branches also carry parent and fork_hlc.
```

---

## Document Operations

The concurrency token is the `<<"_rev">>` key. It is a version, not a revision-tree revision: the format is `<hex(hlc)>@<author>`, where the HLC gives causal last-write-wins ordering and the author is the id of the database that made the write. Pass the current token back as `rev` (or `<<"_rev">>` in the body) to update a live document.

### barrel_docdb:put_doc/2,3

Create or update a document.

```erlang
-spec put_doc(Db, Doc) -> {ok, Result} | {error, term()}
    when Db :: binary() | pid(),
         Doc :: map(),
         Result :: #{<<"id">> := binary(), <<"rev">> := binary()}.

-spec put_doc(Db, Doc, Opts) -> {ok, Result} | {error, term()}
    when Db :: binary() | pid(),
         Doc :: map(),
         Opts :: map(),
         Result :: #{<<"id">> := binary(), <<"rev">> := binary()}.
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `rev` | binary() | Current version token, required to update a live document |
| `replicate` | `sync` | Wait for the write to reach replicas before returning |
| `wait_for` | [binary()] | Targets to wait for (with `replicate => sync`) |

**Example:**
```erlang
%% Create new document (ID auto-generated)
{ok, #{<<"id">> := DocId, <<"rev">> := Rev}} =
    barrel_docdb:put_doc(<<"mydb">>, #{
        <<"type">> => <<"user">>,
        <<"name">> => <<"Alice">>
    }).

%% Create with specific ID
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"_id">> => <<"user1">>,
    <<"name">> => <<"Alice">>
}).

%% Update existing document
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"_id">> => DocId,
    <<"_rev">> => Rev,
    <<"name">> => <<"Alice Smith">>
}).
```

### barrel_docdb:get_doc/2,3

Get a document by ID.

```erlang
-spec get_doc(Db, DocId) -> {ok, Doc} | {error, not_found | term()}
    when Db :: binary() | pid(),
         DocId :: binary(),
         Doc :: map().

-spec get_doc(Db, DocId, Opts) -> {ok, Doc} | {error, not_found | term()}
    when Db :: binary() | pid(),
         DocId :: binary(),
         Opts :: map(),
         Doc :: map().
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `rev` | binary() | Get a specific version token instead of the winner |
| `conflicts` | boolean() | Add `<<"_conflicts">>` (a list of live sibling tokens) to the result |
| `include_deleted` | boolean() | Return the document even if it is a tombstone |

**Example:**
```erlang
{ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"user1">>).

%% Surface conflicting sibling versions
{ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"user1">>, #{conflicts => true}).
```

### barrel_docdb:delete_doc/2,3

Delete a document.

```erlang
-spec delete_doc(Db, DocId) -> {ok, Result} | {error, term()}.
-spec delete_doc(Db, DocId, Opts) -> {ok, Result} | {error, term()}.
```

**Example:**
```erlang
{ok, _} = barrel_docdb:delete_doc(<<"mydb">>, <<"user1">>, #{rev => Rev}).
```

---

## Queries

### barrel_docdb:find/2,3

Query documents using declarative conditions.

```erlang
-spec find(Db, Query) -> {ok, Docs, Meta} | {error, term()}
    when Db :: binary() | pid(),
         Query :: map(),
         Docs :: [map()],
         Meta :: map().
```

**Query Options:**

| Option | Type | Description |
|--------|------|-------------|
| `where` | list() | List of conditions |
| `limit` | integer() | Maximum results |
| `offset` | integer() | Skip results |
| `order_by` | path() | Sort field |
| `order` | asc \| desc | Sort direction |

**Condition Types:**

```erlang
%% Path equality
{path, [<<"field">>], Value}

%% Comparison
{path, [<<"age">>], '>=', 18}

%% Boolean logic
{'and', [Cond1, Cond2]}
{'or', [Cond1, Cond2]}
{'not', Cond}

%% Collection operators
{path, [<<"status">>], 'in', [<<"active">>, <<"pending">>]}
{path, [<<"tags">>], 'contains', <<"erlang">>}

%% Pattern matching
{path, [<<"email">>], 'prefix', <<"admin@">>}
{path, [<<"name">>], 'regex', <<"^A.*">>}
```

**Example:**
```erlang
{ok, Users, Meta} = barrel_docdb:find(<<"mydb">>, #{
    where => [
        {path, [<<"type">>], <<"user">>},
        {'and', [
            {path, [<<"age">>], '>=', 18},
            {path, [<<"status">>], <<"active">>}
        ]}
    ],
    limit => 100,
    order_by => [<<"created_at">>],
    order => desc
}).
```

### barrel_docdb:fold_docs/3

Iterate over all documents.

```erlang
-spec fold_docs(Db, Fun, Acc) -> {ok, Acc} | {error, term()}
    when Db :: binary() | pid(),
         Fun :: fun((Doc :: map(), Acc) -> {ok, Acc} | stop),
         Acc :: term().
```

**Example:**
```erlang
{ok, Count} = barrel_docdb:fold_docs(<<"mydb">>,
    fun(Doc, Acc) -> {ok, Acc + 1} end,
    0
).
```

---

## Subscriptions

### barrel_docdb:subscribe/2,3

Subscribe to document changes using MQTT-style path patterns.

```erlang
-spec subscribe(Db, Pattern) -> {ok, Ref} | {error, term()}
    when Db :: binary(),
         Pattern :: binary(),
         Ref :: reference().
```

**Pattern Syntax:**

| Pattern | Description |
|---------|-------------|
| `type/user` | Exact match |
| `type/user/#` | Match prefix |
| `type/+/active` | Single-level wildcard |

**Example:**
```erlang
%% Subscribe to all user changes
{ok, Ref} = barrel_docdb:subscribe(<<"mydb">>, <<"type/user/#">>),

%% Receive changes
receive
    {barrel_change, Ref, Change} ->
        io:format("Change: ~p~n", [Change])
end.

%% Unsubscribe
barrel_docdb:unsubscribe(Ref).
```

---

## Changes Feed

### barrel_docdb:get_changes/2,3

Get changes since an HLC timestamp.

```erlang
-spec get_changes(Db, Since) -> {ok, Changes, LastHlc} | {error, term()}
    when Db :: binary() | pid(),
         Since :: barrel_hlc:timestamp() | first,
         Changes :: [map()],
         LastHlc :: barrel_hlc:timestamp().

-spec get_changes(Db, Since, Opts) -> {ok, Changes, LastHlc} | {error, term()}
    when Db :: binary() | pid(),
         Since :: barrel_hlc:timestamp() | first,
         Opts :: map(),
         Changes :: [map()],
         LastHlc :: barrel_hlc:timestamp().
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `limit` | integer() | Maximum changes to return |
| `include_docs` | boolean() | Include full documents |
| `descending` | boolean() | Reverse order |
| `doc_ids` | [binary()] | Filter to specific document IDs |

**Example:**
```erlang
%% Get all changes
{ok, Changes, LastHlc} = barrel_docdb:get_changes(<<"mydb">>, first).

%% Get incremental changes
{ok, NewChanges, NewHlc} = barrel_docdb:get_changes(<<"mydb">>, LastHlc).

%% With options
{ok, Changes, _} = barrel_docdb:get_changes(<<"mydb">>, first, #{limit => 100}).
```

---

## Conflicts

Concurrent writes to the same document (from replication) do not fail. The database keeps a deterministic last-write-wins winner and retains the losing versions as live conflict siblings. Use these functions to surface and resolve them.

### barrel_docdb:get_conflicts/2

List the tokens of the live siblings that conflict with the current winner. An empty list means no conflicts.

```erlang
-spec get_conflicts(Db, DocId) -> {ok, [binary()]} | {error, term()}
    when Db :: binary() | pid(),
         DocId :: binary().
```

**Example:**
```erlang
{ok, Conflicts} = barrel_docdb:get_conflicts(<<"mydb">>, <<"doc1">>).
%% Conflicts = [<<"0000018abc...@f1e0...">>]
```

### barrel_docdb:resolve_conflict/4

Resolve a conflict by either choosing one existing sibling or writing a merged body. `BaseRev` is the current winning token (optimistic lock).

```erlang
-spec resolve_conflict(Db, DocId, BaseRev, Resolution) -> {ok, map()} | {error, term()}
    when Db :: binary() | pid(),
         DocId :: binary(),
         BaseRev :: binary(),
         Resolution :: {choose, binary()} | {merge, map()}.
```

**Example:**
```erlang
%% Keep one existing version, drop the others
{ok, _} = barrel_docdb:resolve_conflict(<<"mydb">>, <<"doc1">>,
    Winner, {choose, ConflictToken}).

%% Write a merged body that supersedes all siblings
{ok, _} = barrel_docdb:resolve_conflict(<<"mydb">>, <<"doc1">>,
    Winner, {merge, #{<<"name">> => <<"Merged">>}}).
```

---

## Branches

A branch is an instant fork of a database (both stores checkpointed via hard links). The branch opens as a normal database with its own author id; merging replays the branch's writes back into the parent through the version protocol.

### barrel_docdb:branch_db/3

```erlang
-spec branch_db(Parent, BranchName, Opts) -> {ok, pid()} | {error, term()}
    when Parent :: binary(),
         BranchName :: binary(),
         Opts :: map().
```

**Example:**
```erlang
{ok, _Pid} = barrel_docdb:branch_db(<<"mydb">>, <<"mydb-exp">>, #{}).
```

### barrel_docdb:list_branches/1

List the open branches of a database.

```erlang
-spec list_branches(Parent :: binary()) -> [binary()].
```

### barrel_docdb:merge_branch/2

Merge a branch's edits back into its parent.

```erlang
-spec merge_branch(Branch :: binary(), Opts :: map()) -> {ok, map()} | {error, term()}.
```

---

## Replication Primitives

These low-level functions implement the version-vector replication protocol used by transports. Most users should use `barrel_rep:replicate/2,3` instead.

### barrel_docdb:get_doc_for_replication/2

Read the current version of a document (tombstones included) with the metadata a transport needs to ship it.

```erlang
-spec get_doc_for_replication(Db, DocId) ->
    {ok, #{doc := map(), version := binary(), vv := binary(), deleted := boolean()}}
    | {error, term()}
    when Db :: binary(),
         DocId :: binary().
```

- `version` - the version token (`<hex(hlc)>@<author>`)
- `vv` - the document's encoded version vector
- `deleted` - whether this version is a tombstone

### barrel_docdb:diff_versions/2

The replication diff. Given `#{DocId => VersionToken}`, answer which offered versions this database does not already cover. `have` means the document's version vector contains the offered version.

```erlang
-spec diff_versions(Db, TokenMap) -> {ok, #{binary() => missing | have}} | {error, term()}
    when Db :: binary() | pid(),
         TokenMap :: #{binary() => binary()}.
```

**Example:**
```erlang
{ok, Diff} = barrel_docdb:diff_versions(<<"target">>, #{
    <<"doc1">> => <<"0000018abc...@f1e0...">>,
    <<"doc2">> => <<"0000018def...@a2b3...">>
}).
%% Diff = #{<<"doc1">> => have, <<"doc2">> => missing}
```

### barrel_docdb:put_version/5

Apply a replicated version. The source's token and vector are preserved; only the change-sequence HLC is issued locally. Outcomes: an already-covered version is an idempotent no-op, a dominating version fast-forwards the document, and a concurrent version creates a conflict sibling with a deterministic last-write-wins winner.

```erlang
-spec put_version(Db, Doc, VersionToken, VVBin, Deleted) ->
    {ok, DocId, WinnerToken} | {error, term()}
    when Db :: binary() | pid(),
         Doc :: map(),
         VersionToken :: binary(),
         VVBin :: binary(),
         Deleted :: boolean(),
         DocId :: binary(),
         WinnerToken :: binary().
```

**Example:**
```erlang
{ok, Doc, #{version := Token, vv := VVBin, deleted := Del}} =
    barrel_docdb:get_doc_for_replication(<<"source">>, <<"doc1">>),
{ok, _DocId, _Winner} =
    barrel_docdb:put_version(<<"target">>, Doc, Token, VVBin, Del).
```

---

## Replication

### barrel_rep:replicate/2,3

One-shot replication between databases.

```erlang
-spec replicate(Source, Target) -> {ok, Result} | {error, term()}.
-spec replicate(Source, Target, Opts) -> {ok, Result} | {error, term()}.
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `filter` | map() | Filter documents |
| `source_transport` | module() | Source transport |
| `target_transport` | module() | Target transport |

**Example:**
```erlang
%% Local to local
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>).

%% Local to remote
{ok, Result} = barrel_rep:replicate(<<"mydb">>, <<"http://remote:8080/db/mydb">>, #{
    source_transport => barrel_rep_transport_local,
    target_transport => barrel_rep_transport_http
}).

%% With filter
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        paths => [<<"users/#">>],
        query => #{where => [{path, [<<"active">>], true}]}
    }
}).
```

