# Document Format

This guide describes the document format used by barrel_docdb, including document structure, revisions, metadata, and internal storage format.

## Document Structure

Documents in barrel_docdb are Erlang maps with binary keys. A document looks like this:

```erlang
#{
    <<"id">> => <<"user:alice">>,
    <<"_rev">> => <<"2-a1b2c3d4e5f6...">>,
    <<"type">> => <<"user">>,
    <<"name">> => <<"Alice Smith">>,
    <<"email">> => <<"alice@example.com">>,
    <<"created_at">> => <<"2024-01-15T10:30:00Z">>
}
```

### Reserved Keys

The following keys have special meaning:

| Key | Type | Description |
|-----|------|-------------|
| `<<"id">>` | binary | Document identifier. Auto-generated if not provided. |
| `<<"_rev">>` | binary | Revision identifier. Managed by the system. |
| `<<"_deleted">>` | boolean | `true` for deleted documents (tombstones). |
| `<<"_attachments">>` | map | Attachment metadata (internal use). |

Keys starting with `<<"_">>` (underscore) are reserved for system metadata and are stripped from the document body when stored.

### Document ID

The document ID (`<<"id">>`) uniquely identifies a document within a database.

**Rules:**
- Must be a binary string
- Should be URL-safe (avoid special characters)
- Case-sensitive
- Maximum recommended length: 256 bytes

**Auto-generation:**
If no ID is provided, barrel_docdb generates one using:
```
MD5(timestamp_microseconds + 8_random_bytes)
```

**ID Patterns:**
Common patterns for document IDs:

```erlang
%% Simple ID
<<"doc123">>

%% Namespaced ID (recommended for typed documents)
<<"user:alice">>
<<"order:2024-001">>
<<"product:sku-12345">>

%% Hierarchical ID
<<"org:acme/team:engineering/member:alice">>
```

## Revisions

barrel_docdb uses Multi-Version Concurrency Control (MVCC) with revision tracking.

### Revision Format

Revisions follow the format: `<generation>-<hash>`

```
1-a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
│ └─────────────────────────────────── Content hash (SHA-256, hex)
└─────────────────────────────────────── Generation number
```

- **Generation**: Increments with each update (1, 2, 3, ...)
- **Hash**: SHA-256 hash of document content + parent revision + deleted flag

### Revision Tree

Documents maintain a revision tree for conflict detection and resolution:

```
         1-aaa
           │
         2-bbb
        ╱     ╲
     3-ccc   3-ddd    ← Conflict! Two branches
       │
     4-eee
```

The revision tree enables:
- **Conflict detection**: Multiple branches indicate conflicts
- **Replication**: Transfer only missing revisions
- **History tracking**: Audit trail of changes

### Creating Revisions

**New document (no revision):**
```erlang
{ok, Result} = barrel_docdb:put_doc(Db, #{
    <<"id">> => <<"doc1">>,
    <<"value">> => <<"hello">>
}),
%% Result: #{<<"id">> => <<"doc1">>, <<"rev">> => <<"1-abc123...">>}
```

**Update (requires current revision):**
```erlang
{ok, Doc} = barrel_docdb:get_doc(Db, <<"doc1">>),
{ok, Result} = barrel_docdb:put_doc(Db, Doc#{<<"value">> => <<"updated">>}),
%% Result: #{<<"id">> => <<"doc1">>, <<"rev">> => <<"2-def456...">>}
```

**Conflict on stale revision:**
```erlang
%% This will fail if document was modified since Rev1
OldDoc = #{<<"id">> => <<"doc1">>, <<"_rev">> => Rev1, <<"value">> => <<"stale">>},
{error, conflict} = barrel_docdb:put_doc(Db, OldDoc).
```

## Deleted Documents

Deleted documents are tombstones that preserve revision history:

```erlang
%% Delete a document
{ok, Result} = barrel_docdb:delete_doc(Db, <<"doc1">>),
%% Result: #{<<"id">> => <<"doc1">>, <<"rev">> => <<"3-...", <<"ok">> => true}

%% Document no longer returned by default
{error, not_found} = barrel_docdb:get_doc(Db, <<"doc1">>).

%% But can be retrieved with include_deleted option
{ok, Doc} = barrel_docdb:get_doc(Db, <<"doc1">>, #{include_deleted => true}),
%% Doc contains: <<"_deleted">> => true
```

**Why tombstones?**
- Enable proper replication (deletions must propagate)
- Prevent resurrection of deleted documents
- Maintain revision history integrity

## Internal Storage Format

### Document Info (doc_info)

Stored at key: `doc_info/{db_name}/{doc_id}`

```erlang
#{
    id => <<"doc1">>,
    rev => <<"2-abc123...">>,        %% Current winning revision
    deleted => false,
    seq => {0, 42},                   %% Last update sequence
    revtree => #{                     %% Full revision tree
        <<"1-aaa...">> => #{
            id => <<"1-aaa...">>,
            parent => undefined,
            deleted => false
        },
        <<"2-abc123...">> => #{
            id => <<"2-abc123...">>,
            parent => <<"1-aaa...">>,
            deleted => false
        }
    }
}
```

### Document Body

Stored at key: `doc_rev/{db_name}/{doc_id}/{rev_id}`

The document body is stored separately from metadata, containing only user data (no `_rev`, `_deleted`, etc.).

### Sequence Entry

Stored at key: `doc_seq/{db_name}/{sequence}`

Links sequence numbers to document changes for the changes feed.

## Document Validation

### Automatic Validation

barrel_docdb validates:
- Document is a map
- ID is a binary (if provided)
- Revision format is valid (if provided)
- `_deleted` is boolean (if provided)

### Custom Validation

Implement validation in your application layer:

```erlang
validate_user(#{<<"type">> := <<"user">>} = Doc) ->
    case maps:is_key(<<"email">>, Doc) of
        true -> ok;
        false -> {error, missing_email}
    end;
validate_user(_) ->
    {error, invalid_type}.

%% Use before put_doc
case validate_user(Doc) of
    ok -> barrel_docdb:put_doc(Db, Doc);
    Error -> Error
end.
```

## Working with Document Types

### Type Field Convention

Use a `<<"type">>` field to categorize documents:

```erlang
%% User document
#{
    <<"id">> => <<"user:alice">>,
    <<"type">> => <<"user">>,
    <<"name">> => <<"Alice">>
}

%% Order document
#{
    <<"id">> => <<"order:2024-001">>,
    <<"type">> => <<"order">>,
    <<"user_id">> => <<"user:alice">>,
    <<"items">> => [...]
}
```

### Querying by Type

Use a declarative query with automatic path indexing:

```erlang
{ok, Orders, _Meta} = barrel_docdb:find(<<"mydb">>, #{
    where => [{path, [<<"type">>], <<"order">>}]
}).
```

## Data Types

### Supported Value Types

| Erlang Type | JSON Equivalent | Example |
|-------------|-----------------|---------|
| `binary()` | string | `<<"hello">>` |
| `integer()` | number | `42` |
| `float()` | number | `3.14` |
| `true/false` | boolean | `true` |
| `null` | null | `null` |
| `list()` | array | `[1, 2, 3]` |
| `map()` | object | `#{<<"a">> => 1}` |

### Binary Keys

All map keys should be binaries for consistency:

```erlang
%% Good
#{<<"name">> => <<"Alice">>, <<"age">> => 30}

%% Avoid (atom keys)
#{name => <<"Alice">>, age => 30}
```

## Size Limits

### Recommended Limits

| Item | Recommended Limit |
|------|-------------------|
| Document ID | 256 bytes |
| Document size | 1 MB |
| Attachment size | 10 MB |
| Revisions per document | 1000 |

### Large Documents

For documents larger than 1 MB:
- Consider splitting into multiple documents
- Use attachments for binary data
- Store references to external storage

## Examples

### Complete Document Lifecycle

```erlang
%% 1. Create
{ok, R1} = barrel_docdb:put_doc(Db, #{
    <<"id">> => <<"user:alice">>,
    <<"type">> => <<"user">>,
    <<"name">> => <<"Alice">>,
    <<"email">> => <<"alice@example.com">>
}),
Rev1 = maps:get(<<"rev">>, R1),

%% 2. Read
{ok, Doc1} = barrel_docdb:get_doc(Db, <<"user:alice">>),

%% 3. Update
{ok, R2} = barrel_docdb:put_doc(Db, Doc1#{
    <<"name">> => <<"Alice Smith">>,
    <<"updated_at">> => <<"2024-01-15T12:00:00Z">>
}),
Rev2 = maps:get(<<"rev">>, R2),

%% 4. Delete
{ok, R3} = barrel_docdb:delete_doc(Db, <<"user:alice">>),
Rev3 = maps:get(<<"rev">>, R3).

%% Revision progression: 1-xxx -> 2-yyy -> 3-zzz (deleted)
```

## See Also

- [Getting Started](getting-started.md) - Basic usage guide
- [Replication](replication.md) - How revisions enable replication
- [Erlang API Reference](api/erlang.md) - Complete API documentation
