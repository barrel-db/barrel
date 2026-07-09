# Document Format

This guide describes the document format used by barrel_docdb, including document structure, versions, metadata, and internal storage format.

## Document Structure

Documents in barrel_docdb are Erlang maps with binary keys. A document looks like this:

```erlang
#{
    <<"id">> => <<"user:alice">>,
    <<"_rev">> => <<"0000018abc1234...@f1e061a70714abcd">>,
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
| `<<"_rev">>` | binary | Version token, format `<hex(hlc)>@<author>`. Managed by the system; pass it back to update a live document. |
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

## Versions

barrel_docdb uses Multi-Version Concurrency Control (MVCC) based on Hybrid Logical Clocks, not revision trees. Each write is a **version**, and each document carries a **version vector** used for replication and conflict detection.

### Version Token

The `<<"_rev">>` value is a version token: `<hex(hlc)>@<author>`

```
0000018abc1234...@f1e061a70714abcd
└────────┬───────┘ └──────┬───────┘
   hex(HLC), fixed      author (writing database's source id)
```

- **HLC**: a Hybrid Logical Clock giving causal last-write-wins ordering. It is fixed width, so token order equals causal order.
- **author**: the id of the database that made the write (per-database, so writes are attributable and two databases detect each other's writes as concurrent).

The token is opaque: treat it as a value to read and pass back, not to parse.

### Conflicts

There is no revision tree. When two databases write the same document concurrently (neither version's vector dominates the other), replication keeps a deterministic last-write-wins winner and retains the losing version as a live conflict sibling. See the [Replication Guide](replication.md) for how conflicts are surfaced (`get_conflicts/2`) and resolved (`resolve_conflict/4`).

### Creating Versions

**New document (no token):**
```erlang
{ok, Result} = barrel_docdb:put_doc(Db, #{
    <<"id">> => <<"doc1">>,
    <<"value">> => <<"hello">>
}),
%% Result: #{<<"id">> => <<"doc1">>, <<"rev">> => <<"0000018abc...@f1e0...">>}
```

**Update (requires the current token):**
```erlang
{ok, Doc} = barrel_docdb:get_doc(Db, <<"doc1">>),
{ok, Result} = barrel_docdb:put_doc(Db, Doc#{<<"value">> => <<"updated">>}),
%% Result carries a new token with a later HLC
```

**Conflict on a stale token:**
```erlang
%% This fails if the document was modified since Rev1
OldDoc = #{<<"id">> => <<"doc1">>, <<"_rev">> => Rev1, <<"value">> => <<"stale">>},
{error, conflict} = barrel_docdb:put_doc(Db, OldDoc).
```

## Deleted Documents

Deleting a document writes a tombstone: a new version with the deleted flag set.

```erlang
%% Delete a document
{ok, Result} = barrel_docdb:delete_doc(Db, <<"doc1">>, #{rev => Rev}),
%% Result carries a new (deleted) version token

%% Document no longer returned by default
{error, not_found} = barrel_docdb:get_doc(Db, <<"doc1">>).

%% But can be retrieved with the include_deleted option
{ok, Doc} = barrel_docdb:get_doc(Db, <<"doc1">>, #{include_deleted => true}),
%% Doc contains: <<"_deleted">> => true
```

**Why tombstones?**
- Enable proper replication (deletions must propagate as versions)
- Prevent resurrection of deleted documents

## Internal Storage Format

### Document Entity

The document is stored as one wide-column entity (key prefix `DOC_ENTITY`, 0x18), a fixed positional binary. The body and its version metadata live together:

```
<<VerLen:16, Version/binary,        %% COL_VERSION: encoded {HLC, Author}
  Deleted:8,                        %% COL_DELETED
  HlcLen:16, Hlc/binary,            %% COL_HLC: change-sequence HLC
  VVLen:32, VV/binary,              %% COL_VV: encoded version vector
  CreatedAtLen:16, CreatedAt/binary,%% COL_CREATED_AT
  ExpiresAt:64,                     %% COL_EXPIRES_AT (TTL)
  Tier:8,                           %% COL_TIER (hot/warm/cold)
  NConflicts:16,                    %% COL_NCONFLICTS: live conflict siblings
  Ext/binary>>                      %% optional extension tail
```

The `COL_VERSION` / `COL_VV` / `COL_NCONFLICTS` columns replaced the old rev-tree fields. The extension tail is backward compatible: an embedding appends `<<EmbLen:32, Emb/binary, Src:8>>`; entities also carrying provenance use a tagged form. This codec is version v4 (see `barrel_store_rocksdb:encode_entity/1`).

### Version Chain and History

Non-winning versions are retained, not discarded:

- **Version chain** (`DOC_VERSION`, 0x1D): one row per superseded or conflict sibling, keyed by `{doc_id, version}`, with the version's body archived under its token so it stays resolvable within the retention window.
- **Retained history log** (`HISTORY`, 0x1C): an append-only, HLC-ordered log of every write (local, replicated, resolution), carrying identity only (no bodies).

A retention sweep prunes both below the history floor.

### Change Entry

Stored at key prefix `DOC_HLC` (0x0D), keyed by the change-sequence HLC. Links each write to the changes feed. Path and prefix indexes (`PATH_HLC`, `PREFIX_CHANGES`) mirror the same change for filtered and wildcard change queries.

## Document Validation

### Automatic Validation

barrel_docdb validates:
- Document is a map
- ID is a binary (if provided)
- `_rev` version token is well-formed (if provided)
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
| Conflict siblings per document | 8 (`max_conflict_versions`) |

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
{ok, R3} = barrel_docdb:delete_doc(Db, <<"user:alice">>, #{rev => Rev2}),
Rev3 = maps:get(<<"rev">>, R3).

%% Each step mints a new version with a later HLC; the last is a tombstone
```

## See Also

- [Getting Started](getting-started.md) - Basic usage guide
- [Replication](replication.md) - How versions enable replication
- [API reference](api-reference.html) - Complete API documentation
