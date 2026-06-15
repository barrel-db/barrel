# Synchronization

Barrel replicates documents between databases using a CouchDB-style incremental
protocol: causal ordering with a hybrid logical clock (HLC), revision trees for
conflict tracking, and checkpoints for resumption. Read this when you need to
copy or keep two databases in sync, or to understand what sync does and does not
cover today.

## What it is

Replication is a one-shot pull from a source database to a target. It lives in
`barrel_docdb` (`barrel_rep`). It is incremental (only changed revisions move),
resumable (checkpointed), causal (HLC), and conflict-aware (revision trees).

How one run works:

1. Read the checkpoint to find the last replicated sequence.
2. Fetch changes from the source since that sequence.
3. For each change, call `revsdiff` on the target to find missing revisions.
4. Fetch the missing revisions with their history and `put_rev` them on the target.
5. Write a checkpoint after each batch.

## When to use it

- Copy a database to another database in the same Erlang VM.
- Keep a target eventually consistent with a source by running replication
  repeatedly (each run resumes from its checkpoint).
- Selectively replicate a subset of documents with a path or query filter.

## How (replicate all)

```erlang
{ok, _} = barrel_docdb:create_db(<<"source">>),
{ok, _} = barrel_docdb:create_db(<<"target">>),
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{<<"id">> => <<"a">>, <<"v">> => 1}),

{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>),
#{docs_written := N, last_seq := _} = Result.
```

Run it again later to pick up new changes; it resumes from the checkpoint.

## How (selective replication)

Filters use AND logic: a document must match every filter given.

```erlang
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        paths => [<<"users/#">>],
        query => #{where => [{path, [<<"status">>], <<"active">>}]}
    }
}).
```

## Causality and conflicts

- Every revision is ordered by an HLC timestamp. When you receive a clock from
  another node, merge it so local events stay causally after it:

  ```erlang
  {ok, _Merged} = barrel_docdb:sync_hlc(RemoteHlc).   %% {error, clock_skew} if too far apart
  ```

- Concurrent edits to the same document produce conflicting revisions in the
  revision tree (they are tracked, not lost). Resolve a conflict by writing a new
  revision whose history supersedes the conflicting ones.

## Transport

Replication talks to databases through the `barrel_rep_transport` behaviour. The
only implementation today is `barrel_rep_transport_local`, which works between
databases in the same Erlang VM. Pass a custom module to replicate over a
network:

```erlang
{ok, _} = barrel_rep:replicate(Source, Target, #{
    source_transport => barrel_rep_transport_local,
    target_transport => my_http_transport
}).
```

## What is not covered yet

- **Network replication.** Only the local (same-VM) transport ships today. An
  HTTP/gRPC transport is planned (Phase 3).
- **Attachments and vectors.** Replication carries documents and their revision
  history only. Attachment blobs and vector indexes are not replicated. To
  replicate a vector's text and metadata, configure `barrel_vectordb` with the
  docdb-backed docstore so they are stored as documents; the vector index itself
  is local and is rebuilt, not shipped.
- **Facade and server.** `barrel` (the embeddable facade) and `barrel_server`
  (REST) do not expose replication yet. Call `barrel_rep` directly on the
  underlying docdb database name.
