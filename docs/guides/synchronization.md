# Synchronization

Barrel replicates documents between databases with a version-vector protocol:
causal ordering from a hybrid logical clock (HLC), deterministic winners,
idempotent delivery, and checkpoints for resumption. Replication runs in the
same VM or over HTTP against a `barrel_server`. Read this when you need to
copy or keep two databases in sync, replicate a subset with channels, or move
attachments along with documents.

## What it is

Replication lives in `barrel_docdb` (`barrel_rep`). One run:

1. Read the checkpoint (a local doc on both sides) to find the last shipped HLC.
2. Fetch changes from the source since that HLC, filter applied at the source.
3. Diff the offered version tokens against the target in one round trip.
4. `put_version` the missing documents on the target (idempotent, so
   at-least-once delivery is safe).
5. Checkpoint per batch, then run the attachment phase (see below).

Both sides converge to the same winner, body, and deletion state for every
document, whatever the interleaving of writes. Conflicts are recorded where
the concurrency was observed and resolve by writing a superseding version.

Transports implement the `barrel_rep_transport` behaviour. Two ship today:
`barrel_rep_transport_local` (same VM, database names) and
`barrel_rep_transport_http` (hackney client speaking the
`/db/:db/_sync/*` wire served by `barrel_server`).

## When to use it

- Copy a database to another database, in the same VM or on another node.
- Keep replicas eventually consistent, one-shot per run or continuously.
- Replicate a subset of documents with a channel, path, or query filter.
- Move attachment blobs with their documents, content-addressed.

## How (same VM)

```erlang
{ok, _} = barrel_docdb:create_db(<<"source">>),
{ok, _} = barrel_docdb:create_db(<<"target">>),
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{<<"id">> => <<"a">>, <<"v">> => 1}),

{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>),
#{docs_written := N, att_sync := _} = Result.
```

Run it again later to pick up new changes; it resumes from the checkpoint.

## How (over HTTP)

The remote side is a URL under a running `barrel_server`. Build an endpoint,
then pass the matching transport:

```erlang
Endpoint = barrel_rep_transport_http:endpoint(
    <<"http://edge-1.example.com:8080/db/inventory">>),

%% push
{ok, _} = barrel_rep:replicate(<<"inventory">>, Endpoint,
    #{target_transport => barrel_rep_transport_http}),

%% pull
{ok, _} = barrel_rep:replicate(Endpoint, <<"inventory">>,
    #{source_transport => barrel_rep_transport_http}).
```

`endpoint/1` normalizes the URL (scheme and host lowercased, no trailing
slash). The normalized URL is the replication identity: changing its text
starts a fresh checkpoint, credentials and tuning do not. The endpoint map
also takes `pool`, `connect_timeout`, `recv_timeout`, and `headers`.

## How (auth)

Protect a server with static bearer tokens; a list accepts old and new during
a rotation. `/health` stays open, everything else requires the token:

```erlang
%% server side (app env, before barrel_server starts)
application:set_env(barrel_server, auth, #{tokens => [<<"s3cret">>]}).
```

Give the client its token on the endpoint, or keyed by origin in the app env
so persisted task configs stay secret-free:

```erlang
Authed = Endpoint#{auth => #{token => <<"s3cret">>}},
%% or
application:set_env(barrel_docdb, sync_auth,
                    #{<<"http://edge-1.example.com:8080">> => <<"s3cret">>}).
```

## How (selective replication)

Filters apply at the source. Path and query filters compose with AND:

```erlang
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        paths => [<<"users/#">>],
        query => #{where => [{path, [<<"status">>], <<"active">>}]}
    }
}).
```

Channels are the indexed alternative: named pattern sets declared at database
creation, materialized into a per-channel feed at write time, so a filtered
pull is one bounded scan instead of a full feed walk:

```erlang
{ok, _} = barrel_docdb:create_db(<<"source">>, #{
    channels => #{<<"mobile">> => [<<"type/task">>, <<"owner/+">>]}
}),
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{channel => <<"mobile">>}
}).
```

Channel notes:

- Patterns are MQTT style (`+` one segment, trailing `#` for the rest).
- Channels are fixed at creation and index writes made after creation.
- A document that stops matching stops being sent; replicas keep their last
  copy (no target-side delete). A fresh replica never sees departed docs.
- A filtered replication keeps its own checkpoint (the filter joins the
  replication id).

## Attachments

Attachment sync rides every run (`attachments => true` by default). It is a
second phase after the document loop: the source's attachment feed is
digest-diffed against the target, only missing content transfers, and writes
converge last-write-wins on their origin HLC. The result lands under
`att_sync` in the replication result:

```erlang
{ok, #{att_sync := #{atts_written := _, atts_skipped := _}}} =
    barrel_rep:replicate(<<"source">>, Endpoint,
        #{target_transport => barrel_rep_transport_http}).
```

Notes:

- Blobs stream in chunks both directions over HTTP; nothing buffers a blob
  whole. Pulls have no size ceiling. Pushes are currently capped at 8 MiB per
  blob by the HTTP engine's request parser; a larger push fails for that
  attachment only and the run reports it in `att_write_failures`.
- There is no ordering between documents and their attachments: a doc can
  arrive before its blob (reads 404 until the attachment phase lands).
- Attachments written before this feature do not sync until rewritten, or
  synthesize feed rows once with `barrel_docdb:rebuild_attachment_feed/1`.
- Turn the phase off with `attachments => false`; it degrades to
  `att_sync => skipped` on its own when a side cannot track attachments.

## Continuous replication (tasks)

The task manager (`barrel_rep_tasks`) persists replications across restarts.
Remote ends are URLs; the HTTP transport is picked automatically:

```erlang
{ok, TaskId} = barrel_rep_tasks:start_task(#{
    source => <<"inventory">>,
    target => <<"http://edge-1.example.com:8080/db/inventory">>,
    mode => continuous,          %% or one_shot
    direction => push,           %% push | pull | both
    filter => #{channel => <<"mobile">>}
}),
{ok, #{status := running}} = barrel_rep_tasks:get_task(TaskId),
ok = barrel_rep_tasks:stop_task(TaskId).
```

Continuous behavior:

- Local sources are event driven: the task wakes on the changes stream and
  drains through its filter, so local convergence is tens of milliseconds.
- Remote sources poll adaptively, 500 ms after data and backing off to 15 s
  while idle.
- Transient errors do not kill a continuous task: it backs off (1 s to 60 s,
  with jitter), records `last_error` on the task doc, and stays `running`.
  One-shot tasks fail fast.
- Task docs store endpoints as URLs only; tokens come from `sync_auth`.

## Clocks

Every wire exchange carries an `x-barrel-hlc` header and both sides fold it
into their clock, on top of the explicit handshake at the start of each run.
A peer too far ahead is rejected with `{error, clock_skew}` (409 on the
wire). To couple clocks by hand:

```erlang
{ok, _Merged} = barrel_docdb:sync_hlc(RemoteHlc).
```

## What is not covered yet

- **TLS serving.** `barrel_server` listens on plain HTTP; the client can
  reach https servers. Put a TLS terminator in front for now.
- **Vectors.** Vector indexes are not shipped; with record mode the text and
  metadata replicate as documents and the index rebuilds locally. Quantized
  vector sync is planned with the TypeScript client.
- **Facade.** `barrel` (the embeddable facade) does not expose replication;
  call `barrel_rep` and `barrel_rep_tasks` on the underlying docdb name.
