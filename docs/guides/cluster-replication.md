# Replicate inside an Erlang cluster

When your nodes already form an Erlang cluster, a database on one node can
replicate with a database on another over distributed Erlang: no HTTP, no
`barrel_server`, no URLs. Read this when your application embeds
barrel_docdb on several connected nodes and each node keeps its own copy of
a database. To replicate across sites or with nodes outside the cluster, use
[HTTP replication](synchronization.md) instead.

## What it is

The endpoint for a database on another node is a map:

```erlang
#{node => 'app@10.0.0.2', db => <<"config">>}      %% optional: timeout => Ms (default 30000)
```

The transport (`barrel_rep_transport_dist`) runs every call on the node that
owns the database, through `erpc`. Replication works as over HTTP: version
vectors, the same winner and conflicts on every node, checkpoints, filters,
attachments. Attachments move one chunk per message, each acknowledged, so a
large one never holds up the connection.

It runs over whatever carrier your cluster uses: TCP, TLS or QUIC
distribution.

## Keep two nodes in sync

Start one continuous task on one of the nodes. With `direction => both` it
pushes and pulls:

```erlang
{ok, _} = barrel_docdb:create_db(<<"config">>),          %% on both nodes
{ok, TaskId} = barrel_rep_tasks:start_task(#{
    source => <<"config">>,
    target => #{node => 'app@10.0.0.2', db => <<"config">>},
    mode => continuous,
    direction => both
}).
```

The target database must exist on the other node; it is opened there if it is
closed. With three nodes or more, start a task from each node to each other
node it should exchange with, or from one hub to every other node.

Changes on the local side wake the task at once. Changes on the other node
wake it too: the task subscribes to that node's changes feed, and polls if
the subscription goes away.

## Replicate once

```erlang
{ok, #{docs_written := N}} =
    barrel_rep:replicate(<<"config">>, #{node => 'app@10.0.0.2', db => <<"config">>},
                         #{target_transport => barrel_rep_transport_dist}).
```

## When a node goes away

A continuous task keeps running: each call to the lost node fails with
`{error, {transport, noconnection}}`, the task records it in `last_error`,
and it backs off (1 s up to 60 s). Once the node is connected again, the task
resumes from its checkpoint and the writes made meanwhile go through.

The task does not connect nodes. Your cluster setup (`net_kernel:connect_node/1`,
a discovery library) does.

## Run the cluster over QUIC

erlang_quic's `quic_dist` carries distribution over QUIC, with TLS 1.3. Add
`quic` to your release, then on each node:

`vm.args`:

```
-name app@10.0.0.1
-setcookie a-long-random-cookie
-proto_dist quic
-epmd_module quic_epmd
-start_epmd false
-quic_dist_port 4433
-quic_dist_cert /etc/app/cert.pem
-quic_dist_key /etc/app/key.pem
```

`sys.config`:

```erlang
{quic, [{dist, [{cert_file, "/etc/app/cert.pem"},
                {key_file, "/etc/app/key.pem"},
                {cacert_file, "/etc/app/ca.pem"},
                {verify, verify_peer},
                {discovery_module, quic_discovery_static},
                {nodes, [{'app@10.0.0.1', {"10.0.0.1", 4433}},
                         {'app@10.0.0.2', {"10.0.0.2", 4433}}]}]}]}
```

With `verify_peer`, each node's certificate must be signed by the CA in
`cacert_file`, carry both TLS server and client usage, and name its host in
an IP or DNS SAN. The replication task is the same as above.

## Notes

- **Trust.** Nodes that share the cookie can do anything on each other,
  replication included. There are no per-database rights over distributed
  Erlang. Use a long random cookie, and TLS or QUIC distribution between
  hosts.
- **Endpoints in task docs.** The endpoint holds no secret, and the task doc
  stores it as is.
- **Backups.** A node restored from a backup is a new author: its peers
  resync it rather than resume. See [Back up and restore](backup.md).
