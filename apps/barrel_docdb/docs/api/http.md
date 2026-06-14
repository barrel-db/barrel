# HTTP API Reference

Barrel DocDB provides a RESTful HTTP API for all operations. The server listens on port 8080 by default.

## Server Configuration

### Starting the HTTP Server

```erlang
%% Basic start (HTTP/1.1 and HTTP/2 cleartext)
barrel_http_server:start_link(#{port => 8080}).

%% With TLS (HTTPS with HTTP/2 ALPN)
barrel_http_server:start_link(#{
    port => 8443,
    certfile => "/path/to/cert.pem",
    keyfile => "/path/to/key.pem"
}).

%% Full configuration
barrel_http_server:start_link(#{
    port => 8443,
    num_acceptors => 100,
    max_connections => 10000,
    protocols => [http2, http],
    certfile => "/path/to/cert.pem",
    keyfile => "/path/to/key.pem",
    cacertfile => "/path/to/ca.pem",
    verify => verify_peer
}).
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `port` | integer | 8080 | Listen port |
| `num_acceptors` | integer | 100 | Number of acceptor processes |
| `max_connections` | integer | infinity | Maximum concurrent connections |
| `protocols` | list | `[http2, http]` | Enabled protocols |
| `certfile` | string | - | Path to TLS certificate (enables HTTPS) |
| `keyfile` | string | - | Path to TLS private key |
| `cacertfile` | string | - | Path to CA certificate (optional) |
| `verify` | atom | `verify_none` | TLS verification: `verify_none` or `verify_peer` |

### HTTP/2 Support

The server supports HTTP/2 with automatic degradation to HTTP/1.1:

**HTTPS Mode (recommended for production):**

- Uses ALPN (Application-Layer Protocol Negotiation) to negotiate HTTP/2 or HTTP/1.1
- Requires TLS certificates (`certfile` and `keyfile`)
- Clients that support HTTP/2 will use it automatically
- Legacy clients fall back to HTTP/1.1

**HTTP Mode (cleartext):**

- Supports HTTP/2 cleartext (h2c) via:
  - HTTP/2 prior knowledge (client sends HTTP/2 preface directly)
  - HTTP/1.1 Upgrade header
- Falls back to HTTP/1.1 for clients that don't support h2c
- Suitable for internal/trusted networks

### Get Server Info

```erlang
{ok, Info} = barrel_http_server:get_info().
%% Info = #{port => 8080, tls => false, protocols => [http2, http], http2 => true, http11 => true}
```

### Environment Variables (for Releases)

When running as a release, configure the HTTP server via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_HTTP_ENABLED` | `true` | Enable/disable HTTP server |
| `BARREL_HTTP_PORT` | `8080` | Listen port |
| `BARREL_HTTP_MAX_BODY_BYTES` | `4194304` | Cap on buffered request body size (413 on oversize). Streamed attachment uploads bypass this by design. |
| `BARREL_HTTP_MAX_IN_FLIGHT` | `1000` | Maximum concurrent in-flight requests (503 above the cap). |
| `BARREL_QUERY_TIMEOUT_MS` | `30000` | Per-query deadline for the document-fetch pipeline. On expiry the API returns 504 with a structured `query_timeout` payload. |
| `BARREL_REPLICATION_REQUIRE_REGISTERED_PEER` | `true` | When set, `/_replicate` only allows targets present in the peer registry and inbound `_revsdiff` / `_put_rev` / `_sync_hlc` require an Ed25519-signed request from a registered peer. Set to `false` only during migration. Scheduled for removal in 0.8.0. |
| `BARREL_HTTP_TLS_ENABLED` | `false` | Enable HTTPS with HTTP/2 ALPN |
| `BARREL_HTTP_CERTFILE` | - | Path to TLS certificate |
| `BARREL_HTTP_KEYFILE` | - | Path to TLS private key |
| `BARREL_HTTP_CACERTFILE` | - | Path to CA certificate (optional) |
| `BARREL_HTTP_VERIFY` | `verify_none` | TLS verification (`verify_none` or `verify_peer`) |

**Example - HTTPS with HTTP/2:**

```bash
export BARREL_HTTP_PORT=8443
export BARREL_HTTP_TLS_ENABLED=true
export BARREL_HTTP_CERTFILE=/etc/barrel/server.pem
export BARREL_HTTP_KEYFILE=/etc/barrel/server-key.pem
./bin/barrel_docdb start
```

**Example - HTTP/1.1 only:**

```bash
# In sys.config.src, protocols can be configured
# For HTTP/1.1 only, edit sys.config:
# {http_protocols, [http]}
```

## Content Types

The API supports both JSON and CBOR:

- `application/json` (default)
- `application/cbor`

Set the `Content-Type` and `Accept` headers accordingly.

## Endpoints Overview

| Endpoint | Methods | Description |
|----------|---------|-------------|
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |
| `/db/:db` | GET, PUT, POST, DELETE | Database (PUT=create db, POST=create doc with auto-ID) |
| `/db/:db/:doc_id` | GET, PUT, DELETE | Document operations |
| `/db/:db/_find` | POST | Query documents |
| `/db/:db/_changes` | GET | Changes feed |
| `/db/:db/_changes/stream` | GET | SSE changes stream |
| `/db/:db/_bulk_docs` | POST | Bulk operations |
| `/db/:db/_replicate` | POST | Trigger replication |
| `/db/:db/:doc_id/_attachments/:name` | GET, PUT, DELETE | Attachments |
| `/db/:db/_revsdiff`, `/db/:db/_put_rev` | POST | Replication primitives |
| `/db/:db/_local/:doc_id` | GET, PUT, DELETE | Local (replication checkpoint) documents |

---

## Health & Metrics

### Health Check

```bash
GET /health
```

Returns server health status including database information.

**Response:**
```json
{
  "status": "ok",
  "databases": [
    {"name": "mydb", "doc_count": 1234}
  ]
}
```

### Prometheus Metrics

```bash
GET /metrics
```

Returns metrics in Prometheus text format.

---

## Database Operations

### Create Database

```bash
PUT /db/:db
```

**Example:**
```bash
curl -X PUT http://localhost:8080/db/mydb
```

**Response:** `201 Created`

### Get Database Info

```bash
GET /db/:db
```

**Response:**
```json
{
  "name": "mydb",
  "doc_count": 1234,
  "update_seq": "1-abc123"
}
```

### Delete Database

```bash
DELETE /db/:db
```

**Response:** `200 OK`

---

## Document Operations

### Create Document (Auto-Generated ID)

```bash
POST /db/:db
```

Creates a new document with an auto-generated ID.

**Headers:**
- `Content-Type: application/json`

**Example:**
```bash
curl -X POST http://localhost:8080/db/mydb \
  -H "Content-Type: application/json" \
  -d '{"type": "user", "name": "Alice", "email": "alice@example.com"}'
```

**Response:**
```json
{
  "ok": true,
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "rev": "1-abc123"
}
```

### Create/Update Document (Specific ID)

```bash
PUT /db/:db/:doc_id
```

**Headers:**
- `Content-Type: application/json`
- `If-Match: <rev>` (optional, for updates)

**Example - Create:**
```bash
curl -X PUT http://localhost:8080/db/mydb/user1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'
```

**Example - Update:**
```bash
curl -X PUT http://localhost:8080/db/mydb/user1 \
  -H "Content-Type: application/json" \
  -H "If-Match: 1-abc123" \
  -d '{"name": "Alice Smith", "email": "alice@example.com"}'
```

**Response:**
```json
{
  "ok": true,
  "id": "user1",
  "rev": "1-abc123"
}
```

### Get Document

```bash
GET /db/:db/:doc_id
```

**Query Parameters:**

| Parameter | Description |
|-----------|-------------|
| `rev` | Specific revision |
| `revs` | Include revision history |
| `conflicts` | Include conflicting revisions |

**Example:**
```bash
curl http://localhost:8080/db/mydb/user1
```

**Response:**
```json
{
  "_id": "user1",
  "_rev": "1-abc123",
  "name": "Alice",
  "email": "alice@example.com"
}
```

### Delete Document

```bash
DELETE /db/:db/:doc_id
```

**Headers:**
- `If-Match: <rev>` (required)

**Example:**
```bash
curl -X DELETE http://localhost:8080/db/mydb/user1 \
  -H "If-Match: 1-abc123"
```

---

## Queries

### Find Documents

```bash
POST /db/:db/_find
```

**Request Body:**
```json
{
  "where": [
    {"path": ["type"], "value": "user"},
    {"path": ["age"], "op": ">=", "value": 18}
  ],
  "limit": 100,
  "offset": 0
}
```

**Operators:**

| Operator | Description |
|----------|-------------|
| `=` (default) | Equals |
| `>`, `>=`, `<`, `<=` | Comparisons |
| `!=` | Not equals |
| `in` | Value in list |
| `contains` | Array contains value |
| `prefix` | String prefix match |
| `regex` | Regular expression |

**Example:**
```bash
curl -X POST http://localhost:8080/db/mydb/_find \
  -H "Content-Type: application/json" \
  -d '{
    "where": [{"path": ["status"], "value": "active"}],
    "limit": 10
  }'
```

**Response:**
```json
{
  "docs": [
    {"_id": "doc1", "_rev": "1-abc", "status": "active", ...},
    {"_id": "doc2", "_rev": "1-def", "status": "active", ...}
  ],
  "meta": {
    "total": 42,
    "offset": 0,
    "limit": 10
  }
}
```

---

## Changes Feed

### Poll Changes

```bash
GET /db/:db/_changes
```

**Query Parameters:**

| Parameter | Description |
|-----------|-------------|
| `since` | Start from sequence (use `first` for beginning, `now` for future changes) |
| `limit` | Maximum changes to return |
| `feed` | `normal` or `longpoll` |
| `timeout` | Long-poll timeout in ms |
| `include_docs` | Include full document body (default: false) |

**Example:**
```bash
curl "http://localhost:8080/db/mydb/_changes?since=first&limit=100"
```

### SSE Stream

```bash
GET /db/:db/_changes/stream
```

Server-Sent Events stream for real-time updates.

**Query Parameters:**

| Parameter | Description |
|-----------|-------------|
| `since` | Start from sequence (use `now` for only new changes) |
| `include_docs` | Include full document body |
| `heartbeat` | Heartbeat interval in ms (default: 30000) |

**Example:**
```bash
curl http://localhost:8080/db/mydb/_changes/stream
```

**Events:**
```
data: {"seq": "1-abc", "id": "doc1", "changes": [{"rev": "1-xyz"}]}

data: {"seq": "2-def", "id": "doc2", "changes": [{"rev": "1-uvw"}]}
```

### Filtered Changes

Filter changes by specific document IDs or query conditions.

#### Filter by Document IDs

Subscribe to changes for specific documents only:

```bash
curl "http://localhost:8080/db/mydb/_changes?doc_ids=doc1,doc2,doc3"
```

Or via POST body:

```bash
curl -X POST http://localhost:8080/db/mydb/_changes \
  -H "Content-Type: application/json" \
  -d '{
    "doc_ids": ["doc1", "doc2", "doc3"],
    "since": "first"
  }'
```

#### Filter by Query

Filter changes using query conditions:

```bash
curl -X POST http://localhost:8080/db/mydb/_changes \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "where": [{"path": ["type"], "value": "user"}]
    },
    "include_docs": true
  }'
```

This returns only changes to documents matching the query conditions.

---

## Bulk Operations

### Bulk Docs

```bash
POST /db/:db/_bulk_docs
```

**Request Body:**
```json
{
  "docs": [
    {"_id": "doc1", "name": "Alice"},
    {"_id": "doc2", "name": "Bob"},
    {"_id": "doc3", "_rev": "1-abc", "_deleted": true}
  ]
}
```

**Response:**
```json
[
  {"ok": true, "id": "doc1", "rev": "1-abc"},
  {"ok": true, "id": "doc2", "rev": "1-def"},
  {"ok": true, "id": "doc3", "rev": "2-ghi"}
]
```

---

## Attachments

### Put Attachment

```bash
PUT /db/:db/:doc_id/_attachments/:name
```

**Headers:**
- `Content-Type: <mime-type>`
- `If-Match: <rev>` (for existing documents)

**Example:**
```bash
curl -X PUT http://localhost:8080/db/mydb/doc1/_attachments/photo.jpg \
  -H "Content-Type: image/jpeg" \
  --data-binary @photo.jpg
```

### Get Attachment

```bash
GET /db/:db/:doc_id/_attachments/:name
```

**Example:**
```bash
curl http://localhost:8080/db/mydb/doc1/_attachments/photo.jpg > photo.jpg
```

### Delete Attachment

```bash
DELETE /db/:db/:doc_id/_attachments/:name
```

---

## Replication

### Trigger Replication

```bash
POST /db/:db/_replicate
```

**Request Body:**
```json
{
  "target": "http://remote:8080/db/target",
  "auth": {"bearer_token": "api_key"},
  "filter": {"paths": ["type/user"]}
}
```

**Response:**
```json
{
  "ok": true,
  "docs_read": 100,
  "docs_written": 100
}
```

