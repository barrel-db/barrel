# Advanced Features Guide

This guide provides practical examples using curl for Barrel DocDB's advanced features: replication and clock synchronization.

All examples assume Barrel DocDB is running on `localhost:8080` with authentication enabled. Set your API key:

```bash
export API_KEY="your_api_key_here"
```

---

## Replication

Replication synchronizes documents between databases. Documents are transferred with their full revision history, enabling automatic conflict detection.

### One-Shot Replication

Copy all documents from source to target:

```bash
# Create source and target databases
curl -X PUT "http://localhost:8080/db/source" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://localhost:8080/db/target" -H "Authorization: Bearer $API_KEY"

# Add documents to source
curl -X PUT "http://localhost:8080/db/source/user1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"name": "Alice", "role": "admin", "active": true}'

curl -X PUT "http://localhost:8080/db/source/user2" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"name": "Bob", "role": "user", "active": true}'

# Replicate source -> target
curl -X POST "http://localhost:8080/db/source/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"target": "http://localhost:8080/db/target"}'
```

Response:
```json
{
  "ok": true,
  "docs_read": 2,
  "docs_written": 2
}
```

### Replication to Remote Node

Replicate to another Barrel DocDB instance:

```bash
# Replicate to remote node with authentication
curl -X POST "http://localhost:8080/db/source/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "target": "http://remote-node:8080/db/target",
    "auth": {"bearer_token": "remote_api_key"}
  }'
```

### Filtered Replication by Path

Replicate only documents matching specific path patterns:

```bash
# Create documents with different types
curl -X PUT "http://localhost:8080/db/source/order1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"type": "order", "total": 150, "customer": "alice"}'

curl -X PUT "http://localhost:8080/db/source/invoice1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"type": "invoice", "amount": 150, "customer": "alice"}'

# Replicate only orders (filter by type/order path)
curl -X POST "http://localhost:8080/db/source/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "target": "http://localhost:8080/db/orders_only",
    "filter": {"paths": ["type/order"]}
  }'
```

### Filtered Replication by Query

Replicate documents matching query conditions:

```bash
# Replicate only active users
curl -X POST "http://localhost:8080/db/source/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "target": "http://localhost:8080/db/active_users",
    "filter": {
      "query": {
        "where": [
          {"path": ["role"], "value": "user"},
          {"path": ["active"], "value": true}
        ]
      }
    }
  }'
```

### Bidirectional Replication

Sync changes in both directions between two databases:

```bash
# Replicate A -> B
curl -X POST "http://localhost:8080/db/db_a/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"target": "http://localhost:8080/db/db_b"}'

# Replicate B -> A
curl -X POST "http://localhost:8080/db/db_b/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"target": "http://localhost:8080/db/db_a"}'
```

### Verify Replication with Changes Feed

Check that documents were replicated by comparing changes feeds:

```bash
# Get changes from source
curl "http://localhost:8080/db/source/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY"

# Get changes from target (should match)
curl "http://localhost:8080/db/target/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY"
```

---

## Clock Synchronization (HLC)

Barrel DocDB uses Hybrid Logical Clocks (HLC) to maintain causal ordering across distributed nodes. HLC combines physical wall clock time with a logical counter, ensuring that events are correctly ordered even when physical clocks drift.

### Automatic Synchronization

Clock synchronization happens automatically during all inter-node communication:

- **Replication**: During document sync between databases

Every HTTP response from Barrel DocDB includes an `x-barrel-hlc` header containing the current HLC timestamp (base64-encoded). Clients automatically sync their local clock when they receive responses.

### Check Current HLC

You can see the current HLC in any response header:

```bash
curl -v http://localhost:8080/health 2>&1 | grep -i x-barrel-hlc
```

Output:
```
< x-barrel-hlc: AAAB1aBcdefghijklmnopqrs==
```

### Automatic Clock Sync

HLC synchronization happens automatically during replication: every
exchange between peers piggybacks the current clock value, so causal
ordering across nodes is preserved without an explicit sync step. The
`X-Barrel-HLC` response header on every HTTP reply exposes the local
clock for diagnostics.

### Why HLC Matters

HLC ensures:

1. **Causal ordering**: Events that causally depend on each other are correctly ordered
2. **Conflict detection**: During replication, conflicts are detected based on HLC timestamps
3. **Change feed consistency**: The `_changes` feed returns events in HLC order
4. **Cross-node consistency**: All nodes agree on event ordering regardless of clock drift

### HLC in Changes Feed

The changes feed uses HLC for the `last_seq` value:

```bash
curl "http://localhost:8080/db/mydb/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY"
```

Response:
```json
{
  "results": [...],
  "last_seq": "{timestamp,1736500000000,42}"
}
```

Use the `last_seq` value to resume the changes feed from where you left off:

```bash
curl "http://localhost:8080/db/mydb/_changes?since={timestamp,1736500000000,42}" \
  -H "Authorization: Bearer $API_KEY"
```

---

## Troubleshooting

### Verify Document Counts

```bash
# Count documents using _find
curl -X POST "http://localhost:8080/db/mydb/_find" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"where": [], "limit": 0}'
```

### Check Changes Feed

```bash
# Get all changes
curl "http://localhost:8080/db/mydb/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY"

# Get changes count
curl "http://localhost:8080/db/mydb/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY" | jq '.results | length'
```

### Health Check

```bash
curl "http://localhost:8080/health" -H "Authorization: Bearer $API_KEY"
```
