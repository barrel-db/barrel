# Design

This document describes the architecture and internals of barrel_embed.

## Architecture Overview

barrel_embed uses a two-layer architecture that bridges Erlang and Python:

```
┌─────────────────────────────────────────────────────────┐
│                    Erlang Application                    │
├─────────────────────────────────────────────────────────┤
│  barrel_embed (API)                                      │
│       ↓                                                  │
│  barrel_embed_local/fastembed/splade/... (Providers)    │
│       ↓                                                  │
│  barrel_embed_port_server (gen_server)                  │
│       ↓ JSON-lines over stdio                           │
├─────────────────────────────────────────────────────────┤
│  Python Process                                          │
│       ↓                                                  │
│  barrel_embed (asyncio server)                          │
│       ↓                                                  │
│  providers/* (sentence_transformers, fastembed, etc.)   │
└─────────────────────────────────────────────────────────┘
```

**Key components:**

- **barrel_embed** - Main API module that coordinates providers
- **Provider modules** - Implement the `barrel_embed_provider` behaviour
- **barrel_embed_port_server** - gen_server managing Python port communication
- **Python async server** - Asyncio-based server handling embedding requests
- **Python providers** - Model-specific implementations

## Request Multiplexing

### The Problem

Without multiplexing, concurrent Erlang processes calling `embed_batch` would serialize - each would wait for the previous to complete. This is problematic because:

1. Embedding generation is CPU-bound (model inference)
2. Multiple Erlang processes often need embeddings concurrently
3. Serializing requests significantly increases latency

### The Solution

Request multiplexing allows concurrent requests to the same model:

1. Erlang gen_server assigns a unique ID to each request
2. All requests are sent immediately to Python (non-blocking)
3. Python processes requests concurrently using a thread pool
4. Python includes the request ID in each response
5. gen_server routes responses to the correct caller

### Sequence Diagram

```
Process A ──┐
Process B ──┼──► gen_server ──► Python ──► Model
Process C ──┘        │              │
                     │◄─────────────┘
                     │ (routes by ID)
            ┌────────┼────────┐
            ↓        ↓        ↓
         Proc A   Proc B   Proc C
```

Each process receives its response as soon as it's ready, not waiting for other processes.

## Erlang Port Server

`barrel_embed_port_server` is a gen_server that manages Python port communication.

### State Structure

```erlang
-record(state, {
    port :: port(),                                    % Erlang port to Python
    pending = #{} :: #{integer() => {pid(), reference()}},  % Request ID to caller
    next_id = 1 :: integer(),                          % Next request ID
    timeout :: timeout(),                              % Default timeout
    buffer = <<>> :: binary()                          % Partial line buffer
}).
```

### Request Flow

1. **Receive call** - `handle_call({embed_batch, Texts}, From, State)`
2. **Build request** - JSON with action, texts, and unique ID
3. **Send to port** - `port_command(Port, [Json, "\n"])`
4. **Track caller** - Store `From` in pending map with ID as key
5. **Return noreply** - Caller blocks waiting for response

### Response Flow

1. **Port data arrives** - `handle_info({Port, {data, {eol, Line}}}, State)`
2. **Parse JSON** - Extract response and ID
3. **Lookup caller** - Find `From` in pending map by ID
4. **Reply** - `gen_server:reply(From, Result)`
5. **Cleanup** - Remove ID from pending map

### Buffer Handling

Large embedding responses may exceed line buffer size:

```erlang
handle_info({Port, {data, {noeol, Partial}}}, State) ->
    %% Accumulate partial data
    {noreply, State#state{buffer = <<Buffer/binary, Partial/binary>>}};

handle_info({Port, {data, {eol, Line}}}, State) ->
    %% Combine buffer with complete line
    FullLine = <<Buffer/binary, Line/binary>>,
    handle_response(FullLine, State#state{buffer = <<>>});
```

## Python Async Server

### AsyncEmbedServer Base Class

The `AsyncEmbedServer` provides the async infrastructure:

```python
class AsyncEmbedServer(ABC):
    def __init__(self, max_workers: int = 4):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.write_lock = asyncio.Lock()

    async def run(self):
        # Setup async stdin/stdout
        # Main loop: read request, dispatch, write response

    async def handle_request(self, line: bytes, writer):
        # Parse JSON, dispatch, include ID in response

    async def dispatch(self, request: dict) -> dict:
        # Route to appropriate handler

    async def handle_embed(self, texts: list) -> dict:
        # Run embed_sync in thread pool
        result = await loop.run_in_executor(self.executor, self.embed_sync, texts)
        return result
```

### Concurrency Model

- **asyncio** - Non-blocking I/O for stdin/stdout
- **ThreadPoolExecutor** - CPU-bound embedding work runs in threads
- **write_lock** - Prevents output interleaving from concurrent handlers
- **uvloop** (optional) - Higher performance event loop

### Request Handling

```python
async def handle_request(self, line: bytes, writer):
    request_id = None
    try:
        request = json.loads(line.decode())
        request_id = request.get("id")
        response = await self.dispatch(request)
    except Exception as e:
        response = {"ok": False, "error": str(e)}

    if request_id is not None:
        response["id"] = request_id

    async with self.write_lock:
        output = json.dumps(response) + "\n"
        writer.write(output.encode())
        await writer.drain()
```

## JSON Protocol

Communication uses newline-delimited JSON (JSON Lines).

### Request Format

```json
{"id": 1, "action": "embed", "texts": ["hello", "world"]}
{"id": 2, "action": "info"}
{"id": 3, "action": "embed_image", "images": ["base64..."]}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Unique request identifier |
| `action` | string | One of: `embed`, `embed_image`, `info` |
| `texts` | array | Text strings to embed |
| `images` | array | Base64-encoded images |

### Response Format

**Success:**
```json
{"id": 1, "ok": true, "embeddings": [[0.1, 0.2, ...], [0.3, 0.4, ...]]}
{"id": 2, "ok": true, "dimensions": 768, "model": "BAAI/bge-base-en-v1.5", "backend": "fastembed"}
```

**Error:**
```json
{"id": 3, "ok": false, "error": "Invalid image data"}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Matches request ID |
| `ok` | boolean | Success indicator |
| `embeddings` | array | List of embedding vectors |
| `error` | string | Error message (when ok=false) |

## Provider Lifecycle

### Initialization

1. **Erlang provider init** - `barrel_embed_fastembed:init(Config)`
2. **Start port server** - `barrel_embed_port_server:start_link(Python, Args, Opts)`
3. **Python starts** - Parses args, imports provider module
4. **Load model** - `server.load_model()` downloads/loads model
5. **Get info** - Erlang calls `info/2` to get dimensions
6. **Ready** - Provider stores server pid in config

### Embedding Request

1. **User calls** - `barrel_embed:embed(Text, State)`
2. **Provider dispatches** - `barrel_embed_fastembed:embed_batch([Text], Config)`
3. **Port server sends** - JSON request to Python
4. **Python embeds** - Thread pool runs model inference
5. **Response returns** - JSON with embeddings
6. **Result delivered** - To original caller

### Cleanup

When the Erlang process terminates:
1. Port server's `terminate/2` called
2. `port_close(Port)` signals Python
3. Python's stdin closes, async loop exits
4. Thread pool shuts down

## One Server Per Model

Each provider instance creates its own:

- gen_server process
- Python port/process
- Loaded model in memory

This is intentional:

- **Different models** need separate Python processes
- **Model isolation** prevents memory/state conflicts
- **Multiplexing** benefits concurrent calls to the SAME model

If you need multiple embedding models, initialize multiple provider states:

```erlang
{ok, BgeState} = barrel_embed:init(#{embedder => {fastembed, #{model => "BAAI/bge-small-en-v1.5"}}}).
{ok, NomicState} = barrel_embed:init(#{embedder => {fastembed, #{model => "nomic-ai/nomic-embed-text-v1.5"}}}).
```

## Error Handling

### Python Errors

```python
try:
    embeddings = self.model.encode(texts)
    return {"ok": True, "embeddings": embeddings.tolist()}
except Exception as e:
    return {"ok": False, "error": str(e)}
```

### Erlang Error Decoding

```erlang
decode_response(#{<<"ok">> := false, <<"error">> := Err}) ->
    {error, {python_error, Err}};
```

### Port Exit Handling

If Python crashes:

```erlang
handle_info({Port, {exit_status, Status}}, State) ->
    %% Fail all pending requests
    maps:foreach(fun(_, From) ->
        gen_server:reply(From, {error, {port_exited, Status}})
    end, Pending),
    {stop, {port_exited, Status}, State}.
```

All pending callers receive an error tuple rather than hanging.

## Performance Considerations

### Thread Pool Sizing

Default `max_workers=4` balances:
- **CPU utilization** - Multiple concurrent embeddings
- **Memory** - Each thread may hold model state
- **Contention** - Too many threads cause lock contention

### Large Batches

For very large batches, consider:
- Using `batch_size` option to chunk requests
- The line buffer is 10MB but very large responses may need buffering

### uvloop

Install `uvloop` for better async performance:

```bash
pip install uvloop
```

The server auto-detects and uses it when available.
