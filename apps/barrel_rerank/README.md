# barrel_rerank

Cross-encoder reranking for Erlang.

[Documentation](https://barrel-db.eu/docs/lib/rerank/) |
[HexDocs](https://hexdocs.pm/barrel_rerank) |
[Repository](https://github.com/barrel-db/barrel)

A gen_server that manages communication with a Python cross-encoder server. Supports concurrent requests by tracking request IDs and routing responses back to the correct callers.

Cross-encoders score query-document pairs directly, providing more accurate relevance scores than bi-encoder similarity for reranking candidate results.

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_rerank, "0.2.0"}
]}.
```

## Requirements

Python 3.8+ with the following packages:

```bash
pip install transformers torch
```

Or let barrel_rerank manage the venv automatically:

```erlang
{ok, _} = barrel_rerank_venv:ensure_venv().
```

## Usage

```erlang
%% Start the reranker
{ok, Server} = barrel_rerank:start_link(#{}).

%% Rerank documents by relevance to query
Query = <<"What is machine learning?">>,
Documents = [
    <<"Machine learning is a subset of AI.">>,
    <<"Python is a programming language.">>,
    <<"Deep learning uses neural networks.">>
],

{ok, Results} = barrel_rerank:rerank(Server, Query, Documents).
%% Results = [{0, 0.95}, {2, 0.82}, {1, 0.12}]
%% Sorted by score descending, tuples are {Index, Score}

%% Stop when done
barrel_rerank:stop(Server).
```

## Configuration

```erlang
Config = #{
    python => "python3",                               %% Python executable (fallback)
    model => "cross-encoder/ms-marco-MiniLM-L-6-v2",   %% Model name
    timeout => 120000                                  %% Timeout in ms
}.
{ok, Server} = barrel_rerank:start_link(Config).
```

## Supported Models

- `cross-encoder/ms-marco-MiniLM-L-6-v2` - Default, fast, good quality
- `cross-encoder/ms-marco-MiniLM-L-12-v2` - Better quality, slower
- `BAAI/bge-reranker-base` - Good quality
- `BAAI/bge-reranker-large` - Best quality, slowest

## Integration with Search

Typical two-stage retrieval:

```erlang
%% Stage 1: Fast vector search (top 100)
{ok, Candidates} = barrel_vectordb:search(Store, Query, #{k => 100}),

%% Stage 2: Rerank top candidates
Docs = [maps:get(text, C) || C <- Candidates],
{ok, Ranked} = barrel_rerank:rerank(Server, Query, Docs),

%% Get top 10 after reranking
Top10Indices = [Idx || {Idx, _Score} <- lists:sublist(Ranked, 10)],
Top10 = [lists:nth(Idx + 1, Candidates) || Idx <- Top10Indices].
```

## License

Apache-2.0
