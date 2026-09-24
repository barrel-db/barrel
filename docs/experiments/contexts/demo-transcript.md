# Barrel contexts demo transcript

Corpus: corpus.jsonl

## Start three nodes

## Load one OTP application per node

## Register the cards on node L

### POST 18081/contexts

Request:

```json
{
  "name": "otp/tools",
  "title": "OTP tools",
  "topics": [
    "erlang",
    "profiling"
  ],
  "locations": [
    {
      "kind": "local",
      "db": "otp_tools"
    }
  ]
}
```

Response:

```json
{
  "discoverable": "listed",
  "format": 1,
  "id": "ctx_al7b5k4oxhx6ovr27otwx6vc",
  "locations": [
    {
      "db": "otp_tools",
      "kind": "local"
    }
  ],
  "name": "otp/tools",
  "title": "OTP tools",
  "topics": [
    "erlang",
    "profiling"
  ],
  "type": "context_card",
  "updated_at": "2026-09-25T05:26:26Z"
}
```

### POST 18081/contexts

Request:

```json
{
  "name": "otp/sasl",
  "title": "OTP sasl",
  "topics": [
    "erlang",
    "release"
  ],
  "locations": [
    {
      "kind": "remote",
      "endpoint": "http://127.0.0.1:18082",
      "db": "otp_sasl"
    }
  ]
}
```

Response:

```json
{
  "discoverable": "listed",
  "format": 1,
  "id": "ctx_ammby6gfi66i67y5m5t5ippx",
  "locations": [
    {
      "db": "otp_sasl",
      "endpoint": "http://127.0.0.1:18082",
      "kind": "remote"
    }
  ],
  "name": "otp/sasl",
  "title": "OTP sasl",
  "topics": [
    "erlang",
    "release"
  ],
  "type": "context_card",
  "updated_at": "2026-09-25T05:26:26Z"
}
```

### POST 18081/contexts

Request:

```json
{
  "name": "otp/eunit",
  "title": "OTP eunit",
  "topics": [
    "erlang",
    "testing"
  ],
  "locations": [
    {
      "kind": "remote",
      "endpoint": "http://127.0.0.1:18083",
      "db": "otp_eunit"
    }
  ]
}
```

Response:

```json
{
  "discoverable": "listed",
  "format": 1,
  "id": "ctx_irfj2ordxwg4cccvrmv7orqf",
  "locations": [
    {
      "db": "otp_eunit",
      "endpoint": "http://127.0.0.1:18083",
      "kind": "remote"
    }
  ],
  "name": "otp/eunit",
  "title": "OTP eunit",
  "topics": [
    "erlang",
    "testing"
  ],
  "type": "context_card",
  "updated_at": "2026-09-25T05:26:26Z"
}
```

## Agent session over MCP

### MCP tools/call context_capabilities

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "context_capabilities",
    "arguments": {}
  }
}
```

Response:

```json
{
  "rejected": [
    "SUBSCRIBE",
    "OFFSET",
    "UNNEST",
    "continuation"
  ],
  "offline": false,
  "bql": "Strings use single quotes ('text'); double quotes name fields. The collection alias is free (FROM c).",
  "limits": {
    "deadline_ms": {
      "default": 5000,
      "max": 60000
    },
    "per_context_timeout_ms": {
      "default": 4000,
      "max": 60000
    },
    "max_parallel": {
      "default": 8,
      "max": 16
    },
    "max_contexts": 8,
    "max_limit": 1000
  },
  "merges": {
    "ordered": "rows sorted by the ORDER BY key across contexts",
    "score": "global order by vector score; vector_top_k only, same embedding fingerprint and cosine",
    "grouped": "one group per context, each in its own order",
    "interleave": "round-robin by rank; presentation only, relevance false"
  },
  "refused_merges": {
    "rrf": "rank fusion over disjoint corpora is not a relevance order",
    "rerank": "no validated reranker yet"
  },
  "shapes": [
    {
      "merge": "ordered",
      "example": "SELECT id, path, lines FROM c WHERE lines > 300 ORDER BY lines DESC LIMIT 10",
      "requires": [
        "LIMIT n",
        "ORDER BY a selected field"
      ],
      "shape": "ordered_rows"
    },
    {
      "merge": "grouped",
      "example": "SELECT id, path FROM c WHERE lines > 300 LIMIT 10",
      "requires": [
        "LIMIT n"
      ],
      "shape": "unordered_rows"
    },
    {
      "functions": [
        "bm25_top_k",
        "vector_top_k",
        "hybrid_top_k"
      ],
      "merge": "grouped; vector_top_k is merged by score when every context reports the same embedding",
      "example": "SELECT b.id, b.path, b._score FROM bm25_top_k('release upgrade', k => 5) AS b",
      "requires": [
        "k => n"
      ],
      "shape": "retrieval"
    }
  ],
  "working_set_budget": {
    "bytes": 1073741824,
    "deadline_ms": 5000,
    "contexts": 8,
    "transfer_bytes": 268435456,
    "remote_parallel": 4
  }
}
```

- ok: context_capabilities states the shapes and limits before any query

### MCP tools/call context_list

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "context_list",
    "arguments": {}
  }
}
```

Response:

```json
{
  "contexts": [
    {
      "discoverable": "listed",
      "format": 1,
      "id": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "locations": [
        {
          "db": "otp_eunit",
          "endpoint": "http://127.0.0.1:18083",
          "kind": "remote"
        }
      ],
      "name": "otp/eunit",
      "title": "OTP eunit",
      "topics": [
        "erlang",
        "testing"
      ],
      "type": "context_card",
      "updated_at": "2026-09-25T05:26:26Z"
    },
    {
      "discoverable": "listed",
      "format": 1,
      "id": "ctx_ammby6gfi66i67y5m5t5ippx",
      "locations": [
        {
          "db": "otp_sasl",
          "endpoint": "http://127.0.0.1:18082",
          "kind": "remote"
        }
      ],
      "name": "otp/sasl",
      "title": "OTP sasl",
      "topics": [
        "erlang",
        "release"
      ],
      "type": "context_card",
      "updated_at": "2026-09-25T05:26:26Z"
    },
    {
      "discoverable": "listed",
      "format": 1,
      "id": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "locations": [
        {
          "db": "otp_tools",
          "kind": "local"
        }
      ],
      "name": "otp/tools",
      "title": "OTP tools",
      "topics": [
        "erlang",
        "profiling"
      ],
      "type": "context_card",
      "updated_at": "2026-09-25T05:26:26Z"
    }
  ]
}
```

- ok: context_list shows the three cards

### MCP tools/call context_discover

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "context_discover",
    "arguments": {
      "q": "release"
    }
  }
}
```

Response:

```json
{
  "contexts": [
    {
      "discoverable": "listed",
      "format": 1,
      "id": "ctx_ammby6gfi66i67y5m5t5ippx",
      "locations": [
        {
          "db": "otp_sasl",
          "endpoint": "http://127.0.0.1:18082",
          "kind": "remote"
        }
      ],
      "name": "otp/sasl",
      "title": "OTP sasl",
      "topics": [
        "erlang",
        "release"
      ],
      "type": "context_card",
      "updated_at": "2026-09-25T05:26:26Z"
    }
  ],
  "summary": "1 context matches 'release': otp/sasl. A filter, not a ranking: other contexts may still hold answers."
}
```

- ok: context_discover filters by topic and says so

### MCP tools/call context_inspect

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "tools/call",
  "params": {
    "name": "context_inspect",
    "arguments": {
      "context": "otp/sasl"
    }
  }
}
```

Response:

```json
{
  "discoverable": "listed",
  "format": 1,
  "id": "ctx_ammby6gfi66i67y5m5t5ippx",
  "locations": [
    {
      "db": "otp_sasl",
      "endpoint": "http://127.0.0.1:18082",
      "kind": "remote"
    }
  ],
  "name": "otp/sasl",
  "title": "OTP sasl",
  "topics": [
    "erlang",
    "release"
  ],
  "type": "context_card",
  "updated_at": "2026-09-25T05:26:26Z"
}
```

- ok: context_inspect takes a name and returns the remote location

### MCP tools/call context_inspect

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "tools/call",
  "params": {
    "name": "context_inspect",
    "arguments": {
      "context": "otp/sasll"
    }
  }
}
```

Response:

```json
{
  "error": "unknown_context",
  "message": "No context has the id or name otp/sasll.",
  "hint": "Did you mean: otp/sasl? context_list (GET /contexts) shows every context.",
  "details": {
    "context": "otp/sasll",
    "suggestions": [
      {
        "id": "ctx_ammby6gfi66i67y5m5t5ippx",
        "name": "otp/sasl"
      }
    ]
  }
}
```

- ok: a mistyped name is refused with a close match

### MCP tools/call context_query

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "method": "tools/call",
  "params": {
    "name": "context_query",
    "arguments": {
      "query": "SELECT id, path, lines FROM c WHERE lines > 300 ORDER BY lines DESC LIMIT 10",
      "contexts": [
        "otp/tools",
        "otp/sasl",
        "otp/eunit"
      ]
    }
  }
}
```

Response:

```json
{
  "merge": "ordered",
  "sources": [
    {
      "name": "otp/tools",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHdDgAAAAF",
          "instance_id": "1dc52ec14cc9eb6c"
        }
      },
      "membership": "live",
      "bound": "limit_reached",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "location": {
        "kind": "local",
        "db": "otp_tools"
      },
      "rows": 10,
      "retrieval": "exact",
      "elapsed_ms": 12
    },
    {
      "name": "otp/sasl",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHdmIAAAAJ",
          "instance_id": "47c0d3a395ba71f2"
        }
      },
      "membership": "live",
      "bound": "exhausted",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "location": {
        "endpoint": "http://127.0.0.1:18082",
        "kind": "remote",
        "db": "otp_sasl"
      },
      "bytes": 753,
      "rows": 8,
      "retrieval": "exact",
      "elapsed_ms": 31
    },
    {
      "name": "otp/eunit",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHeKsAAAAL",
          "instance_id": "e236f807bfba4969"
        }
      },
      "membership": "live",
      "bound": "exhausted",
      "context": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "location": {
        "endpoint": "http://127.0.0.1:18083",
        "kind": "remote",
        "db": "otp_eunit"
      },
      "bytes": 605,
      "rows": 6,
      "retrieval": "exact",
      "elapsed_ms": 31
    }
  ],
  "rows": [
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "fprof",
      "lines": 3631,
      "path": "tools-4.2.1/src/fprof.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "cover",
      "lines": 3120,
      "path": "tools-4.2.1/src/cover.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "release_handler",
      "lines": 3092,
      "path": "sasl-4.4/src/release_handler.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_parser",
      "lines": 2808,
      "path": "tools-4.2.1/src/xref_parser.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "systools_make",
      "lines": 2513,
      "path": "sasl-4.4/src/systools_make.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref",
      "lines": 2219,
      "path": "tools-4.2.1/src/xref.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_base",
      "lines": 2099,
      "path": "tools-4.2.1/src/xref_base.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "tprof",
      "lines": 1483,
      "path": "tools-4.2.1/src/tprof.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "lcnt",
      "lines": 1466,
      "path": "tools-4.2.1/src/lcnt.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "rb",
      "lines": 1147,
      "path": "sasl-4.4/src/rb.erl"
    }
  ],
  "execution": "succeeded",
  "summary": "All 3 contexts answered (otp/tools, otp/sasl, otp/eunit). 10 rows are merged across contexts in ORDER BY order. otp/tools filled the LIMIT or k; more matches may exist.",
  "coverage": {
    "missing": [],
    "failed": 0,
    "skipped": 0,
    "requested": 3,
    "answered": 3,
    "scope_origin": "explicit"
  },
  "elapsed_ms": 38
}
```

- ok: ordered rows: all three sources answered, by name

- ok: ordered rows: sorted by lines desc across contexts

- ok: ordered rows: rows from more than one context, named

- ok: provenance: local and remote locations with observed versions

- ok: the summary says who answered and how rows were merged

### MCP tools/call context_query

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 7,
  "method": "tools/call",
  "params": {
    "name": "context_query",
    "arguments": {
      "query": "SELECT b.id, b.path, b._score FROM bm25_top_k('release upgrade', k => 3) AS b",
      "contexts": [
        "otp/tools",
        "otp/sasl",
        "otp/eunit"
      ]
    }
  }
}
```

Response:

```json
{
  "merge": "grouped",
  "sources": [
    {
      "name": "otp/tools",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHdDgAAAAF",
          "instance_id": "1dc52ec14cc9eb6c"
        }
      },
      "membership": "live",
      "bound": "limit_reached",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "location": {
        "kind": "local",
        "db": "otp_tools"
      },
      "rows": 3,
      "retrieval": "exact",
      "elapsed_ms": 1
    },
    {
      "name": "otp/sasl",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHdmIAAAAJ",
          "instance_id": "47c0d3a395ba71f2"
        }
      },
      "membership": "live",
      "bound": "limit_reached",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "location": {
        "endpoint": "http://127.0.0.1:18082",
        "kind": "remote",
        "db": "otp_sasl"
      },
      "bytes": 416,
      "rows": 3,
      "retrieval": "exact",
      "elapsed_ms": 2
    },
    {
      "name": "otp/eunit",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHeKsAAAAL",
          "instance_id": "e236f807bfba4969"
        }
      },
      "membership": "live",
      "bound": "exhausted",
      "context": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "location": {
        "endpoint": "http://127.0.0.1:18083",
        "kind": "remote",
        "db": "otp_eunit"
      },
      "bytes": 121,
      "rows": 0,
      "retrieval": "exact",
      "elapsed_ms": 2
    }
  ],
  "groups": [
    {
      "name": "otp/tools",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "rows": [
        {
          "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
          "_score": 2.798410656725898,
          "id": "xref",
          "path": "tools-4.2.1/src/xref.erl"
        },
        {
          "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
          "_score": 2.6358169784808108,
          "id": "xref_base",
          "path": "tools-4.2.1/src/xref_base.erl"
        },
        {
          "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
          "_score": 1.5502923220212412,
          "id": "xref_utils",
          "path": "tools-4.2.1/src/xref_utils.erl"
        }
      ]
    },
    {
      "name": "otp/sasl",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "rows": [
        {
          "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
          "_score": 5.142367995611812,
          "id": "systools_relup",
          "path": "sasl-4.4/src/systools_relup.erl"
        },
        {
          "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
          "_score": 4.999862756301905,
          "id": "release_handler",
          "path": "sasl-4.4/src/release_handler.erl"
        },
        {
          "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
          "_score": 3.7013144153700077,
          "id": "systools",
          "path": "sasl-4.4/src/systools.erl"
        }
      ]
    },
    {
      "name": "otp/eunit",
      "context": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "rows": []
    }
  ],
  "execution": "succeeded",
  "summary": "All 3 contexts answered (otp/tools, otp/sasl, otp/eunit). Results are grouped per context because BM25 scores are not comparable across contexts. otp/tools, otp/sasl filled the LIMIT or k; more matches may exist.",
  "coverage": {
    "missing": [],
    "failed": 0,
    "skipped": 0,
    "requested": 3,
    "answered": 3,
    "scope_origin": "explicit"
  },
  "relevance": false,
  "elapsed_ms": 2
}
```

- ok: bm25 retrieval is grouped, never a cross-context ranking

- ok: bm25 retrieval reports exact retrieval per source

- ok: the summary says why results are grouped

### MCP tools/call context_query

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 8,
  "method": "tools/call",
  "params": {
    "name": "context_query",
    "arguments": {
      "query": "SELECT b.id, b.path, b._score FROM bm25_top_k('release upgrade', k => 3) AS b",
      "contexts": [
        "otp/tools",
        "otp/sasl"
      ],
      "merge": "rrf"
    }
  }
}
```

Response:

```json
{
  "error": "merge_not_supported",
  "message": "The merge rrf is not supported: its scores do not compare across contexts.",
  "hint": "Leave merge unset (grouped per context) or use interleave.",
  "details": {
    "merge": "rrf",
    "allowed": [
      "ordered",
      "grouped",
      "interleave",
      "score"
    ]
  }
}
```

- ok: rrf is refused, with the merges to use instead

### MCP tools/call context_query

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 9,
  "method": "tools/call",
  "params": {
    "name": "context_query",
    "arguments": {
      "query": "SELECT * FROM c ORDER BY path",
      "contexts": [
        "otp/tools"
      ]
    }
  }
}
```

Response:

```json
{
  "error": "limit_required",
  "message": "A row query over contexts needs a LIMIT.",
  "hint": "Add LIMIT n (n <= 1000), and ORDER BY a selected field to merge rows in order.",
  "details": {
    "max_limit": 1000
  }
}
```

- ok: a row query without LIMIT is rejected with the fix

## Working set: attach, then materialize a slice from a remote context

### MCP tools/call context_attach

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 10,
  "method": "tools/call",
  "params": {
    "name": "context_attach",
    "arguments": {
      "context": "otp/tools"
    }
  }
}
```

Response:

```json
{
  "id": "ws_yctcc3ilcr3qdvw3sqwmh46h",
  "owner": null,
  "usage": {
    "bytes": 0
  },
  "created_at": "2026-09-25T05:26:27Z",
  "summary": "Working set ws_yctcc3ilcr3qdvw3sqwmh46h has 1 member. otp/tools: local database otp_tools, live. Offline, 1 of 1 can answer. Local copies use 0 of 1073741824 bytes.",
  "budget": {
    "bytes": 1073741824,
    "deadline_ms": 5000,
    "contexts": 8,
    "transfer_bytes": 268435456,
    "remote_parallel": 4,
    "open_dbs": 0
  },
  "members": [
    {
      "name": "otp/tools",
      "version": {
        "kind": "live"
      },
      "mode": "local",
      "membership": "live",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "answers_offline": true,
      "local_db": "otp_tools"
    }
  ]
}
```

- ok: context_attach creates a working set with a local member

### MCP tools/call context_attach

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 11,
  "method": "tools/call",
  "params": {
    "name": "context_attach",
    "arguments": {
      "working_set": "ws_yctcc3ilcr3qdvw3sqwmh46h",
      "context": "otp/sasl"
    }
  }
}
```

Response:

```json
{
  "id": "ws_yctcc3ilcr3qdvw3sqwmh46h",
  "owner": null,
  "usage": {
    "bytes": 0
  },
  "created_at": "2026-09-25T05:26:27Z",
  "summary": "Working set ws_yctcc3ilcr3qdvw3sqwmh46h has 2 members. otp/tools: local database otp_tools, live. otp/sasl: remote, queried over the network (not offline). Offline, 1 of 2 can answer. Local copies use 0 of 1073741824 bytes.",
  "budget": {
    "bytes": 1073741824,
    "deadline_ms": 5000,
    "contexts": 8,
    "transfer_bytes": 268435456,
    "remote_parallel": 4,
    "open_dbs": 0
  },
  "members": [
    {
      "name": "otp/tools",
      "version": {
        "kind": "live"
      },
      "mode": "local",
      "membership": "live",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "answers_offline": true,
      "local_db": "otp_tools"
    },
    {
      "name": "otp/sasl",
      "version": {
        "kind": "live"
      },
      "mode": "remote",
      "membership": "live",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "location": {
        "endpoint": "http://127.0.0.1:18082",
        "db": "otp_sasl"
      },
      "answers_offline": false
    }
  ]
}
```

- ok: second member is remote, attached without copying

### MCP tools/call context_materialize

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 12,
  "method": "tools/call",
  "params": {
    "name": "context_materialize",
    "arguments": {
      "working_set": "ws_yctcc3ilcr3qdvw3sqwmh46h",
      "from_query": {
        "query": "SELECT b.id FROM bm25_top_k('test', k => 5) AS b",
        "contexts": [
          "otp/eunit"
        ]
      },
      "include": {
        "embeddings": false
      }
    }
  }
}
```

Response:

```json
{
  "usage": {
    "bytes": 878,
    "budget_bytes": 1073741824
  },
  "summary": "Working set ws_yctcc3ilcr3qdvw3sqwmh46h: saved 5 documents from otp/eunit (878 bytes) into wslice_yctcc3il_irfj2ord.",
  "working_set": "ws_yctcc3ilcr3qdvw3sqwmh46h",
  "slices": [
    {
      "name": "otp/eunit",
      "status": "complete",
      "context": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "bytes": 878,
      "docs": 5,
      "local_db": "wslice_yctcc3il_irfj2ord",
      "derived": {
        "bytes": 878,
        "created_at": "2026-09-25T05:26:27Z",
        "docs": 5,
        "from": "ctx_irfj2ordxwg4cccvrmv7orqf",
        "ids_hash": "sha256:aebb8f14dde7ba8ee414351f9482a390e2dffb86c9be2c77c3c19caacc3a2f3e",
        "missing": [],
        "observed": {
          "instance_id": "e236f807bfba4969",
          "last_seq": "AAABoNcHeKsAAAAL"
        },
        "selection": "ids",
        "source": {
          "db": "otp_eunit",
          "endpoint": "http://127.0.0.1:18083",
          "kind": "remote"
        }
      }
    }
  ]
}
```

- ok: materialize saved a complete slice of the remote context

- ok: slice provenance records what the source observed

### MCP tools/call context_working_sets

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 13,
  "method": "tools/call",
  "params": {
    "name": "context_working_sets",
    "arguments": {
      "working_set": "ws_yctcc3ilcr3qdvw3sqwmh46h"
    }
  }
}
```

Response:

```json
{
  "id": "ws_yctcc3ilcr3qdvw3sqwmh46h",
  "owner": null,
  "usage": {
    "bytes": 878
  },
  "created_at": "2026-09-25T05:26:27Z",
  "summary": "Working set ws_yctcc3ilcr3qdvw3sqwmh46h has 3 members. otp/tools: local database otp_tools, live. otp/sasl: remote, queried over the network (not offline). otp/eunit: saved slice of 5 documents (not the whole context). Offline, 2 of 3 can answer. Local copies use 878 of 1073741824 bytes.",
  "budget": {
    "bytes": 1073741824,
    "deadline_ms": 5000,
    "contexts": 8,
    "transfer_bytes": 268435456,
    "remote_parallel": 4,
    "open_dbs": 0
  },
  "members": [
    {
      "name": "otp/tools",
      "version": {
        "kind": "live"
      },
      "mode": "local",
      "membership": "live",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "answers_offline": true,
      "local_db": "otp_tools"
    },
    {
      "name": "otp/sasl",
      "version": {
        "kind": "live"
      },
      "mode": "remote",
      "membership": "live",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "location": {
        "endpoint": "http://127.0.0.1:18082",
        "db": "otp_sasl"
      },
      "answers_offline": false
    },
    {
      "name": "otp/eunit",
      "version": {
        "kind": "retrieved_set",
        "observed": {
          "instance_id": "e236f807bfba4969",
          "last_seq": "AAABoNcHeKsAAAAL"
        }
      },
      "mode": "retrieved_set",
      "membership": "retrieved_set",
      "context": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "bytes": 878,
      "answers_offline": true,
      "local_db": "wslice_yctcc3il_irfj2ord",
      "derived": {
        "bytes": 878,
        "created_at": "2026-09-25T05:26:27Z",
        "docs": 5,
        "from": "ctx_irfj2ordxwg4cccvrmv7orqf",
        "ids_hash": "sha256:aebb8f14dde7ba8ee414351f9482a390e2dffb86c9be2c77c3c19caacc3a2f3e",
        "missing": [],
        "observed": {
          "instance_id": "e236f807bfba4969",
          "last_seq": "AAABoNcHeKsAAAAL"
        },
        "selection": "ids",
        "source": {
          "db": "otp_eunit",
          "endpoint": "http://127.0.0.1:18083",
          "kind": "remote"
        }
      }
    }
  ]
}
```

- ok: the working set says which members answer offline

## Node R2 goes down

### MCP tools/call context_query

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 14,
  "method": "tools/call",
  "params": {
    "name": "context_query",
    "arguments": {
      "query": "SELECT id, path, lines FROM c WHERE lines > 300 ORDER BY lines DESC LIMIT 10",
      "contexts": [
        "otp/tools",
        "otp/sasl",
        "otp/eunit"
      ],
      "per_context_timeout_ms": 1500
    }
  }
}
```

Response:

```json
{
  "merge": "ordered",
  "sources": [
    {
      "name": "otp/tools",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHdDgAAAAF",
          "instance_id": "1dc52ec14cc9eb6c"
        }
      },
      "membership": "live",
      "bound": "limit_reached",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "location": {
        "kind": "local",
        "db": "otp_tools"
      },
      "rows": 10,
      "retrieval": "exact",
      "elapsed_ms": 0
    },
    {
      "name": "otp/sasl",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHdmIAAAAJ",
          "instance_id": "47c0d3a395ba71f2"
        }
      },
      "membership": "live",
      "bound": "exhausted",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "location": {
        "endpoint": "http://127.0.0.1:18082",
        "kind": "remote",
        "db": "otp_sasl"
      },
      "bytes": 753,
      "rows": 8,
      "retrieval": "exact",
      "elapsed_ms": 1
    },
    {
      "error": {
        "message": "the server did not accept the connection (econnrefused)",
        "reason": "econnrefused",
        "hint": "Check that the remote node is up; to answer without it, attach and materialize or import a local copy.",
        "rows_received_before_failure": 0
      },
      "name": "otp/eunit",
      "status": "unreachable",
      "context": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "location": {
        "endpoint": "http://127.0.0.1:18083",
        "kind": "remote",
        "db": "otp_eunit"
      },
      "rows": 0,
      "elapsed_ms": 0
    }
  ],
  "rows": [
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "fprof",
      "lines": 3631,
      "path": "tools-4.2.1/src/fprof.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "cover",
      "lines": 3120,
      "path": "tools-4.2.1/src/cover.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "release_handler",
      "lines": 3092,
      "path": "sasl-4.4/src/release_handler.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_parser",
      "lines": 2808,
      "path": "tools-4.2.1/src/xref_parser.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "systools_make",
      "lines": 2513,
      "path": "sasl-4.4/src/systools_make.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref",
      "lines": 2219,
      "path": "tools-4.2.1/src/xref.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_base",
      "lines": 2099,
      "path": "tools-4.2.1/src/xref_base.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "tprof",
      "lines": 1483,
      "path": "tools-4.2.1/src/tprof.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "lcnt",
      "lines": 1466,
      "path": "tools-4.2.1/src/lcnt.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "rb",
      "lines": 1147,
      "path": "sasl-4.4/src/rb.erl"
    }
  ],
  "execution": "partial",
  "summary": "2 of 3 contexts answered; rows from the others are not included. otp/eunit: the server did not accept the connection (econnrefused). 10 rows are merged across contexts in ORDER BY order. otp/tools filled the LIMIT or k; more matches may exist.",
  "coverage": {
    "missing": [
      "ctx_irfj2ordxwg4cccvrmv7orqf"
    ],
    "failed": 1,
    "skipped": 0,
    "requested": 3,
    "answered": 2,
    "scope_origin": "explicit"
  },
  "elapsed_ms": 1
}
```

- ok: partial coverage: R2's context is reported unreachable, with a hint

- ok: no row comes from the failed source

- ok: the summary names the missing context

## Offline: the working set answers from local copies

### MCP tools/call context_offline

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 15,
  "method": "tools/call",
  "params": {
    "name": "context_offline",
    "arguments": {
      "offline": true
    }
  }
}
```

Response:

```json
{
  "offline": true
}
```

- ok: node L is offline

### MCP tools/call context_query

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 16,
  "method": "tools/call",
  "params": {
    "name": "context_query",
    "arguments": {
      "query": "SELECT id, path, lines FROM c ORDER BY lines DESC LIMIT 50",
      "working_set": "ws_yctcc3ilcr3qdvw3sqwmh46h"
    }
  }
}
```

Response:

```json
{
  "merge": "ordered",
  "sources": [
    {
      "name": "otp/tools",
      "status": "ok",
      "version": {
        "kind": "live",
        "observed": {
          "last_seq": "AAABoNcHdDgAAAAF",
          "instance_id": "1dc52ec14cc9eb6c"
        }
      },
      "membership": "live",
      "bound": "exhausted",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "location": {
        "kind": "local",
        "db": "otp_tools"
      },
      "rows": 16,
      "retrieval": "exact",
      "elapsed_ms": 1
    },
    {
      "error": {
        "message": "skipped: offline and no local copy",
        "reason": "no_local_copy",
        "hint": "Materialize a slice or import a snapshot while online to answer offline."
      },
      "name": "otp/sasl",
      "status": "skipped_offline",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "location": {
        "endpoint": "http://127.0.0.1:18082",
        "kind": "remote",
        "db": "otp_sasl"
      },
      "rows": 0,
      "elapsed_ms": 0
    },
    {
      "name": "otp/eunit",
      "status": "ok",
      "version": {
        "kind": "retrieved_set",
        "observed": {
          "instance_id": "e236f807bfba4969",
          "last_seq": "AAABoNcHeKsAAAAL"
        }
      },
      "membership": "retrieved_set",
      "bound": "exhausted",
      "context": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "location": {
        "kind": "local",
        "db": "wslice_yctcc3il_irfj2ord"
      },
      "rows": 5,
      "retrieval": "exact",
      "note": "answers cover only the 5 saved documents, not the source context",
      "elapsed_ms": 80
    }
  ],
  "rows": [
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "fprof",
      "lines": 3631,
      "path": "tools-4.2.1/src/fprof.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "cover",
      "lines": 3120,
      "path": "tools-4.2.1/src/cover.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_parser",
      "lines": 2808,
      "path": "tools-4.2.1/src/xref_parser.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref",
      "lines": 2219,
      "path": "tools-4.2.1/src/xref.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_base",
      "lines": 2099,
      "path": "tools-4.2.1/src/xref_base.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "tprof",
      "lines": 1483,
      "path": "tools-4.2.1/src/tprof.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "lcnt",
      "lines": 1466,
      "path": "tools-4.2.1/src/lcnt.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_compiler",
      "lines": 943,
      "path": "tools-4.2.1/src/xref_compiler.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "eprof",
      "lines": 804,
      "path": "tools-4.2.1/src/eprof.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_utils",
      "lines": 732,
      "path": "tools-4.2.1/src/xref_utils.erl"
    },
    {
      "_ctx": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "_ctx_name": "otp/eunit",
      "id": "eunit_proc",
      "lines": 729,
      "path": "eunit-2.11/src/eunit_proc.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "make",
      "lines": 478,
      "path": "tools-4.2.1/src/make.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "tags",
      "lines": 464,
      "path": "tools-4.2.1/src/tags.erl"
    },
    {
      "_ctx": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "_ctx_name": "otp/eunit",
      "id": "eunit_test",
      "lines": 424,
      "path": "eunit-2.11/src/eunit_test.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "cprof",
      "lines": 401,
      "path": "tools-4.2.1/src/cprof.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_reader",
      "lines": 365,
      "path": "tools-4.2.1/src/xref_reader.erl"
    },
    {
      "_ctx": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "_ctx_name": "otp/eunit",
      "id": "eunit",
      "lines": 294,
      "path": "eunit-2.11/src/eunit.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "crashdump",
      "lines": 223,
      "path": "tools-4.2.1/src/crashdump.erl"
    },
    {
      "_ctx": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "_ctx_name": "otp/eunit",
      "id": "eunit_autoexport",
      "lines": 115,
      "path": "eunit-2.11/src/eunit_autoexport.erl"
    },
    {
      "_ctx": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "_ctx_name": "otp/tools",
      "id": "xref_scanner",
      "lines": 98,
      "path": "tools-4.2.1/src/xref_scanner.erl"
    },
    {
      "_ctx": "ctx_irfj2ordxwg4cccvrmv7orqf",
      "_ctx_name": "otp/eunit",
      "id": "eunit_tests",
      "lines": 63,
      "path": "eunit-2.11/src/eunit_tests.erl"
    }
  ],
  "execution": "partial",
  "summary": "2 of 3 contexts answered; rows from the others are not included. otp/sasl: skipped: offline and no local copy. 21 rows are merged across contexts in ORDER BY order. otp/eunit answered from a saved slice: answers cover only the 5 saved documents, not the source context.",
  "working_set": "ws_yctcc3ilcr3qdvw3sqwmh46h",
  "coverage": {
    "missing": [
      "ctx_ammby6gfi66i67y5m5t5ippx"
    ],
    "failed": 0,
    "skipped": 1,
    "requested": 3,
    "answered": 2,
    "scope_origin": "explicit"
  },
  "elapsed_ms": 80
}
```

- ok: working set query is partial with one skipped member

- ok: local member answers live

- ok: remote member is skipped_offline, never contacted, and says what to do

- ok: slice answers with retrieved_set membership

- ok: the summary says what the slice covers

### MCP tools/call context_materialize

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 17,
  "method": "tools/call",
  "params": {
    "name": "context_materialize",
    "arguments": {
      "working_set": "ws_yctcc3ilcr3qdvw3sqwmh46h",
      "from_query": {
        "query": "SELECT b.id FROM bm25_top_k('release', k => 3) AS b",
        "contexts": [
          "otp/sasl"
        ]
      }
    }
  }
}
```

Response:

```json
{
  "error": "offline",
  "message": "The node is offline and this operation needs a remote source.",
  "hint": "Switch offline mode off (context_offline or PUT /contexts/_offline {\"offline\": false}) and retry.",
  "details": {
    "operation": "materialize",
    "remote_contexts": [
      "ctx_ammby6gfi66i67y5m5t5ippx"
    ]
  }
}
```

- ok: materialize while offline is refused before any work

### MCP tools/call context_offline

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 18,
  "method": "tools/call",
  "params": {
    "name": "context_offline",
    "arguments": {
      "offline": false
    }
  }
}
```

Response:

```json
{
  "offline": false
}
```

## Import a published snapshot of otp/sasl

### Export on R1 (operator, Erlang)

```erlang
{ok,#{context => <<"ctx_ammby6gfi66i67y5m5t5ippx">>,bytes => 882077,
      generation => 1,
      dest =>
          "$WORK/export_sasl_g1",
      artifacts => 42,elapsed_ms => 85,
      manifest =>
          #{<<"context">> => <<"ctx_ammby6gfi66i67y5m5t5ippx">>,
            <<"engine">> =>
                #{<<"barrel">> => <<"1.10.0">>,
                  <<"barrel_docdb">> => <<"1.7.0">>,
                  <<"barrel_vectordb">> => <<"2.5.0">>},
            <<"format">> => 1,<<"generation">> =
```

### MCP tools/call context_import

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 19,
  "method": "tools/call",
  "params": {
    "name": "context_import",
    "arguments": {
      "dir": "$WORK/export_sasl_g1"
    }
  }
}
```

Response:

```json
{
  "id": "ws_4ajlkjia325hinndknqhj2qd",
  "owner": null,
  "usage": {
    "bytes": 882077
  },
  "created_at": "2026-09-25T05:26:29Z",
  "summary": "Working set ws_4ajlkjia325hinndknqhj2qd has 1 member. otp/sasl: imported snapshot, generation 1. Offline, 1 of 1 can answer. Local copies use 882077 of 1073741824 bytes.",
  "budget": {
    "bytes": 1073741824,
    "deadline_ms": 5000,
    "contexts": 8,
    "transfer_bytes": 268435456,
    "remote_parallel": 4,
    "open_dbs": 0
  },
  "members": [
    {
      "name": "otp/sasl",
      "version": {
        "kind": "generation",
        "generation": 1
      },
      "mode": "snapshot",
      "membership": "complete_generation",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "bytes": 882077,
      "answers_offline": true,
      "local_db": "wsnap_ammby6gf_1",
      "generation": 1
    }
  ]
}
```

- ok: import creates a working set with a snapshot member at generation 1

### MCP tools/call context_query

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 20,
  "method": "tools/call",
  "params": {
    "name": "context_query",
    "arguments": {
      "query": "SELECT id, path, lines FROM c ORDER BY lines DESC LIMIT 50",
      "working_set": "ws_4ajlkjia325hinndknqhj2qd",
      "offline": true
    }
  }
}
```

Response:

```json
{
  "merge": "ordered",
  "sources": [
    {
      "name": "otp/sasl",
      "status": "ok",
      "version": {
        "kind": "generation",
        "generation": 1
      },
      "membership": "complete_generation",
      "bound": "exhausted",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "location": {
        "kind": "local",
        "db": "wsnap_ammby6gf_1"
      },
      "rows": 17,
      "retrieval": "exact",
      "elapsed_ms": 82
    }
  ],
  "rows": [
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "release_handler",
      "lines": 3092,
      "path": "sasl-4.4/src/release_handler.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "systools_make",
      "lines": 2513,
      "path": "sasl-4.4/src/systools_make.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "rb",
      "lines": 1147,
      "path": "sasl-4.4/src/rb.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "systools_rc",
      "lines": 1097,
      "path": "sasl-4.4/src/systools_rc.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "release_handler_1",
      "lines": 824,
      "path": "sasl-4.4/src/release_handler_1.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "systools_relup",
      "lines": 649,
      "path": "sasl-4.4/src/systools_relup.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "systools",
      "lines": 447,
      "path": "sasl-4.4/src/systools.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "erlsrv",
      "lines": 444,
      "path": "sasl-4.4/src/erlsrv.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "systools_lib",
      "lines": 251,
      "path": "sasl-4.4/src/systools_lib.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "sasl",
      "lines": 201,
      "path": "sasl-4.4/src/sasl.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "sasl_report",
      "lines": 191,
      "path": "sasl-4.4/src/sasl_report.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "alarm_handler",
      "lines": 176,
      "path": "sasl-4.4/src/alarm_handler.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "rb_format_supp",
      "lines": 162,
      "path": "sasl-4.4/src/rb_format_supp.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "format_lib_supp",
      "lines": 146,
      "path": "sasl-4.4/src/format_lib_supp.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "misc_supp",
      "lines": 127,
      "path": "sasl-4.4/src/misc_supp.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "sasl_report_file_h",
      "lines": 77,
      "path": "sasl-4.4/src/sasl_report_file_h.erl"
    },
    {
      "_ctx": "ctx_ammby6gfi66i67y5m5t5ippx",
      "_ctx_name": "otp/sasl",
      "id": "sasl_report_tty_h",
      "lines": 55,
      "path": "sasl-4.4/src/sasl_report_tty_h.erl"
    }
  ],
  "execution": "succeeded",
  "summary": "otp/sasl answered. 17 rows in ORDER BY order. otp/sasl answered from an imported snapshot (generation 1).",
  "working_set": "ws_4ajlkjia325hinndknqhj2qd",
  "coverage": {
    "missing": [],
    "failed": 0,
    "skipped": 0,
    "requested": 1,
    "answered": 1,
    "scope_origin": "explicit"
  },
  "elapsed_ms": 83
}
```

- ok: snapshot answers offline as a complete generation

## Clean up

### MCP tools/call context_detach

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 21,
  "method": "tools/call",
  "params": {
    "name": "context_detach",
    "arguments": {
      "working_set": "ws_yctcc3ilcr3qdvw3sqwmh46h",
      "context": "otp/eunit"
    }
  }
}
```

Response:

```json
{
  "id": "ws_yctcc3ilcr3qdvw3sqwmh46h",
  "owner": null,
  "usage": {
    "bytes": 0
  },
  "created_at": "2026-09-25T05:26:27Z",
  "summary": "Working set ws_yctcc3ilcr3qdvw3sqwmh46h has 2 members. otp/tools: local database otp_tools, live. otp/sasl: remote, queried over the network (not offline). Offline, 1 of 2 can answer. Local copies use 0 of 1073741824 bytes.",
  "budget": {
    "bytes": 1073741824,
    "deadline_ms": 5000,
    "contexts": 8,
    "transfer_bytes": 268435456,
    "remote_parallel": 4,
    "open_dbs": 0
  },
  "members": [
    {
      "name": "otp/tools",
      "version": {
        "kind": "live"
      },
      "mode": "local",
      "membership": "live",
      "context": "ctx_al7b5k4oxhx6ovr27otwx6vc",
      "answers_offline": true,
      "local_db": "otp_tools"
    },
    {
      "name": "otp/sasl",
      "version": {
        "kind": "live"
      },
      "mode": "remote",
      "membership": "live",
      "context": "ctx_ammby6gfi66i67y5m5t5ippx",
      "location": {
        "endpoint": "http://127.0.0.1:18082",
        "db": "otp_sasl"
      },
      "answers_offline": false
    }
  ]
}
```

- ok: detach by name removes the slice member

### MCP tools/call context_working_sets

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 22,
  "method": "tools/call",
  "params": {
    "name": "context_working_sets",
    "arguments": {}
  }
}
```

Response:

```json
{
  "working_sets": [
    {
      "id": "ws_4ajlkjia325hinndknqhj2qd",
      "owner": null,
      "usage": {
        "bytes": 882077
      },
      "created_at": "2026-09-25T05:26:29Z",
      "members": [
        {
          "name": "otp/sasl",
          "mode": "snapshot",
          "context": "ctx_ammby6gfi66i67y5m5t5ippx"
        }
      ]
    },
    {
      "id": "ws_yctcc3ilcr3qdvw3sqwmh46h",
      "owner": null,
      "usage": {
        "bytes": 0
      },
      "created_at": "2026-09-25T05:26:27Z",
      "members": [
        {
          "name": "otp/tools",
          "mode": "local",
          "context": "ctx_al7b5k4oxhx6ovr27otwx6vc"
        },
        {
          "name": "otp/sasl",
          "mode": "remote",
          "context": "ctx_ammby6gfi66i67y5m5t5ippx"
        }
      ]
    }
  ]
}
```

- ok: context_working_sets lists both working sets

### MCP tools/call context_working_set_delete

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 23,
  "method": "tools/call",
  "params": {
    "name": "context_working_set_delete",
    "arguments": {
      "working_set": "ws_4ajlkjia325hinndknqhj2qd"
    }
  }
}
```

Response:

```json
{
  "ok": true,
  "deleted": "ws_4ajlkjia325hinndknqhj2qd"
}
```

- ok: context_working_set_delete removes a working set

### MCP tools/call context_working_sets

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 24,
  "method": "tools/call",
  "params": {
    "name": "context_working_sets",
    "arguments": {
      "working_set": "ws_4ajlkjia325hinndknqhj2qd"
    }
  }
}
```

Response:

```json
{
  "error": "unknown_working_set",
  "message": "No working set has the id ws_4ajlkjia325hinndknqhj2qd.",
  "hint": "List working sets with context_working_sets (GET /worksets), or omit working_set to create one.",
  "details": {
    "known": [
      "ws_yctcc3ilcr3qdvw3sqwmh46h"
    ],
    "working_set": "ws_4ajlkjia325hinndknqhj2qd"
  }
}
```

- ok: a deleted working set is reported unknown, with the next step


Result: 36 passed, 0 failed
