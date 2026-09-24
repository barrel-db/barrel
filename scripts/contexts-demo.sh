#!/usr/bin/env bash
#
# End-user run of Barrel contexts on three barrel_server nodes (loopback).
#
# Node L serves one local context and registers two remote ones (R1, R2).
# An agent-style session then drives L over MCP JSON-RPC, by context
# name: capabilities, list, discover, inspect, query across three
# contexts, read the errors and summaries, build a working set,
# materialize a slice, lose a node, go offline, import a published
# snapshot, and clean up.
#
# Usage:  scripts/contexts-demo.sh [corpus.jsonl]
# Needs:  rebar3 as server compile, curl, jq, python3, erl (epmd for the
#         snapshot export step). No embedder and no network service: BM25
#         and row queries only, vectors are deterministic hashes.
# Output: $OUT_DIR/transcript.md (requests and responses). Exit 0 = every
#         expected property held.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS="${1:-${BARREL_CTX_CORPUS:-}}"
WORK="${WORK_DIR:-${TMPDIR:-/tmp}/barrel-contexts-demo}"
OUT_DIR="${OUT_DIR:-$WORK}"
TRANSCRIPT="$OUT_DIR/transcript.md"
COOKIE=ctxdemo
NODE=${NODE_PREFIX:-ctxdemo}
PORT_L=${PORT_L:-18081}
PORT_R1=${PORT_R1:-18082}
PORT_R2=${PORT_R2:-18083}
L=http://127.0.0.1:$PORT_L
R1=http://127.0.0.1:$PORT_R1
R2=http://127.0.0.1:$PORT_R2
EBIN=("$ROOT"/_build/server/lib/*/ebin)

pass=0
fail=0
PIDS=()

[ -n "$CORPUS" ] || { echo "usage: $0 corpus.jsonl (or BARREL_CTX_CORPUS)"; exit 2; }
[ -f "$CORPUS" ] || { echo "corpus not found: $CORPUS"; exit 2; }
[ -d "$ROOT/_build/server/lib/barrel_server/ebin" ] || {
    echo "build first: rebar3 as server compile"; exit 2; }

# ---------------------------------------------------------------- helpers

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

check() {  # check <description> <jq filter> <json>
    if printf '%s' "$3" | jq -e "$2" >/dev/null 2>&1; then
        echo "  ok   $1"; pass=$((pass + 1))
        printf -- '- ok: %s\n\n' "$1" >> "$TRANSCRIPT"
    else
        echo "  FAIL $1 ($2)"; fail=$((fail + 1))
        printf -- '- FAIL: %s\n\n' "$1" >> "$TRANSCRIPT"
    fi
}

note() {
    echo "--- $1"
    printf '## %s\n\n' "$1" >> "$TRANSCRIPT"
}

log_exchange() {  # log_exchange <title> <request> <response>
    {
        printf '### %s\n\nRequest:\n\n```json\n' "$1"
        printf '%s' "$2" | jq . 2>/dev/null || printf '%s\n' "$2"
        printf '```\n\nResponse:\n\n```json\n'
        printf '%s' "$3" | jq . 2>/dev/null || printf '%s\n' "$3"
        printf '```\n\n'
    } >> "$TRANSCRIPT"
}

rest() {  # rest <method> <url> [json body] -> prints body
    local method=$1 url=$2 body=${3:-}
    local resp
    if [ -n "$body" ]; then
        resp=$(curl -sS -X "$method" "$url" -H 'content-type: application/json' -d "$body")
    else
        resp=$(curl -sS -X "$method" "$url")
    fi
    log_exchange "$method ${url#http://127.0.0.1:}" "${body:-{\}}" "$resp"
    printf '%s' "$resp"
}

start_node() {  # start_node <name> <port>
    local name=$1 port=$2 dir="$WORK/$1"
    mkdir -p "$dir"
    cat > "$dir/sys.config" <<EOF
[{barrel_server, [{http_port, $port}, {data_dir, "$dir/data"},
                  {open_opts, #{vectordb => #{dimension => 8,
                                              bm25_backend => memory}}}]},
 {barrel, [{ctx_dir, "$dir/ctx"}]},
 {barrel_embed, [{managed_venv, false}]},
 {kernel, [{logger_level, warning}]}].
EOF
    erl -name "${NODE}_$name@127.0.0.1" -setcookie "$COOKIE" -noshell -noinput \
        -pa "${EBIN[@]}" -config "$dir/sys" \
        -eval 'case application:ensure_all_started(barrel_server) of {ok, _} -> ok; E -> io:format("~p~n", [E]), halt(1) end' \
        > "$dir/node.log" 2>&1 &
    PIDS+=($!)
    eval "PID_$name=$!"
    for _ in $(seq 1 100); do
        curl -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    echo "node $name did not start; see $dir/node.log"; exit 1
}

# Load one OTP application as one database: docs over _bulk_docs, then a
# text entry per doc (BM25) with a deterministic 8-dim hash vector.
load_app() {  # load_app <base-url> <db> <app>
    curl -fsS -X PUT "$1/db/$2" -H 'content-type: application/json' -d '{}' >/dev/null
    python3 - "$CORPUS" "$3" "$WORK/load_$2" <<'EOF'
import hashlib, json, sys
corpus, app, out = sys.argv[1], sys.argv[2], sys.argv[3]
docs, vecs = [], []
for line in open(corpus):
    r = json.loads(line)
    if r["app"] != app:
        continue
    body = r.get("body_nodoc") or r.get("body", "")
    docs.append({"id": r["id"], "app": app, "path": r["path"],
                 "moduledoc": r.get("moduledoc", ""),
                 "lines": r.get("body", "").count("\n") + 1,
                 "size": len(r.get("body", ""))})
    text = (r.get("moduledoc", "") + "\n" + body)[:4000]
    h = hashlib.sha256(r["id"].encode()).digest()
    vecs.append({"id": r["id"], "text": text,
                 "vector": [b / 255.0 for b in h[:8]]})
json.dump({"docs": docs}, open(out + ".docs.json", "w"))
with open(out + ".vecs.jsonl", "w") as f:
    for v in vecs:
        f.write(json.dumps(v) + "\n")
print(len(docs))
EOF
    curl -fsS -X POST "$1/db/$2/_bulk_docs" -H 'content-type: application/json' \
        --data-binary "@$WORK/load_$2.docs.json" >/dev/null
    while IFS= read -r v; do
        curl -fsS -X POST "$1/db/$2/vector" -H 'content-type: application/json' \
            -d "$v" >/dev/null
    done < "$WORK/load_$2.vecs.jsonl"
}

# MCP over streamable HTTP: initialize once, keep the session id.

MCP_SESSION=""
mcp_post() {  # mcp_post <json-rpc body> -> prints the JSON-RPC response
    local hdrs body
    hdrs=$(mktemp)
    body=$(curl -sS -D "$hdrs" -X POST "$L/mcp" \
        -H 'content-type: application/json' \
        -H 'accept: application/json, text/event-stream' \
        ${MCP_SESSION:+-H "mcp-session-id: $MCP_SESSION"} \
        -d "$1")
    local sid
    sid=$(grep -i '^mcp-session-id:' "$hdrs" | tr -d '\r' | awk '{print $2}' || true)
    [ -n "$sid" ] && MCP_SESSION=$sid
    rm -f "$hdrs"
    # an SSE answer carries the JSON-RPC message on its data: line
    if printf '%s' "$body" | grep -q '^data:'; then
        body=$(printf '%s' "$body" | grep '^data:' | tail -1 | sed 's/^data: *//')
    fi
    printf '%s' "$body"
}

mcp_init() {
    local req='{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"contexts-demo","version":"1"}}}'
    mcp_post "$req" >/dev/null
    mcp_post '{"jsonrpc":"2.0","method":"notifications/initialized"}' >/dev/null
}

tool() {  # tool <name> <arguments json> -> prints the tool result (decoded)
    # tool runs in a command substitution: keep the request id in a file
    MCP_ID=$(( $(cat "$WORK/mcp_id" 2>/dev/null || echo 0) + 1 ))
    echo "$MCP_ID" > "$WORK/mcp_id"
    local req resp result
    req=$(jq -cn --arg n "$1" --argjson a "$2" --argjson id "$MCP_ID" \
        '{jsonrpc:"2.0", id:$id, method:"tools/call", params:{name:$n, arguments:$a}}')
    resp=$(mcp_post "$req")
    result=$(printf '%s' "$resp" | jq -c '.result.content[0].text | fromjson' 2>/dev/null || printf '%s' "$resp")
    log_exchange "MCP tools/call $1" "$req" "$result"
    printf '%s' "$result"
}

rpc() {  # rpc <node> <erlang expr returning a term> -> prints the term
    erl -name "${NODE}_ctl_$$@127.0.0.1" -setcookie "$COOKIE" -noshell -noinput \
        -eval "R = rpc:call('${NODE}_$1@127.0.0.1', erlang, apply, [fun() -> $2 end, []]), io:format(\"~p~n\", [R]), halt()."
}

# ---------------------------------------------------------------- run

rm -rf "$WORK"
mkdir -p "$WORK" "$OUT_DIR"
: > "$TRANSCRIPT"
printf '# Barrel contexts demo transcript\n\nCorpus: %s\n\n' "$(basename "$CORPUS")" >> "$TRANSCRIPT"

note "Start three nodes"
start_node l "$PORT_L"
start_node r1 "$PORT_R1"
start_node r2 "$PORT_R2"
echo "  L=$L R1=$R1 R2=$R2"

note "Load one OTP application per node"
load_app "$L" otp_tools tools
load_app "$R1" otp_sasl sasl
load_app "$R2" otp_eunit eunit

note "Register the cards on node L"
TOOLS=$(rest POST "$L/contexts" '{"name":"otp/tools","title":"OTP tools","topics":["erlang","profiling"],"locations":[{"kind":"local","db":"otp_tools"}]}' | jq -r .id)
SASL=$(rest POST "$L/contexts" "{\"name\":\"otp/sasl\",\"title\":\"OTP sasl\",\"topics\":[\"erlang\",\"release\"],\"locations\":[{\"kind\":\"remote\",\"endpoint\":\"$R1\",\"db\":\"otp_sasl\"}]}" | jq -r .id)
EUNIT=$(rest POST "$L/contexts" "{\"name\":\"otp/eunit\",\"title\":\"OTP eunit\",\"topics\":[\"erlang\",\"testing\"],\"locations\":[{\"kind\":\"remote\",\"endpoint\":\"$R2\",\"db\":\"otp_eunit\"}]}" | jq -r .id)
echo "  tools=$TOOLS sasl=$SASL eunit=$EUNIT"

note "Agent session over MCP"
mcp_init
R=$(tool context_capabilities '{}')
check "context_capabilities states the shapes and limits before any query" '(.shapes | length) == 3 and .limits.max_contexts == 8 and .limits.max_limit == 1000 and .offline == false' "$R"

R=$(tool context_list '{}')
check "context_list shows the three cards" '[.contexts[].name] | index("otp/tools") != null and index("otp/sasl") != null and index("otp/eunit") != null' "$R"

R=$(tool context_discover '{"q":"release"}')
check "context_discover filters by topic and says so" "[.contexts[].id] == [\"$SASL\"] and (.summary | test(\"otp/sasl\"))" "$R"

R=$(tool context_inspect '{"context":"otp/sasl"}')
check "context_inspect takes a name and returns the remote location" ".id == \"$SASL\" and .locations[0].kind == \"remote\" and .locations[0].db == \"otp_sasl\"" "$R"

R=$(tool context_inspect '{"context":"otp/sasll"}')
check "a mistyped name is refused with a close match" '.error == "unknown_context" and .details.suggestions[0].name == "otp/sasl" and (.hint | test("otp/sasl"))' "$R"

ORDERED='SELECT id, path, lines FROM c WHERE lines > 300 ORDER BY lines DESC LIMIT 10'
ALL='["otp/tools","otp/sasl","otp/eunit"]'
R=$(tool context_query "$(jq -cn --arg q "$ORDERED" --argjson c "$ALL" '{query:$q, contexts:$c}')")
check "ordered rows: all three sources answered, by name" '.execution == "succeeded" and .merge == "ordered" and .coverage.answered == 3 and ([.sources[].name] == ["otp/tools","otp/sasl","otp/eunit"])' "$R"
check "ordered rows: sorted by lines desc across contexts" '[.rows[].lines] as $l | $l == ($l | sort | reverse) and ($l | length) == 10' "$R"
check "ordered rows: rows from more than one context, named" '[.rows[]._ctx_name] | unique | length > 1' "$R"
check "provenance: local and remote locations with observed versions" '[.sources[].location.kind] == ["local","remote","remote"] and all(.sources[]; .version.kind == "live" and (.version.observed.instance_id | type) == "string")' "$R"
check "the summary says who answered and how rows were merged" '.summary | test("All 3 contexts answered") and test("ORDER BY order")' "$R"

BM25="SELECT b.id, b.path, b._score FROM bm25_top_k('release upgrade', k => 3) AS b"
R=$(tool context_query "$(jq -cn --arg q "$BM25" --argjson c "$ALL" '{query:$q, contexts:$c}')")
check "bm25 retrieval is grouped, never a cross-context ranking" '.merge == "grouped" and .relevance == false and (.groups | length) == 3 and ([.groups[].name] | length) == 3' "$R"
check "bm25 retrieval reports exact retrieval per source" 'all(.sources[]; .retrieval == "exact" and .status == "ok")' "$R"
check "the summary says why results are grouped" '.summary | test("BM25 scores are not comparable")' "$R"

R=$(tool context_query "$(jq -cn --arg q "$BM25" '{query:$q, contexts:["otp/tools","otp/sasl"], merge:"rrf"}')")
check "rrf is refused, with the merges to use instead" '.error == "merge_not_supported" and .details.merge == "rrf" and (.details.allowed | index("grouped")) != null and (.hint | length) > 0' "$R"

R=$(tool context_query '{"query":"SELECT * FROM c ORDER BY path","contexts":["otp/tools"]}')
check "a row query without LIMIT is rejected with the fix" '.error == "limit_required" and .details.max_limit == 1000 and (.hint | test("LIMIT"))' "$R"

note "Working set: attach, then materialize a slice from a remote context"
R=$(tool context_attach '{"context":"otp/tools"}')
WS=$(printf '%s' "$R" | jq -r .id)
check "context_attach creates a working set with a local member" '.members[0].mode == "local" and .members[0].name == "otp/tools" and (.id | startswith("ws_"))' "$R"
R=$(tool context_attach "{\"working_set\":\"$WS\",\"context\":\"otp/sasl\"}")
check "second member is remote, attached without copying" '.members[1].mode == "remote" and .members[1].answers_offline == false and .usage.bytes == 0' "$R"

SLICEQ="SELECT b.id FROM bm25_top_k('test', k => 5) AS b"
R=$(tool context_materialize "$(jq -cn --arg ws "$WS" --arg q "$SLICEQ" '{working_set:$ws, from_query:{query:$q, contexts:["otp/eunit"]}, include:{embeddings:false}}')")
check "materialize saved a complete slice of the remote context" ".slices[0].context == \"$EUNIT\" and .slices[0].name == \"otp/eunit\" and .slices[0].status == \"complete\" and .slices[0].docs > 0" "$R"
check "slice provenance records what the source observed" '.slices[0].derived.observed.instance_id | type == "string"' "$R"
SLICE_DOCS=$(printf '%s' "$R" | jq .slices[0].docs)

R=$(tool context_working_sets "{\"working_set\":\"$WS\"}")
check "the working set says which members answer offline" '[.members[].answers_offline] == [true, false, true] and (.summary | test("saved slice"))' "$R"

note "Node R2 goes down"
kill "$PID_r2"; wait "$PID_r2" 2>/dev/null || true
R=$(tool context_query "$(jq -cn --arg q "$ORDERED" --argjson c "$ALL" '{query:$q, contexts:$c, per_context_timeout_ms:1500}')")
check "partial coverage: R2's context is reported unreachable, with a hint" ".execution == \"partial\" and .coverage.missing == [\"$EUNIT\"] and .sources[2].status == \"unreachable\" and (.sources[2].error.hint | length) > 0" "$R"
check "no row comes from the failed source" "[.rows[]._ctx] | index(\"$EUNIT\") == null" "$R"
check "the summary names the missing context" '.summary | test("2 of 3 contexts answered") and test("otp/eunit:")' "$R"

note "Offline: the working set answers from local copies"
R=$(tool context_offline '{"offline":true}')
check "node L is offline" '.offline == true' "$R"
WSQ='SELECT id, path, lines FROM c ORDER BY lines DESC LIMIT 50'
R=$(tool context_query "$(jq -cn --arg q "$WSQ" --arg ws "$WS" '{query:$q, working_set:$ws}')")
check "working set query is partial with one skipped member" '.execution == "partial" and .coverage.skipped == 1 and .working_set != null' "$R"
check "local member answers live" ".sources[0].status == \"ok\" and .sources[0].membership == \"live\"" "$R"
check "remote member is skipped_offline, never contacted, and says what to do" ".sources[1].context == \"$SASL\" and .sources[1].status == \"skipped_offline\" and .sources[1].error.reason == \"no_local_copy\" and (.sources[1].error.hint | test(\"import\"))" "$R"
check "slice answers with retrieved_set membership" ".sources[2].context == \"$EUNIT\" and .sources[2].status == \"ok\" and .sources[2].membership == \"retrieved_set\" and .sources[2].version.kind == \"retrieved_set\" and .sources[2].rows == $SLICE_DOCS" "$R"
check "the summary says what the slice covers" '.summary | test("saved slice")' "$R"
R=$(tool context_materialize "$(jq -cn --arg ws "$WS" '{working_set:$ws, from_query:{query:"SELECT b.id FROM bm25_top_k('"'"'release'"'"', k => 3) AS b", contexts:["otp/sasl"]}}')")
check "materialize while offline is refused before any work" '.error == "offline" and .details.remote_contexts == ["'"$SASL"'"]' "$R"
tool context_offline '{"offline":false}' >/dev/null

note "Import a published snapshot of otp/sasl"
EXPORT_DIR="$WORK/export_sasl_g1"
OUT=$(rpc r1 "barrel_ctx_export:export(<<\"otp_sasl\">>, \"$EXPORT_DIR\", #{owner => barrel_server, context => <<\"$SASL\">>, generation => 1})")
printf '### Export on R1 (operator, Erlang)\n\n```erlang\n%s\n```\n\n' "$(printf '%s' "$OUT" | head -c 600)" >> "$TRANSCRIPT"
[ -f "$EXPORT_DIR/manifest.json" ] || ls "$EXPORT_DIR" >/dev/null
R=$(tool context_import "$(jq -cn --arg d "$EXPORT_DIR" '{dir:$d}')")
WS2=$(printf '%s' "$R" | jq -r .id)
check "import creates a working set with a snapshot member at generation 1" ".members[0].mode == \"snapshot\" and .members[0].generation == 1 and .members[0].context == \"$SASL\" and .members[0].answers_offline == true" "$R"
R=$(tool context_query "$(jq -cn --arg q "$WSQ" --arg ws "$WS2" '{query:$q, working_set:$ws, offline:true}')")
check "snapshot answers offline as a complete generation" '.execution == "succeeded" and .sources[0].membership == "complete_generation" and .sources[0].version == {"kind":"generation","generation":1} and (.summary | test("generation 1"))' "$R"

note "Clean up"
R=$(tool context_detach "{\"working_set\":\"$WS\",\"context\":\"otp/eunit\"}")
check "detach by name removes the slice member" "[.members[].context] | index(\"$EUNIT\") == null" "$R"
R=$(tool context_working_sets '{}')
check "context_working_sets lists both working sets" "[.working_sets[].id] | index(\"$WS\") != null and index(\"$WS2\") != null" "$R"
R=$(tool context_working_set_delete "{\"working_set\":\"$WS2\"}")
check "context_working_set_delete removes a working set" ".deleted == \"$WS2\"" "$R"
R=$(tool context_working_sets "{\"working_set\":\"$WS2\"}")
check "a deleted working set is reported unknown, with the next step" '.error == "unknown_working_set" and (.hint | length) > 0' "$R"

echo
echo "passed: $pass, failed: $fail"
printf '\nResult: %d passed, %d failed\n' "$pass" "$fail" >> "$TRANSCRIPT"
echo "transcript: $TRANSCRIPT"
[ "$fail" -eq 0 ]
