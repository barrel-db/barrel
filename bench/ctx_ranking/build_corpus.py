# Builds corpus.jsonl and queries.jsonl from OTP's installed source.
import glob, json, os, re
ROOT = "/opt/local/lib/erlang/lib"
MD = re.compile(r'-moduledoc\s*(?:"""(.*?)"""|"((?:[^"\\]|\\.)*)")\s*\.', re.S)
docs, queries = [], []
for path in sorted(glob.glob(f"{ROOT}/*/src/**/*.erl", recursive=True)):
    app = os.path.relpath(path, ROOT).split("/")[0].rsplit("-", 1)[0]
    mod = os.path.basename(path)[:-4]
    try:
        body = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        continue
    m = MD.search(body)
    doc = (m.group(1) or m.group(2) or "").strip() if m else ""
    first = re.split(r"(?<=[.!?])\s", " ".join(doc.split()), 1)[0] if doc else ""
    nodoc = MD.sub("", body) if m else body
    docs.append({"id": mod, "app": app, "path": os.path.relpath(path, ROOT),
                 "body": body, "body_nodoc": nodoc, "moduledoc": first})
    if len(first) >= 30 and not first.lower().startswith(("false", "this module is internal")):
        queries.append({"query": first, "target": mod, "app": app})
with open("corpus.jsonl", "w") as f:
    for d in docs: f.write(json.dumps(d) + "\n")
with open("queries.jsonl", "w") as f:
    for q in queries: f.write(json.dumps(q) + "\n")
apps = {}
for d in docs: apps[d["app"]] = apps.get(d["app"], 0) + 1
print(len(docs), "docs,", len(queries), "queries,", len(apps), "apps")
print(sorted(apps.items(), key=lambda x: -x[1])[:10])
