#!/usr/bin/env python3
"""Validate Erlang code snippets in markdown docs against the umbrella's exports.

Usage: check_snippets.py <umbrella_root> <markdown_file>...

Extracts every `mod:fun(Args)` call from erlang code fences, resolves the arity
by balanced-paren scanning, and confirms mod:fun/arity is exported by
apps/*/src/mod.erl.

Skipped: known OTP and dependency modules; remote types (`mod:type()` written
after a `::`, a type reference rather than a call); and the placeholder modules
of illustrative snippets, named `my_*`, `your_*`, or `example_*`.
"""
import re
import sys
import glob
import os

PLACEHOLDER = re.compile(r"^(my|your|example)_")

OTP = {
    "application", "erlang", "io", "io_lib", "lists", "maps", "sets", "dict",
    "timer", "os", "file", "filename", "filelib", "logger", "json", "binary",
    "string", "gen_server", "gen_statem", "supervisor", "proplists", "rand",
    "crypto", "base64", "unicode", "code", "ets", "persistent_term", "re",
    "httpc", "inets", "ssl", "public_key", "sys", "queue", "math", "rpc",
    "net_kernel", "erl_eval", "ct", "eunit", "meck", "hackney", "jsx",
    "opentelemetry", "otel_tracer", "otel_span", "instrument", "rocksdb",
    "prometheus", "telemetry", "hlc", "match_trie",
}

CLOSERS = {"(": ")", "[": "]", "{": "}", "<<": ">>"}


def arity_of(text, open_idx):
    """Count top-level args starting at text[open_idx] == '('. Return -1 if unbalanced."""
    depth = 0
    commas = 0
    saw_arg = False
    i = open_idx
    n = len(text)
    while i < n:
        c = text[i]
        if c in "\"'":
            q = c
            i += 1
            while i < n and text[i] != q:
                if text[i] == "\\":
                    i += 1
                i += 1
        elif c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth == 0:
                return commas + 1 if saw_arg else 0
        elif c == "," and depth == 1:
            commas += 1
        elif depth == 1 and not c.isspace():
            saw_arg = True
        i += 1
    return -1


def exports_of(path):
    src = open(path, encoding="utf-8", errors="replace").read()
    out = set()
    for m in re.finditer(r"-export\(\s*\[(.*?)\]\s*\)", src, re.S):
        body = re.sub(r"%.*$", "", m.group(1), flags=re.M)
        for item in body.split(","):
            item = item.strip()
            mm = re.match(r"^([a-z_][a-zA-Z0-9_]*)\s*/\s*(\d+)$", item)
            if mm:
                out.add((mm.group(1), int(mm.group(2))))
    return out


def main():
    root, files = sys.argv[1], sys.argv[2:]
    srcs = {}
    for pat in ("apps/*/src/*.erl", "apps/*/src/*/*.erl",
                "apps/*/bench/*.erl", "apps/*/bench/src/*.erl"):
        for p in glob.glob(os.path.join(root, pat)):
            srcs.setdefault(os.path.basename(p)[:-4], p)

    problems = []
    checked = 0
    for md in files:
        text = open(md, encoding="utf-8", errors="replace").read()
        for fence in re.finditer(r"```(erlang|erl)\n(.*?)```", text, re.S):
            code = fence.group(2)
            base = text[: fence.start()].count("\n") + 1
            for call in re.finditer(r"\b([a-z_][a-zA-Z0-9_]*):([a-z_][a-zA-Z0-9_]*)\(", code):
                mod, fun = call.group(1), call.group(2)
                if mod in OTP or PLACEHOLDER.match(mod):
                    continue
                # A remote type, e.g. `shards :: #{_ => barrel_faiss:index()}`.
                line_start = code.rfind("\n", 0, call.start()) + 1
                if "::" in code[line_start : call.start()]:
                    continue
                line = base + code[: call.start()].count("\n") + 1
                if mod not in srcs:
                    problems.append(f"{md}:{line}: unknown module {mod} (in {mod}:{fun})")
                    continue
                ar = arity_of(code, call.end() - 1)
                exps = exports_of(srcs[mod])
                checked += 1
                if ar < 0:
                    if not any(f == fun for f, _ in exps):
                        problems.append(f"{md}:{line}: {mod}:{fun} not exported")
                    continue
                if (fun, ar) not in exps:
                    alt = sorted(a for f, a in exps if f == fun)
                    if alt:
                        problems.append(
                            f"{md}:{line}: {mod}:{fun}/{ar} not exported (exported arities: {alt})")
                    else:
                        problems.append(f"{md}:{line}: {mod}:{fun}/{ar} does not exist")

    for p in problems:
        print(p)
    print(f"\n{checked} calls checked, {len(problems)} unresolved", file=sys.stderr)
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
