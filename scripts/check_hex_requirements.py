#!/usr/bin/env python3
"""Check that a built Hex tarball declares every dependency the app needs.

Usage: check_hex_requirements.py apps/<app>

rebar3_hex builds the package's "requirements" from rebar.lock, not from
rebar.config. A dependency resolved through _checkouts never enters the lock,
so it is silently dropped from the tarball: the package publishes with a
missing dependency, and a consumer only finds out at runtime with an undef.
Publishing is irreversible, so check before, not after.

Run `rebar3 hex build` in the app directory first (this script does not build).
"""
import glob
import os
import re
import sys
import tarfile
import tempfile


def rebar_config_deps(app_dir):
    """Top-level {deps, [...]} names from rebar.config. Profiles are ignored:
    only the default profile lands in the package."""
    path = os.path.join(app_dir, "rebar.config")
    text = open(path, encoding="utf-8").read()
    text = re.sub(r"^\s*%.*$", "", text, flags=re.M)
    m = re.search(r"^\{deps,\s*\[(.*?)\]\}\s*\.", text, re.S | re.M)
    if not m:
        return set()
    return set(re.findall(r"\{\s*([a-z_][a-z_0-9]*)\s*,", m.group(1)))


def tarball_requirements(app_dir, app):
    pattern = os.path.join(app_dir, "_build", "**", f"{app}-*.tar")
    tars = [t for t in glob.glob(pattern, recursive=True) if "-docs" not in t]
    if not tars:
        sys.exit(f"{app}: no tarball; run `rebar3 hex build` in {app_dir} first")
    tar = max(tars, key=os.path.getmtime)
    with tempfile.TemporaryDirectory() as tmp:
        with tarfile.open(tar) as tf:
            tf.extract("metadata.config", tmp, filter="data")
        meta = open(os.path.join(tmp, "metadata.config"), encoding="utf-8").read()
    m = re.search(r'\{<<"requirements">>,\s*\[(.*?)\]\}\.', meta, re.S)
    if not m:
        return tar, set()
    # A dep is a name whose value opens with the "app" key. Matching bare
    # binaries would also pick up the inner app/optional/requirement keys.
    names = re.findall(r'\{<<"([a-z_][a-z_0-9]*)">>,\s*\[\{<<"app">>', m.group(1))
    return tar, set(names)


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    app_dir = sys.argv[1].rstrip("/")
    app = os.path.basename(app_dir)

    declared = rebar_config_deps(app_dir)
    tar, shipped = tarball_requirements(app_dir, app)
    missing = sorted(declared - shipped)

    print(f"{app}")
    print(f"  tarball:      {tar}")
    print(f"  rebar.config: {sorted(declared) or '-'}")
    print(f"  requirements: {sorted(shipped) or '-'}")

    if missing:
        print(f"\n  MISSING from the package: {missing}")
        if os.path.isdir(os.path.join(app_dir, "_checkouts")):
            print("  _checkouts/ is present. Deps resolved there never reach")
            print("  rebar.lock, so they are dropped. Remove it, publish the")
            print("  siblings first, then rebuild.")
        sys.exit(1)

    print("\n  OK: every rebar.config dep is declared in the package")


if __name__ == "__main__":
    main()
