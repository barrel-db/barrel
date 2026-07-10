# Distribute the umbrella apps

The umbrella is the source of truth: you develop every app in one tree so
cross-app changes stay atomic. This guide is for when you need to hand an
individual app to someone outside the umbrella. There are three ways, and you
can use all of them at once: publish to Hex (a versioned package), mirror a
per-app git repo (a browsable standalone repo), and consume an app from another
project. Read this before cutting a release or wiring a downstream dependency.

## Prerequisites

Before publishing an app:

- Bump its `vsn` in `apps/<app>/src/<app>.app.src` (Hex refuses to overwrite a
  released version), and bump the coupled sibling pins in lockstep in the default `deps` of the
  apps that depend on it (for example bumping `barrel_vectordb` means updating
  the `{barrel_vectordb, ...}` pin in `barrel`).
- Ensure the app has a `README.md`, a `LICENSE`, a `CHANGELOG.md`, an `ex_doc`
  block, and `rebar3_hex` in its `project_plugins`.
- Tag the release: per app `<app>-v<version>` (e.g. `barrel_docdb-v1.0.0`), and
  the umbrella `v<version>`. CI recognizes both tag globs.

## Publish order

The apps depend on each other, so publish leaves first and work up. A package
can only be published after every sibling it depends on is already on Hex.
Current versions and order:

1. `barrel_crypto` 1.0.0
2. `barrel_embed` 2.3.0
3. `barrel_docdb` 1.0.0 (needs barrel_crypto)
4. `barrel_rerank` 1.0.0
5. `barrel_faiss` 1.0.0 (optional; needs the FAISS C++ library to build)
6. `barrel_vectordb` 2.1.2 (needs barrel_embed, barrel_crypto)
7. `barrel` 1.0.1 (needs barrel_docdb, barrel_vectordb, barrel_crypto)
8. `barrel_spaces` 1.0.1 (needs barrel, barrel_docdb, barrel_crypto)
9. `barrel_server` 1.0.1 (needs barrel, barrel_spaces)

`barrel_vectordb` and `barrel_embed` keep their 2.x lines: they were already
past 1.0, and `barrel_embed` 2.2.1 is on Hex. Everything else moves to 1.0.0,
which is a promise that its API will not break without a major bump.

## Publish to Hex

Each publishable app declares its sibling apps as Hex dependencies in the
**default** `{deps, [...]}` of its `rebar.config`. This is not optional and not
cosmetic: rebar3_hex builds the package's `requirements` from the default-profile
lockfile, so a dependency placed in a `hex` (or any other) profile is silently
dropped from the tarball. The package then publishes and installs cleanly and the
consumer hits an `undef` at runtime. Do not put sibling deps in a `hex` profile.

For example `apps/barrel_spaces/rebar.config`:

```erlang
{deps, [
    {barrel_crypto, "~> 1.0"},
    {barrel_docdb, "~> 1.0"},
    {barrel, "~> 1.0"}
]}.
```

In the umbrella these resolve as local project apps; on Hex they resolve from
Hex. Use `~>` pins so a sibling's patch release does not force a re-release of
every app that depends on it.

```console
$ cd apps/barrel_embed
$ rebar3 hex publish
```

Publish in dependency order:

```console
$ for app in barrel_crypto barrel_embed barrel_docdb barrel_rerank \
             barrel_faiss barrel_vectordb barrel barrel_spaces barrel_server; do
    (cd apps/$app && rebar3 hex publish --yes)
  done
```

Before publishing, dry-run each tarball with `rebar3 hex build` and check
its contents (especially the NIF apps `barrel_vectordb`/`barrel_faiss`, whose
`.app.src` `{files, [...]}` must carry `c_src` and the root `do_cmake.sh` /
`do_faiss.sh` build scripts).

## Check the declared dependencies first

rebar3_hex builds the package's `requirements` from `rebar.lock`, not from
`rebar.config`. Two things silently drop a dependency from the tarball:

- **`_checkouts/`.** A sibling resolved through a checkout never enters the
  lock, so it disappears from `requirements`. The package publishes and installs
  cleanly, and the consumer hits an `undef` at runtime.
- **A stale `apps/<app>/rebar.lock`.** It is gitignored, so it is whatever your
  last local build left behind. A dep locked at a transitive level does not
  become a requirement even when `rebar.config` names it directly.

Publishing is irreversible: a version cannot be re-cut. So for each app, in
order, remove both, rebuild, and check:

```console
$ cd apps/<app>
$ rm -rf _checkouts rebar.lock
$ rebar3 compile                     # regenerate rebar.lock from Hex
$ rebar3 hex build                   # siblings resolve from Hex (default deps)
$ cd ../..
$ python3 scripts/check_hex_requirements.py apps/<app>
```

`rebar3 hex build` needs a lockfile, so run `rebar3 compile` first (removing
`rebar.lock` without regenerating it makes the build error with "No lockfile
detected"). Siblings resolve from Hex, so every sibling an app depends on must
already be published (the publish order above guarantees this).

The script compares the tarball's `requirements` against the app's `rebar.config`
deps and exits non-zero on any omission. It only passes once every sibling the
app depends on is already on Hex, which is exactly the publish order above.

Notes:

- The `{files, [...]}` list in each `.app.src` controls what ships in the
  tarball. Keep it current when you add `priv/` assets or includes.
- Every dependency resolves from Hex, including `barrel_server`'s `livery`
  (0.5.1) and `barrel_mcp` (2.2.4). Hex rejects git deps, so keep it that way:
  do not reintroduce a `{git, ...}` dep in an app you intend to publish.
- `barrel_faiss` ships an NIF that needs the FAISS C++ library at build time.
  The package builds only where that toolchain is present.
- Bump the `vsn` in `.app.src` before publishing; Hex refuses to overwrite a
  released version.

## Mirror a per-app git repo

Use this when you want a standalone, browsable git repo for an app (for example
to depend on it with a `{git, ...}` dep, or to publish it on its own forge). The
umbrella stays the source of truth and you push a one-app history out.

`git subtree split` produces a branch holding only that app's history. It is
repeatable, so you can re-run it each release to update the mirror.

```console
$ git subtree split --prefix=apps/barrel_embed -b export/barrel_embed
$ git push git@code.barrel-db.eu:barrel-db/barrel_embed.git export/barrel_embed:main
```

`git filter-repo` does a one-time clean extraction with the app rewritten to the
repo root. It rewrites history, so it is for seeding a fresh repo, not syncing.

```console
$ git clone . /tmp/barrel_embed && cd /tmp/barrel_embed
$ git filter-repo --subdirectory-filter apps/barrel_embed
$ git remote add origin git@code.barrel-db.eu:barrel-db/barrel_embed.git
$ git push -u origin main
```

Notes:

- Prefer `subtree split` for ongoing mirrors and `filter-repo` for the initial
  carve-out.
- Replace the example remote with your forge. Package `links` in each
  `.app.src` point at the umbrella repo; per-app mirrors are optional (the
  umbrella is the source of truth).
- Drive the mirrors from a release job keyed on the `barrel_*-v*` tags that CI
  already recognizes, so a tagged release fans out to the standalone repos.

## Consume an app elsewhere

From another project, depend on a distributed app in the form that matches how
you shipped it.

Hex package (after publishing):

```erlang
{deps, [
    {barrel_embed, "~> 2.2"}
]}.
```

Git mirror:

```erlang
{deps, [
    {barrel_embed, {git, "https://code.barrel-db.eu/barrel-db/barrel_embed.git",
                    {tag, "v2.2.1"}}}
]}.
```

Local checkout of the umbrella (fastest for co-development, no publish step):

```erlang
{deps, [
    {barrel_embed, {path, "../barrel/apps/barrel_embed"}}
]}.
```

If instead you want the apps to live in their own repos and have the umbrella
pull them in (the inverse of mirroring), use git submodules:

```console
$ git submodule add git@code.barrel-db.eu:barrel-db/barrel_embed.git apps/barrel_embed
```

This makes the per-app repos the source of truth and the umbrella a consumer
that pins each app to a commit. It brings back coordinated multi-repo changes,
so use it only if you decide to invert the current model.
