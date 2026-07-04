# Distribute the umbrella apps

The umbrella is the source of truth: you develop every app in one tree so
cross-app changes stay atomic. This guide is for when you need to hand an
individual app to someone outside the umbrella. There are three ways, and you
can use all of them at once: publish to Hex (a versioned package), mirror a
per-app git repo (a browsable standalone repo), and consume an app from another
project. Read this before cutting a release or wiring a downstream dependency.

## Publish order

The apps depend on each other, so publish leaves first and work up. A package
can only be published after every sibling it depends on is already available.

```
barrel_docdb (0.8.0)   barrel_embed (2.2.1)   barrel_rerank (0.1.1)   barrel_faiss (0.2.1)
                              |
                       barrel_vectordb (2.0.0)
                              |
            barrel_docdb + barrel_vectordb
                              |
                         barrel (0.1.0)
                              |
                      barrel_server (0.1.0)
```

## Publish to Hex

Each publishable app carries a `hex` profile in its `rebar.config` that
re-declares its sibling apps as Hex dependencies. In the umbrella those siblings
are discovered as local apps; on Hex they must be versioned packages. Always
publish with that profile.

```console
$ cd apps/barrel_embed
$ rebar3 as hex hex publish
```

For an app with siblings, the `hex` profile already lists them. For example
`apps/barrel_vectordb/rebar.config`:

```erlang
{hex, [
    {deps, [
        {barrel_embed, "~> 2.2"}
    ]}
]}.
```

Publish in dependency order:

```console
$ for app in barrel_docdb barrel_embed barrel_rerank barrel_faiss \
             barrel_vectordb barrel barrel_server; do
    (cd apps/$app && rebar3 as hex hex publish --yes)
  done
```

Notes:

- The `{files, [...]}` list in each `.app.src` controls what ships in the
  tarball. Keep it current when you add `priv/` assets or includes.
- Every dependency resolves from Hex, including `barrel_server`'s `livery`
  (0.4.4). Hex rejects git deps, so keep it that way: do not reintroduce a
  `{git, ...}` dep in an app you intend to publish.
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
- Replace the example remote with your forge. The `links` in each `.app.src`
  still point at old per-repo GitHub URLs; update them to the real mirror or the
  umbrella.
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
