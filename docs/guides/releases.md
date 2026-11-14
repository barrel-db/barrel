# Barrel releases

Barrel is released as a bundle: one dated snapshot of the whole repository,
cut each time apps are bumped and released, with every app at the version it
carries at that commit. Read this page when you want to know which app versions
go together, deploy barrel_server from a release, or pin a dependency to a
known state.

## What a bundle is

A merge to `main` that passes CI and changes the version of at least one app is
a bundle named `YYYY.MM.N`: the year and month of the merge, and N counting the
bundles of that month from 0 (`2026.11.0`, `2026.11.1`, ...). Several bundles
can come out the same day. A merge that bumps no app releases nothing.

Each bundle has a git tag `vYYYY.MM.N` and a GitHub release holding:

- the version of every app at that commit, and the CHANGELOG section of each
  app whose version changed since the previous bundle;
- `barrel_server` tarballs for Linux x64, Linux arm64 and macOS arm64;
- the `barrel-server` image, `ghcr.io/barrel-db/barrel-server:YYYY.MM.N`.

Apps are not tagged on their own. Each app is published to Hex under its own
version, which the bundle notes record.

## Find the app versions of a bundle

```sh
gh release view v2026.11.8 --repo barrel-db/barrel
```

Or from a clone:

```sh
git show v2026.11.8:apps/barrel_docdb/src/barrel_docdb.app.src | grep vsn
```

## Run a bundle's barrel_server

The tarball carries its own Erlang runtime:

```sh
gh release download v2026.11.9 --repo barrel-db/barrel -p 'barrel_server-2026.11.9-linux-x64.tar.gz'
mkdir barrel && tar xzf barrel_server-2026.11.9-linux-x64.tar.gz -C barrel
barrel/bin/barrel_server foreground
```

Or the image:

```sh
docker run -p 8080:8080 -e BARREL_AUTH_TOKENS=secret ghcr.io/barrel-db/barrel-server:2026.11.9
```

`barrel/bin/barrel_server versions` and the image's
`org.opencontainers.image.version` label give the bundle.

## Pin a bundle

Depend on Hex versions for libraries. To build against an exact state of the
repository, use the bundle tag:

```erlang
{deps, [
    {barrel, {git, "https://github.com/barrel-db/barrel.git", {tag, "v2026.11.8"}}}
]}.
```

## Notes

- `:latest` follows the newest bundle; pin a bundle tag in production.
- Sort bundles with a version-aware sort (`sort -V`, `git tag --sort=v:refname`):
  past ten bundles in a month, `2026.11.10` sorts before `2026.11.9` by name.
- Bundles before the release workflow existed were backfilled at the commits
  that changed an app's version to one published on Hex; they have notes, no
  tarballs or image.
- The tags `v0.2.0` to `v1.1.0` predate bundles and are kept as they are.
