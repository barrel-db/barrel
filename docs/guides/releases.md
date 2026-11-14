# Barrel releases

Barrel is released as a bundle: one numbered snapshot of the whole repository,
with every app at the version it carries at that commit. Read this page when you
want to know which app versions go together, deploy barrel_server from a
release, or pin a dependency to a known state.

## What a bundle is

Every merge to `main` that passes CI is a bundle named `b<N>`, where N is the
number of commits on `main` at that point (the scheme llama.cpp uses). Numbers
only go up; the gaps between them are the commits of each merged change.

Each bundle has a git tag and a GitHub release holding:

- the version of every app at that commit, and the CHANGELOG section of each
  app whose version changed since the previous bundle;
- `barrel_server` tarballs for Linux x64, Linux arm64 and macOS arm64;
- the `barrel-server` image, `ghcr.io/barrel-db/barrel-server:b<N>`.

Apps are not tagged on their own. Each app is published to Hex under its own
version, which the bundle notes record.

## Find the app versions of a bundle

```sh
gh release view b336 --repo barrel-db/barrel
```

Or from a clone:

```sh
git show b336:apps/barrel_docdb/src/barrel_docdb.app.src | grep vsn
```

## Run a bundle's barrel_server

The tarball carries its own Erlang runtime:

```sh
gh release download b336 --repo barrel-db/barrel -p 'barrel_server-b336-linux-x64.tar.gz'
mkdir barrel && tar xzf barrel_server-b336-linux-x64.tar.gz -C barrel
barrel/bin/barrel_server foreground
```

Or the image:

```sh
docker run -p 8080:8080 -e BARREL_AUTH_TOKENS=secret ghcr.io/barrel-db/barrel-server:b336
```

`barrel/bin/barrel_server versions` and the image's
`org.opencontainers.image.version` label give the bundle.

## Pin a bundle

Depend on Hex versions for libraries. To build against an exact state of the
repository, use the bundle tag:

```erlang
{deps, [
    {barrel, {git, "https://github.com/barrel-db/barrel.git", {tag, "b336"}}}
]}.
```

## Notes

- `:latest` follows the newest bundle; pin `:b<N>` in production.
- Bundles before the release workflow existed were backfilled at the commits
  that changed an app's version; they have notes, no tarballs or image.
- The tags `v0.2.0` to `v1.1.0` predate bundles and are kept as they are.
