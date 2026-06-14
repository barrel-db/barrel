# Planned responsibility moves

This note records which responsibilities may move between applications later,
and what is deliberately left in place for now. Read it before proposing a
refactor that crosses app boundaries, so the first import stays mechanical and
the later moves stay intentional.

## Not moved in this import

The first import is an organization change only. The following stay exactly
where they are today:

- BM25 and hybrid search stay in `barrel_vectordb`.
- Cluster and remote APIs stay where they are.
- Storage internals (RocksDB usage, on-disk formats, NIFs) stay in their
  current apps.

No public API changes. No code logic moves.

## May move later

As the fabric takes shape, these responsibilities are candidates to move. None
of this happens in this import.

| Responsibility | Target app | Status |
|----------------|------------|--------|
| Metadata / catalog truth | `barrel_docdb` | exists |
| Object bytes | `barrel_object` | future |
| Vector segments | `barrel_vectordb` | exists |
| Durability and query-routing agents | `barrel_fabric` | future |

## Why defer

Moving a responsibility means changing app boundaries and possibly public APIs.
Doing that in the same step as the import would make the change hard to review
and risk breaking standalone and local users. Keeping the import mechanical lets
each later move be its own reviewable change with its own tests.

See [../architecture/overview.md](../architecture/overview.md) for the intended
split between `docdb`, `vectordb`, future `object`, and future `fabric`.
