# barrel_objectdb

Object storage abstraction for Barrel. Stores opaque object bytes under binary
keys behind a pluggable backend (`barrel_objectdb_backend`). The built-in backend
is `barrel_objectdb_fs` (local disk), which needs no external services. An S3
backend (via `livery_s3`) and a remote backend are opt-in and added later.

This is the foundation for document attachments (the docdb attachment backend can
delegate here) and a future agent filesystem.

## Use

```erlang
{ok, Store} = barrel_objectdb:open(barrel_objectdb_fs, #{dir => "/tmp/barrel_objects"}),
ok = barrel_objectdb:put(Store, <<"blob/1">>, <<"bytes">>),
{ok, <<"bytes">>} = barrel_objectdb:get(Store, <<"blob/1">>),
{ok, [<<"blob/1">>]} = barrel_objectdb:list(Store, <<"blob/">>),
ok = barrel_objectdb:delete(Store, <<"blob/1">>).
```
