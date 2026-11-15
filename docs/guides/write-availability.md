# Writes while a database restarts

Each database has one writer process. When it crashes, its supervisor starts
it again, and a write that arrives in between cannot be applied. Since
barrel_docdb 3.2 that write returns an error instead of exiting your process.
Read this when you handle write errors, retry writes, or run a service that
must stay up while a database restarts.

## What you get back

A write (`put_doc`, `put_docs`, `delete_doc`, `resolve_conflict`,
`put_local_doc`, `delete_local_doc`, the attachment writes and the other
write functions) returns one of:

| Result | Meaning |
|---|---|
| `{error, unavailable}` | The writer died during the call, or is between two lives. |
| `{error, timeout}` | The writer did not answer in time. |
| `{error, {db_failed, Reason}}` | The writer crashed more than 3 times in 60 seconds; the database stays down until you reopen it. |
| `{error, db_restarted}` | An attachment upload met a writer restart (unchanged). |

Reads are not affected: they never go through the writer.

An exit that does not come from the database's writer is not caught. A bug in
your own code still crashes as before.

## Retry a write

`unavailable` and `timeout` do not tell you whether the write was applied:
the writer can commit a batch and die before it answers. Retry with the
revision you wrote against, so a write that already went through answers
`conflict` instead of writing twice:

```erlang
put_with_retry(Db, Doc, 0) ->
    barrel_docdb:put_doc(Db, Doc);
put_with_retry(Db, Doc, N) ->
    case barrel_docdb:put_doc(Db, Doc) of
        {error, E} when E =:= unavailable; E =:= timeout ->
            timer:sleep(100),
            put_with_retry(Db, Doc, N - 1);
        Other ->
            Other
    end.

{ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(Db, <<"cfg">>, #{}),
put_with_retry(Db, #{<<"id">> => <<"cfg">>, <<"_rev">> => Rev, <<"v">> => 2}, 5).
```

A create needs no `_rev`: a retried create that already went through answers
`conflict` too, since the document now exists. A write with `_observed`
instead of `_rev` never answers `conflict` (it keeps concurrent versions as
siblings), so a retry of it can add a second version.

## Notes

- `{error, {db_failed, _}}` does not go away by retrying. Open the database
  again with `reopen_db/1` once you know why it failed.
- Code that caught exits around writes keeps working, but it now sees error
  tuples instead.
