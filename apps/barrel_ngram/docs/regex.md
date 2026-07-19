# Regex search

`regex/2,3` finds documents matching a PCRE regular expression. Use it when a plain
substring is not enough (alternations, character classes, quantifiers). Results are exact:
the trigram index only narrows candidates, and every candidate is confirmed with the real
regex engine.

## Use

```erlang
{ok, Hits} = barrel_ngram:regex(<<"code">>, <<"connect_\\w+_timeout">>),
{ok, More} = barrel_ngram:regex(<<"code">>, <<"error|panic|fatal">>).
```

Each hit is `#{id => Id, spans => Spans}` where spans are the match ranges. A pattern that
does not compile returns `{error, {bad_regex, Reason}}`.

## How it is accelerated

The regex is compiled into a mandatory-trigram boolean query (a necessary condition for
any match): concatenation ANDs the trigrams of its parts, alternation ORs them, a literal
run contributes its trigrams, and anything without a usable constraint (`.`, `*`, `?`,
character classes, anchors) contributes nothing. That query is intersected and unioned
over the posting lists to get candidates, which are then confirmed with `re:run`. The
analysis is always sound: when it is unsure it adds no constraint, so it never drops a real
match.

## What accelerates versus scans

- **Dense corpus:** the trigram query is used, so a regex with literal substrings (for
  example `retry_\d+`) is fast.
- **Sparse corpus:** the index holds only a sample of trigrams, so an arbitrary mandatory
  trigram may be absent; regex there brute-forces the live set and confirms. Correct, but
  not accelerated.
- **Patterns with no literal run** (for example `.*` or a bare character class) have no
  mandatory trigram, so they scan and confirm on any corpus.

## Bounds

Confirmation runs `re:run` with a match limit (100000) and recursion limit, so a
catastrophic-backtracking pattern returns promptly instead of hanging.

## Notes

- The analysis stops short of boundary trigrams that span alternations or groups; those
  patterns still work, just with a slightly wider candidate set.
- For a plain substring, use [`search`](getting-started.md) instead: it is simpler and
  always index-accelerated.
