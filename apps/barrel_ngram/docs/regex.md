# Regex search

`regex/2,3` finds documents matching a PCRE regular expression. Use it when a plain
substring is not enough (alternations, character classes, quantifiers). Results are exact:
the trigram index only narrows candidates, and every candidate is confirmed with the real
regex engine.

## Use

```erlang
{ok, Hits} = barrel_ngram:regex(<<"code">>, <<"connect_\\w+_timeout">>),
{ok, More} = barrel_ngram:regex(<<"code">>, <<"error|panic|fatal">>),

%% case-insensitive
{ok, Ci} = barrel_ngram:regex(<<"code">>, <<"connect_timeout">>, #{case_sensitive => false}).
```

Each hit is `#{id => Id, spans => Spans}` where spans are the match ranges. A pattern that
does not compile returns `{error, {bad_regex, Reason}}`.

## How it is accelerated

The regex is compiled into a mandatory-trigram boolean query (a necessary condition for
any match): concatenation ANDs the trigrams of its parts, alternation ORs them, a literal
run contributes its trigrams, and anything without a usable constraint (`.`, `*`, `?`,
character classes, anchors) contributes nothing. That query is intersected and unioned
over the phase-1 index to get candidates. The analysis is always sound: when it is unsure
it adds no constraint, so it never drops a real match.

A pattern that is a clean AND-chain of literal runs -- no alternation, no `^`/`$`/`\b`
anchor or boundary anywhere -- gets a second, tighter narrowing on top: the longest literal
run with phase-2 data becomes a window anchor (see [selectors](selectors.md)), and instead
of fetching whole candidate documents, confirmation reads just the window around each
candidate match (with a corpus `source` configured; see [design](design.md)) and re-runs
the pattern over that slice. `connect_[0-9]{2}_backoff_ms` gets this treatment;
`connect_\w+_backoff_ms` does not, because `\w+` is unbounded and leaves both neighboring
literals with an unbounded window on the side facing it.

Everything else -- an unsupported construct, alternation, an anchor/boundary, or no
literal run with usable phase-2 data -- gets full-content confirmation: fetch the whole
candidate document and run `re:run` over it. Still exact, just not windowed.

## Unsupported patterns

The parser understands concatenation, alternation, `*`/`+`/`?`/bounded `{n,m}`, `.`,
character classes, anchors, and groups. Anything else -- lookarounds, backreferences,
named groups, `\x{...}` escapes, `\Q...\E`, conditionals, or an inline modifier anywhere
but a leading `(?i)`/`(?s)`/`(?m)` -- makes the whole pattern `unsupported`: no trigram
narrowing at all (every live document is a candidate), full-content confirmation. This is
deliberate: mis-parsing an unfamiliar construct as literal text (`(?=foo)` read as the
literal bytes `?=foo`) would derive a wrong, too-narrow trigram query and silently drop a
real match. `re:run` itself still understands the full pattern, lookarounds included --
only the accelerator's own analysis is limited.

## Case-insensitive search

Pass `case_sensitive => false`, or start the pattern with `(?i)` (recognized without the
option). Either way, phase-2 is skipped -- its sampling is itself case-sensitive, so a
stored gram can't stand in for a case-insensitive match -- and so is windowing: there is no
positional anchor to window around.

- **Pattern is pure ASCII:** any literal run the analyzer extracts narrows through
  phase-1's ASCII case-variant expansion (each letter's upper/lower pair, ANDed across
  trigram positions). Verification compiles with `[caseless]`.
- **Pattern has a non-ASCII byte:** no narrowing (`all`); a full Unicode case fold isn't
  reproduced by hand. The pattern is checked for valid UTF-8 first --
  `{error, {invalid_literal_encoding, Regex}}` if it is not -- then verified with
  `[caseless, unicode]`. A candidate document that is not valid UTF-8 aborts the whole
  call with `{error, {invalid_document_encoding, DocId}}` rather than being silently
  skipped, since a real match could be sitting past the invalid bytes.

## Bounds

Confirmation runs `re:run` with a match limit (100000) and recursion limit, so a
catastrophic-backtracking pattern returns promptly instead of hanging -- inside a window
read too, when the pattern was windowed.

## Notes

- The analysis stops short of boundary trigrams that span alternations or groups; those
  patterns still work, just with a slightly wider candidate set.
- For a plain substring, use [`search`](getting-started.md) instead: it is simpler and
  always index-accelerated.
