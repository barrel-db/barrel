%%%-------------------------------------------------------------------
%%% @doc Windowed literal and regex verification.
%%%
%%% A planner candidate match start is necessary, never sufficient.
%%% `windowed/4' confirms a literal candidate by reading just
%%% `byte_size(Literal)' bytes at its start; `windowed_regex/7' reads the
%%% window `barrel_ngram_planner:regex_plan/2' computed around a chosen
%%% anchor and re-runs the pattern over that slice. Both are
%%% case-sensitive only -- phase-2 sampling is itself case-sensitive, so a
%%% caseless query never reaches this module.
%%%
%%% Both reduce to the same non-overlapping matches a plain left-to-right
%%% scan (`binary:matches/2', `re:run' `global') reports: two
%%% independently distance-checked candidates can be real but overlapping
%%% matches (`"aaa"' at both offset 0 and 1 of `"aaaa"'), and without the
%%% reduction this module would report both, diverging from every other
%%% lane. See {@link non_overlapping/1}.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_verify).

-export([windowed/4, windowed_regex/7]).
-export([non_overlapping/1]).

-define(RE_MATCH_LIMIT, 100000).

%% @doc The subset of `Starts' where `Source''s bytes for `Key', read at
%% that start for `byte_size(Literal)' bytes, equal `Literal' exactly. A
%% failed read (deleted document, or a race) drops that candidate silently.
-spec windowed({module(), term()}, binary(), binary(), [non_neg_integer()]) ->
    [non_neg_integer()].
windowed(Source, Key, Literal, Starts) ->
    Len = byte_size(Literal),
    Matched = lists:filter(
        fun(Start) ->
            case barrel_ngram_source:pread(Source, Key, Start, Len) of
                {ok, Literal} -> true;
                {ok, _Other} -> false;
                {error, _} -> false
            end
        end, Starts),
    [S || {S, _Len} <- non_overlapping([{S, Len} || S <- Matched])].

%% @doc The real `{Start, Length}' matches of `RE', found by reading only
%% the window around each `AnchorStart' in `AnchorStarts':
%% `[AnchorStart - PrefixMax, AnchorStart + AnchorLen + SuffixMax)',
%% clamped to `[0, doc_size)'. Spans are translated back to absolute
%% offsets and deduplicated (more than one anchor can rediscover the same
%% match). A failed `doc_size'/`pread' (deleted document, or a race)
%% drops that document or window silently.
-spec windowed_regex({module(), term()}, binary(), re:mp(),
                     non_neg_integer(), non_neg_integer(), non_neg_integer(),
                     [non_neg_integer()]) ->
    [{non_neg_integer(), non_neg_integer()}].
windowed_regex(Source, Key, RE, PrefixMax, AnchorLen, SuffixMax, AnchorStarts) ->
    case barrel_ngram_source:doc_size(Source, Key) of
        {ok, Size} ->
            Found = lists:usort(
                      lists:flatmap(
                        fun(AnchorStart) ->
                            window_matches(Source, Key, RE, PrefixMax, AnchorLen, SuffixMax,
                                           AnchorStart, Size)
                        end, AnchorStarts)),
            non_overlapping(Found);
        {error, _} ->
            []
    end.

window_matches(Source, Key, RE, PrefixMax, AnchorLen, SuffixMax, AnchorStart, Size) ->
    ClampedStart = max(0, AnchorStart - PrefixMax),
    ClampedEnd = min(Size, AnchorStart + AnchorLen + SuffixMax),
    Len = ClampedEnd - ClampedStart,
    case Len > 0 of
        false ->
            [];
        true ->
            case barrel_ngram_source:pread(Source, Key, ClampedStart, Len) of
                {ok, Window} -> window_re_matches(Window, RE, ClampedStart);
                {error, _} -> []
            end
    end.

window_re_matches(Window, RE, ClampedStart) ->
    case re:run(Window, RE,
                [global, {capture, first, index},
                 {match_limit, ?RE_MATCH_LIMIT},
                 {match_limit_recursion, ?RE_MATCH_LIMIT}]) of
        {match, Matches} -> [{S + ClampedStart, L} || [{S, L}] <- Matches];
        nomatch -> [];
        {error, _} -> []
    end.

%% @doc `Spans' reduced to the same leftmost-greedy non-overlapping set a
%% left-to-right scan produces: sorted by start, keep the earliest
%% available span and skip anything starting before it ends.
-spec non_overlapping([{non_neg_integer(), non_neg_integer()}]) ->
    [{non_neg_integer(), non_neg_integer()}].
non_overlapping(Spans) ->
    keep_non_overlapping(lists:sort(Spans), 0).

keep_non_overlapping([], _MinStart) ->
    [];
keep_non_overlapping([{Start, Len} | Rest], MinStart) when Start >= MinStart ->
    [{Start, Len} | keep_non_overlapping(Rest, Start + Len)];
keep_non_overlapping([_ | Rest], MinStart) ->
    keep_non_overlapping(Rest, MinStart).
