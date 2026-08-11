%%%-------------------------------------------------------------------
%%% @doc Gram-selection behaviour: the shared seam between the indexer
%%% and the query planner.
%%%
%%% A selector maps a byte string to the set of trigrams it contributes
%%% to the index. The SAME selector is applied by the indexer (over full
%%% document bytes) and by the query planner (over the query literal), so
%%% that the grams a literal produces are always a subset of the grams
%%% its containing documents produced. That subset relationship is what
%%% makes the trigram intersection a correct necessary-condition filter.
%%%
%%% Two callbacks, each taking the bytes and a selector-options map:
%%%
%%% <ul>
%%%   <li>`select_grams/2' - the grams a byte string contributes to the
%%%       index. Used at index time.</li>
%%%   <li>`reliable_grams/2' - the grams of a query literal the planner
%%%       may safely intersect over, or `brute_force' when it may not
%%%       (too short, or every gram sits on an unreliable boundary). Used
%%%       at query time.</li>
%%% </ul>
%%%
%%% For the dense selector every gram is reliable, so `reliable_grams/2'
%%% returns all of them (or `brute_force' below the trigram length). The
%%% boundary/interior distinction bites the content-defined (sparse)
%%% selector, which lives behind this same behaviour without changing
%%% anything above it. The options map carries per-selector tuning (e.g.
%%% the sparse selector's window radius and sample rate).
%%%
%%% Two further callbacks, both optional, add byte-offset positions for a
%%% selector backing a positional (phase-2) index (today, only the sparse
%%% selector implements them; dense stays non-positional):
%%% `select_grams_positional/2' (index time), `reliable_grams_positional/2'
%%% (query time).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_selector).

-export([select_grams/3, reliable_grams/3, covers_all_grams/2]).
-export([select_grams_positional/3, reliable_grams_positional/3]).

-export_type([gram/0, reliable/0, reliable_positional/0]).

%% A byte-level trigram packed big-endian into 24 bits.
-type gram() :: 0..16#FFFFFF.
-type offset() :: barrel_ngram_postings_positional:offset().
-type reliable() :: {reliable, [gram()]} | brute_force.
-type reliable_positional() :: {reliable, [{gram(), offset()}]} | brute_force.

-callback select_grams(binary(), map()) -> [gram()].
-callback reliable_grams(binary(), map()) -> reliable().
%% Whether the selector indexes EVERY trigram of a document. The regex
%% planner needs this: an arbitrary mandatory trigram is only guaranteed
%% present when the selector covers all grams (dense). A sampling selector
%% (sparse) does not, so regex there must brute-force.
-callback covers_all_grams(map()) -> boolean().

%% The grams a byte string contributes, each with the byte offset it
%% occurs at (index time). Unlike `select_grams/2', a gram value can
%% legitimately repeat (once per sampled occurrence).
-callback select_grams_positional(binary(), map()) -> [{gram(), offset()}].
%% The query-literal analog of `reliable_grams/2': the offset carried
%% alongside each gram is the position within the QUERY literal itself
%% (not a document), for the planner's distance-check math.
-callback reliable_grams_positional(binary(), map()) -> reliable_positional().
-optional_callbacks([select_grams_positional/2, reliable_grams_positional/2]).

%% @doc Dispatch `select_grams/2' to a selector module.
-spec select_grams(module(), map(), binary()) -> [gram()].
select_grams(Mod, Opts, Bytes) ->
    Mod:select_grams(Bytes, Opts).

%% @doc Dispatch `reliable_grams/2' to a selector module.
-spec reliable_grams(module(), map(), binary()) -> reliable().
reliable_grams(Mod, Opts, Query) ->
    Mod:reliable_grams(Query, Opts).

%% @doc Whether the selector indexes every trigram (dispatch).
-spec covers_all_grams(module(), map()) -> boolean().
covers_all_grams(Mod, Opts) ->
    Mod:covers_all_grams(Opts).

%% @doc Dispatch `select_grams_positional/2' to a selector module.
-spec select_grams_positional(module(), map(), binary()) -> [{gram(), offset()}].
select_grams_positional(Mod, Opts, Bytes) ->
    Mod:select_grams_positional(Bytes, Opts).

%% @doc Dispatch `reliable_grams_positional/2' to a selector module.
-spec reliable_grams_positional(module(), map(), binary()) -> reliable_positional().
reliable_grams_positional(Mod, Opts, Query) ->
    Mod:reliable_grams_positional(Query, Opts).
