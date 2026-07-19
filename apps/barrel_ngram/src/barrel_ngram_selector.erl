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
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_selector).

-export([select_grams/3, reliable_grams/3]).

-export_type([gram/0, reliable/0]).

%% A byte-level trigram packed big-endian into 24 bits.
-type gram() :: 0..16#FFFFFF.
-type reliable() :: {reliable, [gram()]} | brute_force.

-callback select_grams(binary(), map()) -> [gram()].
-callback reliable_grams(binary(), map()) -> reliable().

%% @doc Dispatch `select_grams/2' to a selector module.
-spec select_grams(module(), map(), binary()) -> [gram()].
select_grams(Mod, Opts, Bytes) ->
    Mod:select_grams(Bytes, Opts).

%% @doc Dispatch `reliable_grams/2' to a selector module.
-spec reliable_grams(module(), map(), binary()) -> reliable().
reliable_grams(Mod, Opts, Query) ->
    Mod:reliable_grams(Query, Opts).
