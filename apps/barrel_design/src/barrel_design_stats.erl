%% @doc Node-wide counters: guest calls made and guest calls that failed.
-module(barrel_design_stats).

-export([init/0, bump/1, get/0]).

-define(KEY, {?MODULE, counters}).
-define(NAMES, [guest_calls, guest_errors]).

-spec init() -> ok.
init() ->
    case persistent_term:get(?KEY, undefined) of
        undefined -> persistent_term:put(?KEY, counters:new(length(?NAMES), [write_concurrency]));
        _ -> ok
    end.

-spec bump(guest_calls | guest_errors) -> ok.
bump(Name) ->
    case persistent_term:get(?KEY, undefined) of
        undefined -> ok;
        Ref -> counters:add(Ref, index(Name), 1)
    end.

-spec get() -> #{guest_calls := non_neg_integer(), guest_errors := non_neg_integer()}.
get() ->
    case persistent_term:get(?KEY, undefined) of
        undefined -> maps:from_list([{N, 0} || N <- ?NAMES]);
        Ref -> maps:from_list([{N, counters:get(Ref, index(N))} || N <- ?NAMES])
    end.

index(guest_calls) -> 1;
index(guest_errors) -> 2.
