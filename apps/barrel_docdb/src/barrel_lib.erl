%%%-------------------------------------------------------------------
%%% @doc Small shared helpers for wrapping `try ... catch ... end' idioms
%%% that recur across barrel_docdb, so the pattern lives in one place.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_lib).

-export([safe_fold/1, safe/2, safe_apply/1]).

%% @doc Run a fold that early-terminates by throwing `{stop, Result}'.
%% The fold's callback throws `{stop, Acc}' to stop iteration; this returns
%% either the fold's normal result or the thrown accumulator.
-spec safe_fold(fun(() -> Result)) -> Result.
safe_fold(Fun) ->
    try
        Fun()
    catch
        throw:{stop, Result} -> Result
    end.

%% @doc Run `Fun', returning `Default' on any exception. Best-effort calls
%% where a failure should fall back to a fixed value.
-spec safe(fun(() -> Result), Default) -> Result | Default.
safe(Fun, Default) ->
    try
        Fun()
    catch
        _:_ -> Default
    end.

%% @doc Run `Fun', capturing any failure as `{error, {Class, Reason, Stack}}'.
%% Used to ship worker results back without crashing the caller.
-spec safe_apply(fun(() -> Result)) ->
    {ok, Result} | {error, {atom(), term(), list()}}.
safe_apply(Fun) ->
    try
        {ok, Fun()}
    catch
        Class:Reason:Stack ->
            {error, {Class, Reason, Stack}}
    end.
