%%%-------------------------------------------------------------------
%%% @doc Offline coverage (see docs/architecture/contexts.md): decides, per
%%% working-set member, whether it can answer and what its answer
%%% covers, then summarizes the request. Pure: callers pass the facts
%%% (offline flag, local availability, the query's conditions).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_coverage).

-export([member/2, summarize/2, implies/2]).

-export_type([facts/0, source/0]).

-type facts() :: #{offline := boolean(),
                   %% local copy (snapshot import, slice, local db) usable
                   available => boolean(),
                   %% the query's docdb where-conditions, when known
                   where => [term()] | unknown}.
-type source() :: #{context := binary(),
                    status := ok | pending | skipped_offline | error,
                    membership => live | complete_generation |
                                  complete_predicate | predicate_overlap |
                                  retrieved_set,
                    version => map(),
                    note => binary(),
                    error => map()}.

%% @doc Status and membership of one resolved member (see
%% barrel_ctx_ws:members/1) under `Facts'. Remote members are
%% `pending' when online (the executor queries them) and
%% `skipped_offline' when the node is offline: never attempted, never
%% replaced by a download.
-spec member(map(), facts()) -> source().
member(#{mode := remote, context := Ctx}, #{offline := true}) ->
    #{context => Ctx, status => skipped_offline,
      error => #{reason => no_local_copy}};
member(#{mode := remote, context := Ctx}, #{offline := false}) ->
    #{context => Ctx, status => pending, membership => live,
      version => #{kind => live}};
member(#{context := Ctx} = M, #{available := false}) ->
    #{context => Ctx, status => error,
      error => #{reason => local_copy_missing,
                 local_db => maps:get(local_db, M, null)}};
member(#{mode := local, context := Ctx}, _Facts) ->
    #{context => Ctx, status => ok, membership => live,
      version => #{kind => live}};
member(#{mode := snapshot, context := Ctx, generation := Gen} = M, Facts) ->
    Base = #{context => Ctx, status => ok,
             version => #{kind => generation, generation => Gen}},
    case maps:get(predicate, M, undefined) of
        undefined ->
            Base#{membership => complete_generation};
        Pred ->
            case implies(maps:get(where, Facts, unknown), Pred) of
                true -> Base#{membership => complete_predicate};
                false -> Base#{membership => predicate_overlap,
                               note => <<"answered only for the snapshot "
                                         "predicate">>}
            end
    end;
member(#{mode := retrieved_set, context := Ctx} = M, _Facts) ->
    Derived = maps:get(derived, M, #{}),
    Docs = maps:get(<<"docs">>, Derived, 0),
    #{context => Ctx, status => ok, membership => retrieved_set,
      version => #{kind => retrieved_set,
                   observed => maps:get(<<"observed">>, Derived, null)},
      note => iolist_to_binary(
                io_lib:format("answers cover only the ~b saved documents, "
                              "not the source context", [Docs]))}.

%% @doc Response-level coverage over per-member results. `execution' is
%% `complete' only when every requested member answered.
-spec summarize([source()], explicit | discovered) -> map().
summarize(Sources, ScopeOrigin) ->
    Count = fun(S) -> length([X || #{status := St} = X <- Sources,
                                   St =:= S]) end,
    Answered = Count(ok),
    Skipped = Count(skipped_offline),
    Failed = Count(error),
    Missing = [C || #{context := C, status := St} <- Sources,
                    St =/= ok, St =/= pending],
    Execution = case Missing of
        [] -> complete;
        _ -> partial
    end,
    #{execution => Execution,
      coverage => #{requested => length(Sources), answered => Answered,
                    failed => Failed, skipped => Skipped,
                    pending => Count(pending), missing => Missing,
                    scope_origin => ScopeOrigin}}.

%% @doc Sound, syntactic implication: the query's conditions imply the
%% snapshot predicate when every predicate condition appears among
%% them. Unknown conditions never imply.
-spec implies([term()] | unknown, [term()]) -> boolean().
implies(unknown, _Pred) ->
    false;
implies(Where, Pred) when is_list(Where), is_list(Pred) ->
    lists:all(fun(C) -> lists:member(C, Where) end, Pred).
