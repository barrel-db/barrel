%%%-------------------------------------------------------------------
%%% @doc The conflict resolver of the documents barrel_spaces owns.
%%%
%%% Every fact barrel_spaces stores is a document written once or
%%% replaced as a whole, so the version every node shows (the highest
%%% token) is already the answer. A conflict between two such versions
%%% is closed by choosing it: barrel_docdb names the resolution after
%%% its inputs and its result, so every node writes the same one and the
%%% conflict closes everywhere. Two documents prefer another version: a
%%% space document its dropped version, an accept the earliest accept.
%%%
%%% The registry is opened with registry/1 and space databases with
%%% space/1 or with/1. A space database also holds documents of the
%%% application (any id outside `session:'): with/1 hands those to the
%%% application's resolver, the others leave them in conflict. An
%%% application that opens the registry or a space itself sets one of
%%% these, or calls them from its own resolver.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_resolver).

-export([registry/1, space/1, with/1, owns/2]).

-type resolver() :: {module(), atom()} | fun(([map()]) -> term()).
-export_type([resolver/0]).

%% @doc Resolver of the registry: every document there is barrel_spaces'.
-spec registry([map()]) -> term().
registry(Versions) ->
    resolve(registry, Versions, undefined).

%% @doc Resolver of a space database without an application resolver.
-spec space([map()]) -> term().
space(Versions) ->
    resolve(space, Versions, undefined).

%% @doc Resolver of a space database whose other documents go to
%% `Fallback' (`{M, F}' or a fun of arity 1). It must be the same on
%% every node, like any resolver.
-spec with(resolver()) -> fun(([map()]) -> term()).
with(Fallback) ->
    fun(Versions) -> resolve(space, Versions, Fallback) end.

%% @doc Whether barrel_spaces owns a document id in the registry or in a
%% space database.
-spec owns(registry | space, binary()) -> boolean().
owns(registry, _DocId) -> true;
owns(space, <<"session:", _/binary>>) -> true;
owns(space, _DocId) -> false.

%%====================================================================
%% Internal
%%====================================================================

%% docdb before 3.1 passes no doc id: only the application can tell.
resolve(Where, [#{doc_id := DocId} | _] = Versions, Fallback) ->
    case owns(Where, DocId) of
        true -> {choose, pick(DocId, Versions)};
        false -> fallback(Fallback, Versions)
    end;
resolve(_Where, Versions, Fallback) ->
    fallback(Fallback, Versions).

fallback(undefined, _Versions) -> keep;
fallback({M, F}, Versions) -> M:F(Versions);
fallback(Fun, Versions) when is_function(Fun, 1) -> Fun(Versions).

pick(<<"space:", _/binary>>, Versions) ->
    case [V || #{deleted := false, body := #{<<"status">> := <<"dropped">>}} = V
                   <- Versions] of
        [] -> highest(Versions);
        Dropped -> highest(Dropped)
    end;
pick(<<"handoff:", _/binary>> = DocId, Versions) ->
    case binary:match(DocId, <<":accept:">>) of
        nomatch -> highest(Versions);
        _ -> earliest_accept(Versions)
    end;
pick(_DocId, Versions) ->
    highest(Versions).

%% The version every node shows: the highest token, a tombstone too.
highest(Versions) ->
    [#{token := Token} | _] =
        lists:sort(fun(#{token := A}, #{token := B}) ->
                       barrel_version:compare(barrel_version:from_token(A),
                                              barrel_version:from_token(B)) =/= lt
                   end, Versions),
    Token.

%% The first accept of one agent; its tombstone only when nothing else.
earliest_accept(Versions) ->
    case [{At, T} || #{deleted := false, token := T,
                       body := #{<<"accepted_at">> := At}} <- Versions] of
        [] -> highest(Versions);
        Accepts -> element(2, lists:min(Accepts))
    end.
