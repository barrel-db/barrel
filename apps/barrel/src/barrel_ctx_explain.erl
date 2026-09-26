%%%-------------------------------------------------------------------
%%% @doc Self-explaining answers: context names next to ids, a message
%%% and a hint on every source that did not answer, and a one-paragraph
%%% `summary' on query, working-set and materialize answers.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_explain).

-export([query/3, working_set/1, materialize/2, discover/3]).

-type names() :: #{binary() => binary() | null}.
-export_type([names/0]).

%%====================================================================
%% Query
%%====================================================================

%% @doc Annotate an executor answer. `Bql' tells which retrieval
%% function ran (for the merge sentence).
-spec query(map(), names(), binary()) -> map().
query(#{sources := Sources} = Resp, Names, Bql) ->
    Sources1 = [source(S, Names) || S <- Sources],
    Resp1 = Resp#{sources := Sources1},
    Resp2 = case Resp1 of
        #{groups := Groups} ->
            Resp1#{groups := [G#{name => name(C, Names)}
                              || #{context := C} = G <- Groups]};
        #{rows := Rows} ->
            Resp1#{rows := [row_name(R, Names) || R <- Rows]};
        _ ->
            Resp1
    end,
    Resp2#{summary => query_summary(Resp2, Sources1, Names, Bql)}.

source(#{context := C, status := ok} = S, Names) ->
    S#{name => name(C, Names)};
source(#{context := C, status := Status} = S, Names) ->
    Error = barrel_ctx_error:source_error(Status, maps:get(error, S, #{})),
    S#{name => name(C, Names), error => Error}.

row_name(#{<<"_ctx">> := C} = Row, Names) ->
    Row#{<<"_ctx_name">> => name(C, Names)};
row_name(Row, _Names) ->
    Row.

query_summary(#{execution := Exec} = Resp, Sources, Names, Bql) ->
    Ok = [S || #{status := ok} = S <- Sources],
    Failed = [S || #{status := St} = S <- Sources, St =/= ok],
    Head = answered(Exec, length(Ok), length(Sources), Ok, Names),
    Fails = [fmt("~s: ~s.", [label(S), maps:get(message, E)])
             || #{error := E} = S <- Failed],
    Merge = case Ok of
        [] -> [];
        [_] -> [merge_sentence(Resp#{single => true}, Bql, Names)];
        _ -> [merge_sentence(Resp, Bql, Names)]
    end,
    Limited = [label(S) || #{bound := limit_reached} = S <- Ok],
    Limit = case Limited of
        [] -> [];
        _ -> [fmt("~s filled the LIMIT or k; more matches may exist.",
                  [join(Limited)])]
    end,
    Copies = [copy_sentence(S) || #{membership := M} = S <- Ok,
                                  M =/= live],
    iolist_to_binary(lists:join(<<" ">>, [Head | Fails ++ Merge ++ Limit
                                                  ++ Copies])).

answered(succeeded, 1, 1, [S], _Names) ->
    fmt("~s answered.", [label(S)]);
answered(succeeded, N, N, Ok, _Names) ->
    fmt("All ~b contexts answered (~s).", [N, join([label(S) || S <- Ok])]);
answered(_Exec, 0, N, _Ok, _Names) ->
    fmt("No context answered (0 of ~b); no rows are returned.", [N]);
answered(_Exec, A, N, _Ok, _Names) ->
    fmt("~b of ~b contexts answered; rows from the others are not "
        "included.", [A, N]).

merge_sentence(#{merge_fallback := #{reason := R, context := C}}, _Bql,
               Names) ->
    fmt("Results are grouped per context: vector scores are not "
        "comparable (~s at ~s).", [R, name_or_id(C, Names)]);
merge_sentence(#{merge := ordered, rows := Rows, single := true}, _Bql,
               _Names) ->
    fmt("~b rows in ORDER BY order.", [length(Rows)]);
merge_sentence(#{merge := ordered, rows := Rows}, _Bql, _Names) ->
    fmt("~b rows are merged across contexts in ORDER BY order.",
        [length(Rows)]);
merge_sentence(#{merge := score, rows := Rows}, _Bql, _Names) ->
    fmt("~b rows are ranked by vector score across contexts (same "
        "embedding).", [length(Rows)]);
merge_sentence(#{merge := interleave}, _Bql, _Names) ->
    <<"Rows alternate between contexts; the order is not a relevance "
      "ranking.">>;
merge_sentence(#{merge := grouped, relevance := _}, Bql, _Names) ->
    fmt("Results are grouped per context because ~s scores are not "
        "comparable across contexts.", [retrieval_fn(Bql)]);
merge_sentence(#{merge := grouped}, _Bql, _Names) ->
    <<"Rows are grouped per context (no ORDER BY, so no global "
      "order).">>;
merge_sentence(_Resp, _Bql, _Names) ->
    <<>>.

retrieval_fn(Bql) ->
    Lower = string:lowercase(Bql),
    case {string:find(Lower, <<"bm25_top_k">>),
          string:find(Lower, <<"hybrid_top_k">>)} of
        {nomatch, nomatch} -> <<"vector">>;
        {nomatch, _} -> <<"hybrid">>;
        _ -> <<"BM25">>
    end.

copy_sentence(#{membership := retrieved_set, note := Note} = S) ->
    fmt("~s answered from a saved slice: ~s.", [label(S), Note]);
copy_sentence(#{membership := complete_generation,
                version := #{generation := G}} = S) ->
    fmt("~s answered from an imported snapshot (generation ~b).",
        [label(S), G]);
copy_sentence(#{membership := M} = S) ->
    fmt("~s answered with membership ~s.", [label(S), M]).

%%====================================================================
%% Working sets
%%====================================================================

%% @doc Summary of a working-set view (members already carry names).
-spec working_set(map()) -> map().
working_set(#{id := Id, members := Members,
              usage := #{bytes := Used}, budget := #{bytes := Max}} = View) ->
    Offline = length([M || #{answers_offline := true} = M <- Members]),
    Parts = [fmt("Working set ~s has ~s.",
                 [Id, plural(length(Members), <<"member">>)])]
        ++ [member_sentence(M) || M <- Members]
        ++ [fmt("Offline, ~b of ~b can answer.", [Offline, length(Members)])
            || Members =/= []]
        ++ [fmt("Local copies use ~b of ~b bytes.", [Used, Max])],
    View#{summary => iolist_to_binary(lists:join(<<" ">>, Parts))}.

member_sentence(#{mode := local} = M) ->
    fmt("~s: local database ~s, live.", [label(M), maps:get(local_db, M)]);
member_sentence(#{mode := remote} = M) ->
    fmt("~s: remote, queried over the network (not offline).", [label(M)]);
member_sentence(#{mode := snapshot, generation := G} = M) ->
    fmt("~s: imported snapshot, generation ~b.", [label(M), G]);
member_sentence(#{mode := retrieved_set} = M) ->
    Docs = maps:get(<<"docs">>, maps:get(derived, M, #{}), 0),
    fmt("~s: saved slice of ~b documents (not the whole context).",
        [label(M), Docs]);
member_sentence(M) ->
    fmt("~s: ~s.", [label(M), maps:get(mode, M)]).

%%====================================================================
%% Materialize
%%====================================================================

%% @doc Names and a summary on a materialize answer.
-spec materialize(map(), names()) -> map().
materialize(#{working_set := Ws, slices := Slices} = Resp, Names) ->
    Slices1 = [slice(S, Names) || S <- Slices],
    Parts = [slice_sentence(S) || S <- Slices1],
    Head = fmt("Working set ~s:", [Ws]),
    Resp#{slices := Slices1,
          summary => iolist_to_binary(lists:join(<<" ">>, [Head | Parts]))}.

slice(#{context := C, status := complete} = S, Names) ->
    S#{name => name(C, Names)};
slice(#{context := C, status := empty} = S, Names) ->
    S#{name => name(C, Names),
       error => #{reason => no_ids,
                  message => <<"the query returned no document ids from "
                               "this context">>,
                  hint => <<"Change the query so it returns rows with an "
                            "id from this context.">>}};
slice(#{context := C, status := failed, error := Reason} = S, Names) ->
    #{error := Code, message := Msg, hint := Hint, details := D} =
        barrel_ctx_error:to_map(Reason),
    S#{name => name(C, Names),
       error => #{reason => Code, message => Msg, hint => Hint,
                  details => D}};
slice(#{context := C, status := Status} = S, Names) ->
    S#{name => name(C, Names),
       error => barrel_ctx_error:source_error(Status,
                                              maps:get(error, S, #{}))}.

slice_sentence(#{status := complete, docs := N, bytes := B} = S) ->
    fmt("saved ~b documents from ~s (~b bytes) into ~s.",
        [N, label(S), B, maps:get(local_db, S)]);
slice_sentence(#{error := #{message := Msg}} = S) ->
    fmt("~s: nothing saved, ~s.", [label(S), Msg]).

%%====================================================================
%% Discover
%%====================================================================

%% @doc Summary for a discover answer.
-spec discover([map()], binary(), non_neg_integer()) -> binary().
discover([], Query, Total) ->
    fmt("No listed context matches '~s' (~b listed). Every word must "
        "appear in the name, title, description or topics; try fewer "
        "words, or context_list to see them all.", [Query, Total]);
discover(Cards, Query, _Total) ->
    fmt("~s ~s '~s': ~s. A filter, not a ranking: other contexts may "
        "still hold answers.",
        [plural(length(Cards), <<"context">>),
         case Cards of [_] -> <<"matches">>; _ -> <<"match">> end, Query,
         join([N || #{<<"name">> := N} <- Cards])]).

%%====================================================================
%% Helpers
%%====================================================================

plural(1, Word) -> <<"1 ", Word/binary>>;
plural(N, Word) -> fmt("~b ~ss", [N, Word]).

name(Ctx, Names) ->
    maps:get(Ctx, Names, null).

label(#{name := N}) when is_binary(N) -> N;
label(#{context := C}) -> C.

name_or_id(C, Names) ->
    case name(C, Names) of
        null -> C;
        N -> N
    end.

join(Items) ->
    lists:join(<<", ">>, [text(I) || I <- Items]).

text(B) when is_binary(B) -> B;
text(A) when is_atom(A) -> atom_to_binary(A);
text(L) when is_list(L) -> iolist_to_binary(L);
text(Other) -> iolist_to_binary(io_lib:format("~0p", [Other])).

fmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, [arg(A) || A <- Args])).

arg(N) when is_integer(N) -> N;
arg(Other) -> text(Other).
