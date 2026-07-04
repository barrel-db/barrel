%% @doc Canonicalize and validate BQL ASTs; lower them to executable
%% plans (lowering lands with the planner step).
%%
%% Canonical form:
%% - paths are range-variable tagged: {path, Loc, base | unnest, Comps}
%%   with Comps in the engine shape [binary() | integer() | '*'] and the
%%   leading alias stripped (PartiQL name resolution for one binding:
%%   the range variable shadows same-named document fields);
%% - parameters are substituted;
%% - operands are normalized (literal OP path is flipped, constant and
%%   path-to-path predicates are rejected);
%% - BETWEEN is expanded to comparisons;
%% - NOT is eliminated by dualization, so downstream code never sees a
%%   'not' node. The engine evaluates leaves as exists-and-satisfies
%%   (absent path => false), which matches SQL WHERE except under NOT;
%%   dualizing keeps PartiQL's missing-is-filtered semantics.
%%   Negations survive only as flags on in/like/contains, which the
%%   planner guards with {exists, Path};
%% - the top-level conjunction is flattened to a list.
-module(barrel_bql_lower).

-export([
    canonicalize/2,
    validate/1,
    lower/1
]).

-export_type([canon/0, cpath/0, cexpr/0, plan/0, tagged_path/0]).

-type loc() :: barrel_bql_ast:loc().
-type var() :: base | unnest.
-type comps() :: [binary() | integer() | '*'].
-type cpath() :: {path, loc(), var(), comps()}.
-type clit() :: {lit, loc(), binary() | number() | boolean() | null}.

-type cexpr() ::
    {cmp, loc(), '=' | '!=' | '<' | '<=' | '>' | '>=', cpath(), clit()}
    | {is_null, loc(), cpath(), boolean()}
    | {is_missing, loc(), cpath(), boolean()}
    | {in, loc(), cpath(), [clit()], boolean()}
    | {like, loc(), cpath(), binary(), boolean()}
    | {contains, loc(), cpath(), clit(), boolean()}
    | {'and', loc(), cexpr(), cexpr()}
    | {'or', loc(), cexpr(), cexpr()}.

-type projection() :: #{path := cpath(), name := binary() | undefined,
                        loc := loc()}.

-type canon() :: #{
    select := star | [projection()],
    source := {collection, loc(), binary()}
            | {table_fn, loc(), binary(), [barrel_bql_ast:fn_arg()]},
    alias := binary(),
    unnest := undefined | #{path := cpath(), alias := binary(),
                            loc := loc()},
    where := [cexpr()],
    order_by := [{cpath(), asc | desc}],
    limit := undefined | {integer, loc(), non_neg_integer()},
    offset := undefined | {integer, loc(), non_neg_integer()},
    subscribe := boolean()
}.

-define(TABLE_FNS, [<<"vector_top_k">>, <<"bm25_top_k">>, <<"hybrid_top_k">>]).

%% Executable plan. spec is a barrel_query query_spec (a fetch template
%% for table-fn sources). post is applied per row by the executor:
%% residual (unnest-frame conditions, tagged paths), doc_where (table-fn
%% doc conditions in plain engine shape for barrel_query:match/2),
%% order, offset/limit (when not pushed into the spec), project.
-type tagged_path() :: {b, comps()} | {u, comps()}.
-type project_key() :: {b, comps()} | {u, comps()} | {score, binary()}.
-type plan() :: #{
    spec := map(),
    source := {collection, binary()}
            | {table_fn, vector_top_k | bm25_top_k | hybrid_top_k,
               #{query := binary(), k := pos_integer(),
                 ef_search => pos_integer()}},
    unnest := undefined | #{path := comps(), alias := binary()},
    post := #{
        residual := [term()],
        doc_where := [term()],
        order := undefined | {project_key(), asc | desc},
        offset := non_neg_integer(),
        limit := undefined | pos_integer(),
        project := star | [{b | u | score, comps() | binary(), binary()}],
        empty := boolean()
    },
    subscribe := boolean(),
    streamable := boolean(),
    warnings := [term()]
}.

%%====================================================================
%% Canonicalize
%%====================================================================

-spec canonicalize(barrel_bql_ast:ast(), #{binary() => term()}) ->
    {ok, canon()} | {error, {invalid_query, loc() | undefined, tuple()}}.
canonicalize(Ast, Params) ->
    #{select := Select, from := From, where := Where, order_by := Order,
      limit := Limit, offset := Offset, subscribe := Subscribe} = Ast,
    #{source := Source0, alias := Alias0, unnest := Unnest0} = From,
    try
        Source = subst_fn_args(Source0, Params),
        Alias = resolve_alias(Source, Alias0),
        UnnestAlias = case Unnest0 of
            undefined -> undefined;
            #{alias := UA} -> UA
        end,
        Ctx = #{alias => Alias, unnest_alias => UnnestAlias,
                params => Params},
        {ok, #{
            select => canon_select(Select, Ctx),
            source => Source,
            alias => Alias,
            unnest => canon_unnest(Unnest0, Ctx),
            where => canon_where(Where, Ctx),
            order_by => [{canon_path(P, Ctx), Dir} || {P, Dir} <- Order],
            limit => Limit,
            offset => Offset,
            subscribe => Subscribe
        }}
    catch
        throw:{invalid_query, _, _} = Error -> {error, Error}
    end.

resolve_alias({collection, _, Name}, undefined) -> Name;
resolve_alias({table_fn, Loc, _, _}, undefined) ->
    fail(Loc, {alias_required, table_fn});
resolve_alias(_, Alias) -> Alias.

subst_fn_args({collection, _, _} = Source, _Params) ->
    Source;
subst_fn_args({table_fn, Loc, Name, Args}, Params) ->
    {table_fn, Loc, Name,
     [subst_fn_arg(Arg, Params) || Arg <- Args]}.

subst_fn_arg({pos, Operand}, Params) ->
    {pos, subst_literal_operand(Operand, Params)};
subst_fn_arg({named, Name, Operand}, Params) ->
    {named, Name, subst_literal_operand(Operand, Params)}.

canon_select(star, _Ctx) ->
    star;
canon_select(Projections, Ctx) ->
    [#{path => canon_path(Expr, Ctx), name => As, loc => Loc}
     || #{expr := Expr, as := As, loc := Loc} <- Projections].

canon_unnest(undefined, _Ctx) ->
    undefined;
canon_unnest(#{path := Path, alias := Alias, loc := Loc}, Ctx) ->
    case canon_path(Path, Ctx) of
        {path, _, unnest, _} ->
            fail(Loc, {invalid_unnest_path, self_reference});
        {path, _, base, []} ->
            fail(Loc, {invalid_unnest_path, source_reference});
        CPath ->
            #{path => CPath, alias => Alias, loc => Loc}
    end.

canon_where(undefined, _Ctx) ->
    [];
canon_where(Expr, Ctx) ->
    flatten_and(push_not(canon_expr(Expr, Ctx), false)).

%% Bottom-up: substitute params, tag paths, normalize operand order.
canon_expr({'and', Loc, A, B}, Ctx) ->
    {'and', Loc, canon_expr(A, Ctx), canon_expr(B, Ctx)};
canon_expr({'or', Loc, A, B}, Ctx) ->
    {'or', Loc, canon_expr(A, Ctx), canon_expr(B, Ctx)};
canon_expr({'not', Loc, E}, Ctx) ->
    {'not', Loc, canon_expr(E, Ctx)};
canon_expr({cmp, Loc, Op, Lhs0, Rhs0}, Ctx) ->
    Lhs = canon_operand(Lhs0, Ctx),
    Rhs = canon_operand(Rhs0, Ctx),
    case {Lhs, Rhs} of
        {{path, _, _, _}, {lit, _, _}} -> {cmp, Loc, Op, Lhs, Rhs};
        {{lit, _, _}, {path, _, _, _}} -> {cmp, Loc, mirror(Op), Rhs, Lhs};
        {{lit, _, _}, {lit, _, _}} -> fail(Loc, {constant_predicate, cmp});
        {{path, _, _, _}, {path, _, _, _}} ->
            fail(Loc, {unsupported, path_to_path_comparison})
    end;
canon_expr({is_null, Loc, Operand, Negated}, Ctx) ->
    {is_null, Loc, subject_path(Operand, Loc, Ctx, is_null), Negated};
canon_expr({is_missing, Loc, Operand, Negated}, Ctx) ->
    {is_missing, Loc, subject_path(Operand, Loc, Ctx, is_missing), Negated};
canon_expr({in, Loc, Operand, Literals, Negated}, Ctx) ->
    {in, Loc, subject_path(Operand, Loc, Ctx, in), Literals, Negated};
canon_expr({like, Loc, Operand, Pattern, Negated}, Ctx) ->
    {like, Loc, subject_path(Operand, Loc, Ctx, like), Pattern, Negated};
canon_expr({between, Loc, Operand, Low0, High0, Negated}, Ctx) ->
    Path = subject_path(Operand, Loc, Ctx, between),
    Low = literal_bound(Low0, Loc, Ctx),
    High = literal_bound(High0, Loc, Ctx),
    {between, Loc, Path, Low, High, Negated};
canon_expr({contains, Loc, Path, Literal}, Ctx) ->
    {contains, Loc, canon_path(Path, Ctx), Literal, false}.

canon_operand({path, _, _} = Path, Ctx) ->
    canon_path(Path, Ctx);
canon_operand({lit, _, _} = Lit, _Ctx) ->
    Lit;
canon_operand({param, _, _} = Param, #{params := Params}) ->
    subst_param(Param, Params).

subject_path(Operand, Loc, Ctx, Kind) ->
    case canon_operand(Operand, Ctx) of
        {path, _, _, _} = Path -> Path;
        {lit, _, _} -> fail(Loc, {constant_predicate, Kind})
    end.

literal_bound(Operand, Loc, Ctx) ->
    case canon_operand(Operand, Ctx) of
        {lit, _, _} = Lit -> Lit;
        {path, _, _, _} -> fail(Loc, {unsupported, path_between_bound})
    end.

subst_literal_operand({param, _, _} = Param, Params) ->
    subst_param(Param, Params);
subst_literal_operand({lit, _, _} = Lit, _Params) ->
    Lit.

subst_param({param, Loc, Name}, Params) ->
    case maps:find(Name, Params) of
        {ok, Value} when is_binary(Value); is_number(Value);
                         is_boolean(Value); Value =:= null ->
            {lit, Loc, Value};
        {ok, _Other} ->
            fail(Loc, {invalid_param, Name});
        error ->
            fail(Loc, {unbound_param, Name})
    end.

canon_path({path, Loc, [{key, First} | Rest]},
           #{alias := Alias, unnest_alias := UnnestAlias}) ->
    case First of
        UnnestAlias -> {path, Loc, unnest, comps(Rest)};
        Alias -> {path, Loc, base, comps(Rest)};
        _ -> {path, Loc, base, comps([{key, First} | Rest])}
    end.

comps(Components) ->
    lists:map(
        fun({key, Key}) -> Key;
           ({index, Index}) -> Index;
           (wildcard) -> '*'
        end,
        Components).

mirror('<') -> '>';
mirror('<=') -> '>=';
mirror('>') -> '<';
mirror('>=') -> '<=';
mirror('=') -> '=';
mirror('!=') -> '!='.

%% NOT elimination by dualization + BETWEEN expansion. The engine's
%% {'not'} over a leaf is TRUE for an absent path; PartiQL filters such
%% rows out. Dualizing to the complementary operator keeps
%% absent => false at every leaf.
push_not({'not', _, E}, Negated) ->
    push_not(E, not Negated);
push_not({'and', Loc, A, B}, false) ->
    {'and', Loc, push_not(A, false), push_not(B, false)};
push_not({'and', Loc, A, B}, true) ->
    {'or', Loc, push_not(A, true), push_not(B, true)};
push_not({'or', Loc, A, B}, false) ->
    {'or', Loc, push_not(A, false), push_not(B, false)};
push_not({'or', Loc, A, B}, true) ->
    {'and', Loc, push_not(A, true), push_not(B, true)};
push_not({cmp, Loc, Op, Path, Lit}, true) ->
    {cmp, Loc, negate(Op), Path, Lit};
push_not({cmp, _, _, _, _} = Cmp, false) ->
    Cmp;
push_not({is_null, Loc, Path, N}, Negated) ->
    {is_null, Loc, Path, N xor Negated};
push_not({is_missing, Loc, Path, N}, Negated) ->
    {is_missing, Loc, Path, N xor Negated};
push_not({in, Loc, Path, Lits, N}, Negated) ->
    {in, Loc, Path, Lits, N xor Negated};
push_not({like, Loc, Path, Pattern, N}, Negated) ->
    {like, Loc, Path, Pattern, N xor Negated};
push_not({contains, Loc, Path, Lit, N}, Negated) ->
    {contains, Loc, Path, Lit, N xor Negated};
push_not({between, Loc, Path, Low, High, N}, Negated) ->
    case N xor Negated of
        false ->
            {'and', Loc, {cmp, Loc, '>=', Path, Low},
                         {cmp, Loc, '<=', Path, High}};
        true ->
            {'or', Loc, {cmp, Loc, '<', Path, Low},
                        {cmp, Loc, '>', Path, High}}
    end.

negate('=') -> '!=';
negate('!=') -> '=';
negate('<') -> '>=';
negate('<=') -> '>';
negate('>') -> '<=';
negate('>=') -> '<'.

flatten_and({'and', _, A, B}) ->
    flatten_and(A) ++ flatten_and(B);
flatten_and(Expr) ->
    [Expr].

%%====================================================================
%% Validate
%%====================================================================

-spec validate(canon()) ->
    ok | {error, {invalid_query, loc() | undefined, tuple()}}.
validate(Canon) ->
    #{select := Select, source := Source, alias := Alias,
      unnest := Unnest, where := Where, order_by := Order,
      offset := Offset, subscribe := Subscribe} = Canon,
    try
        check_aliases(Alias, Unnest),
        Fn = check_source(Source),
        check_subscribe(Subscribe, Source, Unnest, Order, Offset),
        Paths = collect_paths(Select, Unnest, Where, Order),
        lists:foreach(fun(P) -> check_path(P, Fn) end, Paths),
        check_where(Where),
        check_order(Order),
        check_select(Select, Canon),
        ok
    catch
        throw:{invalid_query, _, _} = Error -> {error, Error}
    end.

check_aliases(Alias, #{alias := Alias, loc := Loc}) ->
    fail(Loc, {duplicate_alias, Alias});
check_aliases(_Alias, _Unnest) ->
    ok.

check_source({collection, _, _}) ->
    undefined;
check_source({table_fn, Loc, Name, Args}) ->
    case lists:member(Name, ?TABLE_FNS) of
        false -> fail(Loc, {unknown_table_function, Name});
        true -> ok
    end,
    Fn = binary_to_atom(Name, utf8),
    _ = normalize_fn_args(Fn, Loc, Args),
    Fn.

%% Shared with the planner: normalizes table-fn args to a map.
%% Positional query first; named options k (any function, default 10)
%% and ef_search (vector_top_k only).
-spec normalize_fn_args(atom(), loc(), [barrel_bql_ast:fn_arg()]) ->
    #{query := binary(), k := pos_integer(),
      ef_search => pos_integer()}.
normalize_fn_args(Fn, _Loc, [{pos, {lit, _, Query}} | Named])
        when is_binary(Query) ->
    {Opts, _Seen} = lists:foldl(
        fun({named, Name, {lit, ALoc, Value}}, {Acc, Seen}) ->
                case lists:member(Name, Seen) of
                    true ->
                        fail(ALoc, {invalid_table_fn_arg, Fn,
                                    {duplicate, Name}});
                    false ->
                        {named_fn_arg(Fn, Name, Value, ALoc, Acc),
                         [Name | Seen]}
                end;
           ({pos, {lit, ALoc, _}}, _Acc) ->
                fail(ALoc, {invalid_table_fn_arg, Fn, extra_positional})
        end,
        {#{}, []},
        Named),
    maps:merge(#{query => Query, k => 10}, Opts);
normalize_fn_args(Fn, Loc, _Args) ->
    fail(Loc, {invalid_table_fn_arg, Fn, query}).

named_fn_arg(_Fn, <<"k">>, K, _Loc, Acc)
        when is_integer(K), K >= 1 ->
    Acc#{k => K};
named_fn_arg(Fn, <<"k">>, _K, Loc, _Acc) ->
    fail(Loc, {invalid_table_fn_arg, Fn, k});
named_fn_arg(vector_top_k, <<"ef_search">>, Ef, _Loc, Acc)
        when is_integer(Ef), Ef >= 1 ->
    Acc#{ef_search => Ef};
named_fn_arg(Fn, <<"ef_search">>, _Ef, Loc, _Acc) ->
    fail(Loc, {invalid_table_fn_arg, Fn, ef_search});
named_fn_arg(Fn, Name, _Value, Loc, _Acc) ->
    fail(Loc, {invalid_table_fn_arg, Fn, Name}).

check_subscribe(false, _Source, _Unnest, _Order, _Offset) ->
    ok;
check_subscribe(true, Source, Unnest, Order, Offset) ->
    case Source of
        {table_fn, Loc0, _, _} ->
            fail(Loc0, {unsupported_with_subscribe, table_function});
        _ ->
            ok
    end,
    case Unnest of
        #{loc := Loc1} -> fail(Loc1, {unsupported_with_subscribe, unnest});
        undefined -> ok
    end,
    case Order of
        [{{path, Loc2, _, _}, _} | _] ->
            fail(Loc2, {unsupported_with_subscribe, order_by});
        [] ->
            ok
    end,
    case Offset of
        {integer, Loc3, _} ->
            fail(Loc3, {unsupported_with_subscribe, offset});
        undefined ->
            ok
    end.

collect_paths(Select, Unnest, Where, Order) ->
    SelectPaths = case Select of
        star -> [];
        Projections -> [P || #{path := P} <- Projections]
    end,
    UnnestPaths = case Unnest of
        undefined -> [];
        #{path := UP} -> [UP]
    end,
    OrderPaths = [P || {P, _Dir} <- Order],
    SelectPaths ++ UnnestPaths ++ expr_paths(Where) ++ OrderPaths.

expr_paths(Conjuncts) when is_list(Conjuncts) ->
    lists:flatmap(fun expr_paths/1, Conjuncts);
expr_paths({'and', _, A, B}) -> expr_paths(A) ++ expr_paths(B);
expr_paths({'or', _, A, B}) -> expr_paths(A) ++ expr_paths(B);
expr_paths({cmp, _, _, Path, _}) -> [Path];
expr_paths({is_null, _, Path, _}) -> [Path];
expr_paths({is_missing, _, Path, _}) -> [Path];
expr_paths({in, _, Path, _, _}) -> [Path];
expr_paths({like, _, Path, _, _}) -> [Path];
expr_paths({contains, _, Path, _, _}) -> [Path].

check_path({path, Loc, Var, Comps}, Fn) ->
    case lists:member('*', Comps) of
        true -> fail(Loc, {unsupported, wildcard_path, use_unnest});
        false -> ok
    end,
    %% Top-level _-prefixed fields are reserved (never indexed, never
    %% stored); the synthetic search columns are the only exceptions.
    %% Unnest-relative keys are not top-level, so they are exempt.
    case {Var, Comps} of
        {base, [<<"_score">>]} when Fn =/= undefined -> ok;
        {base, [<<"_distance">>]} when Fn =:= vector_top_k -> ok;
        {base, [<<"_", _/binary>> = First | _]} ->
            fail(Loc, {reserved_field, First});
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% WHERE rules: null literals, id conditions, source references
%%--------------------------------------------------------------------

check_where(Conjuncts) ->
    lists:foreach(fun check_conjunct/1, Conjuncts).

check_conjunct(Conjunct) ->
    check_null_literals(Conjunct),
    check_source_refs(Conjunct),
    check_score_refs(Conjunct),
    check_id_use(Conjunct, top).

check_null_literals({'and', _, A, B}) ->
    check_null_literals(A), check_null_literals(B);
check_null_literals({'or', _, A, B}) ->
    check_null_literals(A), check_null_literals(B);
check_null_literals({cmp, Loc, _, _, {lit, _, null}}) ->
    fail(Loc, {use_is_null, cmp});
check_null_literals({in, Loc, _, Lits, _}) ->
    case lists:any(fun({lit, _, V}) -> V =:= null end, Lits) of
        true -> fail(Loc, {use_is_null, in});
        false -> ok
    end;
check_null_literals(_) ->
    ok.

check_source_refs({'and', _, A, B}) ->
    check_source_refs(A), check_source_refs(B);
check_source_refs({'or', _, A, B}) ->
    check_source_refs(A), check_source_refs(B);
check_source_refs(Leaf) ->
    case leaf_path(Leaf) of
        {path, Loc, base, []} ->
            fail(Loc, {unsupported, bare_source_reference});
        _ ->
            ok
    end.

%% _score/_distance are search columns, not document fields: a WHERE on
%% them would need a second evaluator alongside barrel_query:match/2.
%% v1 confines them to SELECT and ORDER BY.
check_score_refs({'and', _, A, B}) ->
    check_score_refs(A), check_score_refs(B);
check_score_refs({'or', _, A, B}) ->
    check_score_refs(A), check_score_refs(B);
check_score_refs(Leaf) ->
    case leaf_path(Leaf) of
        {path, Loc, base, [<<"_score">>]} ->
            fail(Loc, {unsupported, score_in_where});
        {path, Loc, base, [<<"_distance">>]} ->
            fail(Loc, {unsupported, score_in_where});
        _ ->
            ok
    end.

leaf_path({cmp, _, _, Path, _}) -> Path;
leaf_path({is_null, _, Path, _}) -> Path;
leaf_path({is_missing, _, Path, _}) -> Path;
leaf_path({in, _, Path, _, _}) -> Path;
leaf_path({like, _, Path, _, _}) -> Path;
leaf_path({contains, _, Path, _, _}) -> Path.

%% The document id is not part of the stored body (it is re-attached on
%% read), so id predicates cannot be residual-evaluated: only shapes
%% that lower to id_prefix/id_range are legal, and only as top-level
%% conjuncts.
check_id_use({'and', _, A, B}, _Level) ->
    check_id_use(A, nested), check_id_use(B, nested);
check_id_use({'or', _, A, B}, _Level) ->
    check_id_use(A, nested), check_id_use(B, nested);
check_id_use(Leaf, Level) ->
    case leaf_path(Leaf) of
        {path, _, base, [<<"id">>]} -> check_id_leaf(Leaf, Level);
        _ -> ok
    end.

check_id_leaf(Leaf, nested) ->
    fail(leaf_loc(Leaf), {unsupported, id_in_disjunction});
check_id_leaf({cmp, Loc, Op, _, {lit, _, Value}}, top) ->
    RangeOp = lists:member(Op, ['=', '<', '<=', '>', '>=']),
    case RangeOp andalso is_binary(Value) of
        true -> ok;
        false -> fail(Loc, {unsupported, id_condition})
    end;
check_id_leaf({like, Loc, _, Pattern, false}, top) ->
    case like_prefix(Pattern) of
        {prefix, _} -> ok;
        regex -> fail(Loc, {unsupported, id_condition})
    end;
check_id_leaf(Leaf, top) ->
    fail(leaf_loc(Leaf), {unsupported, id_condition}).

leaf_loc(Leaf) -> element(2, Leaf).

%% 'abc%' with a single trailing % and no _ is a prefix pattern.
-spec like_prefix(binary()) -> {prefix, binary()} | regex.
like_prefix(Pattern) when byte_size(Pattern) < 2 ->
    regex;
like_prefix(Pattern) ->
    StemSize = byte_size(Pattern) - 1,
    case Pattern of
        <<Stem:StemSize/binary, "%">> ->
            case binary:match(Stem, [<<"%">>, <<"_">>]) of
                nomatch -> {prefix, Stem};
                _ -> regex
            end;
        _ ->
            regex
    end.

%%--------------------------------------------------------------------
%% ORDER BY / SELECT
%%--------------------------------------------------------------------

check_order([]) ->
    ok;
check_order([{{path, Loc, base, []}, _Dir}]) ->
    fail(Loc, {invalid_order_key, source_reference});
check_order([_Single]) ->
    ok;
check_order([_, {{path, Loc, _, _}, _} | _]) ->
    fail(Loc, {unsupported, multi_key_order_by}).

check_select(star, _Canon) ->
    ok;
check_select(Projections, Canon) ->
    Names = [projection_name(P, Canon) || P <- Projections],
    case Names -- lists:usort(Names) of
        [] -> ok;
        [Dup | _] -> fail(undefined, {duplicate_output_name, Dup})
    end.

projection_name(#{name := Name}, _Canon) when Name =/= undefined ->
    Name;
projection_name(#{path := {path, _, unnest, []}}, Canon) ->
    #{unnest := #{alias := Alias}} = Canon,
    Alias;
projection_name(#{path := {path, _, base, []}}, #{alias := Alias}) ->
    Alias;
projection_name(#{path := {path, Loc, _, Comps}}, _Canon) ->
    case lists:last(Comps) of
        Key when is_binary(Key) -> Key;
        _Index -> fail(Loc, {alias_required, indexed_projection})
    end.

%%====================================================================
%% Lower
%%====================================================================

-spec lower(canon()) ->
    {ok, plan()} | {error, {invalid_query, loc() | undefined, tuple()}}.
lower(Canon) ->
    #{select := Select, source := Source0, unnest := Unnest0,
      where := Where, order_by := Order, limit := Limit0,
      offset := Offset0, subscribe := Subscribe} = Canon,
    try
        Source = lower_source(Source0),
        Unnest = lower_unnest(Unnest0),
        {IdCs, EngineCs, DocCs, ResidualCs} =
            classify_conjuncts(Where, Source),
        {IdScan, IdEmpty} = merge_id_conjuncts(IdCs),
        {EngineConds, Warnings0} = lower_conds(EngineCs, fun engine_path/1),
        {DocConds, _} = lower_conds(DocCs, fun engine_path/1),
        {ResidualConds, _} = lower_conds(ResidualCs, fun tagged_path/1),
        Limit = int_opt(Limit0),
        Offset = case int_opt(Offset0) of undefined -> 0; O -> O end,
        Empty = IdEmpty orelse Limit =:= 0,
        IsCollection = element(1, Source) =:= collection,
        Streamable = IsCollection andalso Order =:= []
                     andalso Unnest =:= undefined,
        PushDown = Streamable andalso not Empty,
        Spec0 = #{include_docs => true, select => ['*']},
        Spec1 = case IsCollection of
            true -> add_scan(Spec0, EngineConds, IdScan, Unnest);
            false -> Spec0
        end,
        Spec = case PushDown of
            true -> add_limit(Spec1, Limit, Offset);
            false -> Spec1
        end,
        PostOrder = case Order of
            [] -> undefined;
            [{OrderPath, Dir}] -> {project_key_pair(OrderPath, Source), Dir}
        end,
        Warnings = lists:usort(
            Warnings0 ++ order_warning(Order, IsCollection)),
        {ok, #{
            spec => Spec,
            source => Source,
            unnest => Unnest,
            post => #{
                residual => ResidualConds,
                doc_where => DocConds,
                order => PostOrder,
                offset => case PushDown of true -> 0; false -> Offset end,
                limit => case PushDown of
                    true -> undefined;
                    false -> Limit
                end,
                project => lower_select(Select, Canon, Source),
                empty => Empty
            },
            subscribe => Subscribe,
            streamable => Streamable,
            warnings => Warnings
        }}
    catch
        throw:{invalid_query, _, _} = Error -> {error, Error}
    end.

lower_source({collection, _, Name}) ->
    {collection, Name};
lower_source({table_fn, Loc, NameBin, Args}) ->
    Fn = binary_to_atom(NameBin, utf8),
    {table_fn, Fn, normalize_fn_args(Fn, Loc, Args)}.

lower_unnest(undefined) ->
    undefined;
lower_unnest(#{path := {path, _, base, Comps}, alias := Alias}) ->
    #{path => Comps, alias => Alias}.

%% Conjuncts touching the unnest alias are frame-residual; id conjuncts
%% become the primary-key scan; the rest feed the engine (collection) or
%% the doc-residual filter (table functions, where the hit set is the
%% driving relation and the engine is not consulted).
classify_conjuncts(Where, Source) ->
    lists:foldr(
        fun(Conjunct, {Id, Engine, Doc, Residual}) ->
            case conjunct_uses_unnest(Conjunct) of
                true ->
                    {Id, Engine, Doc, [Conjunct | Residual]};
                false ->
                    case Source of
                        {table_fn, _, _} ->
                            {Id, Engine, [Conjunct | Doc], Residual};
                        {collection, _} ->
                            case id_conjunct(Conjunct) of
                                true -> {[Conjunct | Id], Engine, Doc,
                                         Residual};
                                false -> {Id, [Conjunct | Engine], Doc,
                                          Residual}
                            end
                    end
            end
        end,
        {[], [], [], []},
        Where).

conjunct_uses_unnest(Conjunct) ->
    lists:any(fun({path, _, Var, _}) -> Var =:= unnest end,
              expr_paths(Conjunct)).

id_conjunct({cmp, _, _, {path, _, base, [<<"id">>]}, _}) -> true;
id_conjunct({like, _, {path, _, base, [<<"id">>]}, _, false}) -> true;
id_conjunct(_) -> false.

%%--------------------------------------------------------------------
%% Document-id scan: merge id conjuncts into one end-exclusive range
%%--------------------------------------------------------------------

merge_id_conjuncts([]) ->
    {none, false};
merge_id_conjuncts([{like, _, _, Pattern, false}]) ->
    {prefix, Stem} = like_prefix(Pattern),
    {{prefix, Stem}, false};
merge_id_conjuncts(Conjuncts) ->
    {Start, End} = lists:foldl(
        fun(Conjunct, {S, E}) ->
            {S1, E1} = id_range_of(Conjunct),
            {max_start(S, S1), min_end(E, E1)}
        end,
        {undefined, undefined},
        Conjuncts),
    Empty = Start =/= undefined andalso End =/= undefined
            andalso Start >= End,
    {{range, Start, End}, Empty}.

id_range_of({cmp, _, '=', _, {lit, _, V}}) -> {V, <<V/binary, 0>>};
id_range_of({cmp, _, '>', _, {lit, _, V}}) -> {<<V/binary, 0>>, undefined};
id_range_of({cmp, _, '>=', _, {lit, _, V}}) -> {V, undefined};
id_range_of({cmp, _, '<', _, {lit, _, V}}) -> {undefined, V};
id_range_of({cmp, _, '<=', _, {lit, _, V}}) -> {undefined, <<V/binary, 0>>};
id_range_of({like, _, _, Pattern, false}) ->
    {prefix, Stem} = like_prefix(Pattern),
    End = case bin_increment(Stem) of
        overflow -> undefined;
        Next -> Next
    end,
    {Stem, End}.

max_start(undefined, B) -> B;
max_start(A, undefined) -> A;
max_start(A, B) -> max(A, B).

min_end(undefined, B) -> B;
min_end(A, undefined) -> A;
min_end(A, B) -> min(A, B).

%% Smallest binary greater than all binaries prefixed by Bin.
bin_increment(Bin) ->
    case bin_increment_rev(lists:reverse(binary_to_list(Bin))) of
        overflow -> overflow;
        Bytes -> list_to_binary(lists:reverse(Bytes))
    end.

bin_increment_rev([]) -> overflow;
bin_increment_rev([255 | Rest]) -> bin_increment_rev(Rest);
bin_increment_rev([Byte | Rest]) -> [Byte + 1 | Rest].

%%--------------------------------------------------------------------
%% Spec assembly
%%--------------------------------------------------------------------

add_scan(Spec0, EngineConds, IdScan, Unnest) ->
    Spec1 = case EngineConds of
        [] -> Spec0;
        _ -> Spec0#{where => EngineConds}
    end,
    Spec2 = case IdScan of
        none -> Spec1;
        {prefix, Prefix} -> Spec1#{id_prefix => Prefix};
        {range, Start, End} -> Spec1#{id_range => {Start, End}}
    end,
    HasScan = maps:is_key(where, Spec2) orelse maps:is_key(id_prefix, Spec2)
              orelse maps:is_key(id_range, Spec2),
    case {HasScan, Unnest} of
        {true, _} ->
            Spec2;
        {false, #{path := UnnestComps}} ->
            %% Widest index-friendly approximation: rows without the
            %% array produce zero unnest frames anyway.
            Spec2#{where => [{exists, UnnestComps}]};
        {false, undefined} ->
            %% Unconstrained query: full primary-key scan.
            Spec2#{id_range => {undefined, undefined}}
    end.

add_limit(Spec, undefined, 0) -> Spec;
add_limit(Spec, undefined, Offset) -> Spec#{offset => Offset};
add_limit(Spec, Limit, 0) -> Spec#{limit => Limit};
add_limit(Spec, Limit, Offset) -> Spec#{limit => Limit, offset => Offset}.

order_warning([], _IsCollection) -> [];
order_warning([{{path, _, _, Comps}, _} | _], true) ->
    [{order_by_materializes, Comps}];
order_warning(_, false) -> [].

int_opt(undefined) -> undefined;
int_opt({integer, _, N}) -> N.

%%--------------------------------------------------------------------
%% Condition lowering onto the engine DSL
%%--------------------------------------------------------------------

lower_conds(Conjuncts, PathFun) ->
    lists:mapfoldr(
        fun(Conjunct, Warnings) ->
            {Cond, W} = lower_cond(Conjunct, PathFun),
            {Cond, W ++ Warnings}
        end,
        [],
        Conjuncts).

lower_cond({cmp, _, '=', Path, {lit, _, V}}, PF) ->
    {{path, PF(Path), V}, []};
lower_cond({cmp, _, '!=', Path, {lit, _, V}}, PF) ->
    {{compare, PF(Path), '=/=', V}, [{full_scan, '!='}]};
lower_cond({cmp, _, '<', Path, {lit, _, V}}, PF) ->
    {{compare, PF(Path), '<', V}, []};
lower_cond({cmp, _, '<=', Path, {lit, _, V}}, PF) ->
    {{compare, PF(Path), '=<', V}, []};
lower_cond({cmp, _, '>', Path, {lit, _, V}}, PF) ->
    {{compare, PF(Path), '>', V}, []};
lower_cond({cmp, _, '>=', Path, {lit, _, V}}, PF) ->
    {{compare, PF(Path), '>=', V}, []};
lower_cond({is_null, _, Path, false}, PF) ->
    %% PartiQL IS NULL is true for stored null AND absent
    {{'or', [{path, PF(Path), null}, {missing, PF(Path)}]},
     [{full_scan, is_null}]};
lower_cond({is_null, _, Path, true}, PF) ->
    %% present and not null, in one engine condition
    {{compare, PF(Path), '=/=', null}, [{full_scan, is_not_null}]};
lower_cond({is_missing, _, Path, false}, PF) ->
    {{missing, PF(Path)}, [{full_scan, missing}]};
lower_cond({is_missing, _, Path, true}, PF) ->
    {{exists, PF(Path)}, []};
lower_cond({in, _, Path, Lits, false}, PF) ->
    {{in, PF(Path), lit_values(Lits)}, [{full_scan, in}]};
lower_cond({in, _, Path, Lits, true}, PF) ->
    %% exists guard restores absent => filtered-out under negation
    {guarded(PF(Path), {in, PF(Path), lit_values(Lits)}),
     [{full_scan, not_in}]};
lower_cond({like, _, Path, Pattern, false}, PF) ->
    case like_prefix(Pattern) of
        {prefix, Stem} -> {{prefix, PF(Path), Stem}, []};
        regex ->
            {{regex, PF(Path), like_regex(Pattern)}, [{full_scan, like}]}
    end;
lower_cond({like, _, Path, Pattern, true}, PF) ->
    Inner = case like_prefix(Pattern) of
        {prefix, Stem} -> {prefix, PF(Path), Stem};
        regex -> {regex, PF(Path), like_regex(Pattern)}
    end,
    {guarded(PF(Path), Inner), [{full_scan, not_like}]};
lower_cond({contains, _, Path, {lit, _, V}, false}, PF) ->
    {{contains, PF(Path), V}, [{full_scan, contains}]};
lower_cond({contains, _, Path, {lit, _, V}, true}, PF) ->
    {guarded(PF(Path), {contains, PF(Path), V}),
     [{full_scan, not_contains}]};
lower_cond({'or', _, A, B}, PF) ->
    {CondA, WA} = lower_cond(A, PF),
    {CondB, WB} = lower_cond(B, PF),
    {{'or', [CondA, CondB]}, WA ++ WB ++ [{full_scan, 'or'}]};
lower_cond({'and', _, A, B}, PF) ->
    {CondA, WA} = lower_cond(A, PF),
    {CondB, WB} = lower_cond(B, PF),
    {{'and', [CondA, CondB]}, WA ++ WB}.

guarded(Path, Inner) ->
    {'and', [{exists, Path}, {'not', Inner}]}.

lit_values(Lits) ->
    [V || {lit, _, V} <- Lits].

%% LIKE to anchored regex: % => .*, _ => ., everything else literal.
like_regex(Pattern) ->
    Chars = unicode:characters_to_list(Pattern),
    unicode:characters_to_binary(
        [$^,
         [case Char of
              $% -> ".*";
              $_ -> ".";
              _ -> escape_re(Char)
          end || Char <- Chars],
         $$]).

escape_re(Char) ->
    case lists:member(Char, ".^$*+?()[]{}|\\") of
        true -> [$\\, Char];
        false -> [Char]
    end.

engine_path({path, _, base, Comps}) -> Comps.

tagged_path({path, _, base, Comps}) -> {b, Comps};
tagged_path({path, _, unnest, Comps}) -> {u, Comps}.

%%--------------------------------------------------------------------
%% Projection and order keys
%%--------------------------------------------------------------------

lower_select(star, _Canon, _Source) ->
    star;
lower_select(Projections, Canon, Source) ->
    [begin
         #{path := Path} = Projection,
         {Tag, Key} = project_key_pair(Path, Source),
         {Tag, Key, projection_name(Projection, Canon)}
     end || Projection <- Projections].

project_key_pair({path, _, base, [<<"_score">>]}, {table_fn, _, _}) ->
    {score, <<"_score">>};
project_key_pair({path, _, base, [<<"_distance">>]},
                 {table_fn, vector_top_k, _}) ->
    {score, <<"_distance">>};
project_key_pair({path, _, base, Comps}, _Source) ->
    {b, Comps};
project_key_pair({path, _, unnest, Comps}, _Source) ->
    {u, Comps}.

%%====================================================================
%% Helpers
%%====================================================================

-spec fail(loc() | undefined, tuple()) -> no_return().
fail(Loc, Reason) ->
    throw({invalid_query, Loc, Reason}).
