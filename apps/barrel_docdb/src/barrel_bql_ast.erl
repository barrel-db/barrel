%% @doc BQL AST shapes shared by the parser, the lowering pass and their
%% consumers. Locations are {Line, Column} and sit on every node that can
%% fail validation.
-module(barrel_bql_ast).

-export_type([
    ast/0,
    loc/0,
    path/0,
    path_component/0,
    projection/0,
    from/0,
    source/0,
    fn_arg/0,
    unnest/0,
    expr/0,
    operand/0,
    literal/0,
    order_item/0
]).

-type loc() :: {pos_integer(), pos_integer()}.

-type path_component() :: {key, binary()} | {index, non_neg_integer()} | wildcard.
-type path() :: {path, loc(), [path_component()]}.

-type literal() :: {lit, loc(), binary() | number() | boolean() | null}.
-type operand() :: path() | literal() | {param, loc(), binary()}.

-type projection() :: #{expr := path(), as := binary() | undefined, loc := loc()}.

-type fn_arg() :: {pos, operand()} | {named, binary(), operand()}.
-type source() ::
    {collection, loc(), binary()}
    | {table_fn, loc(), binary(), [fn_arg()]}.
-type unnest() :: #{path := path(), alias := binary(), loc := loc()}.
-type from() :: #{source := source(), alias := binary() | undefined,
                  unnest := undefined | unnest()}.

-type expr() ::
    {'and', loc(), expr(), expr()}
    | {'or', loc(), expr(), expr()}
    | {'not', loc(), expr()}
    | {cmp, loc(), '=' | '!=' | '<' | '<=' | '>' | '>=', operand(), operand()}
    | {is_null, loc(), operand(), Negated :: boolean()}
    | {is_missing, loc(), operand(), Negated :: boolean()}
    | {in, loc(), operand(), [literal()], Negated :: boolean()}
    | {like, loc(), operand(), binary(), Negated :: boolean()}
    | {between, loc(), operand(), operand(), operand(), Negated :: boolean()}
    | {contains, loc(), path(), literal()}.

-type order_item() :: {path(), asc | desc}.

-type ast() :: #{
    type := select,
    select := star | [projection()],
    from := from(),
    where := expr() | undefined,
    order_by := [order_item()],
    limit := undefined | {integer, loc(), non_neg_integer()},
    offset := undefined | {integer, loc(), non_neg_integer()},
    subscribe := boolean()
}.
