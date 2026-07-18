%% BQL grammar. Layered expression grammar (or > and > not > predicate),
%% precedence-declaration-free; any new conflict must be resolved by
%% refactoring, not by raising Expect.
%%
%% Bare boolean paths are not predicates (write d.active = true): that
%% restriction is what keeps BETWEEN/NOT LALR(1)-clean, since an operand
%% can never derive 'and'. Negative numbers are built here from '-'.
%% <> is normalized to != in the actions.

Nonterminals
    statement select_stmt select_list proj_list projection
    from_clause source opt_alias fn_args fn_arg
    opt_where expr and_expr not_expr predicate operand
    literal literal_list in_rhs
    path_expr path_rest path_key
    opt_order order_list order_item opt_dir
    opt_limit opt_offset opt_subscribe.

Terminals
    select from where order by limit offset as unnest asc desc
    'and' 'or' 'not' in like between is null missing true false
    subscribe contains
    ident string integer float param
    '=' '!=' '<>' '<' '<=' '>' '>=' '=>'
    '(' ')' '*' ',' '.' '[' ']' '-'.

Rootsymbol statement.

Expect 0.

statement -> select_stmt : '$1'.

select_stmt -> select select_list from_clause opt_where opt_order
               opt_limit opt_offset opt_subscribe :
    #{type => select,
      select => '$2',
      from => '$3',
      where => '$4',
      order_by => '$5',
      limit => '$6',
      offset => '$7',
      subscribe => '$8'}.

%%--------------------------------------------------------------------
%% SELECT list
%%--------------------------------------------------------------------

select_list -> '*' : star.
select_list -> proj_list : lists:reverse('$1').

proj_list -> projection : ['$1'].
proj_list -> proj_list ',' projection : ['$3' | '$1'].

projection -> path_expr :
    #{expr => '$1', as => undefined, loc => path_loc('$1')}.
projection -> path_expr as ident :
    #{expr => '$1', as => ident_name('$3'), loc => path_loc('$1')}.

%%--------------------------------------------------------------------
%% FROM: collection or table function, optional alias, optional UNNEST
%%--------------------------------------------------------------------

from_clause -> from source opt_alias :
    #{source => '$2', alias => '$3', unnest => undefined}.
from_clause -> from source opt_alias ',' unnest '(' path_expr ')' as ident :
    #{source => '$2', alias => '$3',
      unnest => #{path => '$7', alias => ident_name('$10'),
                  loc => token_loc('$5')}}.

source -> ident : {collection, token_loc('$1'), ident_name('$1')}.
source -> ident '(' fn_args ')' :
    {table_fn, token_loc('$1'), ident_name('$1'), lists:reverse('$3')}.

opt_alias -> '$empty' : undefined.
opt_alias -> as ident : ident_name('$2').
opt_alias -> ident : ident_name('$1').

fn_args -> fn_arg : ['$1'].
fn_args -> fn_args ',' fn_arg : ['$3' | '$1'].

fn_arg -> literal : {pos, '$1'}.
fn_arg -> param : {pos, '$1'}.
fn_arg -> ident '=>' literal : {named, ident_name('$1'), '$3'}.
fn_arg -> ident '=>' param : {named, ident_name('$1'), '$3'}.

%%--------------------------------------------------------------------
%% WHERE expression layer
%%--------------------------------------------------------------------

opt_where -> '$empty' : undefined.
opt_where -> where expr : '$2'.

expr -> expr 'or' and_expr : {'or', token_loc('$2'), '$1', '$3'}.
expr -> and_expr : '$1'.

and_expr -> and_expr 'and' not_expr : {'and', token_loc('$2'), '$1', '$3'}.
and_expr -> not_expr : '$1'.

not_expr -> 'not' not_expr : {'not', token_loc('$1'), '$2'}.
not_expr -> predicate : '$1'.

predicate -> '(' expr ')' : '$2'.
predicate -> operand '=' operand : {cmp, token_loc('$2'), '=', '$1', '$3'}.
predicate -> operand '!=' operand : {cmp, token_loc('$2'), '!=', '$1', '$3'}.
predicate -> operand '<>' operand : {cmp, token_loc('$2'), '!=', '$1', '$3'}.
predicate -> operand '<' operand : {cmp, token_loc('$2'), '<', '$1', '$3'}.
predicate -> operand '<=' operand : {cmp, token_loc('$2'), '<=', '$1', '$3'}.
predicate -> operand '>' operand : {cmp, token_loc('$2'), '>', '$1', '$3'}.
predicate -> operand '>=' operand : {cmp, token_loc('$2'), '>=', '$1', '$3'}.
predicate -> operand is null : {is_null, token_loc('$2'), '$1', false}.
predicate -> operand is 'not' null : {is_null, token_loc('$2'), '$1', true}.
predicate -> operand is missing : {is_missing, token_loc('$2'), '$1', false}.
predicate -> operand is 'not' missing :
    {is_missing, token_loc('$2'), '$1', true}.
predicate -> operand in in_rhs : mk_in(token_loc('$2'), '$1', '$3', false).
predicate -> operand 'not' in in_rhs :
    mk_in(token_loc('$3'), '$1', '$4', true).
predicate -> operand like string :
    {like, token_loc('$2'), '$1', string_value('$3'), false}.
predicate -> operand 'not' like string :
    {like, token_loc('$3'), '$1', string_value('$4'), true}.
predicate -> operand between operand 'and' operand :
    {between, token_loc('$2'), '$1', '$3', '$5', false}.
predicate -> operand 'not' between operand 'and' operand :
    {between, token_loc('$3'), '$1', '$4', '$6', true}.
predicate -> contains '(' path_expr ',' literal ')' :
    {contains, token_loc('$1'), '$3', '$5'}.
predicate -> path_expr contains literal :
    {contains, token_loc('$2'), '$1', '$3'}.

operand -> path_expr : '$1'.
operand -> literal : '$1'.
operand -> param : '$1'.

literal -> string : {lit, token_loc('$1'), string_value('$1')}.
literal -> integer : {lit, token_loc('$1'), int_value('$1')}.
literal -> float : {lit, token_loc('$1'), float_value('$1')}.
literal -> '-' integer : {lit, token_loc('$1'), -int_value('$2')}.
literal -> '-' float : {lit, token_loc('$1'), -float_value('$2')}.
literal -> true : {lit, token_loc('$1'), true}.
literal -> false : {lit, token_loc('$1'), false}.
literal -> null : {lit, token_loc('$1'), null}.

literal_list -> literal : ['$1'].
literal_list -> literal_list ',' literal : ['$3' | '$1'].

%% IN right-hand side: a parenthesized set (scalar `in') or a bare path
%% (array membership, #5). Unified under one nonterminal so `in' stays
%% LALR(1)-clean (the '(' vs ident choice is one-token lookahead).
in_rhs -> '(' literal_list ')' : {set, lists:reverse('$2')}.
in_rhs -> path_expr : {member, '$1'}.

%%--------------------------------------------------------------------
%% Paths: a.b, a[0], a[*], and any keyword after a dot
%%--------------------------------------------------------------------

path_expr -> ident path_rest :
    {path, token_loc('$1'), [{key, ident_name('$1')} | lists:reverse('$2')]}.

path_rest -> '$empty' : [].
path_rest -> path_rest '.' path_key : [{key, '$3'} | '$1'].
path_rest -> path_rest '[' integer ']' : [{index, int_value('$3')} | '$1'].
path_rest -> path_rest '[' '*' ']' : [wildcard | '$1'].

path_key -> ident : ident_name('$1').
path_key -> select : <<"select">>.
path_key -> from : <<"from">>.
path_key -> where : <<"where">>.
path_key -> order : <<"order">>.
path_key -> by : <<"by">>.
path_key -> limit : <<"limit">>.
path_key -> offset : <<"offset">>.
path_key -> as : <<"as">>.
path_key -> unnest : <<"unnest">>.
path_key -> asc : <<"asc">>.
path_key -> desc : <<"desc">>.
path_key -> 'and' : <<"and">>.
path_key -> 'or' : <<"or">>.
path_key -> 'not' : <<"not">>.
path_key -> in : <<"in">>.
path_key -> like : <<"like">>.
path_key -> between : <<"between">>.
path_key -> is : <<"is">>.
path_key -> null : <<"null">>.
path_key -> missing : <<"missing">>.
path_key -> true : <<"true">>.
path_key -> false : <<"false">>.
path_key -> subscribe : <<"subscribe">>.
path_key -> contains : <<"contains">>.

%%--------------------------------------------------------------------
%% ORDER BY / LIMIT / OFFSET / SUBSCRIBE
%%--------------------------------------------------------------------

opt_order -> '$empty' : [].
opt_order -> order by order_list : lists:reverse('$3').

order_list -> order_item : ['$1'].
order_list -> order_list ',' order_item : ['$3' | '$1'].

order_item -> path_expr opt_dir : {'$1', '$2'}.

opt_dir -> '$empty' : asc.
opt_dir -> asc : asc.
opt_dir -> desc : desc.

opt_limit -> '$empty' : undefined.
opt_limit -> limit integer :
    {integer, token_loc('$2'), int_value('$2')}.

opt_offset -> '$empty' : undefined.
opt_offset -> offset integer :
    {integer, token_loc('$2'), int_value('$2')}.

opt_subscribe -> '$empty' : false.
opt_subscribe -> subscribe : true.

Erlang code.

ident_name({ident, _Loc, Name}) -> Name.

int_value({integer, _Loc, N}) -> N.

float_value({float, _Loc, F}) -> F.

string_value({string, _Loc, S}) -> S.

token_loc(Token) -> element(2, Token).

path_loc({path, Loc, _Components}) -> Loc.

%% Unify the two IN right-hand sides (#5). A parenthesized set stays a
%% scalar `in'; a bare path is array membership, i.e. `X IN arr' ==
%% `arr CONTAINS X', reusing the `contains' node ('not' wraps the negated
%% form). Membership requires a literal element for now.
mk_in(Loc, {lit, _, _} = Lit, {member, Path}, false) ->
    {contains, Loc, Path, Lit};
mk_in(Loc, {lit, _, _} = Lit, {member, Path}, true) ->
    {'not', Loc, {contains, Loc, Path, Lit}};
mk_in(Loc, Operand, {set, Literals}, Negated) ->
    {in, Loc, Operand, Literals, Negated};
mk_in(Loc, _Operand, {member, _Path}, _Negated) ->
    return_error(Loc, "membership (IN <array>) requires a literal element").
