%% BQL grammar. Layered and precedence-declaration-free; any new conflict
%% must be resolved by refactoring, not by raising Expect.
%%
%% Step-3 shape: SELECT list, FROM collection with optional alias,
%% ORDER BY, LIMIT, OFFSET, trailing SUBSCRIBE, and path expressions
%% (dotted keys, [N] indexes, [*] wildcard; any keyword is legal after
%% a dot). WHERE, UNNEST and table functions land in step 4.

Nonterminals
    statement select_stmt select_list proj_list projection
    from_clause source opt_alias
    path_expr path_rest path_key
    opt_order order_list order_item opt_dir
    opt_limit opt_offset opt_subscribe.

Terminals
    select from where order by limit offset as unnest asc desc
    'and' 'or' 'not' in like between is null missing true false
    subscribe contains
    ident integer
    '*' ',' '.' '[' ']'.

Rootsymbol statement.

Expect 0.

statement -> select_stmt : '$1'.

select_stmt -> select select_list from_clause opt_order opt_limit
               opt_offset opt_subscribe :
    #{type => select,
      select => '$2',
      from => '$3',
      where => undefined,
      order_by => '$4',
      limit => '$5',
      offset => '$6',
      subscribe => '$7'}.

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
%% FROM
%%--------------------------------------------------------------------

from_clause -> from source opt_alias :
    #{source => '$2', alias => '$3', unnest => undefined}.

source -> ident : {collection, token_loc('$1'), ident_name('$1')}.

opt_alias -> '$empty' : undefined.
opt_alias -> as ident : ident_name('$2').
opt_alias -> ident : ident_name('$1').

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

token_loc(Token) -> element(2, Token).

path_loc({path, Loc, _Components}) -> Loc.
