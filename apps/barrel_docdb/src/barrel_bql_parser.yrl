%% BQL grammar. Step-1 probe shape: proves the leex/yecc toolchain under
%% warnings_as_errors, xref and dialyzer before the real grammar lands.

Nonterminals statement.

Terminals select from ident.

Rootsymbol statement.

Expect 0.

statement -> select ident from ident : {probe, ident_name('$2'), ident_name('$4')}.

Erlang code.

ident_name({ident, _Loc, Name}) -> Name.
