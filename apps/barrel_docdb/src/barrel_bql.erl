%% @doc BQL, barrel's PartiQL dialect: the public compile surface.
%%
%% Pipeline: lex -> parse -> canonicalize -> validate -> lower. The
%% resulting plan carries a barrel_query spec for the document scan, an
%% optional table-function source (executed by the barrel module, never
%% here), an optional unnest, and per-row post stages.
%%
%% ```
%% {ok, Plan} = barrel_bql:compile(
%%     <<"SELECT title FROM db WHERE type = 'post' LIMIT 10">>),
%% {ok, Plan2} = barrel_bql:compile(
%%     <<"SELECT * FROM db WHERE org = $org">>,
%%     #{params => #{<<"org">> => <<"acme">>}}).
%% '''
-module(barrel_bql).

-export([
    parse/1,
    compile/1,
    compile/2,
    format_error/1
]).

-export_type([plan/0, bql_error/0]).

-type plan() :: barrel_bql_lower:plan().
-type loc() :: barrel_bql_ast:loc().
-type bql_error() ::
    {parse_error, loc(), binary()}
    | {invalid_query, loc() | undefined, tuple()}.

%% @doc Parse BQL source to its AST. Locations are {Line, Column}.
-spec parse(binary() | string()) ->
    {ok, barrel_bql_ast:ast()} | {error, bql_error()}.
parse(Source) when is_binary(Source) ->
    parse(unicode:characters_to_list(Source));
parse(Source) when is_list(Source) ->
    case barrel_bql_lexer:string(Source) of
        {ok, Tokens, _EndLoc} ->
            case barrel_bql_parser:parse(Tokens) of
                {ok, Ast} ->
                    {ok, Ast};
                {error, {Loc, Module, Message}} ->
                    {error, {parse_error, Loc,
                             iolist_to_binary(Module:format_error(Message))}}
            end;
        {error, {Loc, Module, Reason}, _EndLoc} ->
            {error, {parse_error, Loc,
                     iolist_to_binary(Module:format_error(Reason))}}
    end.

%% @doc Compile BQL source (or a parsed AST) to an executable plan.
-spec compile(binary() | string() | barrel_bql_ast:ast()) ->
    {ok, plan()} | {error, bql_error()}.
compile(Input) ->
    compile(Input, #{}).

-spec compile(binary() | string() | barrel_bql_ast:ast(),
              #{params => #{binary() => term()}}) ->
    {ok, plan()} | {error, bql_error()}.
compile(Ast, Opts) when is_map(Ast) ->
    Params = maps:get(params, Opts, #{}),
    case barrel_bql_lower:canonicalize(Ast, Params) of
        {ok, Canon} ->
            case barrel_bql_lower:validate(Canon) of
                ok -> barrel_bql_lower:lower(Canon);
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end;
compile(Source, Opts) ->
    case parse(Source) of
        {ok, Ast} -> compile(Ast, Opts);
        {error, _} = Error -> Error
    end.

%% @doc One-line human rendering of a compile error, for logs and the
%% REST 400 body.
-spec format_error(bql_error()) -> binary().
format_error({parse_error, Loc, Message}) ->
    iolist_to_binary([loc_prefix(Loc), "syntax error: ", Message]);
format_error({invalid_query, Loc, Reason}) ->
    iolist_to_binary([loc_prefix(Loc), reason_text(Reason)]).

loc_prefix({Line, Column}) ->
    io_lib:format("line ~b column ~b: ", [Line, Column]);
loc_prefix(undefined) ->
    "".

reason_text({unbound_param, Name}) ->
    ["parameter $", Name, " is not bound"];
reason_text({invalid_param, Name}) ->
    ["parameter $", Name, " must be a scalar value"];
reason_text({alias_required, table_fn}) ->
    "a table function source needs an alias (AS v)";
reason_text({alias_required, indexed_projection}) ->
    "a projection ending in an array index needs AS";
reason_text({duplicate_alias, Alias}) ->
    ["alias '", Alias, "' is used twice"];
reason_text({duplicate_output_name, Name}) ->
    ["duplicate output column '", Name, "'; add AS"];
reason_text({reserved_field, Field}) ->
    ["field '", Field, "' is reserved and not queryable"];
reason_text({use_is_null, _}) ->
    "comparisons with NULL never match; use IS NULL or IS MISSING";
reason_text({constant_predicate, _}) ->
    "predicates must reference a document field";
reason_text({unknown_table_function, Name}) ->
    ["unknown table function ", Name,
     " (vector_top_k, bm25_top_k, hybrid_top_k)"];
reason_text({invalid_table_fn_arg, Fn, query}) ->
    ["", atom_to_binary(Fn, utf8),
     " needs a string query as its first argument"];
reason_text({invalid_table_fn_arg, Fn, {duplicate, Name}}) ->
    ["duplicate option ", Name, " for ", atom_to_binary(Fn, utf8)];
reason_text({invalid_table_fn_arg, Fn, What}) when is_binary(What) ->
    ["unknown option ", What, " for ", atom_to_binary(Fn, utf8)];
reason_text({invalid_table_fn_arg, Fn, What}) ->
    io_lib:format("invalid ~s option for ~s", [What, Fn]);
reason_text({invalid_unnest_path, _}) ->
    "UNNEST needs a document array path";
reason_text({invalid_order_key, _}) ->
    "cannot ORDER BY the source itself";
reason_text({unsupported_with_subscribe, What}) ->
    io_lib:format("~s is not supported with SUBSCRIBE", [What]);
reason_text({unsupported, wildcard_path, use_unnest}) ->
    "wildcard [*] paths are not supported; use UNNEST";
reason_text({unsupported, id_condition}) ->
    "the document id only supports =, range comparisons and "
    "LIKE 'prefix%'";
reason_text({unsupported, id_in_disjunction}) ->
    "document id conditions cannot appear inside OR or NOT";
reason_text({unsupported, multi_key_order_by}) ->
    "ORDER BY supports a single key";
reason_text({unsupported, score_in_where}) ->
    "_score and _distance can be selected and ordered by, "
    "not filtered on";
reason_text({unsupported, What}) ->
    io_lib:format("unsupported: ~s", [What]);
reason_text(Reason) ->
    io_lib:format("~p", [Reason]).
