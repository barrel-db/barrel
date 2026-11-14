%%%-------------------------------------------------------------------
%%% @doc Document writes for the agent layer: write once, replace every
%%% version this node holds, or delete every version. A replace or a
%%% delete hands back all the versions it read, so it leaves no
%%% conflict behind on this node. `Db' is a barrel database (a space)
%%% or a barrel_docdb name (the registry).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_docs).

-export([create/3, replace/3, remove/2, get/2, fold_prefix/4, backoff/1]).

-define(RETRIES, 32).

%% @doc Write a document that must not exist yet: `{error, conflict}'
%% when a live version of it does.
-spec create(term(), map(), map()) -> {ok, map()} | {error, term()}.
create(Db, Doc, Opts) ->
    case put(Db, maps:without([<<"_rev">>, <<"_observed">>], Doc), Opts) of
        {error, {conflict, _}} -> {error, conflict};
        Other -> Other
    end.

%% @doc Write a document over every version of it this node holds
%% (the latest write wins). Retries when a local write lands between
%% the read and the write.
-spec replace(term(), map(), map()) -> {ok, map()} | {error, term()}.
replace(Db, Doc, Opts) ->
    replace(Db, Doc, Opts, ?RETRIES).

replace(Db, #{<<"id">> := Id} = Doc0, Opts, N) ->
    Doc1 = maps:without([<<"_rev">>, <<"_observed">>], Doc0),
    Doc = case get_doc(Db, Id, #{conflicts => true}) of
        {ok, #{<<"_observed">> := Seen}} -> Doc1#{<<"_observed">> => Seen};
        {ok, #{<<"_rev">> := Rev}} -> Doc1#{<<"_rev">> => Rev};
        {error, _} -> Doc1
    end,
    case put(Db, Doc, Opts) of
        {error, conflict} when N > 0 ->
            backoff(N), replace(Db, Doc0, Opts, N - 1);
        {error, {conflict, _}} when N > 0 ->
            backoff(N), replace(Db, Doc0, Opts, N - 1);
        Other -> Other
    end.

%% @doc Delete every version of a document this node holds. A missing
%% document is already gone: `ok'.
-spec remove(term(), binary()) -> ok | {error, term()}.
remove(Db, Id) ->
    remove(Db, Id, ?RETRIES).

remove(Db, Id, N) ->
    case get_doc(Db, Id, #{conflicts => true}) of
        {ok, Doc} ->
            Opts = case Doc of
                #{<<"_observed">> := Seen} -> #{observed => Seen};
                #{<<"_rev">> := Rev} -> #{rev => Rev}
            end,
            case delete(Db, Id, Opts) of
                {ok, _} -> ok;
                {error, {conflict, _}} when N > 0 ->
                    backoff(N), remove(Db, Id, N - 1);
                {error, not_found} -> ok;
                {error, _} = Err -> Err
            end;
        {error, not_found} ->
            ok;
        {error, _} = Err ->
            Err
    end.

%% @doc Read a document.
-spec get(term(), binary()) -> {ok, map()} | {error, term()}.
get(Db, Id) ->
    get_doc(Db, Id, #{}).

%% @doc Fold the live documents whose id starts with `Prefix'.
-spec fold_prefix(term(), binary(), fun((map(), term()) -> term()), term()) ->
    term().
fold_prefix(Db, Prefix, Fun, Acc0) ->
    {ok, Acc} = barrel_docdb:fold_docs(
        docdb(Db), fun(Doc, Acc) -> {ok, Fun(Doc, Acc)} end, Acc0,
        #{id_prefix => Prefix}),
    Acc.

%% @doc Wait a little before retry `N' (counting down from 32) of a
%% write that lost a race with a local writer.
-spec backoff(non_neg_integer()) -> ok.
backoff(N) ->
    timer:sleep(rand:uniform(1 bsl min(5, max(0, ?RETRIES - N)))),
    ok.

%%====================================================================
%% Internal
%%====================================================================

put(#{docdb := _} = Db, Doc, Opts) -> barrel:put_doc(Db, Doc, Opts);
put(Db, Doc, Opts) -> barrel_docdb:put_doc(Db, Doc, Opts).

delete(#{docdb := _} = Db, Id, Opts) -> barrel:delete_doc(Db, Id, Opts);
delete(Db, Id, Opts) -> barrel_docdb:delete_doc(Db, Id, Opts).

get_doc(Db, Id, Opts) -> barrel_docdb:get_doc(docdb(Db), Id, Opts).

docdb(#{docdb := DbBin}) -> DbBin;
docdb(DbBin) when is_binary(DbBin) -> DbBin.
