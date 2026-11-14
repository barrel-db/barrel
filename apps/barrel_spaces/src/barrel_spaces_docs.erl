%%%-------------------------------------------------------------------
%%% @doc Document writes for the agent layer: write once, replace every
%%% version this node holds, or delete every version. Each write hands
%%% back every live version this node holds, tombstones and the
%%% versions behind a tombstone included, so it leaves no conflict
%%% behind on this node. `Db' is a barrel database (a space)
%%% or a barrel_docdb name (the registry).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_docs).

-export([create/3, replace/3, remove/2, get/2, fold_prefix/4, backoff/1,
         state/2]).

-define(RETRIES, 32).

%% @doc Write a document that must not exist yet: `{error, conflict}'
%% when a live version of it does.
-spec create(term(), map(), map()) -> {ok, map()} | {error, term()}.
create(Db, #{<<"id">> := Id} = Doc0, Opts) ->
    Doc = maps:without([<<"_rev">>, <<"_observed">>], Doc0),
    case get_doc(Db, Id, #{}) of
        {ok, _} ->
            {error, conflict};
        {error, _} ->
            %% replace the versions behind a tombstone too, if any
            Doc1 = case state(Db, Id) of
                {ok, Live, _Deleted} when length(Live) > 1 ->
                    Doc#{<<"_observed">> => Live};
                _ ->
                    Doc
            end,
            case put(Db, Doc1, Opts) of
                {error, {conflict, _}} -> {error, conflict};
                Other -> Other
            end
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
            %% shown deleted: delete the versions behind the tombstone
            case state(Db, Id) of
                {ok, Live, true} when length(Live) > 1 ->
                    case delete(Db, Id, #{observed => Live}) of
                        {ok, _} -> ok;
                        {error, {conflict, _}} when N > 0 ->
                            backoff(N), remove(Db, Id, N - 1);
                        {error, _} = Err -> Err
                    end;
                _ ->
                    ok
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc The live versions this node holds of a document, and whether
%% the one it shows is a tombstone: `{ok, Tokens, Deleted}', or
%% `not_found' when the node never held the document.
-spec state(term(), binary()) -> {ok, [binary()], boolean()} | not_found.
state(Db, Id) ->
    case barrel_docdb:get_doc_state(docdb(Db), Id) of
        {ok, #{state := Bin}} ->
            State = barrel_live:decode_state(Bin),
            #{observed := Observed, deleted := Deleted} = barrel_live:view(State),
            {ok, [barrel_version:to_token(T) || T <- Observed], Deleted};
        {error, _} ->
            not_found
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
