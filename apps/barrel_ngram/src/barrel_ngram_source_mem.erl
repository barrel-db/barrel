%%%-------------------------------------------------------------------
%%% @doc In-memory {@link barrel_ngram_source} implementation.
%%%
%%% For tests and small corpora: `InitArg' is a `#{DocId => binary()}'
%%% map of document text, held directly as `State' (no process, no copy
%%% beyond the map itself).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_source_mem).

-behaviour(barrel_ngram_source).

-export([pread/4, doc_size/2]).

-spec pread(#{binary() => binary()}, binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, binary()} | {error, term()}.
pread(Docs, DocId, _Offset, 0) ->
    case maps:is_key(DocId, Docs) of
        true -> {ok, <<>>};
        false -> {error, not_found}
    end;
pread(Docs, DocId, Offset, Len) ->
    case maps:find(DocId, Docs) of
        error ->
            {error, not_found};
        {ok, Bin} ->
            Size = byte_size(Bin),
            case Offset >= Size of
                true ->
                    {error, eof};
                false ->
                    TakeLen = min(Len, Size - Offset),
                    {ok, binary:part(Bin, Offset, TakeLen)}
            end
    end.

-spec doc_size(#{binary() => binary()}, binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
doc_size(Docs, DocId) ->
    case maps:find(DocId, Docs) of
        error -> {error, not_found};
        {ok, Bin} -> {ok, byte_size(Bin)}
    end.
