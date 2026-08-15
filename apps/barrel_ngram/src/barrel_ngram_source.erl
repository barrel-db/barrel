%%%-------------------------------------------------------------------
%%% @doc Content-source behaviour for windowed verification.
%%%
%%% A positional (phase-2) query narrows a match to a small byte window
%%% and only needs to read that back, not the whole document. A corpus
%%% opened with `source => {Module, InitArg}' uses this to do that read
%%% without barrel_ngram calling into barrel_docdb directly; `InitArg' is
%%% stored once and threaded to every call as `State'.
%%%
%%% Optional: without it, verification fetches full current content via
%%% `barrel_docdb:get_docs/2' instead. Candidate narrowing itself never
%%% depends on `source' -- only the windowed read does.
%%%
%%% `pread/4' must return bytes identical to what
%%% {@link barrel_ngram_corpus:doc_text/2} produced at index time (a race
%%% with a concurrent change may return stale bytes -- accepted, since
%%% every read is re-verified before being trusted, so the failure mode
%%% is a false negative, never a false positive). EOF/short-read contract,
%%% matching `file:pread/3':
%%% <ul>
%%%   <li>`Len =:= 0' always returns `{ok, <<>>}', regardless of `Offset'
%%%       (as long as `Offset >= 0') -- this is what makes an empty
%%%       document readable at all, e.g. for a pattern that can
%%%       legitimately match empty content.</li>
%%%   <li>`Len > 0', `Offset >= Size' -- `{error, eof}'.</li>
%%%   <li>`Len > 0', `Offset < Size', `Offset + Len > Size' -- `{ok, Bin}'
%%%       clamped to the available suffix (`byte_size(Bin) =:= Size -
%%%       Offset'), never padded.</li>
%%%   <li>`Len > 0', `Offset < Size', `Offset + Len =< Size' -- `{ok, Bin}'
%%%       with `byte_size(Bin) =:= Len'.</li>
%%%   <li>Missing or deleted document -- `{error, not_found}'.</li>
%%% </ul>
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_source).

-export([pread/4, doc_size/2]).

-callback pread(State :: term(), DocId :: binary(),
                Offset :: non_neg_integer(), Len :: non_neg_integer()) ->
    {ok, binary()} | {error, term()}.

-callback doc_size(State :: term(), DocId :: binary()) ->
    {ok, non_neg_integer()} | {error, term()}.

%% @doc Dispatch `pread/4' to a source module.
-spec pread({module(), term()}, binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, binary()} | {error, term()}.
pread({Mod, State}, DocId, Offset, Len) ->
    Mod:pread(State, DocId, Offset, Len).

%% @doc Dispatch `doc_size/2' to a source module.
-spec doc_size({module(), term()}, binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
doc_size({Mod, State}, DocId) ->
    Mod:doc_size(State, DocId).
