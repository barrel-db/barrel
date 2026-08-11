%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the in-memory barrel_ngram_source implementation.
%%%
%%% Exercises the exact EOF/short-read/empty-document contract documented
%%% on {@link barrel_ngram_source}, both directly and through the
%%% dispatcher.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_source_mem_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_source_mem).
-define(D, barrel_ngram_source).

docs() ->
    #{<<"a">> => <<"hello world">>, <<"empty">> => <<>>}.

doc_size_hit_test() ->
    ?assertEqual({ok, 11}, ?M:doc_size(docs(), <<"a">>)),
    ?assertEqual({ok, 0}, ?M:doc_size(docs(), <<"empty">>)).

doc_size_missing_test() ->
    ?assertEqual({error, not_found}, ?M:doc_size(docs(), <<"nope">>)).

pread_exact_test() ->
    ?assertEqual({ok, <<"hello">>}, ?M:pread(docs(), <<"a">>, 0, 5)),
    ?assertEqual({ok, <<"world">>}, ?M:pread(docs(), <<"a">>, 6, 5)).

pread_clamped_short_read_test() ->
    ?assertEqual({ok, <<"world">>}, ?M:pread(docs(), <<"a">>, 6, 100)).

pread_offset_at_eof_test() ->
    ?assertEqual({error, eof}, ?M:pread(docs(), <<"a">>, 11, 1)),
    ?assertEqual({error, eof}, ?M:pread(docs(), <<"a">>, 20, 5)).

pread_zero_len_always_ok_test() ->
    %% Len =:= 0 succeeds regardless of Offset, including an Offset past
    %% the document's own size.
    ?assertEqual({ok, <<>>}, ?M:pread(docs(), <<"a">>, 0, 0)),
    ?assertEqual({ok, <<>>}, ?M:pread(docs(), <<"a">>, 11, 0)),
    ?assertEqual({ok, <<>>}, ?M:pread(docs(), <<"a">>, 999, 0)).

pread_empty_document_test() ->
    %% doc_size =:= 0: a {0, 0} read must succeed, not be rejected as eof.
    ?assertEqual({ok, <<>>}, ?M:pread(docs(), <<"empty">>, 0, 0)),
    ?assertEqual({error, eof}, ?M:pread(docs(), <<"empty">>, 0, 1)).

pread_missing_document_test() ->
    %% not_found takes precedence over the Len =:= 0 always-ok rule --
    %% a Len 0 read must not silently look like a successful read of an
    %% empty document that never existed.
    ?assertEqual({error, not_found}, ?M:pread(docs(), <<"nope">>, 0, 0)),
    ?assertEqual({error, not_found}, ?M:pread(docs(), <<"nope">>, 0, 5)).

%%====================================================================
%% Through the barrel_ngram_source dispatcher
%%====================================================================

dispatch_test() ->
    Source = {?M, docs()},
    ?assertEqual({ok, 11}, ?D:doc_size(Source, <<"a">>)),
    ?assertEqual({ok, <<"hello">>}, ?D:pread(Source, <<"a">>, 0, 5)),
    ?assertEqual({error, not_found}, ?D:doc_size(Source, <<"nope">>)).
