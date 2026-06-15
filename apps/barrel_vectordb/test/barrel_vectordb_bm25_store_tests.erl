%%%-------------------------------------------------------------------
%%% @doc Store-level BM25 indexing tests.
%%%
%%% Regression coverage: the atomic write path (add_vector / add_vector_batch)
%%% must feed the BM25 index, not only the do_add path used by update/upsert.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_store_tests).

-include_lib("eunit/include/eunit.hrl").

-define(STORE, bm25_store_test).
-define(DIM, 8).

bm25_store_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun add_vector_indexes_bm25/0,
         fun add_vector_batch_indexes_bm25/0,
         fun delete_removes_from_bm25/0
     ]}.

setup() ->
    Dir = "/tmp/barrel_bm25_store_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    {ok, _Pid} = barrel_vectordb:start_link(#{
        name => ?STORE,
        path => Dir,
        dimension => ?DIM,
        bm25_backend => memory
    }),
    Dir.

cleanup(Dir) ->
    catch barrel_vectordb:stop(?STORE),
    timer:sleep(20),
    os:cmd("rm -rf " ++ Dir),
    ok.

%% A single explicit-vector add must be searchable by BM25.
add_vector_indexes_bm25() ->
    ok = barrel_vectordb:add_vector(?STORE, <<"d1">>, <<"the quick brown fox">>,
                                    #{}, vec(0.1)),
    {ok, Hits} = barrel_vectordb:search_bm25(?STORE, <<"quick">>, #{k => 5}),
    ?assert(lists:keymember(<<"d1">>, 1, Hits)).

%% An explicit-vector batch add must index every document for BM25.
add_vector_batch_indexes_bm25() ->
    Batch = [{<<"b1">>, <<"alpha beta">>, #{}, vec(0.1)},
             {<<"b2">>, <<"beta gamma">>, #{}, vec(0.2)}],
    {ok, #{inserted := 2}} = barrel_vectordb:add_vector_batch(?STORE, Batch),
    {ok, Hits} = barrel_vectordb:search_bm25(?STORE, <<"beta">>, #{k => 5}),
    ?assert(lists:keymember(<<"b1">>, 1, Hits)),
    ?assert(lists:keymember(<<"b2">>, 1, Hits)).

%% Deleting a document drops it from BM25 results.
delete_removes_from_bm25() ->
    ok = barrel_vectordb:add_vector(?STORE, <<"d1">>, <<"unique token zorp">>,
                                    #{}, vec(0.1)),
    {ok, Before} = barrel_vectordb:search_bm25(?STORE, <<"zorp">>, #{k => 5}),
    ?assert(lists:keymember(<<"d1">>, 1, Before)),
    ok = barrel_vectordb:delete(?STORE, <<"d1">>),
    {ok, After} = barrel_vectordb:search_bm25(?STORE, <<"zorp">>, #{k => 5}),
    ?assertNot(lists:keymember(<<"d1">>, 1, After)).

vec(Base) ->
    [Base + (I / 100) || I <- lists:seq(1, ?DIM)].
