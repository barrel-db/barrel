%%% @doc Regression: a document re-created over a tombstone must be
%%% re-indexed, not just gettable.
%%%
%%% delete_doc removes a document's path-index entries but leaves the body
%%% column family intact. A naive update path would diff the (unchanged)
%%% old body against the new body, see no change, and never re-add the
%%% removed index paths — so `find' would silently return nothing for a
%%% revived document. The put path detects the tombstone and re-indexes
%%% from scratch instead.
-module(barrel_recreate_index_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([reindex_after_delete_single/1,
         reindex_after_delete_batch/1]).

all() ->
    [reindex_after_delete_single, reindex_after_delete_batch].

init_per_suite(Config) ->
    Dir = "/tmp/barrel_recreate_" ++ integer_to_list(erlang:system_time(millisecond)),
    application:set_env(barrel_docdb, data_dir, Dir),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC, utf8),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => ?config(dir, Config)}),
    [{db, Db} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

find_count(Db) ->
    {ok, Rows, _} = barrel_docdb:find(Db, #{where => [{path, [<<"t">>], <<"a">>}]}),
    length(Rows).

tombstone_rev(Db, Id) ->
    [{ok, Doc}] = barrel_docdb:get_docs(Db, [Id], #{include_deleted => true}),
    maps:get(<<"_rev">>, Doc).

reindex_after_delete_single(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d1">>, <<"t">> => <<"a">>}),
    ?assertEqual(1, find_count(Db)),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"d1">>),
    ?assertEqual(0, find_count(Db)),
    Rev = tombstone_rev(Db, <<"d1">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d1">>, <<"_rev">> => Rev,
                                         <<"t">> => <<"a">>}),
    ?assertEqual(1, find_count(Db)).

reindex_after_delete_batch(Config) ->
    Db = ?config(db, Config),
    [{ok, _}] = barrel_docdb:put_docs(Db, [#{<<"id">> => <<"d2">>, <<"t">> => <<"a">>}]),
    ?assertEqual(1, find_count(Db)),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"d2">>),
    ?assertEqual(0, find_count(Db)),
    Rev = tombstone_rev(Db, <<"d2">>),
    [{ok, _}] = barrel_docdb:put_docs(Db, [#{<<"id">> => <<"d2">>, <<"_rev">> => Rev,
                                             <<"t">> => <<"a">>}]),
    ?assertEqual(1, find_count(Db)).
