%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb application and public API
%%%
%%% Tests the basic application startup, supervision tree,
%%% database server lifecycle, and the public API.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_app_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases - application
-export([
    app_starts_and_stops/1,
    supervisor_starts/1,
    db_sup_starts/1,
    db_server_lifecycle/1,
    db_server_info/1,
    db_server_store_refs/1,
    multiple_db_servers/1
]).

%% Test cases - public API
-export([
    api_create_db/1,
    api_open_close_db/1,
    api_delete_db/1,
    api_list_dbs/1,
    api_put_get_doc/1,
    api_get_docs/1,
    api_put_docs/1,
    api_update_doc/1,
    api_delete_doc/1,
    api_fold_docs/1,
    api_attachments/1,
    api_changes/1
]).

%% Test cases - path indexing
-export([
    path_index_put_doc/1,
    path_index_update_doc/1,
    path_index_delete_doc/1,
    path_index_atomicity/1
]).

%% Test cases - query API
-export([
    api_find_simple/1,
    api_find_with_options/1,
    api_find_multiple_conditions/1,
    api_explain/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, application}, {group, db_server}, {group, public_api}, {group, path_indexing}, {group, query_api}].

groups() ->
    [
        {application, [sequence], [
            app_starts_and_stops,
            supervisor_starts,
            db_sup_starts
        ]},
        {db_server, [sequence], [
            db_server_lifecycle,
            db_server_info,
            db_server_store_refs,
            multiple_db_servers
        ]},
        {public_api, [sequence], [
            api_create_db,
            api_open_close_db,
            api_delete_db,
            api_list_dbs,
            api_put_get_doc,
            api_get_docs,
            api_put_docs,
            api_update_doc,
            api_delete_doc,
            api_fold_docs,
            api_attachments,
            api_changes
        ]},
        {path_indexing, [sequence], [
            path_index_put_doc,
            path_index_update_doc,
            path_index_delete_doc,
            path_index_atomicity
        ]},
        {query_api, [sequence], [
            api_find_simple,
            api_find_with_options,
            api_find_multiple_conditions,
            api_explain
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Ensure application is stopped before each test
    _ = application:stop(barrel_docdb),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Stop application after each test
    _ = application:stop(barrel_docdb),
    ok.

%%====================================================================
%% Test Cases - Application
%%====================================================================

%% @doc Test that the application starts and stops correctly
app_starts_and_stops(_Config) ->
    %% Start the application
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Verify it's running
    Apps = application:which_applications(),
    ?assert(lists:keymember(barrel_docdb, 1, Apps)),

    %% Stop the application
    ok = application:stop(barrel_docdb),

    %% Verify it's stopped
    Apps2 = application:which_applications(),
    ?assertNot(lists:keymember(barrel_docdb, 1, Apps2)),

    ok.

%% @doc Test that the main supervisor starts
supervisor_starts(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Check supervisor is registered
    ?assertNotEqual(undefined, whereis(barrel_docdb_sup)),

    %% Check it's a supervisor
    Pid = whereis(barrel_docdb_sup),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    ok.

%% @doc Test that the database supervisor starts
db_sup_starts(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Check db_sup is registered
    ?assertNotEqual(undefined, whereis(barrel_db_sup)),

    %% Check it's alive
    Pid = whereis(barrel_db_sup),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    ok.

%%====================================================================
%% Test Cases - DB Server
%%====================================================================

%% @doc Test database server lifecycle
db_server_lifecycle(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Start a database server
    DbName = <<"test_db">>,
    TestDir = "/tmp/barrel_lifecycle_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    Config = #{data_dir => TestDir},
    {ok, Pid} = barrel_db_sup:start_db(DbName, Config),

    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Stop the database server
    ok = barrel_db_server:stop(Pid),

    %% Give it time to terminate
    timer:sleep(100),
    ?assertNot(is_process_alive(Pid)),

    %% Cleanup
    os:cmd("rm -rf " ++ TestDir),

    ok.

%% @doc Test database server info
db_server_info(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"info_test_db">>,
    TestDir = "/tmp/barrel_info_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    Config = #{data_dir => TestDir},
    {ok, Pid} = barrel_db_sup:start_db(DbName, Config),

    %% Get info
    {ok, Info} = barrel_db_server:info(Pid),

    ?assertEqual(DbName, maps:get(name, Info)),
    ?assertEqual(Config, maps:get(config, Info)),
    ?assertEqual(Pid, maps:get(pid, Info)),
    ?assert(is_list(maps:get(db_path, Info))),

    %% Cleanup
    ok = barrel_db_server:stop(Pid),
    os:cmd("rm -rf " ++ TestDir),

    ok.

%% @doc Test database server store references are accessible
db_server_store_refs(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"store_refs_test_db">>,
    TestDir = "/tmp/barrel_store_refs_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    Config = #{data_dir => TestDir},
    {ok, Pid} = barrel_db_sup:start_db(DbName, Config),

    %% Get document store ref
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    ?assert(is_map(StoreRef)),
    ?assert(maps:is_key(ref, StoreRef)),
    ?assert(maps:is_key(path, StoreRef)),

    %% Get attachment store ref
    {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
    ?assert(is_map(AttRef)),
    ?assert(maps:is_key(ref, AttRef)),
    ?assert(maps:is_key(path, AttRef)),

    %% Verify stores are in expected paths
    DocPath = maps:get(path, StoreRef),
    AttPath = maps:get(path, AttRef),
    ?assert(string:find(DocPath, "docs") =/= nomatch),
    ?assert(string:find(AttPath, "attachments") =/= nomatch),

    %% Test document store works - write and read
    ok = barrel_store_rocksdb:put(StoreRef, <<"test_key">>, <<"test_value">>),
    {ok, <<"test_value">>} = barrel_store_rocksdb:get(StoreRef, <<"test_key">>),

    %% Test attachment store works - write and read
    {ok, _AttInfo} = barrel_att_store:put(AttRef, DbName, <<"doc1">>, <<"file.txt">>, <<"content">>),
    {ok, <<"content">>} = barrel_att_store:get(AttRef, DbName, <<"doc1">>, <<"file.txt">>),

    %% Cleanup
    ok = barrel_db_server:stop(Pid),
    os:cmd("rm -rf " ++ TestDir),

    ok.

%% @doc Test multiple database servers
multiple_db_servers(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    BaseDir = "/tmp/barrel_multi_" ++ integer_to_list(erlang:system_time(millisecond)),

    %% Start multiple databases
    Dbs = [
        {<<"db1">>, #{data_dir => BaseDir ++ "/db1"}},
        {<<"db2">>, #{data_dir => BaseDir ++ "/db2"}},
        {<<"db3">>, #{data_dir => BaseDir ++ "/db3"}}
    ],

    Pids = lists:map(
        fun({Name, Config}) ->
            {ok, Pid} = barrel_db_sup:start_db(Name, Config),
            {Name, Pid}
        end,
        Dbs
    ),

    %% Verify all are running and have both stores
    lists:foreach(
        fun({_Name, Pid}) ->
            ?assert(is_process_alive(Pid)),
            {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
            {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
            ?assert(is_map(StoreRef)),
            ?assert(is_map(AttRef))
        end,
        Pids
    ),

    %% Stop all
    lists:foreach(
        fun({_Name, Pid}) ->
            ok = barrel_db_server:stop(Pid)
        end,
        Pids
    ),

    %% Give time to terminate
    timer:sleep(100),

    %% Verify all are stopped
    lists:foreach(
        fun({_Name, Pid}) ->
            ?assertNot(is_process_alive(Pid))
        end,
        Pids
    ),

    %% Cleanup
    os:cmd("rm -rf " ++ BaseDir),

    ok.

%%====================================================================
%% Test Cases - Public API
%%====================================================================

%% @doc Test database creation via public API
api_create_db(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_create_test">>,
    TestDir = "/tmp/barrel_api_create_" ++ integer_to_list(erlang:system_time(millisecond)),

    %% Create database
    {ok, Pid} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),
    ?assert(is_pid(Pid)),

    %% Cannot create same db twice
    {error, already_exists} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Cleanup
    ok = barrel_docdb:close_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test database open/close via public API
api_open_close_db(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_open_test">>,
    TestDir = "/tmp/barrel_api_open_" ++ integer_to_list(erlang:system_time(millisecond)),

    %% Open non-existent db fails
    {error, not_found} = barrel_docdb:open_db(DbName),

    %% Create and open
    {ok, Pid1} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),
    {ok, Pid2} = barrel_docdb:open_db(DbName),
    ?assertEqual(Pid1, Pid2),

    %% Close
    ok = barrel_docdb:close_db(DbName),

    %% Open after close fails (db stopped)
    {error, not_found} = barrel_docdb:open_db(DbName),

    %% Cleanup
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test database deletion via public API
api_delete_db(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_delete_test">>,
    TestDir = "/tmp/barrel_api_delete_" ++ integer_to_list(erlang:system_time(millisecond)),

    %% Create database
    {ok, _Pid} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Delete database
    ok = barrel_docdb:delete_db(DbName),

    %% Database no longer exists
    {error, not_found} = barrel_docdb:open_db(DbName),

    %% Delete non-existent is ok
    ok = barrel_docdb:delete_db(DbName),

    ok.

%% @doc Test listing databases
api_list_dbs(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    TestDir = "/tmp/barrel_api_list_" ++ integer_to_list(erlang:system_time(millisecond)),

    %% Create multiple databases
    {ok, _} = barrel_docdb:create_db(<<"list_db1">>, #{data_dir => TestDir ++ "/1"}),
    {ok, _} = barrel_docdb:create_db(<<"list_db2">>, #{data_dir => TestDir ++ "/2"}),
    {ok, _} = barrel_docdb:create_db(<<"list_db3">>, #{data_dir => TestDir ++ "/3"}),

    %% List databases
    Dbs = barrel_docdb:list_dbs(),
    ?assert(lists:member(<<"list_db1">>, Dbs)),
    ?assert(lists:member(<<"list_db2">>, Dbs)),
    ?assert(lists:member(<<"list_db3">>, Dbs)),

    %% Cleanup
    ok = barrel_docdb:delete_db(<<"list_db1">>),
    ok = barrel_docdb:delete_db(<<"list_db2">>),
    ok = barrel_docdb:delete_db(<<"list_db3">>),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test put and get document
api_put_get_doc(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_doc_test">>,
    TestDir = "/tmp/barrel_api_doc_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Put a document
    Doc = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>, <<"value">> => 42},
    {ok, Result} = barrel_docdb:put_doc(DbName, Doc),

    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, Result)),
    ?assertEqual(true, maps:get(<<"ok">>, Result)),
    Rev = maps:get(<<"rev">>, Result),
    ?assert(is_binary(Rev)),

    %% Get the document
    {ok, Retrieved} = barrel_docdb:get_doc(DbName, <<"doc1">>),
    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, Retrieved)),
    ?assertEqual(<<"test">>, maps:get(<<"name">>, Retrieved)),
    ?assertEqual(42, maps:get(<<"value">>, Retrieved)),
    ?assertEqual(Rev, maps:get(<<"_rev">>, Retrieved)),

    %% Get non-existent document
    {error, not_found} = barrel_docdb:get_doc(DbName, <<"nonexistent">>),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test batch get documents (multi_get)
api_get_docs(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_get_docs_test">>,
    TestDir = "/tmp/barrel_api_get_docs_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Put multiple documents
    Doc1 = #{<<"id">> => <<"doc1">>, <<"name">> => <<"Alice">>},
    Doc2 = #{<<"id">> => <<"doc2">>, <<"name">> => <<"Bob">>},
    Doc3 = #{<<"id">> => <<"doc3">>, <<"name">> => <<"Charlie">>},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc1),
    {ok, _} = barrel_docdb:put_doc(DbName, Doc2),
    {ok, _} = barrel_docdb:put_doc(DbName, Doc3),

    %% Get multiple documents at once
    Results = barrel_docdb:get_docs(DbName, [<<"doc1">>, <<"doc2">>, <<"doc3">>]),
    ?assertEqual(3, length(Results)),
    [{ok, R1}, {ok, R2}, {ok, R3}] = Results,
    ?assertEqual(<<"Alice">>, maps:get(<<"name">>, R1)),
    ?assertEqual(<<"Bob">>, maps:get(<<"name">>, R2)),
    ?assertEqual(<<"Charlie">>, maps:get(<<"name">>, R3)),

    %% Get with some non-existent documents
    Results2 = barrel_docdb:get_docs(DbName, [<<"doc1">>, <<"nonexistent">>, <<"doc3">>]),
    [{ok, _}, {error, not_found}, {ok, _}] = Results2,

    %% Get empty list
    Results3 = barrel_docdb:get_docs(DbName, []),
    ?assertEqual([], Results3),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test batch put documents
api_put_docs(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_put_docs_test">>,
    TestDir = "/tmp/barrel_api_put_docs_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Put multiple documents at once
    Docs = [
        #{<<"id">> => <<"doc1">>, <<"name">> => <<"Alice">>, <<"type">> => <<"user">>},
        #{<<"id">> => <<"doc2">>, <<"name">> => <<"Bob">>, <<"type">> => <<"user">>},
        #{<<"id">> => <<"doc3">>, <<"name">> => <<"Charlie">>, <<"type">> => <<"user">>}
    ],
    Results = barrel_docdb:put_docs(DbName, Docs),
    ?assertEqual(3, length(Results)),

    %% All should succeed
    lists:foreach(
        fun({ok, Result}) ->
            ?assertEqual(true, maps:get(<<"ok">>, Result)),
            ?assert(is_binary(maps:get(<<"rev">>, Result)))
        end,
        Results
    ),

    %% Verify documents were created
    {ok, Doc1} = barrel_docdb:get_doc(DbName, <<"doc1">>),
    ?assertEqual(<<"Alice">>, maps:get(<<"name">>, Doc1)),
    {ok, Doc2} = barrel_docdb:get_doc(DbName, <<"doc2">>),
    ?assertEqual(<<"Bob">>, maps:get(<<"name">>, Doc2)),
    {ok, Doc3} = barrel_docdb:get_doc(DbName, <<"doc3">>),
    ?assertEqual(<<"Charlie">>, maps:get(<<"name">>, Doc3)),

    %% Verify path index was updated for all docs
    {ok, QueryResults, _} = barrel_docdb:find(DbName, #{
        where => [{path, [<<"type">>], <<"user">>}]
    }),
    ?assertEqual(3, length(QueryResults)),

    %% Test batch update - get revisions first
    Rev1 = maps:get(<<"_rev">>, Doc1),
    Rev2 = maps:get(<<"_rev">>, Doc2),
    UpdateDocs = [
        #{<<"id">> => <<"doc1">>, <<"_rev">> => Rev1, <<"name">> => <<"Alice Updated">>, <<"type">> => <<"user">>},
        #{<<"id">> => <<"doc2">>, <<"_rev">> => Rev2, <<"name">> => <<"Bob Updated">>, <<"type">> => <<"user">>}
    ],
    UpdateResults = barrel_docdb:put_docs(DbName, UpdateDocs),
    ?assertEqual(2, length(UpdateResults)),

    %% Verify updates
    {ok, UpdatedDoc1} = barrel_docdb:get_doc(DbName, <<"doc1">>),
    ?assertEqual(<<"Alice Updated">>, maps:get(<<"name">>, UpdatedDoc1)),
    {ok, UpdatedDoc2} = barrel_docdb:get_doc(DbName, <<"doc2">>),
    ?assertEqual(<<"Bob Updated">>, maps:get(<<"name">>, UpdatedDoc2)),

    %% Test empty batch
    EmptyResults = barrel_docdb:put_docs(DbName, []),
    ?assertEqual([], EmptyResults),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test document update
api_update_doc(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_update_test">>,
    TestDir = "/tmp/barrel_api_update_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Create document
    Doc1 = #{<<"id">> => <<"doc1">>, <<"value">> => 1},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc1),

    %% Update document
    Doc2 = #{<<"id">> => <<"doc1">>, <<"_rev">> => Rev1, <<"value">> => 2},
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    ?assertNotEqual(Rev1, Rev2),

    %% Verify update
    {ok, Retrieved} = barrel_docdb:get_doc(DbName, <<"doc1">>),
    ?assertEqual(2, maps:get(<<"value">>, Retrieved)),
    ?assertEqual(Rev2, maps:get(<<"_rev">>, Retrieved)),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test document deletion
api_delete_doc(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_deldoc_test">>,
    TestDir = "/tmp/barrel_api_deldoc_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Create document
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => <<"doc1">>, <<"value">> => 1}),

    %% Delete document
    {ok, DeleteResult} = barrel_docdb:delete_doc(DbName, <<"doc1">>),
    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, DeleteResult)),
    ?assertEqual(true, maps:get(<<"ok">>, DeleteResult)),

    %% Document no longer accessible
    {error, not_found} = barrel_docdb:get_doc(DbName, <<"doc1">>),

    %% Delete non-existent document
    {error, not_found} = barrel_docdb:delete_doc(DbName, <<"nonexistent">>),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test fold over documents
api_fold_docs(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_fold_test">>,
    TestDir = "/tmp/barrel_api_fold_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Create multiple documents
    lists:foreach(
        fun(N) ->
            Id = <<"doc", (integer_to_binary(N))/binary>>,
            {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => Id, <<"n">> => N})
        end,
        lists:seq(1, 5)
    ),

    %% Fold over all documents
    {ok, Docs} = barrel_docdb:fold_docs(DbName,
        fun(Doc, Acc) -> {ok, [Doc | Acc]} end,
        []
    ),
    ?assertEqual(5, length(Docs)),

    %% Verify all documents are present
    Ids = [maps:get(<<"id">>, D) || D <- Docs],
    lists:foreach(
        fun(N) ->
            Id = <<"doc", (integer_to_binary(N))/binary>>,
            ?assert(lists:member(Id, Ids))
        end,
        lists:seq(1, 5)
    ),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test attachment API
api_attachments(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_att_test">>,
    TestDir = "/tmp/barrel_api_att_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Create a document
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => <<"doc1">>}),

    %% Put attachment
    {ok, AttInfo} = barrel_docdb:put_attachment(DbName, <<"doc1">>, <<"file.txt">>, <<"hello world">>),
    ?assertEqual(<<"file.txt">>, maps:get(name, AttInfo)),
    ?assertEqual(11, maps:get(length, AttInfo)),

    %% Get attachment
    {ok, Data} = barrel_docdb:get_attachment(DbName, <<"doc1">>, <<"file.txt">>),
    ?assertEqual(<<"hello world">>, Data),

    %% List attachments
    Atts = barrel_docdb:list_attachments(DbName, <<"doc1">>),
    ?assert(lists:member(<<"file.txt">>, Atts)),

    %% Delete attachment
    ok = barrel_docdb:delete_attachment(DbName, <<"doc1">>, <<"file.txt">>),

    %% Attachment no longer accessible
    {error, not_found} = barrel_docdb:get_attachment(DbName, <<"doc1">>, <<"file.txt">>),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test changes API
api_changes(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_changes_test">>,
    TestDir = "/tmp/barrel_api_changes_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Create documents
    lists:foreach(
        fun(N) ->
            {ok, _} = barrel_docdb:put_doc(DbName, #{
                <<"id">> => <<"doc", (integer_to_binary(N))/binary>>,
                <<"n">> => N
            })
        end,
        lists:seq(1, 5)
    ),

    %% Get all changes
    {ok, Changes, _LastSeq} = barrel_docdb:get_changes(DbName, first),
    ?assertEqual(5, length(Changes)),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%%====================================================================
%% Test Cases - Path Indexing
%%====================================================================

%% @doc Test that putting a document indexes its paths
path_index_put_doc(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"path_idx_put_test">>,
    TestDir = "/tmp/barrel_path_idx_put_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, Pid} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Get store ref for direct path index queries
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),

    %% Put a document
    Doc = #{
        <<"id">> => <<"doc1">>,
        <<"type">> => <<"user">>,
        <<"name">> => <<"Alice">>,
        <<"profile">> => #{
            <<"city">> => <<"Paris">>
        }
    },
    {ok, _} = barrel_docdb:put_doc(DbName, Doc),

    %% Verify paths are indexed
    TypeResults = fold_paths(StoreRef, DbName, [<<"type">>]),
    ?assertEqual(1, length(TypeResults)),
    [{TypePath, TypeDocId}] = TypeResults,
    ?assertEqual([<<"type">>, <<"user">>], TypePath),
    ?assertEqual(<<"doc1">>, TypeDocId),

    %% Verify nested paths
    CityResults = fold_paths(StoreRef, DbName, [<<"profile">>, <<"city">>]),
    ?assertEqual(1, length(CityResults)),
    [{CityPath, _}] = CityResults,
    ?assertEqual([<<"profile">>, <<"city">>, <<"Paris">>], CityPath),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test that updating a document updates its paths
path_index_update_doc(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"path_idx_update_test">>,
    TestDir = "/tmp/barrel_path_idx_update_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, Pid} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),

    %% Put initial document
    Doc1 = #{<<"id">> => <<"doc1">>, <<"status">> => <<"active">>, <<"count">> => 1},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc1),

    %% Verify initial paths
    ActiveResults = fold_paths(StoreRef, DbName, [<<"status">>, <<"active">>]),
    ActiveDocs = [D || {_, D} <- ActiveResults, D =:= <<"doc1">>],
    ?assertEqual(1, length(ActiveDocs)),

    %% Update document (change status, remove count, add new field)
    Doc2 = #{<<"id">> => <<"doc1">>, <<"_rev">> => Rev1, <<"status">> => <<"inactive">>, <<"tag">> => <<"new">>},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc2),

    %% Verify old status path is gone
    ActiveResults2 = fold_paths(StoreRef, DbName, [<<"status">>, <<"active">>]),
    ActiveDocs2 = [D || {_, D} <- ActiveResults2, D =:= <<"doc1">>],
    ?assertEqual(0, length(ActiveDocs2)),

    %% Verify new status path exists
    InactiveResults = fold_paths(StoreRef, DbName, [<<"status">>, <<"inactive">>]),
    InactiveDocs = [D || {_, D} <- InactiveResults, D =:= <<"doc1">>],
    ?assertEqual(1, length(InactiveDocs)),

    %% Verify old count path is gone
    CountResults = fold_paths(StoreRef, DbName, [<<"count">>]),
    CountDocs = [D || {_, D} <- CountResults, D =:= <<"doc1">>],
    ?assertEqual(0, length(CountDocs)),

    %% Verify new tag path exists
    TagResults = fold_paths(StoreRef, DbName, [<<"tag">>, <<"new">>]),
    ?assertEqual(1, length(TagResults)),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test that deleting a document removes its paths
path_index_delete_doc(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"path_idx_delete_test">>,
    TestDir = "/tmp/barrel_path_idx_delete_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, Pid} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),

    %% Put document
    Doc = #{<<"id">> => <<"doc1">>, <<"type">> => <<"temp">>, <<"data">> => <<"value">>},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc),

    %% Verify paths are indexed
    TypeResults1 = fold_paths(StoreRef, DbName, [<<"type">>, <<"temp">>]),
    ?assertEqual(1, length(TypeResults1)),

    %% Delete document
    {ok, _} = barrel_docdb:delete_doc(DbName, <<"doc1">>),

    %% Verify paths are removed
    TypeResults2 = fold_paths(StoreRef, DbName, [<<"type">>, <<"temp">>]),
    TypeDocs = [D || {_, D} <- TypeResults2, D =:= <<"doc1">>],
    ?assertEqual(0, length(TypeDocs)),

    DataResults = fold_paths(StoreRef, DbName, [<<"data">>]),
    DataDocs = [D || {_, D} <- DataResults, D =:= <<"doc1">>],
    ?assertEqual(0, length(DataDocs)),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test atomicity - path index and doc are consistent
path_index_atomicity(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"path_idx_atomic_test">>,
    TestDir = "/tmp/barrel_path_idx_atomic_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, Pid} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),

    %% Put multiple documents
    lists:foreach(
        fun(N) ->
            Type = case N rem 2 of 0 -> <<"even">>; 1 -> <<"odd">> end,
            {ok, _} = barrel_docdb:put_doc(DbName, #{
                <<"id">> => <<"doc", (integer_to_binary(N))/binary>>,
                <<"type">> => Type,
                <<"n">> => N
            })
        end,
        lists:seq(1, 10)
    ),

    %% Verify path index matches documents
    EvenResults = fold_paths(StoreRef, DbName, [<<"type">>, <<"even">>]),
    ?assertEqual(5, length(EvenResults)),

    OddResults = fold_paths(StoreRef, DbName, [<<"type">>, <<"odd">>]),
    ?assertEqual(5, length(OddResults)),

    %% Verify each indexed doc exists
    lists:foreach(
        fun({_Path, DocId}) ->
            {ok, _Doc} = barrel_docdb:get_doc(DbName, DocId),
            ok
        end,
        EvenResults ++ OddResults
    ),

    %% Delete all even docs and verify
    lists:foreach(
        fun(N) ->
            DocId = <<"doc", (integer_to_binary(N))/binary>>,
            {ok, _} = barrel_docdb:delete_doc(DbName, DocId)
        end,
        [2, 4, 6, 8, 10]
    ),

    %% Even paths should be gone
    EvenResults2 = fold_paths(StoreRef, DbName, [<<"type">>, <<"even">>]),
    ?assertEqual(0, length(EvenResults2)),

    %% Odd paths still there
    OddResults2 = fold_paths(StoreRef, DbName, [<<"type">>, <<"odd">>]),
    ?assertEqual(5, length(OddResults2)),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%%====================================================================
%% Test Cases - Query API
%%====================================================================

%% @doc Test simple find query
api_find_simple(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_find_simple_test">>,
    TestDir = "/tmp/barrel_api_find_simple_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Create documents
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => <<"user1">>, <<"type">> => <<"user">>, <<"name">> => <<"Alice">>}),
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => <<"user2">>, <<"type">> => <<"user">>, <<"name">> => <<"Bob">>}),
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => <<"post1">>, <<"type">> => <<"post">>, <<"title">> => <<"Hello">>}),

    %% Find all users
    {ok, Results, _} = barrel_docdb:find(DbName, #{
        where => [{path, [<<"type">>], <<"user">>}]
    }),

    ?assertEqual(2, length(Results)),
    Ids = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, Ids)),
    ?assert(lists:member(<<"user2">>, Ids)),

    %% Find posts
    {ok, PostResults, _} = barrel_docdb:find(DbName, #{
        where => [{path, [<<"type">>], <<"post">>}]
    }),
    ?assertEqual(1, length(PostResults)),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test find with options
api_find_with_options(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_find_opts_test">>,
    TestDir = "/tmp/barrel_api_find_opts_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Create documents
    lists:foreach(
        fun(N) ->
            {ok, _} = barrel_docdb:put_doc(DbName, #{
                <<"id">> => <<"doc", (integer_to_binary(N))/binary>>,
                <<"type">> => <<"item">>,
                <<"n">> => N
            })
        end,
        lists:seq(1, 10)
    ),

    %% Find with limit
    {ok, LimitResults, _} = barrel_docdb:find(DbName,
        #{where => [{path, [<<"type">>], <<"item">>}]},
        #{limit => 3}
    ),
    ?assertEqual(3, length(LimitResults)),

    %% Find without docs
    {ok, NoDocs, _} = barrel_docdb:find(DbName,
        #{where => [{path, [<<"type">>], <<"item">>}]},
        #{include_docs => false, limit => 5}
    ),
    ?assertEqual(5, length(NoDocs)),
    %% When include_docs is false, we still get results with id
    lists:foreach(
        fun(R) ->
            ?assert(maps:is_key(<<"id">>, R))
        end,
        NoDocs
    ),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test find with multiple conditions
api_find_multiple_conditions(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_find_multi_test">>,
    TestDir = "/tmp/barrel_api_find_multi_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Create documents
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => <<"u1">>, <<"type">> => <<"user">>, <<"org">> => <<"acme">>, <<"status">> => <<"active">>}),
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => <<"u2">>, <<"type">> => <<"user">>, <<"org">> => <<"acme">>, <<"status">> => <<"inactive">>}),
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => <<"u3">>, <<"type">> => <<"user">>, <<"org">> => <<"other">>, <<"status">> => <<"active">>}),

    %% Find active users in acme org
    {ok, Results, _} = barrel_docdb:find(DbName, #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], <<"acme">>},
            {path, [<<"status">>], <<"active">>}
        ]
    }),

    ?assertEqual(1, length(Results)),
    [#{<<"id">> := Id}] = Results,
    ?assertEqual(<<"u1">>, Id),

    %% Find all acme users
    {ok, AcmeResults, _} = barrel_docdb:find(DbName, #{
        where => [
            {path, [<<"org">>], <<"acme">>}
        ]
    }),
    ?assertEqual(2, length(AcmeResults)),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% @doc Test explain
api_explain(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"api_explain_test">>,
    TestDir = "/tmp/barrel_api_explain_" ++ integer_to_list(erlang:system_time(millisecond)),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => TestDir}),

    %% Explain a simple query
    {ok, Explanation} = barrel_docdb:explain(DbName, #{
        where => [{path, [<<"type">>], <<"user">>}]
    }),

    ?assert(is_map(Explanation)),
    ?assert(maps:is_key(strategy, Explanation)),
    ?assert(maps:is_key(conditions, Explanation)),
    Strategy = maps:get(strategy, Explanation),
    ?assert(lists:member(Strategy, [index_seek, index_scan, multi_index, full_scan])),

    %% Explain a multi-condition query
    {ok, MultiExplanation} = barrel_docdb:explain(DbName, #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ]
    }),
    MultiStrategy = maps:get(strategy, MultiExplanation),
    ?assert(lists:member(MultiStrategy, [index_seek, index_scan, multi_index, full_scan])),

    %% Explain with limit
    {ok, LimitExplanation} = barrel_docdb:explain(DbName, #{
        where => [{path, [<<"type">>], <<"user">>}],
        limit => 10
    }),
    ?assertEqual(10, maps:get(limit, LimitExplanation, undefined)),

    %% Invalid query returns error
    {error, _} = barrel_docdb:explain(DbName, #{}),

    %% Cleanup
    ok = barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

fold_paths(StoreRef, DbName, PathPrefix) ->
    barrel_ars_index:fold_path(
        StoreRef, DbName, PathPrefix,
        fun({Path, DocId}, Acc) -> {ok, [{Path, DocId} | Acc]} end,
        []
    ).
