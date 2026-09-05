%%%-------------------------------------------------------------------
%%% @doc Coverage for `att_opts => #{backend => none}' (barrel_att_store_none):
%%% a database created with it must not materialize an attachments/
%%% RocksDB instance on disk, must still work normally for its actual
%%% (local-doc) workload, and must fail attachment operations cleanly
%%% rather than crash.
%%%
%%% Motivated by a real deployment measurement (hecate-agora,
%%% 2026-09-05): barrel_docdb.erl's `_barrel_system' and
%%% barrel_rep_tasks.erl's `_replication_tasks' only ever call
%%% put_local_doc/3, yet each was paying for a full second RocksDB
%%% instance (~74MB fixed WAL+MANIFEST preallocation, empty or not) for
%%% an attachment feature neither can structurally use.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store_none_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([no_attachments_directory_created/1,
         docs_directory_still_created/1,
         local_docs_still_work/1,
         put_attachment_returns_clean_error/1,
         get_attachment_reports_not_found_not_error/1,
         default_backend_still_creates_attachments_directory/1]).

all() ->
    [no_attachments_directory_created,
     docs_directory_still_created,
     local_docs_still_work,
     put_attachment_returns_clean_error,
     get_attachment_reports_not_found_not_error,
     default_backend_still_creates_attachments_directory].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_att_store_none_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

%% Options mirroring what barrel_docdb.erl's SYSTEM_DB_OPTS and
%% barrel_rep_tasks.erl's TASKS_DB_OPTS actually pass -- a database
%% created for local-doc-only, low-volume use.
none_opts() ->
    #{att_opts => #{backend => none},
      store_opts => #{write_buffer_size => 4 * 1024 * 1024}}.

init_per_testcase(TC, Config) ->
    Name = atom_to_binary(TC, utf8),
    Dir = ?config(dir, Config),
    DbPath = filename:join(Dir, binary_to_list(Name)),
    [{name, Name}, {db_path, DbPath} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(name, Config)) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Cases
%%====================================================================

no_attachments_directory_created(Config) ->
    Name = ?config(name, Config),
    Dir = ?config(dir, Config),
    DbPath = ?config(db_path, Config),
    {ok, _} = barrel_docdb:create_db(Name, (none_opts())#{data_dir => Dir}),
    ?assertNot(filelib:is_dir(filename:join(DbPath, "attachments"))),
    ok.

docs_directory_still_created(Config) ->
    Name = ?config(name, Config),
    Dir = ?config(dir, Config),
    DbPath = ?config(db_path, Config),
    {ok, _} = barrel_docdb:create_db(Name, (none_opts())#{data_dir => Dir}),
    ?assert(filelib:is_dir(filename:join(DbPath, "docs"))),
    ok.

local_docs_still_work(Config) ->
    Name = ?config(name, Config),
    Dir = ?config(dir, Config),
    {ok, _} = barrel_docdb:create_db(Name, (none_opts())#{data_dir => Dir}),
    ok = barrel_docdb:put_local_doc(Name, <<"task_1">>, #{<<"status">> => <<"pending">>}),
    ?assertEqual({ok, #{<<"status">> => <<"pending">>}},
                 barrel_docdb:get_local_doc(Name, <<"task_1">>)),
    ok.

put_attachment_returns_clean_error(Config) ->
    Name = ?config(name, Config),
    Dir = ?config(dir, Config),
    {ok, _} = barrel_docdb:create_db(Name, (none_opts())#{data_dir => Dir}),
    ?assertEqual({error, attachments_disabled},
                 barrel_docdb:put_attachment(Name, <<"doc1">>, <<"a.txt">>, <<"hello">>)),
    ok.

get_attachment_reports_not_found_not_error(Config) ->
    Name = ?config(name, Config),
    Dir = ?config(dir, Config),
    {ok, _} = barrel_docdb:create_db(Name, (none_opts())#{data_dir => Dir}),
    %% Same answer a real backend gives for a document that happens to
    %% have zero attachments -- a caller that never writes one cannot
    %% tell the difference from reads alone.
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Name, <<"doc1">>, <<"a.txt">>)),
    ok.

%% Control case: confirms the default backend (unaffected by this
%% change) still behaves as before -- the absence of an attachments/
%% directory in the other tests is because of `backend => none`, not a
%% regression in database creation generally.
default_backend_still_creates_attachments_directory(Config) ->
    Name = ?config(name, Config),
    Dir = ?config(dir, Config),
    DbPath = ?config(db_path, Config),
    {ok, _} = barrel_docdb:create_db(Name, #{data_dir => Dir}),
    ?assert(filelib:is_dir(filename:join(DbPath, "attachments"))),
    ok.
