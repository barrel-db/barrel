%%%-------------------------------------------------------------------
%%% @doc delete_db locates a closed database's files, including one
%%% created with a per-db data_dir (issue #3).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_delete_db_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_delete_closed_per_db_data_dir/1,
    t_delete_default_idempotent/1,
    t_delete_with_explicit_data_dir/1,
    t_delete_open_db/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_delete_closed_per_db_data_dir, t_delete_default_idempotent,
     t_delete_with_explicit_data_dir, t_delete_open_db].

init_per_suite(Config) ->
    application:load(barrel_docdb),
    Default = filename:join(?config(priv_dir, Config), "default"),
    application:set_env(barrel_docdb, data_dir, Default),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_docdb),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% The repro from #3: a closed db created with a per-db data_dir is
%% deleted by delete_db/1 (no data_dir) and its files are gone.
t_delete_closed_per_db_data_dir(Config) ->
    PerDb = perdir(Config, "perdb1"),
    Name = <<"mydb">>,
    {ok, _} = barrel_docdb:create_db(Name, #{data_dir => PerDb}),
    {ok, _} = barrel_docdb:put_doc(Name, #{<<"id">> => <<"doc1">>}),
    DbPath = filename:join(PerDb, binary_to_list(Name)),
    ?assert(filelib:is_dir(DbPath)),
    ok = barrel_docdb:close_db(Name),
    ok = barrel_docdb:delete_db(Name),
    ?assertNot(filelib:is_dir(DbPath)),
    ok.

%% A default-data_dir db stays idempotent: delete twice, both ok.
t_delete_default_idempotent(_Config) ->
    Name = <<"defdb">>,
    {ok, _} = barrel_docdb:create_db(Name),
    ok = barrel_docdb:close_db(Name),
    ok = barrel_docdb:delete_db(Name),
    ok = barrel_docdb:delete_db(Name),
    ok.

%% The explicit-data_dir workaround still works for a closed db.
t_delete_with_explicit_data_dir(Config) ->
    PerDb = perdir(Config, "perdb2"),
    Name = <<"exdb">>,
    {ok, _} = barrel_docdb:create_db(Name, #{data_dir => PerDb}),
    ok = barrel_docdb:close_db(Name),
    DbPath = filename:join(PerDb, binary_to_list(Name)),
    ?assert(filelib:is_dir(DbPath)),
    ok = barrel_docdb:delete_db(Name, #{data_dir => PerDb}),
    ?assertNot(filelib:is_dir(DbPath)),
    ok.

%% Deleting an OPEN per-db db still removes the files (actual path).
t_delete_open_db(Config) ->
    PerDb = perdir(Config, "perdb3"),
    Name = <<"opendb">>,
    {ok, _} = barrel_docdb:create_db(Name, #{data_dir => PerDb}),
    DbPath = filename:join(PerDb, binary_to_list(Name)),
    ?assert(filelib:is_dir(DbPath)),
    ok = barrel_docdb:delete_db(Name),
    ?assertNot(filelib:is_dir(DbPath)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

perdir(Config, Sub) ->
    filename:join(?config(priv_dir, Config), Sub).
