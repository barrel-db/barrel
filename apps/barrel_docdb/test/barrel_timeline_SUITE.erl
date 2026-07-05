%%%-------------------------------------------------------------------
%%% @doc Timeline: keyspace identity for normal databases. Branch,
%%% lifecycle, and PITR cases join this suite in later steps.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_timeline_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    keyspace_identity/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [keyspace_identity].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_timeline",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    ok = application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(Case, Config) ->
    Db = <<"tl_", (atom_to_binary(Case, utf8))/binary>>,
    {ok, _} = barrel_docdb:create_db(Db, #{
        data_dir => ?config(data_dir, Config)
    }),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

keyspace_identity(Config) ->
    Db = ?config(db, Config),
    {ok, Info} = barrel_docdb:db_info(Db),
    %% a normal database is its own keyspace and has no lineage
    ?assertEqual(Db, maps:get(keyspace, Info)),
    ?assertNot(maps:is_key(parent, Info)),
    ?assertNot(maps:is_key(fork_hlc, Info)),
    %% and nothing is registered in the keyspace registry
    ?assertEqual(Db, barrel_keyspace:resolve(Db)),
    ok.
