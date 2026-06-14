%%%-------------------------------------------------------------------
%%% @doc Database-name validation regression tests
%%%
%%% Verifies that filesystem-traversal and shell-metacharacter names
%%% are rejected before they hit barrel_db_sup:start_db/2 or the
%%% (now-removed) os:cmd-based delete path.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_name_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    reject_traversal/1,
    reject_shell_metachars/1,
    reject_empty/1,
    reject_uppercase/1,
    reject_oversized/1,
    accept_valid_names/1,
    accept_system_prefix/1
]).

all() ->
    [
        reject_traversal,
        reject_shell_metachars,
        reject_empty,
        reject_uppercase,
        reject_oversized,
        accept_valid_names,
        accept_system_prefix
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

reject_traversal(_Config) ->
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"../etc/passwd">>)),
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"..">>)),
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"a/b">>)),
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:delete_db(<<"../etc/passwd">>)),
    ok.

reject_shell_metachars(_Config) ->
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"x; rm -rf /">>)),
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"a`whoami`">>)),
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"a$(id)">>)),
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"a|b">>)),
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"a b">>)),
    ok.

reject_empty(_Config) ->
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<>>)),
    ok.

reject_uppercase(_Config) ->
    %% Lowercase-only to avoid filesystem case-sensitivity surprises.
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(<<"MyDb">>)),
    ok.

reject_oversized(_Config) ->
    Long = binary:copy(<<"a">>, 64),
    ?assertEqual({error, invalid_db_name},
                 barrel_docdb:create_db(Long)),
    ok.

accept_valid_names(_Config) ->
    Names = [<<"mydb">>, <<"my_db">>, <<"my-db">>, <<"db1">>,
             <<"1db">>, binary:copy(<<"a">>, 63)],
    lists:foreach(
      fun(Name) ->
              %% Cleanup any leftover from prior runs.
              _ = barrel_docdb:delete_db(Name),
              ?assertMatch({ok, _}, barrel_docdb:create_db(Name)),
              ok = barrel_docdb:delete_db(Name)
      end, Names),
    ok.

accept_system_prefix(_Config) ->
    %% Internal system DBs use a leading underscore.
    case barrel_docdb:open_db(<<"_barrel_system">>) of
        {ok, _} -> ok;
        {error, not_found} -> ok
    end,
    ok.
