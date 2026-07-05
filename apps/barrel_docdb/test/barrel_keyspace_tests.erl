%%%-------------------------------------------------------------------
%%% @doc Keyspace registry and TIMELINE sidecar codec.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_keyspace_tests).

-include_lib("eunit/include/eunit.hrl").

resolve_default_test() ->
    ?assertEqual(<<"plain">>, barrel_keyspace:resolve(<<"plain">>)).

install_resolve_uninstall_test() ->
    ok = barrel_keyspace:install(<<"branch1">>, <<"parent1">>),
    ?assertEqual(<<"parent1">>, barrel_keyspace:resolve(<<"branch1">>)),
    %% idempotent under the linear-lineage invariant: resolving an
    %% already-resolved name is identity
    ?assertEqual(<<"parent1">>,
                 barrel_keyspace:resolve(
                     barrel_keyspace:resolve(<<"branch1">>))),
    ok = barrel_keyspace:uninstall(<<"branch1">>),
    ?assertEqual(<<"branch1">>, barrel_keyspace:resolve(<<"branch1">>)).

uninstall_absent_test() ->
    ?assertEqual(ok, barrel_keyspace:uninstall(<<"never-installed">>)).

meta_round_trip_test() ->
    Dir = tmp_dir("meta_rt"),
    Hlc = barrel_hlc:encode(barrel_hlc:from_wall_time(1234567)),
    Meta = #{keyspace => <<"parent1">>, parent => <<"parent1">>,
             fork_hlc => Hlc},
    ok = barrel_keyspace:write_meta(Dir, Meta),
    ?assertEqual({ok, Meta}, barrel_keyspace:read_meta(Dir)),
    %% never leaves a temp file behind
    ?assertEqual({ok, ["TIMELINE"]}, file:list_dir(Dir)).

meta_absent_test() ->
    Dir = tmp_dir("meta_absent"),
    ?assertEqual(not_found, barrel_keyspace:read_meta(Dir)).

meta_corrupt_test() ->
    Dir = tmp_dir("meta_corrupt"),
    File = filename:join(Dir, "TIMELINE"),
    ok = file:write_file(File, <<"{not, a, map}.">>),
    ?assertEqual({error, corrupt_timeline_meta},
                 barrel_keyspace:read_meta(Dir)),
    %% wrong shapes are rejected too
    ok = file:write_file(File, <<"#{keyspace => <<\"k\">>}.">>),
    ?assertEqual({error, corrupt_timeline_meta},
                 barrel_keyspace:read_meta(Dir)),
    %% non-erlang garbage
    ok = file:write_file(File, <<0, 255, 1, 2>>),
    ?assertEqual({error, corrupt_timeline_meta},
                 barrel_keyspace:read_meta(Dir)).

tmp_dir(Suffix) ->
    Dir = filename:join("/tmp",
                        "barrel_keyspace_" ++ Suffix ++ "_"
                        ++ os:getpid()),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_path(Dir),
    Dir.
