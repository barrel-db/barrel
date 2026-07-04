-module(barrel_channel_tests).

-include_lib("eunit/include/eunit.hrl").

-define(DB, <<"chan_unit_db">>).

compiled(Config) ->
    {ok, Compiled} = barrel_channel:compile(Config),
    Compiled.

%%--------------------------------------------------------------------
%% Config validation
%%--------------------------------------------------------------------

compile_ok_test() ->
    Compiled = compiled(#{<<"acme">> => [<<"org/acme/#">>, <<"type/post">>],
                          <<"users">> => [<<"type/user">>]}),
    ?assertEqual([<<"acme">>, <<"users">>],
                 lists:sort(maps:keys(Compiled))).

compile_rejects_test() ->
    ?assertMatch({error, {invalid_channels, _}},
                 barrel_channel:compile([not_a_map])),
    ?assertMatch({error, {invalid_channel_name, <<>>}},
                 barrel_channel:compile(#{<<>> => [<<"a">>]})),
    ?assertMatch({error, {invalid_channel_name, _}},
                 barrel_channel:compile(#{<<"a", 0, "b">> => [<<"a">>]})),
    ?assertMatch({error, {invalid_channel_name, _}},
                 barrel_channel:compile(#{<<"a", 16#FF>> => [<<"a">>]})),
    ?assertMatch({error, {invalid_channel_patterns, <<"c">>, []}},
                 barrel_channel:compile(#{<<"c">> => []})),
    %% '#' only as the last segment; empty segments rejected
    ?assertMatch({error, {invalid_channel_pattern, _, _}},
                 barrel_channel:compile(#{<<"c">> => [<<"a/#/b">>]})),
    ?assertMatch({error, {invalid_channel_pattern, _, _}},
                 barrel_channel:compile(#{<<"c">> => [<<"a//b">>]})),
    ?assertMatch({error, {invalid_channel_pattern, _, _}},
                 barrel_channel:compile(#{<<"c">> => [<<>>]})).

%%--------------------------------------------------------------------
%% Matcher
%%--------------------------------------------------------------------

match_exact_test() ->
    C = compiled(#{<<"posts">> => [<<"type/post">>]}),
    ?assertEqual([<<"posts">>],
                 barrel_channel:match(C, [<<"type/post">>, <<"rank/3">>])),
    ?assertEqual([], barrel_channel:match(C, [<<"type/user">>])).

match_plus_test() ->
    C = compiled(#{<<"typed">> => [<<"type/+">>]}),
    ?assertEqual([<<"typed">>], barrel_channel:match(C, [<<"type/post">>])),
    %% '+' is exactly one segment
    ?assertEqual([], barrel_channel:match(C, [<<"type/post/extra">>])),
    ?assertEqual([], barrel_channel:match(C, [<<"type">>])).

match_hash_test() ->
    C = compiled(#{<<"acme">> => [<<"org/acme/#">>]}),
    %% MQTT semantics: a trailing '#' also matches the parent itself
    ?assertEqual([<<"acme">>],
                 barrel_channel:match(C, [<<"org/acme/team/1">>])),
    ?assertEqual([<<"acme">>], barrel_channel:match(C, [<<"org/acme">>])),
    ?assertEqual([], barrel_channel:match(C, [<<"org/globex">>])).

match_multi_pattern_test() ->
    C = compiled(#{<<"mixed">> => [<<"type/post">>, <<"pinned/true">>]}),
    ?assertEqual([<<"mixed">>], barrel_channel:match(C, [<<"pinned/true">>])),
    ?assertEqual([<<"mixed">>],
                 barrel_channel:match(C, [<<"type/post">>])),
    %% one row per channel even when both patterns match
    ?assertEqual([<<"mixed">>],
                 barrel_channel:match(C, [<<"type/post">>, <<"pinned/true">>])).

match_empty_config_test() ->
    ?assertEqual([], barrel_channel:match(#{}, [<<"type/post">>])).

%%--------------------------------------------------------------------
%% Row codec
%%--------------------------------------------------------------------

row_codec_test() ->
    Info = #{id => <<"doc1">>, rev => <<"aabb@author">>, deleted => false,
             num_conflicts => 2},
    ?assertEqual(
        #{flag => member, id => <<"doc1">>, rev => <<"aabb@author">>,
          deleted => false, num_conflicts => 2},
        barrel_channel:decode_row(barrel_channel:encode_row(member, Info))),
    ?assertEqual(
        #{flag => leave, id => <<"doc1">>, rev => <<"aabb@author">>,
          deleted => false, num_conflicts => 2},
        barrel_channel:decode_row(barrel_channel:encode_row(leave, Info))),
    DelInfo = Info#{deleted => true},
    ?assertMatch(
        #{flag := member, deleted := true},
        barrel_channel:decode_row(barrel_channel:encode_row(member, DelInfo))).

row_codec_ignores_doc_test() ->
    Info = #{id => <<"a">>, rev => <<"r">>, deleted => false,
             num_conflicts => 0, doc => #{<<"x">> => 1}},
    Row = barrel_channel:decode_row(barrel_channel:encode_row(member, Info)),
    ?assertEqual(<<"a">>, maps:get(id, Row)).

%%--------------------------------------------------------------------
%% write_ops rules (persistent_term-backed)
%%--------------------------------------------------------------------

ops_fixture_test_() ->
    {setup,
     fun() ->
         {ok, Compiled} = barrel_channel:compile(
             #{<<"posts">> => [<<"type/post">>],
               <<"drafts">> => [<<"status/draft">>]}),
         ok = barrel_channel:install(?DB, Compiled)
     end,
     fun(_) -> barrel_channel:uninstall(?DB) end,
     [fun ops_create_member/0,
      fun ops_update_moves_row/0,
      fun ops_transition_writes_leave/0,
      fun ops_delete_lands_tombstone/0,
      fun ops_no_match_no_rows/0,
      fun ops_empty_config_fast_path/0]}.

info() ->
    #{id => <<"d">>, rev => <<"r@a">>, deleted => false, num_conflicts => 0}.

%% pure timestamps: the clock process is not running under eunit
hlc(WallMs) ->
    barrel_hlc:from_wall_time(WallMs).

ops_create_member() ->
    H = hlc(1),
    Ops = barrel_channel:write_ops(?DB, H, info(),
                                   [<<"type/post">>], undefined, []),
    [{put, Key, Value}] = Ops,
    ?assertEqual(barrel_store_keys:channel_key(?DB, <<"posts">>, H), Key),
    ?assertMatch(#{flag := member}, barrel_channel:decode_row(Value)).

%% A rewrite deletes old rows in ALL channels and writes the new member
%% row at the new HLC.
ops_update_moves_row() ->
    Old = hlc(1),
    New = hlc(2),
    Ops = barrel_channel:write_ops(?DB, New, info(),
                                   [<<"type/post">>], Old,
                                   [<<"type/post">>]),
    Deletes = [K || {delete, K} <- Ops],
    ?assertEqual(
        lists:sort([barrel_store_keys:channel_key(?DB, <<"drafts">>, Old),
                    barrel_store_keys:channel_key(?DB, <<"posts">>, Old)]),
        lists:sort(Deletes)),
    [{put, Key, Value}] = [Op || {put, _, _} = Op <- Ops],
    ?assertEqual(barrel_store_keys:channel_key(?DB, <<"posts">>, New), Key),
    ?assertMatch(#{flag := member}, barrel_channel:decode_row(Value)).

%% Leaving a channel writes a leave row there and a member row in the
%% channel it joined.
ops_transition_writes_leave() ->
    Old = hlc(1),
    New = hlc(2),
    Ops = barrel_channel:write_ops(?DB, New, info(),
                                   [<<"status/draft">>], Old,
                                   [<<"type/post">>]),
    Puts = [{barrel_store_keys:decode_channel_key(?DB, K),
             maps:get(flag, barrel_channel:decode_row(V))}
            || {put, K, V} <- Ops],
    ?assertEqual(
        lists:sort([{{<<"drafts">>, New}, member},
                    {{<<"posts">>, New}, leave}]),
        lists:sort(Puts)).

%% A delete lands the tombstone as a member row in the OLD channels.
ops_delete_lands_tombstone() ->
    Old = hlc(1),
    New = hlc(2),
    Ops = barrel_channel:write_ops(?DB, New, (info())#{deleted => true},
                                   [], Old, [<<"type/post">>]),
    [{put, Key, Value}] = [Op || {put, _, _} = Op <- Ops],
    ?assertEqual(barrel_store_keys:channel_key(?DB, <<"posts">>, New), Key),
    ?assertMatch(#{flag := member, deleted := true},
                 barrel_channel:decode_row(Value)).

ops_no_match_no_rows() ->
    Ops = barrel_channel:write_ops(?DB, hlc(1), info(),
                                   [<<"type/user">>], undefined, []),
    ?assertEqual([], Ops).

ops_empty_config_fast_path() ->
    Ops = barrel_channel:write_ops(<<"no_such_db">>, hlc(1), info(),
                                   [<<"type/post">>], undefined, []),
    ?assertEqual([], Ops).
