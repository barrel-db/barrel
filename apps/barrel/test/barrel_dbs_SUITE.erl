%%%-------------------------------------------------------------------
%%% @doc Lifecycle manager tests: lazy opens with touch-on-use, idle
%%% closing, LRU eviction under a cap, pinning, crash recovery, and
%%% record-mode teardown. Sweeps are driven through the sweep/0 hook,
%%% not wall-clock timers.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_dbs_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_idle_close/1,
    t_touch_resets/1,
    t_pin_blocks/1,
    t_lru_eviction/1,
    t_all_pinned_errors/1,
    t_reopen_preserves_data/1,
    t_crash_restart/1,
    t_record_close_stops_indexer/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_idle_close, t_touch_resets, t_pin_blocks, t_lru_eviction,
     t_all_pinned_errors, t_reopen_preserves_data, t_crash_restart,
     t_record_close_stops_indexer].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    reset_env(),
    ok.

init_per_testcase(_Case, Config) ->
    reset_env(),
    Config.

end_per_testcase(_Case, _Config) ->
    lists:foreach(fun barrel_dbs:close/1, barrel_dbs:list()),
    reset_env(),
    ok.

reset_env() ->
    application:unset_env(barrel, dbs_idle_timeout),
    application:unset_env(barrel, dbs_max_open),
    application:unset_env(barrel, dbs_evict_guard).

open_opts(Name, Config) ->
    Priv = ?config(priv_dir, Config),
    #{vectordb => #{dimension => 3,
                    db_path => filename:join(Priv,
                                             binary_to_list(Name) ++ "_vec"),
                    bm25_backend => memory}}.

uname(Prefix) ->
    <<Prefix/binary, "_",
      (integer_to_binary(erlang:unique_integer([positive])))/binary>>.

%%====================================================================
%% Cases
%%====================================================================

t_idle_close(Config) ->
    Name = uname(<<"idle">>),
    application:set_env(barrel, dbs_idle_timeout, 30),
    {ok, _} = barrel_dbs:ensure(Name, open_opts(Name, Config)),
    ?assertEqual([Name], barrel_dbs:list()),
    timer:sleep(60),
    ok = barrel_dbs:sweep(),
    ?assertEqual([], barrel_dbs:list()),
    %% the docdb really closed
    ?assertMatch({error, not_found}, barrel_docdb:open_db(Name)),
    ok.

t_touch_resets(Config) ->
    Name = uname(<<"touch">>),
    application:set_env(barrel, dbs_idle_timeout, 100),
    {ok, _} = barrel_dbs:ensure(Name, open_opts(Name, Config)),
    timer:sleep(60),
    %% a use inside the window resets last_used
    {ok, _} = barrel_dbs:ensure(Name),
    timer:sleep(60),
    ok = barrel_dbs:sweep(),
    ?assertEqual([Name], barrel_dbs:list()),
    %% and past the full window it closes
    timer:sleep(120),
    ok = barrel_dbs:sweep(),
    ?assertEqual([], barrel_dbs:list()),
    ok.

t_pin_blocks(Config) ->
    Name = uname(<<"pin">>),
    application:set_env(barrel, dbs_idle_timeout, 30),
    {ok, _} = barrel_dbs:ensure(Name, open_opts(Name, Config)),
    ok = barrel_dbs:pin(Name),
    timer:sleep(60),
    ok = barrel_dbs:sweep(),
    ?assertEqual([Name], barrel_dbs:list()),
    ok = barrel_dbs:unpin(Name),
    timer:sleep(60),
    ok = barrel_dbs:sweep(),
    ?assertEqual([], barrel_dbs:list()),
    ?assertEqual({error, not_open}, barrel_dbs:pin(Name)),
    ok.

t_lru_eviction(Config) ->
    A = uname(<<"lru_a">>),
    B = uname(<<"lru_b">>),
    C = uname(<<"lru_c">>),
    application:set_env(barrel, dbs_max_open, 2),
    application:set_env(barrel, dbs_evict_guard, 0),
    {ok, _} = barrel_dbs:ensure(A, open_opts(A, Config)),
    timer:sleep(5),
    {ok, _} = barrel_dbs:ensure(B, open_opts(B, Config)),
    timer:sleep(5),
    %% touching A makes B the LRU
    {ok, _} = barrel_dbs:ensure(A),
    timer:sleep(5),
    {ok, _} = barrel_dbs:ensure(C, open_opts(C, Config)),
    Open = lists:sort(barrel_dbs:list()),
    ?assertEqual(lists:sort([A, C]), Open),
    ?assertMatch({error, not_found}, barrel_docdb:open_db(B)),
    ok.

t_all_pinned_errors(Config) ->
    A = uname(<<"cap_a">>),
    B = uname(<<"cap_b">>),
    C = uname(<<"cap_c">>),
    application:set_env(barrel, dbs_max_open, 2),
    application:set_env(barrel, dbs_evict_guard, 0),
    {ok, _} = barrel_dbs:ensure(A, open_opts(A, Config)),
    {ok, _} = barrel_dbs:ensure(B, open_opts(B, Config)),
    ok = barrel_dbs:pin(A),
    ok = barrel_dbs:pin(B),
    ?assertEqual({error, too_many_open_dbs},
                 barrel_dbs:ensure(C, open_opts(C, Config))),
    %% an already-open db is still reachable at the cap
    {ok, _} = barrel_dbs:ensure(A),
    ok.

t_reopen_preserves_data(Config) ->
    Name = uname(<<"data">>),
    application:set_env(barrel, dbs_idle_timeout, 30),
    Opts = open_opts(Name, Config),
    {ok, Db} = barrel_dbs:ensure(Name, Opts),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    timer:sleep(60),
    ok = barrel_dbs:sweep(),
    ?assertEqual([], barrel_dbs:list()),
    {ok, Db2} = barrel_dbs:ensure(Name, Opts),
    {ok, #{<<"v">> := 1}} = barrel:get_doc(Db2, <<"a">>),
    ok.

t_crash_restart(Config) ->
    Name = uname(<<"crash">>),
    Opts = open_opts(Name, Config),
    {ok, Db} = barrel_dbs:ensure(Name, Opts),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    Pid0 = erlang:whereis(barrel_dbs),
    exit(Pid0, kill),
    ok = wait_until(fun() ->
        case erlang:whereis(barrel_dbs) of
            undefined -> false;
            Pid -> Pid =/= Pid0
        end
    end),
    %% fresh cache: the db reopens lazily and the data is there
    {ok, Db2} = barrel_dbs:ensure(Name, Opts),
    {ok, _} = barrel:get_doc(Db2, <<"a">>),
    ok.

t_record_close_stops_indexer(Config) ->
    Name = uname(<<"rec">>),
    mock_embed(),
    try
        Opts = (open_opts(Name, Config))#{
            embedding => #{fields => [<<"title">>]}},
        {ok, _} = barrel_dbs:ensure(Name, Opts),
        ?assert(is_pid(barrel_record_indexer:whereis_pid(Name))),
        ok = barrel_dbs:close(Name),
        ?assertEqual(undefined, barrel_record_indexer:whereis_pid(Name))
    after
        unmock_embed()
    end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

wait_until(Fun) ->
    wait_until(Fun, 100).

wait_until(_Fun, 0) ->
    ct:fail(condition_never_met);
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Fun, N - 1)
    end.

mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed, fun(Text, _State) ->
        {ok, mock_vec(Text)}
    end),
    meck:expect(barrel_embed, embed_batch, fun(Texts, _State) ->
        {ok, [mock_vec(T) || T <- Texts]}
    end).

unmock_embed() ->
    try meck:unload(barrel_embed) catch _:_ -> ok end,
    ok.

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].
