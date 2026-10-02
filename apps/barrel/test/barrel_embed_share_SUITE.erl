%%%-------------------------------------------------------------------
%%% @doc Record-mode databases on the same local model share one model
%%% process, which stops when the last of them closes.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_share_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_same_model_one_process/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_same_model_one_process].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    ok.

t_same_model_one_process(Config) ->
    ok = meck:new(barrel_embed_port_server, [passthrough, no_link]),
    meck:expect(barrel_embed_port_server, start_link,
                fun(_Python, _Args, _Opts) ->
                    {ok, spawn_link(fun() -> receive stop -> ok end end)}
                end),
    meck:expect(barrel_embed_port_server, info,
                fun(_Server, _Timeout) -> {ok, #{dimensions => 3}} end),
    try
        Open = fun(Name) ->
            barrel:open(Name, #{
                embedding => #{fields => [<<"text">>],
                               embedder => {local, #{model => <<"m">>}}},
                vectordb => #{dimension => 3,
                              db_path => filename:join(?config(priv_dir, Config),
                                                       binary_to_list(Name))}})
        end,
        {ok, A} = Open(<<"share_a">>),
        {ok, B} = Open(<<"share_b">>),
        Server = server(A),
        ?assertEqual(Server, server(B)),
        ?assertEqual(1, meck:num_calls(barrel_embed_port_server, start_link, '_')),
        MRef = monitor(process, Server),
        ok = barrel:close(A),
        ?assert(is_process_alive(Server)),
        ok = barrel:close(B),
        receive {'DOWN', MRef, process, Server, _} -> ok
        after 5000 -> ct:fail(model_process_still_running)
        end
    after
        meck:unload(barrel_embed_port_server)
    end.

server(#{embed := #{providers := [{_Module, #{server := Server}}]}}) ->
    Server.
