%%%-------------------------------------------------------------------
%%% @doc Compiles and runs examples/agent_layer.erl.
%%%
%%% The example is what a reader copies, so a stale one is worse than none.
%%% Running it here pins every call, every return shape, and the order of
%%% the flow against the real API.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_agent_example_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_example_runs/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_example_runs].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_spaces),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_spaces),
    ok.

%% The example lives outside any app's src, so find it from the suite's
%% own location: <root>/apps/barrel_spaces/test -> <root>/examples.
example_path() ->
    Dir = filename:dirname(code:which(?MODULE)),
    Candidates = [filename:join(Root, "examples/agent_layer.erl")
                  || Root <- ancestors(Dir)],
    case [P || P <- Candidates, filelib:is_regular(P)] of
        [Path | _] -> Path;
        [] -> ct:fail({example_not_found, Dir})
    end.

ancestors(Dir) ->
    case filename:dirname(Dir) of
        Dir -> [Dir];
        Parent -> [Dir | ancestors(Parent)]
    end.

t_example_runs(Config) ->
    Src = example_path(),
    Out = ?config(priv_dir, Config),
    {ok, agent_layer} = compile:file(Src, [{outdir, Out}, return_errors]),
    true = code:add_patha(Out),
    {module, agent_layer} = code:ensure_loaded(agent_layer),
    ?assertEqual(ok, agent_layer:run()).
