%% @private
-module(barrel_design_app).
-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

start(_Type, _Args) ->
    case application:get_env(barrel_design, page_limit, undefined) of
        Pages when is_integer(Pages), Pages > 0 -> ok = wasm_engine:set_page_limit(Pages);
        _ -> ok
    end,
    ok = barrel_design_stats:init(),
    case barrel_design_sup:start_link() of
        {ok, _} = Ok ->
            ok = barrel_bql:register_source(<<"view">>, barrel_design_bql),
            Ok;
        Error ->
            Error
    end.

prep_stop(State) ->
    ok = barrel_bql:unregister_source(<<"view">>),
    State.

stop(_State) ->
    ok.
