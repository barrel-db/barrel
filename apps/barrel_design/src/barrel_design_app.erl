%% @private
-module(barrel_design_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    case application:get_env(barrel_design, page_limit, undefined) of
        Pages when is_integer(Pages), Pages > 0 -> ok = wasm_engine:set_page_limit(Pages);
        _ -> ok
    end,
    ok = barrel_design_stats:init(),
    barrel_design_sup:start_link().

stop(_State) ->
    ok.
