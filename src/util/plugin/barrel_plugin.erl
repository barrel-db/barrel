%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Sep 2018 19:13
%%%-------------------------------------------------------------------
-module(barrel_plugin).
-author("benoitc").

-include("plugin.hrl").

-type barrel_plugin() :: #barrel_plugin{}.


-export_types([
  plugin_type/0,
  plugin_maturity/0,
  barrel_plugin/0
]).


-callback barrel_declare_plugin() -> barrel_plugin().