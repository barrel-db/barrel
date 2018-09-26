%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Sep 2018 19:09
%%%-------------------------------------------------------------------
-author("benoitc").


-type plugin_type() :: storage | metric.

-type plugin_maturity() :: unknown | experimental | alpha | beta | gamma | stable.

-record(barrel_plugin, {
  type :: plugin_type(),
  name :: string(),
  mod :: atom(),
  author :: string(),
  license :: string(),
  version :: string(),
  maturity :: plugin_maturity()
}).

-define(PLUGINS, barrel_plugins).

-define(DEFAULT_PLUGINS, [
  barrel_rocksdb
]).