%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Sep 2018 20:47
%%%-------------------------------------------------------------------
-module(barrel_plugins).
-author("benoitc").

%% API
-export([
  available/0,
  by_type/1,
  init/0,
  i/0
]).

-include_lib("stdlib/include/ms_transform.hrl").
-include("plugin.hrl").

available() ->
  AppPlugins = application:get_env(barrel, plugins, []),
  ?DEFAULT_PLUGINS ++ AppPlugins.


by_type(Type) ->
  MS = ets:fun2ms(fun(#barrel_plugin{type=T}=Plugin) when T =:= Type -> Plugin end),
  ets:select(?PLUGINS, MS).


init() ->
  _ = ets:new(?PLUGINS, [named_table, ordered_set, public, {keypos, #barrel_plugin.mod}]),
  lists:foreach(
    fun(Mod) ->
      _ = code:ensure_loaded(Mod),
      case erlang:function_exported(Mod, barrel_declare_plugin, 0) of
        false ->
          lager:warning("plugin ~p can't be loaded~n", [Mod]),
          ok;
        true ->
          Spec = Mod:barrel_declare_plugin(),
          true = ets:insert_new(?PLUGINS, Spec)
      end
    end,
    available()
  ).

i() ->
  hform('type', 'name', 'mod', 'version', 'maturity'),
  io:format(" -------------------------------------"
            "---------------------------------------\n"),
  Plugins = ets:tab2list(?PLUGINS),
  lists:foreach(fun prinfo/1, Plugins).

prinfo(Plugin) ->
  #barrel_plugin{type = Type,
                 name = Name,
                 mod  = Mod,
                 version = Version,
                 maturity = Maturity} = Plugin,
  hform(Type, Name, Mod, Version, Maturity).

hform(A0, B0, C0, D0, E0) ->
  [A,B,C,D,E] = [to_string(T) || T <- [A0,B0,C0,D0,E0]],
  A1 = pad_right(A, 15),
  B1 = pad_right(B, 17),
  C1 = pad_right(C, 17),
  D1 = pad_right(D, 8),
  E1 = pad_right(E, 10),
  %% no need to pad the last entry on the line
  io:format(" ~s ~s ~s ~s ~s\n", [A1,B1,C1,D1,E1]).

pad_right(String, Len) ->
  if
    length(String) >= Len ->
      String;
    true ->
      [Space] = " ",
      String ++ lists:duplicate(Len - length(String), Space)
  end.

to_string(X) ->
  lists:flatten(io_lib:format("~p", [X])).