%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Nov 2018 10:30
%%%-------------------------------------------------------------------
-module(barrel_tx_SUITE).
-author("benoitc").

%% API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  basic/1
]).

-include_lib("eunit/include/eunit.hrl").


all() ->
  [
    basic
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, _Config) ->
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.

basic(_Config) ->
  Self = self(),
  Pid = spawn(
    fun() ->
      barrel_tx:transact(
        fun() ->
          true = barrel_tx:register_write(a),
          Self ! ok,
          receive
            {done, Pid} -> Pid ! ok
          end
        end
      )
    end
  ),
  receive ok -> ok end,
  2 = ets:info(barrel_tx_uncommited_keys, size),
  0 = ets:info(barrel_tx_commited_keys, size),
  spawn(
    fun() ->
      barrel_tx:transact(
        fun() ->
          Res = barrel_tx:register_write(a),
          Self ! {ok, Res}
        end
      )
    end
  ),
  
  receive
    {ok, false} ->
      Pid ! {done, self()},
      receive ok -> ok end,
      0 = ets:info(barrel_tx_uncommited_keys, size),
      0 = ets:info(barrel_tx_commited_keys, size),
      commited = barrel_tx:transact(fun() ->
                                      true = barrel_tx:register_write(a),
                                      barrel_tx:commit(),
                                      commited
                                    end),
      0 = ets:info(barrel_tx_uncommited_keys, size),
      1 = ets:info(barrel_tx_commited_keys, size),
      [{a, 3}] = ets:tab2list(barrel_tx_commited_keys)
end,
  ok.