-module(barrel_view_adapter).


-export([start_link/1]).
%% gen_statem callbacks
-export([init/1,
         callback_mode/0,
         terminate/3,
         code_change/4]).

%% states
-export([upgrade/3,
         online/3]).

-export([bg_index_loop/4]).

%% should be an option
-define(MAX_BATCH_SIZE, 100).
-define(GC_INTERVAL, 60 * 5 * 1000000). %% we run gc every 5 mn.
-define(CHECKPOINT_INTERVAL, 60 * 1000000).


-include("barrel.hrl").
-include("barrel_logger.hrl").


%% TODO: make opening more robust
start_link( Conf) ->
  gen_server:start_link(?MODULE, Conf, []).


init(#{ barrel := BarrelId,
        view := ViewId,
        mod := ViewMod,
        config := ViewConfig0 }) ->
  process_flag(trap_exit, true),

  {ok, Barrel} = barrel_db:open_barrel(BarrelId),
  {ok, ViewConfig} = ViewMod:init(ViewConfig0),
  Version = ViewMod:version(),
  View0 = case open_view(Barrel, ViewId) of
             {ok, V} ->  V;
             not_found ->
               InitialView = #{ id => ViewId,
                                version => Version,
                                indexed_seq => 0 },
               ok = update_view(Barrel, InitialView),
               InitialView
           end,

  %% init batch server
  BatchServer = gen_batch_server:start_link(
                  barrel_view_writer,
                  [BarrelId, ViewId]
                 ),

  %% initialize gen_statem data
  Data =
    #{ barrel => Barrel,
       view => View0,
       mod => ViewMod,
       batch_server => BatchServer,
       mref => monitor_barrel(Barrel) },


  %% if version of the module changed or an
  %% upgrade was running, got to upgrade state.
  _ = register_view(BarrelId, ViewId, ViewMod, ViewConfig),
  case get_upgrade_task(Barrel, ViewId) of
    {ok, #{ version := UpgradeVersion } = BgState0} ->
      %% an upgrade was already running, try to catch up from there.
      {View, BgState} = if
                          UpgradeVersion /= Version ->
                            %% an upgrade task was running but the version changed in between
                            %% we restart it from 0 to the last indexed ses.
                            View1 = View0#{ version => Version },
                            BgState1 = init_upgrade_task(Barrel, View1),
                            %% make sure to update the view info since its version changed
                            %% we do it after starting the background task
                            ok = update_view(Barrel, View1),
                            {View1, BgState1};
                          true ->
                            {View0, BgState0}
                        end,
      Reindexer =
      spawn_link(?MODULE, bg_index_loop,
                 [Barrel, BatchServer, ViewId, BgState]),
      {ok, upgrade, Data#{ view => View, reindexer => Reindexer}};
    not_found ->
      case should_upgrade(View0, Version) of
        true ->
          View = View0#{ version => Version },
          BgState =  init_upgrade_task(Barrel, View),
          ok = update_view(Barrel, View),
          Reindexer =
          spawn_link(?MODULE, bg_index_loop,
                     [Barrel, BatchServer, ViewId, BgState]),

          {ok, upgrade, Data#{ view => View, reindexer => Reindexer}};

        false ->
          %% trigger refresh
          self() ! refresh_view,
          {ok, online, Data}
      end
  end.



callback_mode() -> state_functions.

terminate(_Reason, _State, #{ barrel := #{ name := Name }, view :=#{ id := Id }}) ->
  io:format("termuinate ~s conf=~p~n", [?MODULE_STRING, [Name, Id]]),
  unregister_view(Name, Id).

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.


monitor_barrel(#{ name := Name }) ->
  Server =  barrel_registry:where_is(Name),
  erlang:monitor(process, Server).


upgrade(info, refresh_view, Data) ->
  {keep_state, refresh_view(Data)}.


online(info, refresh_view, Data) ->
  {keep_state, refresh_view(Data)}.


open_view(#{ store_mod := Store, ref := Ref }, ViewId) ->
  Store:open_view(Ref, ViewId).

update_view(#{ store_mod := Store, ref := Ref }, #{ id := ViewId } = View) ->
  Store:update_view(Ref, ViewId, View).


%% ---------------
%% upgrade

init_upgrade_task(Barrel,
                  #{ id := ViewId,
                     indexed_seq := Seq,
                     version := Version }) ->
  BgState = #{ pos => 0,
               end_at => Seq,
               version => Version },
  ok = put_upgrade_task(Barrel, ViewId, BgState),
  BgState.

put_upgrade_task(#{ store_mod := Store, ref := Ref }, ViewId, Task) ->
  Store:put_view_upgrade_task(Ref, ViewId, Task).


get_upgrade_task(#{ store_mod := Store, ref := Ref}, ViewId) ->
  Store:get_view_upgrade_task(Ref, ViewId).


delete_upgrade_task(#{ store_mod := Store, ref := Ref}, ViewId) ->
  Store:delete_view_upgtade_task(Ref, ViewId).


should_upgrade(#{ version := V}, V) -> false;
should_upgrade(_, _) ->true.


bg_index_loop(Barrel, BatchServer, ViewId,
              #{ pos := Start, end_at := EndAt } = State) ->

  {ok, _, _} = barrel_db:fold_changes(
                 Barrel,  Start,
                 fun
                   (#{ <<"seq">> := Seq, <<"doc">> := Doc }, {Len, Ts}) when Seq < EndAt ->
                     Doc1 = Doc#{ <<"_seq">> => Seq },
                     ok = gen_batch_server:cast(BatchServer, {index_doc, Doc1}),
                     Len1 = Len + 1,
                     if
                       Len1 > ?MAX_BATCH_SIZE ->
                         ok = gen_batch_server:call(BatchServer, wait_commit),
                         ok = put_upgrade_task(Barrel, ViewId, State#{ pos => Seq }),
                         erlang:garbage_collect(),
                         {0, maybe_gc(Ts)};
                       true ->
                         {Len1, maybe_gc(Ts)}
                     end;
                   (_, _) ->
                     stop
                 end,
                 {0, erlang:timestamp()},
                 [#{include_doc => true }]),
  ok = delete_upgrade_task(Barrel, ViewId),
  ok.

refresh_view(#{ barrel := Barrel,
                batch_server := BatchServer,
                view := #{ indexed_seq := Start } = View} = State) ->
  ?LOG_DEBUG("start indexing barrel=~p view=~p~n", [Barrel, View]),
  {ok, {NState, _}, _} = barrel_db:fold_changes(
                                 Barrel, Start,
                                 fun(#{ <<"seq">> := Seq, <<"doc">> := Doc }, {State1, Ts}) ->
                                     Doc1 = Doc#{ <<"_seq">> => Seq },
                                     ok = gen_batch_server:cast(BatchServer, {index_doc, Doc1}),
                                     maybe_checkpoint(State1, Seq, Ts)
                                 end,
                                 {State, erlang:timestamp()},
                                 [#{ include_doc => true }]),
  ?LOG_DEBUG("end indexing barrel=~p view=~p~n", [Barrel, maps:get(view, NState)]),
  NState.


maybe_gc(Ts)  ->
  Now = erlang:timestamp(),
  Diff = timer:now_diff(Now, Ts),
  if
    Diff > ?GC_INTERVAL ->
      _ = erlang:garbage_collect(),
      Now;
    true ->
      Ts
  end.


maybe_checkpoint(#{ barrel := Barrel,
                    view := View,
                    batch_server := BatchServer } = State, Seq, Ts) ->
  Now = erlang:timestamp(),
  Diff = timer:now_diff(Now, Ts),
  if
    Diff > ?CHECKPOINT_INTERVAL ->
      ok = gen_batch_server:call(BatchServer, wait_commit),
      View1 = View#{ indexed_seq => Seq },
      ok = update_view(Barrel, View1),
      {State#{ view => View1 }, Now};
    true ->
      {State, Ts}
  end.



register_view(Barrel, View, ViewMod, ViewConfig) ->
  ets:insert(?VIEWS, [{{Barrel, View}, {ViewMod, ViewConfig}}]).

unregister_view(Barrel, View) ->
  ets:delete(?VIEWS, {Barrel, View}).
