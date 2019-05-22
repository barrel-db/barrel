-module(barrel_view_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include("barrel.hrl").
-include("barrel_view.hrl").

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
  jobs:add_queue(barrel_view_queue, [passive]),
  jobs:add_queue(barrel_view_worker_queue,
                 [{standard_counter, barrel_config:get(index_worker_threads)},
                  {producer, fun view_worker/0}]),

  Spec = #{ id => view,
            start => {barrel_view, start_link, []}},
  SupFlags = #{ strategy => simple_one_for_one },
  {ok, {SupFlags, [Spec]}}.



view_worker() ->
  [{_Ts, Job}] = jobs:dequeue(barrel_view_queue, 1),

  {Ref,
   #{ <<"id">> := DocId} = Doc,
   #view{mod=Mod,
         config=Config,
         ref=ViewRef},
   ViewPid} = Job,

 case do_handle_doc(Mod, Doc, Config) of
   {ok, KVs} ->
     Res = (catch ?STORE:update_view_index(ViewRef, DocId, KVs)),
     ViewPid ! {Ref, Res};
   Error ->
     ViewPid ! {Ref, Error}
 end.

do_handle_doc(Mod, Doc, Config) ->
  try
    KVs = Mod:handle_doc(Doc, Config),
    {ok, KVs}
  catch
    C:E:T ->
      ?LOG_DEBUG("view error class=~p, error=~p, traceback=~p~n", [C, E, T]),
      view_error
  end.
