-module(barrel_view_sup).
-behaviour(supervisor).



-export([index_worker/0]).


-export([start_link/0]).
-export([init/1]).

-include("barrel.hrl").

-include("barrel_view.hrl").

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    Spec = #{ id => view,
            start => {barrel_view, start_link, []}},
  SupFlags = #{ strategy => simple_one_for_one },
  {ok, {SupFlags, [Spec]}}.

index_worker() ->
  [{_Ts, Job}] = jobs:dequeue(barrel_index_queue, 1),
   {JobRef, Doc, View, ViewPid} = Job,

  Res = (catch handle_doc(Doc, View)),
  io:format(user, "ref=~p result=~p~n", [JobRef, Res]),
  ViewPid ! {JobRef, Res}.

handle_doc( #{ <<"id">> := DocId } = Doc,
             #view{mod=Mod, config=Config, ref=ViewRef} ) ->
  KVs = Mod:handle_doc(Doc, Config),
  ?STORE:update_view_index(ViewRef, DocId, KVs).
