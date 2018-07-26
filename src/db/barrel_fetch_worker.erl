%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jul 2018 21:21
%%%-------------------------------------------------------------------
-module(barrel_fetch_worker).
-author("benoitc").

%% API
-export([]).

%% API
-export([
  init/1,
  handle_call/3,
  handle_cast/2
]).

init([]) ->
  {ok, #{}}.


handle_call(_Msg, _From, St) -> {reply, ok, St}.

handle_cast({fetch_doc, From, Db, DocId, Options}, St) ->
  Reply = barrel_db:fetch_doc(Db, DocId, Options),
  reply(From, Reply),
  {noreply, St}.

reply({Pid, Ref}, Reply) ->
  Pid ! {Ref, Reply}.
  