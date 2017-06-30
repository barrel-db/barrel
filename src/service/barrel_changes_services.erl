%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Jun 2017 08:39
%%%-------------------------------------------------------------------
-module(barrel_changes_services).
-author("benoitc").

%% API
-export([
  execute/4
]).

-export([
  'ChangesSince'/3,
  'ChangesStream'/3
]).

execute(Context, Writer, Method, Args) ->
  _ = lager:info("~s: handle ~p with ~p", [?MODULE_STRING, Method, Args]),
  erlang:apply(
    ?MODULE,
    barrel_lib:to_atom(Method),
    [Context, Writer, Args]
  ).

%% TODO: maybe deprecate ChangesSince ?
'ChangesSince'( #{ stream_id := StreamId}, Writer, [DbId, Since, Options] ) ->
  barrel_db:changes_since(
    DbId,
    Since,
    fun(Change, _Acc) ->
      _ = barrel_rpc:response_stream(Writer, StreamId, Change),
      {ok, nil}
    end,
    nil,
    Options
  ),
  barrel_rpc:response_end_stream(Writer, StreamId);
'ChangesSince'( _, _, _ ) -> erlang:error(badarg).

'ChangesStream'( #{ stream_id := StreamId}, Writer, [DbId, Since0, Options] ) ->
  %% register to the changes
  _ = barrel_event:reg(DbId),
  Since1 = stream_changes(StreamId, Writer, DbId, Since0, Options),
  enter_changes_loop(StreamId, Writer, DbId, Since1, Options).

%% ==============================
%% internal helpers

stream_changes(StreamId, Writer, DbId, Since, Options) ->
  barrel_db:changes_since(
    DbId,
    Since,
    fun(Change, OldSeq) ->
      Seq = maps:get(<<"seq">>, Change),
      _ = barrel_rpc:response_stream(Writer, StreamId, Change),
      {ok, erlang:max(OldSeq, Seq)}
    end,
    Since,
    Options
  ).

enter_changes_loop(StreamId, Writer, DbId, Since0, Options) ->
  receive
    {rpc_data, StreamId, end_stream} ->
      barrel_rpc:response_end_stream(Writer, StreamId);
    {'$barrel_event', _, db_updated} ->
      Since1 = stream_changes(StreamId, Writer, DbId, Since0, Options),
      enter_changes_loop(StreamId, Writer, DbId, Since1, Options)
  end.