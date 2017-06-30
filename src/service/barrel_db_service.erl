%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jun 2017 22:38
%%%-------------------------------------------------------------------
-module(barrel_db_service).
-author("benoitc").

%% API
-export([
  execute/4
]).

-export([
  'CreateDatabase'/3,
  'DatabaseInfos'/3,
  'DeleteDatabase'/3,
  'DatabaseNames'/3
]).

-export([
  'GetDoc'/3,
  'PutDoc'/3,
  'DeleteDoc'/3,
  'InsertDoc'/3,
  'PutRev'/3,
  'MultiGetDoc'/3,
  'WriteBatch'/3,
  'FoldById'/3
]).

-export([
  'PutSystemDoc'/3,
  'GetSystemDoc'/3,
  'DeleteSystemDoc'/3
]).

execute(Context, Writer, Method, Args) ->
  _ = lager:info("~s: handle ~p with ~p", [?MODULE_STRING, Method, Args]),
  erlang:apply(
    ?MODULE,
    barrel_lib:to_atom(Method),
    [Context, Writer, Args]
  ).

%% ==============================
%% DATABASE

'DatabaseNames'( _Context, _Writer, _Args ) ->
  barrel_store:databases().

'CreateDatabase'( _Context, _Writer, DbConfig ) when is_map(DbConfig)->
  barrel_store:create_db(DbConfig);
'CreateDatabase'( _, _, _ ) -> erlang:error(badarg).

'DeleteDatabase'( _Context, _Writer, [DbId] ) ->
  barrel_store:delete_db(DbId);
'DeleteDatabase'( _, _, _) -> erlang:error(badarg).

'DatabaseInfos'(  _Context, _Writer, [DbId] ) ->
  barrel_db:infos(DbId);
'DatabaseInfos'( _, _, _) -> erlang:error(badarg).

%% ==============================
%% DOCS

%% TODO: use maps has option so we don't have to transform them
'GetDoc'( _Context, _Writer, [DbId, DocId, Options] ) ->
  barrel_db:get(DbId, DocId, Options).

'PutDoc'( _Context, _Writer, [DbId, Doc, Options] ) ->
  Rev = maps:get(rev, Options, <<>>),
  Async = maps:get(async, Options, false),
  
  Batch = barrel_write_batch:put(Doc, Rev, barrel_write_batch:new(Async)),
  update_doc(DbId, Batch);
'PutDoc'( _, _, _ ) -> erlang:error(badarg).

'DeleteDoc'( _Context, _Writer, [DbId, DocId, Options] ) ->
  Rev = maps:get(rev, Options, <<>>),
  Async = maps:get(async, Options, false),
  Batch = barrel_write_batch:delete(DocId, Rev, barrel_write_batch:new(Async)),
  update_doc(DbId, Batch);
'DeleteDoc'( _, _, _ ) -> erlang:error(badarg).

'InsertDoc'( _Context, _Writer, [DbId, Doc, Options] ) ->
  Async = maps:get(async, Options, false),
  Upsert = maps:get(is_upsert, Options, false),
  Batch = barrel_write_batch:post(Doc, Upsert, barrel_write_batch:new(Async)),
  update_doc(DbId, Batch);
'InsertDoc'( _, _, _ ) -> erlang:error(badarg).

'PutRev'( _Context, _Writer, [DbId, Doc, History, Deleted, Options] ) ->
  Async = maps:get(async, Options, false),
  Batch = barrel_write_batch:put_rev(Doc, History, Deleted, barrel_write_batch:new(Async)),
  update_doc(DbId, Batch);
'PutRev'( _, _, _ ) -> erlang:error(badarg).

'MultiGetDoc'( #{ stream_id := StreamId}, Writer, [DbId, DocIds, Options ]) ->
  barrel_db:multi_get(
    DbId,
    fun(Doc, Meta, _Acc) ->
      barrel_rpc:response_stream(Writer, StreamId, {Doc, Meta})
    end,
    ok,
    DocIds,
    Options
  ),
  barrel_rpc:response_end_stream(Writer, StreamId);
'MultiGetDoc'( _, _, _ ) -> erlang:error(badarg).

'WriteBatch'( #{ stream_id := StreamId}, Writer, [DbId, Options] ) ->
  Async = maps:get(async, Options, false),
  Batch = wait_batch_stream(StreamId, barrel_write_batch:new(Async)),
  case  barrel_db:update_docs(DbId, Batch) of
    ok -> ok;
    Results ->
      send_batch_stream(Results, StreamId, Writer)
  end.

'FoldById'( #{ stream_id := StreamId}, Writer, [DbId, Options] ) ->
  barrel_db:fold_by_id(
    DbId,
    fun(Doc, Meta, _Acc) ->
      _ = barrel_rpc:response_stream(Writer, StreamId, {Doc, Meta}),
      {ok, nil}
    end,
    nil,
    Options
  ),
  barrel_rpc:response_end_stream(Writer, StreamId);
'FoldById'( _, _, _ ) -> erlang:error(badarg).


%% ==============================
%% SYSTEM DOCS

'PutSystemDoc'( _Context, _Writer, [DbId, DocId, Doc] ) ->
  barrel_db:put_system_doc(DbId, DocId, Doc);
'PutSystemDoc'( _, _, _ ) -> erlang:error(badarg).

'GetSystemDoc'( _Context, _Writer, [DbId, DocId] ) ->
  barrel_db:get_system_doc(DbId, DocId);
'GetSystemDoc'( _, _, _ ) -> erlang:error(badarg).

'DeleteSystemDoc'( _Context, _Writer, [DbId, DocId] ) ->
  barrel_db:delete_system_doc(DbId, DocId);
'DeleteSystemDoc'( _, _, _ ) -> erlang:error(badarg).


%% ==============================
%% internal helpers

update_doc(Db, Batch) ->
  Result = barrel_db:update_docs(Db, Batch),
  case Result of
    ok -> ok;
    [Res] -> Res
  end.

%% TODO: add deadline
wait_batch_stream(StreamId, Batch0) ->
  receive
    {rpc_stream, StreamId, {data, end_batch}} ->
      Batch0;
    {rpc_stream, StreamId, {data, Op}} ->
      Batch1 = barrel_write_batch:add_op(Op, Batch0),
      wait_batch_stream(StreamId, Batch1)
  end.

send_batch_stream([Result | Rest], StreamId, Writer) ->
  barrel_rpc:response_stream(Writer, StreamId,Result),
  send_batch_stream(Rest, StreamId, Writer);
send_batch_stream([], StreamId, Writer) ->
  barrel_rpc:response_end_stream(Writer, StreamId).
