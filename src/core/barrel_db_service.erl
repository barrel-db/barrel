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
  'InsertDoc'/3
]).

execute(Context, Writer, Method, Args) ->
  _ = lager:info("~s: handle ~p with ~p", [?MODULE_STRING, Method, Args]),
  erlang:apply(
    ?MODULE,
    barrel_lib:to_atom(Method),
    [Context, Writer, Args]
  ).

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

update_doc(Db, Batch) ->
  Result = barrel_db:update_docs(Db, Batch),
  case Result of
    ok -> ok;
    [Res] -> Res
  end.