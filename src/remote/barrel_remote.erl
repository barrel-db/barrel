%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Jun 2017 15:31
%%%-------------------------------------------------------------------
-module(barrel_remote).
-author("benoitc").

%% API
-export([
  start_channel/1,
  close_channel/1, close_channel/2
]).

-export([
  database_names/1,
  create_database/2,
  delete_database/2,
  database_infos/2
]).

-export([
  get/4,
  put/4,
  delete/4,
  post/4,
  multi_get/6,
  put_rev/6,
  write_batch/4,
  fold_by_id/5,
  revsdiff/4
]).

-export([
  put_system_doc/4,
  get_system_doc/3,
  delete_system_doc/3
]).

-export([
  changes_since/6,
  subscribe_changes/4,
  await_change/2, await_change/3,
  unsubscribe_changes/2
]).

start_channel(Params) ->
  barrel_rpc:start_channel(Params).

close_channel(ChPid) ->
  barrel_rpc:close_channel(ChPid).

close_channel(ChPid, Timeout) ->
  barrel_rpc:close_channel(ChPid, Timeout).

%% ==============================
%% database operations

database_names(ChPid) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'DatabaseNames', []}),
  barrel_rpc:await(ChPid, Ref).

create_database(ChPid, Config) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'CreateDatabase', Config}),
  barrel_rpc:await(ChPid, Ref).

delete_database(ChPid, DbId) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'DeleteDatabase', [DbId]}),
  barrel_rpc:await(ChPid, Ref).

database_infos(ChPid, DbId) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'DatabaseInfos', [DbId]}),
  barrel_rpc:await(ChPid, Ref).


%% ==============================
%% doc operations

get(ChPid, DbId, DocId, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'GetDoc', [DbId, DocId, Options]}),
  barrel_rpc:await(ChPid, Ref).

put(ChPid, DbId, Doc, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'PutDoc', [DbId, Doc, Options]}),
  barrel_rpc:await(ChPid, Ref).

delete(ChPid, DbId, DocId, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'DeleteDoc', [DbId, DocId, Options]}),
  barrel_rpc:await(ChPid, Ref).

post(ChPid, DbId, Doc, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'InsertDoc', [DbId, Doc, Options]}),
  barrel_rpc:await(ChPid, Ref).

multi_get(ChPid, DbId, Fun, Acc, DocIds, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'MultiGetDoc', [DbId, DocIds, Options]}),
  do_fold(ChPid, Ref, Fun, Acc).

put_rev(ChPid, DbId, Doc, History, Deleted, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'PutRev', [DbId, Doc, History, Deleted, Options]}),
  barrel_rpc:await(ChPid, Ref).

write_batch(ChPid, DbId, Updates, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'WriteBatch', [DbId, Options]}),
  ok = stream_batch_updates(Updates, ChPid, Ref),
  wait_batch_results(ChPid, Ref, []).

fold_by_id(ChPid, DbId, Fun, Acc, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'FoldById', [DbId, Options]}),
  do_fold(ChPid, Ref, Fun, Acc).

revsdiff(ChPid, DbId, DocId, RevIds) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'RevsDiff', [DbId, DocId, RevIds]}),
  barrel_rpc:await(ChPid, Ref).

%% ==============================
%% system docs operations


put_system_doc(ChPid, DbId, DocId, Doc) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'PutSystemDoc', [DbId, DocId, Doc]}),
  barrel_rpc:await(ChPid, Ref).

get_system_doc(ChPid, DbId, DocId) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'GetSystemDoc', [DbId, DocId]}),
  barrel_rpc:await(ChPid, Ref).

delete_system_doc(ChPid, DbId, DocId) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'DeleteSystemDoc', [DbId, DocId]}),
  barrel_rpc:await(ChPid, Ref).

%% ==============================
%% changes operations

changes_since(ChPid, DbId, Since, Fun, Acc, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.DatabaseChanges', 'ChangesSince', [DbId, Since, Options]}),
  do_fold_changes(ChPid, Ref, Fun, Acc).


subscribe_changes(ChPid, DbId, Since, Options) ->
  StreamRef = barrel_rpc:request(ChPid, {'barrel.v1.DatabaseChanges', 'ChangesStream', [DbId, Since, Options]}),
  _ = erlang:put({StreamRef, last_seq},  Since),
  StreamRef.

await_change(ChPid, StreamRef) -> await_change(ChPid, StreamRef, infinity).

await_change(ChPid, StreamRef, Timeout) ->
  case barrel_rpc:await(ChPid, StreamRef, Timeout) of
    {data, Change} ->
      Seq = maps:get(<<"seq">>, Change),
      OldSeq = erlang:get({StreamRef, last_seq}),
      _ = erlang:put({StreamRef, last_seq}, erlang:max(OldSeq, Seq)),
      Change;
    end_stream ->
      {end_stream, erlang:erase({StreamRef, last_seq})}
  end.

unsubscribe_changes(ChPid, StreamRef) ->
  ok = barrel_rpc:end_stream(ChPid, StreamRef),
  {ok, erlang:erase({StreamRef, last_seq})}.


%% ==============================
%% internal helpers

do_fold(ChPid, Ref, Fun, Acc) ->
  case barrel_rpc:await(ChPid, Ref) of
    {data, {Doc, Meta}} ->
      Acc2 = Fun(Doc, Meta, Acc),
      do_fold(ChPid, Ref, Fun, Acc2);
    end_stream ->
      Acc
  end.

do_fold_changes(ChPid, Ref, Fun, Acc) ->
  case barrel_rpc:await(ChPid, Ref) of
    {data, Change} ->
      Acc2 = Fun(Change, Acc),
      do_fold_changes(ChPid, Ref, Fun, Acc2);
    end_stream ->
      Acc
  end.

stream_batch_updates([Update | Rest], ChPid, Ref) ->
  _ = barrel_rpc:stream(ChPid, Ref, Update),
  stream_batch_updates(Rest,  ChPid, Ref);
stream_batch_updates([], ChPid, Ref) ->
  barrel_rpc:stream(ChPid, Ref, end_batch).

wait_batch_results(ChPid, Ref, Acc) ->
  case barrel_rpc:await(ChPid, Ref) of
    {data, Result} -> wait_batch_results(ChPid, Ref, [ Result | Acc ]);
    end_stream -> lists:reverse(Acc)
  end.