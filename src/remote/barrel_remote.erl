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
  connect/1,
  disconnect/1, disconnect/2
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

connect(Params) ->
  barrel_rpc:start_channel(Params).

disconnect(Channel) ->
  barrel_rpc:close_channel(Channel).

disconnect(Channel, Timeout) ->
  barrel_rpc:close_channel(Channel, Timeout).

%% ==============================
%% database operations

database_names(Channel) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'DatabaseNames', []}),
  barrel_rpc:await(Channel, Ref).

create_database(Channel, Config) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'CreateDatabase', Config}),
  barrel_rpc:await(Channel, Ref).

delete_database(Channel, DbId) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'DeleteDatabase', [DbId]}),
  barrel_rpc:await(Channel, Ref).

database_infos(Channel, DbId) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'DatabaseInfos', [DbId]}),
  barrel_rpc:await(Channel, Ref).


%% ==============================
%% doc operations

get(Channel, DbId, DocId, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'GetDoc', [DbId, DocId, Options]}),
  barrel_rpc:await(Channel, Ref).

put(Channel, DbId, Doc, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'PutDoc', [DbId, Doc, Options]}),
  barrel_rpc:await(Channel, Ref).

delete(Channel, DbId, DocId, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'DeleteDoc', [DbId, DocId, Options]}),
  barrel_rpc:await(Channel, Ref).

post(Channel, DbId, Doc, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'InsertDoc', [DbId, Doc, Options]}),
  barrel_rpc:await(Channel, Ref).

multi_get(Channel, DbId, Fun, Acc, DocIds, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'MultiGetDoc', [DbId, DocIds, Options]}),
  do_fold(Channel, Ref, Fun, Acc).

put_rev(Channel, DbId, Doc, History, Deleted, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'PutRev', [DbId, Doc, History, Deleted, Options]}),
  barrel_rpc:await(Channel, Ref).

write_batch(Channel, DbId, Updates, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'WriteBatch', [DbId, Options]}),
  ok = stream_batch_updates(Updates, Channel, Ref),
  wait_batch_results(Channel, Ref, []).

fold_by_id(Channel, DbId, Fun, Acc, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'FoldById', [DbId, Options]}),
  do_fold(Channel, Ref, Fun, Acc).

revsdiff(Channel, DbId, DocId, RevIds) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'RevsDiff', [DbId, DocId, RevIds]}),
  barrel_rpc:await(Channel, Ref).

%% ==============================
%% system docs operations

put_system_doc(Channel, DbId, DocId, Doc) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'PutSystemDoc', [DbId, DocId, Doc]}),
  barrel_rpc:await(Channel, Ref).

get_system_doc(Channel, DbId, DocId) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'GetSystemDoc', [DbId, DocId]}),
  barrel_rpc:await(Channel, Ref).

delete_system_doc(Channel, DbId, DocId) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.Database', 'DeleteSystemDoc', [DbId, DocId]}),
  barrel_rpc:await(Channel, Ref).

%% ==============================
%% changes operations

changes_since(Channel, DbId, Since, Fun, Acc, Options) ->
  Ref = barrel_rpc:request(Channel, {'barrel.v1.DatabaseChanges', 'ChangesSince', [DbId, Since, Options]}),
  do_fold_changes(Channel, Ref, Fun, Acc).


subscribe_changes(Channel, DbId, Since, Options) ->
  StreamRef = barrel_rpc:request(Channel, {'barrel.v1.DatabaseChanges', 'ChangesStream', [DbId, Since, Options]}),
  _ = erlang:put({StreamRef, last_seq},  Since),
  StreamRef.

await_change(Channel, StreamRef) -> await_change(Channel, StreamRef, infinity).

await_change(Channel, StreamRef, Timeout) ->
  case barrel_rpc:await(Channel, StreamRef, Timeout) of
    {data, Change} ->
      Seq = maps:get(<<"seq">>, Change),
      OldSeq = erlang:get({StreamRef, last_seq}),
      _ = erlang:put({StreamRef, last_seq}, erlang:max(OldSeq, Seq)),
      Change;
    end_stream ->
      {end_stream, erlang:erase({StreamRef, last_seq})};
    {error, {server_down,shutdown}} ->
      {end_stream, erlang:erase({StreamRef, last_seq})};
    Error ->
      Error
  end.

unsubscribe_changes(Channel, StreamRef) ->
  ok = barrel_rpc:end_stream(Channel, StreamRef),
  {ok, erlang:erase({StreamRef, last_seq})}.


%% ==============================
%% internal helpers

do_fold(Channel, Ref, Fun, Acc) ->
  case barrel_rpc:await(Channel, Ref) of
    {data, {Doc, Meta}} ->
      Acc2 = Fun(Doc, Meta, Acc),
      do_fold(Channel, Ref, Fun, Acc2);
    end_stream ->
      Acc
  end.

do_fold_changes(Channel, Ref, Fun, Acc) ->
  case barrel_rpc:await(Channel, Ref) of
    {data, Change} ->
      Acc2 = Fun(Change, Acc),
      do_fold_changes(Channel, Ref, Fun, Acc2);
    end_stream ->
      Acc
  end.

stream_batch_updates([Update | Rest], Channel, Ref) ->
  _ = barrel_rpc:stream(Channel, Ref, Update),
  stream_batch_updates(Rest, Channel, Ref);
stream_batch_updates([], Channel, Ref) ->
  barrel_rpc:stream(Channel, Ref, end_batch).

wait_batch_results(Channel, Ref, Acc) ->
  case barrel_rpc:await(Channel, Ref) of
    {data, Result} -> wait_batch_results(Channel, Ref, [ Result | Acc ]);
    end_stream -> lists:reverse(Acc)
  end.