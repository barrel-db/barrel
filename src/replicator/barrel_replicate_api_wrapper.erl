%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Jul 2017 12:58
%%%-------------------------------------------------------------------
-module(barrel_replicate_api_wrapper).
-author("benoitc").

%% API

-export([
  get/3,
  put_rev/5,
  revsdiff/3,
  subscribe_changes/3,
  await_change/2,
  unsubscribe_changes/2
]).

-export([
  put_system_doc/3,
  get_system_doc/2,
  delete_system_doc/2
]).



%% ==============================‡
%% barrel_replicate_alg

get({Node, DbName}, DocId, Opts) when is_atom(Node), is_binary(DbName) ->
  F = fun(Doc, Meta, Acc) -> [{Doc, Meta} | Acc] end,
  case barrel_rpc:fetch_docs(Node, DbName, F, [], [DocId], Opts) of
    [] -> {error, not_found};
    [{Doc, Meta}] -> {ok, Doc, Meta}
  end;
get(DbName, DocId, Opts) ->
  barrel:get(DbName, DocId, Opts).


put_rev({Node, DbName}, Doc, History, Deleted, Opts) ->
  Batch = [{put_rev, Doc, History, Deleted}],
  [Result] = barrel_rpc:update_docs(Node, DbName, Batch, Opts),
  Result;
put_rev(DbName, Doc, History, Deleted, Opts) ->
  barrel:put_rev(DbName, Doc, History, Deleted, Opts).


revsdiff({Node, DbName}, DocId, History) ->
  barrel_rpc:revsdiff(Node, DbName, DocId, History);
revsdiff(DbName, DocId, History) ->
  barrel:revsdiff(DbName, DocId, History).

subscribe_changes({Node, DbName}, Since, Options) ->
  barrel_rpc:subscribe_changes(Node, DbName, Options#{ since => Since });
subscribe_changes(DbName, Since, Options) ->
  barrel:subscribe_changes(DbName, Since, Options).

await_change({_, _}=Stream, Timeout) ->
  barrel_rpc:await_changes(Stream, Timeout);
await_change(Stream, Timeout) when is_pid(Stream)->
  case barrel:await_change(Stream, Timeout) of
    {end_stream, normal, LastSeq} -> {done, LastSeq};
    {end_stream, _, LastSeq} -> {done, LastSeq};
    Change -> Change
  end.


unsubscribe_changes({Node, _}, Stream) ->
  barrel_rpc:unsubscribe_changes(Node, Stream);
unsubscribe_changes(_DbName, Stream) ->
  barrel:unsubscribe_changes(Stream).


%% ==============================
%% barrel_replicate_checkpoint

put_system_doc({Node, DbName}, Id, Doc) ->
  barrel_rpc:put_system_doc(Node, DbName, Id, Doc);
put_system_doc(DbName, Id, Doc) ->
  barrel:put_system_doc(DbName, Id, Doc).


get_system_doc({Node, DbName}, Id) ->
  barrel_rpc:get_system_doc(Node, DbName, Id);
get_system_doc(DbName, Id) ->
  barrel:get_system_doc(DbName, Id).

delete_system_doc({Node, DbName}, Id) ->
  barrel_rpc:delete_system_doc(Node, DbName, Id);
delete_system_doc(DbName, Id) ->
  barrel:delete_system_doc(DbName, Id).
