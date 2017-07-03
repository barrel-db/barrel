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
  setup_channel/1
]).

-export([
  get/3,
  put_rev/5,
  revsdiff/3,
  subscribe_changes/3,
  await_change/3,
  unsubscribe_changes/2
]).

-export([
  put_system_doc/3,
  get_system_doc/2,
  delete_system_doc/2
]).


setup_channel(DbId) when is_binary(DbId) ->
  #{ mod => barrel_local, init_args => [], db => DbId };
setup_channel({Mod, Channel, DbId}) ->
  #{ mod => Mod, init_args => [Channel], db => DbId };
setup_channel({Channel, DbId}) when is_pid(Channel), is_binary(DbId) ->
  #{ mod => barrel_remote, init_args => [Channel], db => DbId };
setup_channel(_) ->
  erlang:error(badarg).

%% ==============================‡
%% barrel_replicate_alg

get(Ctx, DocId, Opts) ->
  db_exec(Ctx, get, [DocId, Opts]).

put_rev(Ctx, Doc, History, Deleted, Opts) ->
  db_exec(Ctx, put_rev, [Doc, History, Deleted, Opts]).

revsdiff(Ctx, DocId, History) ->
  db_exec(Ctx, revsdiff, [DocId, History]).

subscribe_changes(Ctx, Since, Options) ->
  db_exec(Ctx, subscribe_changes, [Since, Options]).

await_change(Ctx, Stream, Timeout) ->
  stream_exec(Ctx, await_change, [Stream, Timeout]).

unsubscribe_changes(Ctx, Stream) ->
  stream_exec(Ctx, unsubscribe_changes, [Stream]).


%% ==============================
%% barrel_replicate_checkpoint

put_system_doc(Ctx, Id, Doc) ->
  db_exec(Ctx, put_system_doc, [Id, Doc]).

get_system_doc(Ctx, Id) ->
  db_exec(Ctx, get_system_doc, [Id]).

delete_system_doc(Ctx, Id) ->
  db_exec(Ctx, delete_system_doc, [Id]).


%% ==============================
%% internal helpers

db_exec(#{ mod := Mod, init_args := InitArgs, db := Db}, Method, Args) ->
  erlang:apply(Mod, Method, InitArgs ++ [Db | Args]).

stream_exec(#{ mod := Mod, init_args := InitArgs}, Method, Args) ->
  erlang:apply(Mod, Method, InitArgs ++ Args).