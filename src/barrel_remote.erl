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
-export([start_channel/1]).

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
  insert/4
]).


start_channel(Params) ->
  barrel_rpc:start_channel(Params).

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

insert(ChPid, DbId, Doc, Options) ->
  Ref = barrel_rpc:request(ChPid, {'barrel.v1.Database', 'InsertDoc', [DbId, Doc, Options]}),
  barrel_rpc:await(ChPid, Ref).