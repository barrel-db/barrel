%% @doc Test-only attachment backend implementing only the REQUIRED
%% barrel_att_backend callbacks (plus delete/5, which isn't safely
%% optional -- barrel_att_store:delete/5 calls it unconditionally). No
%% att_changes/4, att_floor/2, sweep_att_feed/3, rebuild_feed/2, or
%% checkpoint/2 -- the same shape a real minimal backend (e.g.
%% barrel_att_s3_store, a separate app) actually has. Used to exercise:
%%
%% - barrel_timeline's handling of a backend lacking checkpoint/2
%%   (barrel_att_store:checkpoint/2 detects this via
%%   erlang:function_exported/3, so simply not defining the function is
%%   enough)
%% - barrel_rep_att's behavior against a feedless backend: source-lacks-
%%   feed -> att_sync => skipped; target-lacks-feed -> puts/deletes still
%%   land, with no origin-HLC LWW guard applied
%%
%% without needing a real non-RocksDB backend just for these tests.
%%
%% Delegates storage to barrel_att_store_blob, but strips `origin_hlc`
%% before every delegated call: blob's put/delete always consult its own
%% internal feed for the LWW guard whenever `origin_hlc` is present
%% (regardless of whether att_changes/4 is exported), so passing it
%% through here would silently reintroduce the very guarantee this stub
%% exists to prove is absent. A real feedless backend has no feed to
%% check recency against at all and always overwrites; stripping
%% `origin_hlc` before delegating reproduces exactly that.
-module(barrel_docdb_test_att_backend_minimal).
-behaviour(barrel_att_backend).

-export([open/2, close/1]).
-export([put/5, put/6, get/4, delete/4, delete/5]).
-export([delete_all/3]).
-export([fold/5]).
-export([get_info/4]).
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, read_chunk/1, close_stream/1]).

open(Path, Options) -> barrel_att_store_blob:open(Path, Options).
close(AttRef) -> barrel_att_store_blob:close(AttRef).
put(AttRef, DbName, DocId, AttName, Data) ->
    barrel_att_store_blob:put(AttRef, DbName, DocId, AttName, Data).
put(AttRef, DbName, DocId, AttName, Data, Opts) ->
    barrel_att_store_blob:put(AttRef, DbName, DocId, AttName, Data,
                              strip_origin(Opts)).
get(AttRef, DbName, DocId, AttName) ->
    barrel_att_store_blob:get(AttRef, DbName, DocId, AttName).
delete(AttRef, DbName, DocId, AttName) ->
    barrel_att_store_blob:delete(AttRef, DbName, DocId, AttName).
delete(AttRef, DbName, DocId, AttName, Opts) ->
    barrel_att_store_blob:delete(AttRef, DbName, DocId, AttName,
                                 strip_origin(Opts)).
delete_all(AttRef, DbName, DocId) ->
    barrel_att_store_blob:delete_all(AttRef, DbName, DocId).
fold(AttRef, DbName, DocId, Fun, Acc) ->
    barrel_att_store_blob:fold(AttRef, DbName, DocId, Fun, Acc).
get_info(AttRef, DbName, DocId, AttName) ->
    barrel_att_store_blob:get_info(AttRef, DbName, DocId, AttName).
put_stream(AttRef, DbName, DocId, AttName, ContentType) ->
    barrel_att_store_blob:put_stream(AttRef, DbName, DocId, AttName, ContentType).
put_stream(AttRef, DbName, DocId, AttName, ContentType, Opts) ->
    barrel_att_store_blob:put_stream(AttRef, DbName, DocId, AttName, ContentType,
                                     strip_origin(Opts)).
write_chunk(Stream, Data) -> barrel_att_store_blob:write_chunk(Stream, Data).
finish_stream(Stream) -> barrel_att_store_blob:finish_stream(Stream).
abort_stream(Stream) -> barrel_att_store_blob:abort_stream(Stream).
get_stream(AttRef, DbName, DocId, AttName) ->
    barrel_att_store_blob:get_stream(AttRef, DbName, DocId, AttName).
read_chunk(Stream) -> barrel_att_store_blob:read_chunk(Stream).
close_stream(Stream) -> barrel_att_store_blob:close_stream(Stream).

strip_origin(Opts) -> maps:remove(origin_hlc, Opts).
