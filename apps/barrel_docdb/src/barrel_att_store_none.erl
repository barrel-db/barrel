%%%-------------------------------------------------------------------
%%% @doc Attachment backend that stores nothing.
%%%
%%% For databases that structurally never accept attachments -- barrel's
%%% own `_barrel_system' and `_replication_tasks' (see barrel_docdb.erl's
%%% ensure_system_db/0 and barrel_rep_tasks.erl's ensure_tasks_db/0),
%%% which only ever call put_local_doc/3. `barrel_db_server' opens an
%%% attachment store unconditionally for every database (see its init/8),
%%% and the default backend, `barrel_att_store_blob', is a real RocksDB
%%% instance: even completely empty, RocksDB preallocates its WAL file
%%% (~1.1x write_buffer_size -- 64MB default -> ~70.4MB) and its MANIFEST
%%% (4MB), a fixed ~74MB floor paid once per database regardless of
%%% whether a single attachment is ever stored. Measured on a real
%%% deployment (hecate-agora, 2026-09-05): two internal databases that
%%% only ever hold a handful of small local docs were paying this floor
%%% TWICE each (once for `docs', once for `attachments') -- ~298MB of a
%%% 455MB total, dwarfing the actual content by two orders of magnitude.
%%%
%%% Select this backend via `att_opts => #{backend => none}' on
%%% `barrel_docdb:create_db/2'. Every mutating call returns
%%% `{error, attachments_disabled}'; every read call returns the same
%%% "nothing here" result a real backend would return for a document
%%% that happens to have zero attachments (`not_found' from get/4,
%%% `{error, not_found}' from get_info/4, the accumulator unchanged from
%%% fold/5), so a caller that never writes an attachment cannot tell the
%%% difference. `checkpoint/2' and `destroy/2' are deliberately not
%%% exported (like `barrel_att_s3_store' and the test-only minimal
%%% backend): `barrel_att_store' degrades those to `{error, unsupported}'
%%% / a no-op respectively -- a `none'-backed database cannot be forked
%%% via timeline branching, which is fine for `_barrel_system' and
%%% `_replication_tasks' (neither is ever branched) but is a real
%%% limitation for anyone else opting into this backend.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store_none).
-behaviour(barrel_att_backend).

-export([open/2, close/1]).
-export([put/5, put/6, get/4, delete/4]).
-export([delete_all/3]).
-export([fold/5]).
-export([get_info/4]).
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, read_chunk/1, close_stream/1]).

%% @doc No RocksDB instance, no directory -- Path is intentionally unused.
-spec open(string(), map()) -> {ok, map()}.
open(_Path, _Options) -> {ok, #{}}.

-spec close(map()) -> ok.
close(_AttRef) -> ok.

-spec put(map(), binary(), binary(), binary(), binary()) -> {error, attachments_disabled}.
put(_AttRef, _DbName, _DocId, _AttName, _Data) -> {error, attachments_disabled}.

-spec put(map(), binary(), binary(), binary(), binary(), map()) -> {error, attachments_disabled}.
put(_AttRef, _DbName, _DocId, _AttName, _Data, _Opts) -> {error, attachments_disabled}.

%% @doc Same shape as a real backend asked for an attachment that was
%% never stored: `not_found', not an error.
-spec get(map(), binary(), binary(), binary()) -> not_found.
get(_AttRef, _DbName, _DocId, _AttName) -> not_found.

-spec delete(map(), binary(), binary(), binary()) -> {error, attachments_disabled}.
delete(_AttRef, _DbName, _DocId, _AttName) -> {error, attachments_disabled}.

%% @doc Deleting all (zero) attachments a document has is trivially
%% successful, same as a real backend would report for a document that
%% never had any.
-spec delete_all(map(), binary(), binary()) -> ok.
delete_all(_AttRef, _DbName, _DocId) -> ok.

%% @doc Nothing to fold over; the accumulator passes through unchanged,
%% same as a real backend would do for a document with zero attachments.
-spec fold(map(), binary(), binary(), fun(), term()) -> term().
fold(_AttRef, _DbName, _DocId, _Fun, Acc) -> Acc.

-spec get_info(map(), binary(), binary(), binary()) -> {error, not_found}.
get_info(_AttRef, _DbName, _DocId, _AttName) -> {error, not_found}.

-spec put_stream(map(), binary(), binary(), binary(), binary()) -> {error, attachments_disabled}.
put_stream(_AttRef, _DbName, _DocId, _AttName, _ContentType) -> {error, attachments_disabled}.

-spec put_stream(map(), binary(), binary(), binary(), binary(), map()) -> {error, attachments_disabled}.
put_stream(_AttRef, _DbName, _DocId, _AttName, _ContentType, _Opts) -> {error, attachments_disabled}.

-spec write_chunk(map(), binary()) -> {error, attachments_disabled}.
write_chunk(_Stream, _Data) -> {error, attachments_disabled}.

-spec finish_stream(map()) -> {error, attachments_disabled}.
finish_stream(_Stream) -> {error, attachments_disabled}.

-spec abort_stream(map()) -> ok.
abort_stream(_Stream) -> ok.

-spec get_stream(map(), binary(), binary(), binary()) -> {error, not_found}.
get_stream(_AttRef, _DbName, _DocId, _AttName) -> {error, not_found}.

-spec read_chunk(map()) -> eof.
read_chunk(_Stream) -> eof.

-spec close_stream(map()) -> ok.
close_stream(_Stream) -> ok.
