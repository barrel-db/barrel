%%%-------------------------------------------------------------------
%%% @doc Replication between the nodes of one Erlang cluster, over
%%% distributed Erlang: no HTTP, no barrel_server.
%%%
%%% The endpoint is `#{node => Node, db => DbName}', with an optional
%%% `timeout' (ms, default 30000) per call. Every call runs on the node
%%% that owns the database (barrel_rep_dist_server), so it works over any
%%% distribution carrier: TCP, TLS or QUIC (erlang_quic's `quic_dist').
%%% Attachment bytes move one chunk per message with an acknowledgement,
%%% so a large attachment never blocks the connection.
%%%
%%% Nodes that share the cookie trust each other completely: there are
%%% no per-database rights. Use a real cookie and an encrypted carrier.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_transport_dist).

-behaviour(barrel_rep_transport).

-export([get_doc/3, put_version/5, full_state/1, get_doc_state/2,
         put_doc_state/4, put_doc_state/5, repair_doc_bodies/3,
         diff_states/2, diff_versions/2, get_changes/3, get_local_doc/2,
         put_local_doc/3, delete_local_doc/2, db_info/1, sync_hlc/2]).
-export([att_changes/3, diff_attachments/2, get_attachment_stream/3,
         put_attachment/5, delete_attachment/4, touch_attachment/4,
         att_state/3, att_list/2, flush_attachments/1,
         att_capabilities/1]).
-export([rep_id_term/1, subscribe_changes/3]).

-define(TIMEOUT, 30000).

get_doc(E, DocId, Opts) -> call(E, get_doc, [DocId, Opts]).
put_version(E, Doc, Token, VV, Deleted) -> call(E, put_version, [Doc, Token, VV, Deleted]).
full_state(_E) -> true.
get_doc_state(E, DocId) -> call(E, get_doc_state, [DocId]).
put_doc_state(E, DocId, State, Bodies) -> call(E, put_doc_state, [DocId, State, Bodies]).
put_doc_state(E, DocId, State, Bodies, Vectors) ->
    call(E, put_doc_state, [DocId, State, Bodies, Vectors]).
repair_doc_bodies(E, DocId, Bodies) -> call(E, repair_doc_bodies, [DocId, Bodies]).
diff_states(E, Digests) -> call(E, diff_states, [Digests]).
diff_versions(E, Tokens) -> call(E, diff_versions, [Tokens]).
get_changes(E, Since, Opts) -> call(E, get_changes, [Since, Opts]).
get_local_doc(E, DocId) -> call(E, get_local_doc, [DocId]).
put_local_doc(E, DocId, Doc) -> call(E, put_local_doc, [DocId, Doc]).
delete_local_doc(E, DocId) -> call(E, delete_local_doc, [DocId]).
db_info(E) -> call(E, db_info, []).

%% Both clocks move: the remote one past ours, ours past the answer.
sync_hlc(E, Hlc) ->
    case call(E, sync_hlc, [Hlc]) of
        {ok, Remote} -> barrel_docdb:sync_hlc(Remote);
        {error, _} = Err -> Err
    end.

att_changes(E, Since, Opts) -> call(E, att_changes, [Since, Opts]).
diff_attachments(E, Entries) -> call(E, diff_attachments, [Entries]).
delete_attachment(E, DocId, Name, Meta) -> call(E, delete_attachment, [DocId, Name, Meta]).
touch_attachment(E, DocId, Name, Meta) -> call(E, touch_attachment, [DocId, Name, Meta]).
att_state(E, DocId, Name) -> call(E, att_state, [DocId, Name]).
att_list(E, DocIds) -> call(E, att_list, [DocIds]).
flush_attachments(E) -> call(E, flush_attachments, []).
att_capabilities(E) -> call(E, att_capabilities, []).

%% @doc The replication identity: the node and the database.
rep_id_term(#{node := Node, db := Db}) -> {dist, Node, Db}.

%% @doc A push changes stream on the database's node, owned by `Owner'.
subscribe_changes(#{node := Node, db := Db} = E, Since, Owner) ->
    guard(fun() ->
              erpc:call(Node, barrel_rep_dist_server, subscribe_changes,
                        [Db, Since, Owner], timeout(E))
          end).

%%====================================================================
%% Attachments: one process on the database's node per transfer
%%====================================================================

%% The reader on the remote node holds the stream; each read_fun call
%% asks it for the next chunk. `close' ends it early.
get_attachment_stream(#{node := Node, db := Db} = E, DocId, Name) ->
    case guard(fun() ->
                   erpc:call(Node, barrel_rep_dist_server, start_reader,
                             [Db, DocId, Name, self()], timeout(E))
               end) of
        {ok, Info, Reader} ->
            {ok, Info#{close => fun() -> Reader ! close, ok end},
             read_fun(Reader, timeout(E))};
        {error, _} = Err ->
            Err
    end.

read_fun(Reader, Timeout) ->
    fun() ->
        case exchange(Reader, next, Timeout) of
            {ok, Chunk} -> {ok, Chunk, read_fun(Reader, Timeout)};
            eof -> eof;
            {error, _} = Err -> Err
        end
    end.

%% The writer on the remote node holds the attachment writer; chunks go
%% one at a time, each acknowledged before the next.
put_attachment(#{node := Node, db := Db} = E, DocId, Name, Meta, ReadFun) ->
    case guard(fun() ->
                   erpc:call(Node, barrel_rep_dist_server, start_writer,
                             [Db, DocId, Name, Meta, self()], timeout(E))
               end) of
        {ok, Writer} -> push(Writer, ReadFun, timeout(E));
        {error, _} = Err -> Err
    end.

push(Writer, ReadFun, Timeout) ->
    case ReadFun() of
        {ok, Chunk, Next} ->
            case exchange(Writer, {chunk, Chunk}, Timeout) of
                ok -> push(Writer, Next, Timeout);
                {error, _} = Err -> Err
            end;
        eof ->
            exchange(Writer, finish, Timeout);
        {error, _} = Err ->
            Writer ! abort,
            Err
    end.

%% One request, one answer; the remote process dying, or the node going
%% away, answers an error.
exchange(Pid, Msg, Timeout) ->
    MRef = monitor(process, Pid),
    Pid ! {Msg, self(), MRef},
    receive
        {MRef, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, {transport, Reason}}
    after Timeout ->
        demonitor(MRef, [flush]),
        Pid ! abort,
        {error, {transport, timeout}}
    end.

%%====================================================================
%% Internal
%%====================================================================

call(#{node := Node, db := Db} = E, F, Args) ->
    guard(fun() ->
              erpc:call(Node, barrel_rep_dist_server, call, [Db, F, Args],
                        timeout(E))
          end).

%% A node out of reach is a transport error: a continuous task backs off
%% and retries. A function the remote node raises in is raised here.
guard(Fun) ->
    try Fun()
    catch
        error:{erpc, Reason} -> {error, {transport, Reason}}
    end.

timeout(E) -> maps:get(timeout, E, ?TIMEOUT).
