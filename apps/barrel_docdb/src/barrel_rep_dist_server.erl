%%%-------------------------------------------------------------------
%%% @doc The database's side of barrel_rep_transport_dist: runs on the
%%% node that owns the database, called over erpc. A closed database
%%% still on disk opens the way the application opens it (the
%%% `task_db_opener' env, set by barrel_server), then the call goes to
%%% barrel_rep_transport_local. Attachment transfers run in a process of
%%% their own here, so no stream or writer handle leaves this node.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_dist_server).

-export([call/3, subscribe_changes/3, start_reader/4, start_writer/5]).

%% An idle transfer whose caller stopped asking ends after this.
-define(IDLE, 60000).

-spec call(binary(), atom(), list()) -> term().
call(Db, F, Args) ->
    ok = ensure_open(Db),
    apply(barrel_rep_transport_local, F, [Db | Args]).

-spec subscribe_changes(binary(), term(), pid()) -> {ok, pid()} | {error, term()}.
subscribe_changes(Db, Since, Owner) ->
    ok = ensure_open(Db),
    case barrel_docdb:subscribe_changes(Db, Since, #{mode => push, owner => Owner}) of
        {ok, Stream} ->
            %% started linked to this short-lived erpc process: the owner
            %% monitor is what ends it
            true = unlink(Stream),
            {ok, Stream};
        {error, _} = Err ->
            Err
    end.

%% @doc Open an attachment stream in a process of its own; answers its
%% info and the process serving the chunks to `Caller'.
-spec start_reader(binary(), binary(), binary(), pid()) ->
    {ok, map(), pid()} | {error, term()}.
start_reader(Db, DocId, Name, Caller) ->
    ok = ensure_open(Db),
    start(fun() ->
              case barrel_rep_transport_local:get_attachment_stream(Db, DocId, Name) of
                  {ok, Info, ReadFun} -> {ok, maps:without([close], Info), ReadFun};
                  {error, _} = Err -> Err
              end
          end,
          fun({ok, _Info, ReadFun}) -> reader_loop(ReadFun, monitor(process, Caller)) end,
          fun({ok, Info, _}, Pid) -> {ok, Info, Pid} end).

%% @doc Open an attachment writer in a process of its own; answers the
%% process taking the chunks from `Caller'.
-spec start_writer(binary(), binary(), binary(), map(), pid()) ->
    {ok, pid()} | {error, term()}.
start_writer(Db, DocId, Name, Meta, Caller) ->
    ok = ensure_open(Db),
    #{content_type := Type, digest := Digest, origin_hlc := Origin} = Meta,
    start(fun() ->
              barrel_docdb:open_attachment_writer(
                Db, DocId, Name, Type,
                #{origin_hlc => Origin, expected_digest => Digest})
          end,
          fun({ok, Writer}) -> writer_loop(Writer, monitor(process, Caller)) end,
          fun({ok, _}, Pid) -> {ok, Pid} end).

%%====================================================================
%% Internal
%%====================================================================

%% Open the handle inside the process that keeps it, report how it went.
start(Open, Loop, Reply) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() ->
                    case Open() of
                        {ok, _} = Ok -> Parent ! {Ref, Ok}, Loop(Ok);
                        {ok, _, _} = Ok -> Parent ! {Ref, Ok}, Loop(Ok);
                        {error, _} = Err -> Parent ! {Ref, Err}
                    end
                end),
    receive
        {Ref, {error, _} = Err} -> Err;
        {Ref, Ok} -> Reply(Ok, Pid)
    end.

reader_loop(ReadFun, CallerRef) ->
    receive
        {next, From, MRef} ->
            case ReadFun() of
                {ok, Chunk, Next} ->
                    From ! {MRef, {ok, Chunk}},
                    reader_loop(Next, CallerRef);
                eof ->
                    From ! {MRef, eof};
                {error, _} = Err ->
                    From ! {MRef, Err}
            end;
        close -> ok;
        abort -> ok;
        {'DOWN', CallerRef, process, _, _} -> ok
    after ?IDLE ->
        ok
    end.

writer_loop(Writer, CallerRef) ->
    receive
        {{chunk, Chunk}, From, MRef} ->
            case barrel_docdb:write_attachment_chunk(Writer, Chunk) of
                {ok, Writer2} ->
                    From ! {MRef, ok},
                    writer_loop(Writer2, CallerRef);
                {error, _} = Err ->
                    _ = barrel_docdb:abort_attachment_writer(Writer),
                    From ! {MRef, Err}
            end;
        {finish, From, MRef} ->
            From ! {MRef, barrel_docdb:finish_attachment_writer(Writer)};
        abort ->
            _ = barrel_docdb:abort_attachment_writer(Writer);
        {'DOWN', CallerRef, process, _, _} ->
            _ = barrel_docdb:abort_attachment_writer(Writer)
    after ?IDLE ->
        _ = barrel_docdb:abort_attachment_writer(Writer)
    end.

ensure_open(Db) ->
    case barrel_docdb:open_db(Db) of
        {ok, _} ->
            ok;
        {error, _} ->
            case {barrel_docdb:db_exists(Db, #{}),
                  application:get_env(barrel_docdb, task_db_opener)} of
                {true, {ok, {M, F}}} -> _ = M:F(Db), ok;
                _ -> ok
            end
    end.
