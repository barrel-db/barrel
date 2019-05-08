-module(barrel_fs_att).

%% pubnlic API
-export([start_link/1]).

-export([evict/1]).
-export([fetch_attachment/1,
         fetch_attachment/2]).
-export([put_attachment/4]).
%% gen_statem callbacks
-export([
  terminate/3,
  code_change/4,
  init/1,
  callback_mode/0
]).

%% states
-export([
  active/3,
  evicted/3
]).



-include("barrel.hrl").

-define(DEFAULT_WINDOW, 8192).

put_attachment(BarrelId, DocId, AttName, {ReaderFun, ReaderState}) ->
  {ok, Fd, TempFile} = tempfile(BarrelId, DocId, AttName),
  try do_write_blob(Fd, TempFile, {ReaderFun, ReaderState})
  after
    file:close(Fd)
  end.

 do_write_blob(Fd, TempFile, {ReaderFun, ReaderState}) ->
  case write_blob1(ReaderFun, ReaderState, Fd, crypto:hash_init(sha256)) of
    {ok, Hash, NReaderState} ->
      AttFile = att_path(Hash),
      case filelib:is_file(AttFile) of
        true ->
          _ = spawn(file, delete, [TempFile]),
          {ok, AttFile, NReaderState};
        false ->
          case file:rename(TempFile, AttFile) of
            ok ->
              ok = refc(AttFile),
              {ok, AttFile, NReaderState};
            Error ->
              Error
          end
      end;
    Error ->
      Error
  end.




write_blob1(ReaderFun, ReaderState, Fd, State) ->
  case ReaderFun(ReaderState) of
    {ok, eob, NewReaderState} ->
      file:close(Fd),
      file:sync(Fd), %% we sync on each write
      Digest = crypto:hash_final(State),
      {ok, NewReaderState, barrel_lib:to_hex(Digest)};
    {ok, Bin, NewReaderState} ->
      ok = file:write(Fd, Bin),
      file:sync(Fd), %% we sync on each write
      NState = crypto:hash_update(State, Bin),
      write_blob1(ReaderFun, NewReaderState, Fd, NState);
    Error ->
      file:close(Fd),
      Error
  end.



fetch_attachment(Path) ->
  fetch_attachment(Path, ?DEFAULT_WINDOW).

fetch_attachment(Path, Window) ->
  ReaderFun = fun({AttPid, Pos}) ->
                  case pread(AttPid, Pos, Window) of
                    {ok, Data, NewPos} ->
                      Ctx = {AttPid, NewPos},
                      {ok, Data, Ctx};
                    Else ->
                      Else
                  end
              end,

  %% get process maintaining the attachment
  %% first we check if it's cached, then see if it's sill loaded
  %% else we load it and create its process
  Proc = case lru:get(attachment_files, Path) of
           undefined ->
             case gproc:where(?att(Path)) of
               Pid when is_pid(Pid) ->
                 cache_and_activate(Path, Pid),
                 Pid;
               undefined ->
                 case supervisor:start_child(barrel_fs_att_sup, [Path]) of
                   {ok, Pid} ->
                     cache_and_activate(Path, Pid),
                     Pid;
                   {error, {already_started, Pid}} ->
                     cache_and_activate(Path, Pid),
                     Pid;
                   Error ->
                     exit(Error)
                 end
             end;
           Pid ->
             Pid
         end,
  Ctx = {Proc, 0},
  {ok, {ReaderFun, Ctx}}.



%% internal only used for eviction
evict(Path) ->
  gen_statem:cast(?att_proc(Path), evicted).

wakeup(Pid) ->
  gen_statem:cast(Pid, wakeup).


refc(Path) ->
   gen_statem:cast(?att_proc(Path), refc).




pread(AttPid, Pos, Size) ->
  gen_statem:cast(AttPid, {pread, Pos, Size}).


start_link(Path) ->
  gen_statem:start_link(?att_proc(Path), ?MODULE, [Path], []).

init([Path]) ->
  State = init_state(Path),
  case file:open(Path, [read, raw, binary]) of
    {ok, Fd} ->
      {ok, Eof} = file:position(Fd, eof),
      {ok, active, #{ path => Path, fd => Fd, eof => Eof, state => State }};
    {error, enoent} ->
      {stop, attachment_not_found};
    {error, Reason} ->
      {stop, {io_error, Reason}}
  end.


init_state(Path) ->
  case ?STORE:get_counter(<<"att">>, Path) of
    {ok, Count} ->
      Count;
    not_found ->
      ok = ?STORE:set_counter(<<"att">>, Path, 1),
      0
  end.


callback_mode() -> state_functions.

terminate(_Reason, _State, #{ fd := nil }) ->
  ok;
terminate(_Reason, _State, #{ fd := Fd }) ->
  ok = file:close(Fd),
  ok.

code_change(_OldVsn, State, Data, _Extra) ->
  {ok, State, Data}.



active({call, From}, {pread, Pos, Sz}, Data) ->
  handle_read(Pos, Sz, From, Data);

active({call, From}, delete, #{ path := Path } = Data0) ->
 case maybe_delete(Data0) of
   {true, Data1} ->
     %% make sure to remove us from the cache
     _ = lru:remove(attachment_files, Path),
     {stop, normal, Data1, [{reply, From, ok}]};
   {false, Data1} ->
     {keep_state, Data1, [{reply, From, ok}]}
 end;

active({call, From}, refc, Data) ->
  handle_refc(From, Data);

active(cast, evicted, Data) ->
  {next_state, evicted, Data, [{timeout, timeout()}]};

active(cast, wakeup, Data) ->
  {keep_state, Data};

active(timeout, _Content, Data) ->
  {keep_state, Data};

active(EventType, EventContent, Data) ->
  handle_event(EventType, active, EventContent,Data).


evicted({call, From}, {pread, Pos, Sz}, Data) ->
  handle_read(Pos, Sz, From, Data);

evicted({call, From}, delete, Data) ->
 case maybe_delete(Data) of
   {true, Data1} ->
     {stop, normal, Data1, [{reply, From, ok}]};
   {false, Data1} ->
     {keep_state, Data1, [{reply, From, ok}]}
 end;

evicted({call, From}, refc, Data) ->
  handle_refc(From, Data);


evicted(cast, wakeup, Data) ->
  {next_state, active, Data};


evicted(timeout, _Undefined, #{ fd := Fd } = Data) ->
  ok = file:close(Fd),
  {stop, normal, Data#{ fd => nil }};

evicted(EventType, EventContent, Data) ->
  handle_event(EventType, active, EventContent,Data).

%% handlers

handle_read(Pos, Sz, From, #{ fd := Fd, eof := Eof } = Data) ->
   Reply = case file:pread(Fd, Pos, Sz) of
            {ok, Data} ->
              NextPos = erlang:min(Pos + Sz, Eof),
              {ok, Data, NextPos};
            Error ->
              Error
          end,
  {keep_state, Data, [{reply, From, Reply}]}.

handle_event(EventType, State, Content, Data) ->
  ?LOG_INFO("~s got unknown event: type=~p, state=~p, content=~p",
            [?MODULE_STRING, EventType, State, Content]),
  {keep_state, Data}.

handle_refc(From, #{ path := Path, state := State } = Data) ->
   ?STORE:add_counter(<<"att">>, Path, -1) ,
   {keep_state, Data#{ state => State + 1 }, [{reply, From, ok}]}.

%% helpers

timeout() ->
  barrel_config:get(attachment_timeout).


cache_and_activate(Path, Pid) ->
  lru:add_with(attachment_files,
               Path,
               fun() ->
                   wakeup(Pid),
                   {ok, Pid}
               end).

maybe_delete(#{ path := Path, fd := Fd, state := State }= Data) ->
  if
    State =:= 1 ->
      ?STORE:delete_counter(<<"att">>, Path),
      ok = file:close(Fd),
      %% if the file exist we first delete it,
      %% deletion will be handled asynchronously to not
      %% wait more than needed.
      DelFile = filename:join([barrel_config:get(data_dir),
                               ".delete",
                               binary_to_list(uuid:get_v4())]),
      filelib:ensure_dir(DelFile),
      case file:rename(Path, DelFile) of
        ok ->
          spawn(file, delete, [DelFile]),
          {true, Data#{ fd => nil, state => 0 }};
        Error ->
          ?LOG_ERROR("error while deleting attachment=~p~n", [Path]),
          exit(Error)
      end;
    true ->
      ?STORE:add_counter(<<"att">>, Path, -1) ,
      {false, Data#{ fd => nil, state => State + 1 }}
  end.


tempdir() ->
  Dir = filename:joit([barrel_config:get(data_dir), ".temp"]),
  ok = filelib:ensure_dir(Dir),
  Dir.

tempfile(BarrelId, DocId, AttName) ->
  Prefix = << BarrelId/binary, DocId/binary, AttName/binary >>,
  TempDir = tempdir(),
  temp_file_1(TempDir, Prefix).


temp_file_1(TempDir, Prefix) ->
  Name = filename:join([TempDir, barrel_lib:make_uid(Prefix)]),
  case file:open(Name, [write, append]) of
    {ok, Fd} ->
      {ok, Fd, Name};
    _Error ->
      timer:sleep(10),
      temp_file_1(TempDir, Prefix)
  end.

att_dir() ->
  Dir = filename:join([barrel_config:get(data_dir), "attachments"]),
  ok = filelib:ensure_dir(Dir),
  Dir.

att_path(<< P1:2/binary, P2:2/binary, P3:2/binary, P4:2/binary, Name/binary >>) ->
  AttFile = filename:join(att_dir(),[<<"sha256">>, P1, P2, P3, P4, Name]),
  ok = filelib:ensure_dir(AttFile),
  AttFile.
