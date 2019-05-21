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
-include_lib("opencensus/include/opencensus.hrl").

-define(DEFAULT_WINDOW, 8192).

put_attachment(BarrelId, DocId, AttName, {ReaderFun, ReaderState}) ->
  ?start_span(#{ <<"log">> => <<"put attachment">> }),
  ocp:record('barrel/attachments/put_num', 1),
  StartTime = erlang:timestamp(),
  {ok, Fd, TempFile} = tempfile(BarrelId, DocId, AttName),
  try do_write_blob(Fd, TempFile, {ReaderFun, ReaderState})
  after
    ocp:record('barrel/attachments/put_duration',
               timer:now_diff(erlang:timestamp(), StartTime)),
    _ = file:close(Fd),
    ?end_span
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
      _ = file:close(Fd),
      _ = file:sync(Fd), %% we sync on each write
      Digest = crypto:hash_final(State),
      {ok, barrel_lib:to_hex(Digest), NewReaderState};
    {ok, Bin, NewReaderState} ->
      ok = file:write(Fd, Bin),
      _  =  file:sync(Fd), %% we sync on each write
      NState = crypto:hash_update(State, Bin),
      write_blob1(ReaderFun, NewReaderState, Fd, NState);
    Error ->
      _ = file:close(Fd),
      Error
  end.

fetch_attachment(Path) ->
  fetch_attachment(Path, ?DEFAULT_WINDOW).

fetch_attachment(Path, Window) ->
  ocp:record('barrel/attachments/fetch_num', 1),
  StartTime = erlang:timestamp(),
  ReaderFun = fun({AttPid, Pos}) ->
                  case pread(AttPid, Pos, Window) of
                    {ok, Data, NewPos} ->
                      Ctx = {AttPid, NewPos},
                      {ok, Data, Ctx};
                    Else ->
                      ocp:record('barrel/attachments/fetch_duration',
                                 timer:now_diff(erlang:timestamp(), StartTime)),
                      Else
                  end
              end,

  Proc = get_proc(Path),
  Proc ! start_read,
  Ctx = {Proc, 0},
  {ok, ReaderFun, Ctx}.

%% internal only used for eviction
evict(Path) ->
  gen_statem:cast(?att_proc(Path), evicted).


refc(Path) ->
   gen_statem:call(get_proc(Path), refc).


%% get process maintaining the attachment
%% first we check if it's cached, then see if it's sill loaded
%% else we load it and create its process
get_proc(Path) ->
  ?start_span(#{ <<"log">> => <<"get attachment process">> }),
  try do_get_proc(Path)
  after
    ?end_span
  end.

do_get_proc(Path) ->
  case lru:get(attachment_files, Path) of
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
  end.



pread(AttPid, Pos, Size) ->
  gen_statem:call(AttPid, {pread, Pos, Size}).

start_link(Path) ->
  gen_statem:start_link(?att_proc(Path), ?MODULE, [Path], []).

init([Path]) ->
  State = init_state(Path),
  case file:open(Path, [read, raw, binary]) of
    {ok, Fd} ->
      {ok, Eof} = file:position(Fd, eof),
      ocp:record('barrel/attachments/active', 1),
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
  ocp:record('barrel/attachments/active', -1),
  ok = file:close(Fd),
  ok.

code_change(_OldVsn, State, Data, _Extra) ->
  {ok, State, Data}.



active({call, From}, {pread, Pos, Sz}, Data) ->
  handle_read(Pos, Sz, From, active, Data);

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
  handle_refc(From, active, Data);

active(cast, evicted, Data) ->
  {next_state, evicted, Data, timeout()};

active(cast, wakeup, Data) ->
  {keep_state, Data};

active(timeout, _Content, Data) ->
  {keep_state, Data};

active(info, start_read, Data) ->
  {keep_state, Data};

active(EventType, EventContent, Data) ->
  handle_event(EventType, active, EventContent,Data).


evicted({call, From}, {pread, Pos, Sz}, Data) ->
  handle_read(Pos, Sz, From, evicted, Data);

evicted({call, From}, delete, Data) ->
 case maybe_delete(Data) of
   {true, Data1} ->
     ocp:record('barrel/attachments/active', -1),
     {stop, normal, Data1, [{reply, From, ok}]};
   {false, Data1} ->
     {keep_state, Data1, [{reply, From, ok}]}
 end;

evicted({call, From}, refc, Data) ->
  handle_refc(From, evicted, Data);


evicted(cast, wakeup, Data) ->
  {next_state, active, Data};


evicted(timeout, _Undefined, #{ fd := Fd } = Data) ->
  ocp:record('barrel/attachments/active', -1),
  ok = file:close(Fd),
  {stop, normal, Data#{ fd => nil }};

evicted(info, start_read, Data) ->
  {next_state, active, Data};

evicted(EventType, EventContent, Data) ->
  handle_event(EventType, active, EventContent,Data).

%% handlers

handle_read(Pos, Sz, From, State, #{ fd := Fd, eof := Eof } = Data) ->
  Reply = case file:pread(Fd, Pos, Sz) of
            {ok, Bin} ->
              NextPos = erlang:min(Pos + Sz, Eof),
              {ok, Bin, NextPos};
            eof ->
              eob;
            {error, _} = Error ->
              Error
          end,

  case State of
    active ->
      {keep_state, Data, [{reply, From, Reply}]};
    evicted ->
      {next_state, active, Data,  [{reply, From, Reply}]}
  end.

handle_event(EventType, State, Content, Data) ->
  ?LOG_INFO("~s got unknown event: type=~p, state=~p, content=~p",
            [?MODULE_STRING, EventType, State, Content]),
  {keep_state, Data}.

handle_refc(From, State, #{ path := Path, state := AttState } = Data) ->
   ?STORE:add_counter(<<"att">>, Path, -1) ,
   case State of
     active ->
      {keep_state, Data#{ state => AttState + 1 }, [{reply, From, ok}]};
     evicted ->
       {next_state, active, Data#{ state => AttState + 1 }, [{reply, From, ok}]}
   end.

%% helpers

timeout() ->
  barrel_config:get(attachment_timeout).


cache_and_activate(Path, Pid) ->
  lru:add(attachment_files, Path, Pid).

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
      _ = filelib:ensure_dir(DelFile),
      case file:rename(Path, DelFile) of
        ok ->
          spawn(file, delete, [DelFile]),
          {true, Data#{ fd => nil, state => 0 }};
        Error ->
          ocp:record('barrel/attachments/active', -1),
          ?LOG_ERROR("error while deleting attachment=~p~n", [Path]),
          exit(Error)
      end;
    true ->
      ?STORE:add_counter(<<"att">>, Path, -1) ,
      {false, Data#{ state => State + 1 }}
  end.


tempdir() ->
  Dir = filename:join([att_dir(), ".barrel_tmp"]),
  ok = filelib:ensure_dir(filename:join([Dir, "dummy"])),
  Dir.

tempfile(BarrelId, DocId, AttName) ->
  Prefix = << BarrelId/binary, DocId/binary, AttName/binary >>,
  TempDir = tempdir(),
  temp_file_1(TempDir, Prefix).


temp_file_1(TempDir, Prefix) ->
  Uid = barrel_lib:make_uid(Prefix),
  Name = filename:join([TempDir, Uid]),
  case file:open(Name, [raw, append]) of
    {ok, Fd} ->
      {ok, Fd, Name};
    _Error ->
      timer:sleep(10),
      temp_file_1(TempDir, Prefix)
  end.

att_dir() ->
  barrel_config:get(attachment_dir).

att_path(<< P1:2/binary, P2:2/binary, P3:2/binary, P4:2/binary, Name/binary >>) ->
  AttFile = filename:join([att_dir(), <<"sha256">>, P1, P2, P3, P4, Name]),
  ok = filelib:ensure_dir(AttFile),
  AttFile.
