%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Nov 2018 10:09
%%%-------------------------------------------------------------------
-module(barrel_docs).
-author("benoitc").

%% API
-export([get_mem_usage/0]).

-include("barrel.hrl").

%% @doc return the process memory usage
-spec get_mem_usage() -> non_neg_integer().
get_mem_usage() ->
  get_mem_usage(os:type()).

get_mem_usage({unix, linux}) ->
  OsPid  = os:getpid(),
  PageSize = get_linux_pagesize(),
  ProcFile = io_lib:format("/proc/~s/statm", [OsPid]),
  try
    Data = read_proc_file(ProcFile),
    [_|[RssPagesStr|_]] = string:tokens(Data, " "),
    list_to_integer(RssPagesStr) * PageSize
  catch
    _:Err  ->
      ?LOG_ERROR("can't read the proc file ~p. Err=~p~n", [ProcFile, Err]),
      0
  end;
get_mem_usage({unix, _}) ->
  OsPid  = os:getpid(),
  Cmd = io_lib:format("ps -ao rss,pid | grep ~s", [OsPid]),
  CmdOutput = os:cmd(Cmd),
  case re:run(CmdOutput, "[0-9]+", [{capture, first, list}]) of
    {match, [Match]} ->
      try
        ProcMem = list_to_integer(Match),
        ProcMem bsl 10
      catch
        error:badarg -> 0
      end;
    _ ->
      %% in case of error use the erlang way
      recon_alloc:memory(allocated)

  end;
get_mem_usage(_) ->
  %% in case of error use the erlang way
  recon_alloc:memory(allocated).


%%----------------------------------------------------------------------------
%% Internal Helpers
%%----------------------------------------------------------------------------
cmd(Command) ->
  Exec = hd(string:tokens(Command, " ")),
  case os:find_executable(Exec) of
    false -> throw({command_not_found, Exec});
    _     -> os:cmd(Command)
  end.

get_linux_pagesize() ->
  CmdOutput = cmd("getconf PAGESIZE"),
  case re:run(CmdOutput, "^[0-9]+", [{capture, first, list}]) of
    {match, [Match]} -> list_to_integer(Match);
    _ ->
      ?LOG_WARNING(
        "Failed to get memory page size, using 4096:~n~p~n",
        [CmdOutput]
      ),
      4096
  end.

%% file:read_file does not work on files in /proc as it seems to get
%% the size of the file first and then read that many bytes. But files
%% in /proc always have length 0, we just have to read until we get
%% eof.
read_proc_file(File) ->
  {ok, IoDevice} = file:open(File, [read, raw]),
  Res = read_proc_file(IoDevice, []),
  _ = file:close(IoDevice),
  lists:flatten(lists:reverse(Res)).

-define(BUFFER_SIZE, 1024).
read_proc_file(IoDevice, Acc) ->
  case file:read(IoDevice, ?BUFFER_SIZE) of
    {ok, Res} -> read_proc_file(IoDevice, [Res | Acc]);
    eof       -> Acc
  end.
