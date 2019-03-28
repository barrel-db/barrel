-module(barrel_memory).
-author("benoitc").

%% API
-export([get_mem_usage/0]).
-export([get_total_memory/0]).

-include("barrel.hrl").

-define(ONE_MB, 1048576).

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

-spec get_total_memory() -> (non_neg_integer() | 'unknown').
get_total_memory() ->
    try
        get_total_memory(os:type())
    catch _:Error:Tb ->
            _  = lager:warning(
                   "Failed to get total system memory: ~n~p~n~p~n",
                   [Error, Tb]),
            unknown
    end.


%% get_total_memory(OS) -> Total
%% Windows and Freebsd code based on: memsup:get_memory_usage/1
%% Original code was part of OTP and released under "Erlang Public License".

get_total_memory({unix,darwin}) ->
    File = cmd("/usr/bin/vm_stat"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_mach/1, Lines)),
    [PageSize, Inactive, Active, Free, Wired] =
        [dict:fetch(Key, Dict) ||
            Key <- [page_size, 'Pages inactive', 'Pages active', 'Pages free',
                    'Pages wired down']],
    PageSize * (Inactive + Active + Free + Wired);

get_total_memory({unix,freebsd}) ->
    PageSize  = sysctl("vm.stats.vm.v_page_size"),
    PageCount = sysctl("vm.stats.vm.v_page_count"),
    PageCount * PageSize;

get_total_memory({unix,openbsd}) ->
    sysctl("hw.usermem");

get_total_memory({win32,_OSname}) ->
    [Result|_] = os_mon_sysinfo:get_mem_info(),
    {ok, [_MemLoad, TotPhys, _AvailPhys, _TotPage, _AvailPage, _TotV, _AvailV],
     _RestStr} =
        io_lib:fread("~d~d~d~d~d~d~d", Result),
    TotPhys;

get_total_memory({unix, linux}) ->
    File = read_proc_file("/proc/meminfo"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_linux/1, Lines)),
    dict:fetch('MemTotal', Dict);

get_total_memory({unix, sunos}) ->
    File = cmd("/usr/sbin/prtconf"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_sunos/1, Lines)),
    dict:fetch('Memory size', Dict);

get_total_memory({unix, aix}) ->
    File = cmd("/usr/bin/vmstat -v"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line_aix/1, Lines)),
    dict:fetch('memory pages', Dict) * 4096;

get_total_memory(_OsType) ->
    unknown.


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


%% A line looks like "Foo bar: 123456."
parse_line_mach(Line) ->
    [Name, RHS | _Rest] = string:tokens(Line, ":"),
    case Name of
        "Mach Virtual Memory Statistics" ->
            ["(page", "size", "of", PageSize, "bytes)"] =
                string:tokens(RHS, " "),
            {page_size, list_to_integer(PageSize)};
        _ ->
            [Value | _Rest1] = string:tokens(RHS, " ."),
            {list_to_atom(Name), list_to_integer(Value)}
    end.

%% A line looks like "MemTotal:         502968 kB"
%% or (with broken OS/modules) "Readahead      123456 kB"
parse_line_linux(Line) ->
    {Name, Value, UnitRest} =
        case string:tokens(Line, ":") of
            %% no colon in the line
            [S] ->
                [K, RHS] = re:split(S, "\s", [{parts, 2}, {return, list}]),
                [V | Unit] = string:tokens(RHS, " "),
                {K, V, Unit};
            [K, RHS | _Rest] ->
                [V | Unit] = string:tokens(RHS, " "),
                {K, V, Unit}
        end,
    Value1 = case UnitRest of
        []     -> list_to_integer(Value); %% no units
        ["kB"] -> list_to_integer(Value) * 1024
    end,
    {list_to_atom(Name), Value1}.

%% A line looks like "Memory size: 1024 Megabytes"
parse_line_sunos(Line) ->
    case string:tokens(Line, ":") of
        [Name, RHS | _Rest] ->
            [Value1 | UnitsRest] = string:tokens(RHS, " "),
            Value2 = case UnitsRest of
                         ["Gigabytes"] ->
                             list_to_integer(Value1) * ?ONE_MB * 1024;
                         ["Megabytes"] ->
                             list_to_integer(Value1) * ?ONE_MB;
                         ["Kilobytes"] ->
                             list_to_integer(Value1) * 1024;
                         _ ->
                             Value1 ++ UnitsRest %% no known units
                     end,
            {list_to_atom(Name), Value2};
        [Name] -> {list_to_atom(Name), none}
    end.

%% Lines look like " 12345 memory pages"
%% or              "  80.1 maxpin percentage"
parse_line_aix(Line) ->
    [Value | NameWords] = string:tokens(Line, " "),
    Name = string:join(NameWords, " "),
    {list_to_atom(Name),
     case lists:member($., Value) of
         true  -> trunc(list_to_float(Value));
         false -> list_to_integer(Value)
     end}.

sysctl(Def) ->
    list_to_integer(cmd("/sbin/sysctl -n " ++ Def) -- "\n").
