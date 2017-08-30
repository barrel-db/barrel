%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Aug 2017 18:22
%%%-------------------------------------------------------------------
-module(barrel_file_utils).
-author("benoitc").

%% API
-export([
  rm_rf/1,
  cp_r/2,
  canonical_path/1
]).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).


%% @doc Remove files and directories.
%% Target is a single filename, directoryname or wildcard expression.
-spec rm_rf(string()) -> 'ok'.
rm_rf(Target) ->
  case os:type() of
    {unix, _} ->
      EscTarget = barrel_os_utils:escape_chars(Target),
      {ok, []} = barrel_os_utils:sh(
        ?FMT("rm -rf ~ts", [EscTarget]),
        [{use_stdout, false}]
      ),
      ok;
    {win32, _} ->
      Filelist = filelib:wildcard(Target),
      Dirs = [F || F <- Filelist, filelib:is_dir(F)], Files = Filelist -- Dirs,
      ok = delete_each(Files),
      ok = delete_each_dir_win32(Dirs),
      ok
  end.

-spec cp_r(list(string()), file:filename()) -> 'ok'.
cp_r([], _Dest) ->
  ok;
cp_r(Sources, Dest) ->
  case os:type() of
    {unix, _} ->
      EscSources = [barrel_os_utils:escape_chars(Src) || Src <- Sources],
      SourceStr = string:join(EscSources, " "),
      {ok, []} = barrel_os_utils:sh(
        ?FMT(
          "cp -Rp ~ts \"~ts\"",
          [SourceStr, barrel_os_utils:escape_double_quotes(Dest)]),
        [{use_stdout, false}]),
      ok;
    {win32, _} ->
      lists:foreach(fun(Src) -> ok = cp_r_win32(Src,Dest) end, Sources),
      ok
  end.

win32_ok({ok, _}) -> true;
win32_ok({error, {Rc, _}}) when Rc<9; Rc=:=16 -> true;
win32_ok(_) -> false.

delete_each([]) ->
  ok;
delete_each([File | Rest]) ->
  case file:delete(File) of
    ok ->
      delete_each(Rest);
    {error, enoent} ->
      delete_each(Rest);
    {error, Reason}=Error ->
      lager:error("Failed to delete file ~ts: ~p\n", [File, Reason]),
      Error
  end.

%% reduce a filepath by removing all incidences of `.' and `..'
-spec canonical_path(string()) -> string().
canonical_path(Dir) ->
  Canon = canonical_path([], filename:split(filename:absname(Dir))),
  filename:nativename(Canon).

canonical_path([], [])                -> filename:absname("/");
canonical_path(Acc, [])               -> filename:join(lists:reverse(Acc));
canonical_path(Acc, ["."|Rest])       -> canonical_path(Acc, Rest);
canonical_path([_|Acc], [".."|Rest])  -> canonical_path(Acc, Rest);
canonical_path([], [".."|Rest])       -> canonical_path([], Rest);
canonical_path(Acc, [Component|Rest]) -> canonical_path([Component|Acc], Rest).

%% ===================================================================
%% Internal functions
%% ===================================================================

delete_each_dir_win32([]) -> ok;
delete_each_dir_win32([Dir | Rest]) ->
  {ok, []} = barrel_os_utils:sh(
    ?FMT(
      "rd /q /s \"~ts\"",
      [barrel_os_utils:escape_double_quotes(filename:nativename(Dir))]
    ),
    [{use_stdout, false}]
  ),
  delete_each_dir_win32(Rest).

xcopy_win32(Source,Dest)->
  %% "xcopy \"~ts\" \"~ts\" /q /y /e 2> nul", Changed to robocopy to
  %% handle long names. May have issues with older windows.
  Cmd = case filelib:is_dir(Source) of
          true ->
               NewDest = filename:join([Dest, filename:basename(Source)]),
            ?FMT(
              "robocopy \"~ts\" \"~ts\" /e /is 1> nul",
              [barrel_os_utils:escape_double_quotes(filename:nativename(Source)),
                barrel_os_utils:escape_double_quotes(filename:nativename(NewDest))]
            );
          false ->
            ?FMT(
              "robocopy \"~ts\" \"~ts\" \"~ts\" /e /is 1> nul",
              [barrel_os_utils:escape_double_quotes(filename:nativename(filename:dirname(Source))),
                barrel_os_utils:escape_double_quotes(filename:nativename(Dest)),
                barrel_os_utils:escape_double_quotes(filename:basename(Source))]
            )
        end,
  Res = barrel_os_utils:sh(Cmd, [{use_stdout, false}]),
  case win32_ok(Res) of
    true -> ok;
    false ->
      {error, lists:flatten(
        io_lib:format(
          "Failed to copy ~ts to ~ts~n",
          [Source, Dest]
        )
      )}
  end.

cp_r_win32({true, SourceDir}, {true, DestDir}) ->
  %% from directory to directory
  ok = case file:make_dir(DestDir) of
         {error, eexist} -> ok;
         Other -> Other
       end,
  ok = xcopy_win32(SourceDir, DestDir);
cp_r_win32({false, Source} = S,{true, DestDir}) ->
  %% from file to directory
  cp_r_win32(S, {false, filename:join(DestDir, filename:basename(Source))});
cp_r_win32({false, Source},{false, Dest}) ->
  %% from file to file
  {ok,_} = file:copy(Source, Dest),
  ok;
cp_r_win32({true, SourceDir}, {false, DestDir}) ->
  case filelib:is_regular(DestDir) of
    true ->
      %% From directory to file? This shouldn't happen
      {error, lists:flatten(
        io_lib:format(
          "Cannot copy dir (~p) to file (~p)\n",
          [SourceDir, DestDir]
        )
      )};
    false ->
      %% Specifying a target directory that doesn't currently exist.
      %% So let's attempt to create this directory
      case filelib:ensure_dir(filename:join(DestDir, "dummy")) of
        ok ->
          ok = xcopy_win32(SourceDir, DestDir);
        {error, Reason} ->
          {error, lists:flatten(
            io_lib:format(
              "Unable to create dir ~p: ~p\n",
              [DestDir, Reason]
            )
          )}
      end
  end;
cp_r_win32(Source,Dest) ->
  Dst = {filelib:is_dir(Dest), Dest},
  lists:foreach(
    fun(Src) ->
      ok = cp_r_win32( {filelib:is_dir(Src), Src}, Dst)
    end,
    filelib:wildcard(Source)
  ),
  ok.