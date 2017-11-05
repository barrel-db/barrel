%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Aug 2017 18:29
%%%-------------------------------------------------------------------
-module(barrel_os_utils).
-author("benoitc").

%% API
-export([
  escape_chars/1,
  escape_double_quotes/1,
  sh/2
]).


%% escape\ as\ a\ shell\?
escape_chars(Str) when is_atom(Str) ->
  escape_chars(atom_to_list(Str));
escape_chars(Str) ->
  re:replace(Str, "([ ()?`!$&;\"\'])", "\\\\&",
    [global, {return, list}, unicode]).

%% "escape inside these"
escape_double_quotes(Str) ->
  re:replace(Str, "([\"\\\\`!$&*;])", "\\\\&",
    [global, {return, list}, unicode]).

%% Options = [Option] -- defaults to [use_stdout, abort_on_error]
%% Option = ErrorOption | OutputOption | {cd, string()} | {env, Env}
%% ErrorOption = return_on_error | abort_on_error | {abort_on_error, string()}
%% OutputOption = use_stdout | {use_stdout, bool()}
%% Env = [{string(), Val}]
%% Val = string() | false
%%
sh(Command0, Options0) ->
  DefaultOptions = [{use_stdout, false}],
  Options = [expand_sh_flag(V)
    || V <- proplists:compact(Options0 ++ DefaultOptions)],

  OutputHandler = proplists:get_value(output_handler, Options),

  Command = lists:flatten(patch_on_windows(Command0, proplists:get_value(env, Options, []))),
  PortSettings = proplists:get_all_values(port_settings, Options) ++
    [exit_status, {line, 16384}, use_stdio, stderr_to_stdout, hide, eof],
  _ = lager:debug("Port Cmd: ~ts\nPort Opts: ~p\n", [Command, PortSettings]),
  Port = open_port({spawn, Command}, PortSettings),

  try
    case sh_loop(Port, OutputHandler, []) of
      {ok, _Output} = Ok ->
        Ok;
      Error ->
        Error
    end
  after
    port_close(Port)
  end.

%% @doc Given env. variable `FOO' we want to expand all references to
%% it in `InStr'. References can have two forms: `$FOO' and `${FOO}'
%% The end of form `$FOO' is delimited with whitespace or EOL
-spec expand_env_variable(string(), string(), term()) -> string().
expand_env_variable(InStr, VarName, RawVarValue) ->
  case string:chr(InStr, $$) of
    0 ->
      %% No variables to expand
      InStr;
    _ ->
      ReOpts = [global, unicode, {return, list}],
      VarValue = re:replace(RawVarValue, "\\\\", "\\\\\\\\", ReOpts),
      %% Use a regex to match/replace:
      %% Given variable "FOO": match $FOO\s | $FOOeol | ${FOO}
      RegEx = io_lib:format("\\\$(~ts(\\W|$)|{~ts})", [VarName, VarName]),
      re:replace(InStr, RegEx, [VarValue, "\\2"], ReOpts)
  end.


%% We do the shell variable substitution ourselves on Windows and hope that the
%% command doesn't use any other shell magic.
patch_on_windows(Cmd, Env) ->
  case os:type() of
    {win32,nt} ->
      Cmd1 = "cmd /q /c "
        ++ lists:foldl(fun({Key, Value}, Acc) ->
          expand_env_variable(Acc, Key, Value)
                       end, Cmd, Env),
      %% Remove left-over vars
      re:replace(Cmd1, "\\\$\\w+|\\\${\\w+}", "",
        [global, {return, list}, unicode]);
    _ ->
      Cmd
  end.

sh_loop(Port, Fun, Acc) ->
  receive
    {Port, {data, {eol, Line}}} ->
      sh_loop(Port, Fun, Fun(Line ++ "\n", Acc));
    {Port, {data, {noeol, Line}}} ->
      sh_loop(Port, Fun, Fun(Line, Acc));
    {Port, eof} ->
      Data = lists:flatten(lists:reverse(Acc)),
      receive
        {Port, {exit_status, 0}} ->
          {ok, Data};
        {Port, {exit_status, Rc}} ->
          {error, {Rc, Data}}
      end
  end.

expand_sh_flag(use_stdout) ->
  {output_handler,
    fun(Line, Acc) ->
      %% Line already has a newline so don't use ?CONSOLE which adds one
      io:format("~ts", [Line]),
      [Line | Acc]
    end};
expand_sh_flag({use_stdout, false}) ->
  {output_handler,
    fun(Line, Acc) ->
      [Line | Acc]
    end};
expand_sh_flag({cd, _CdArg} = Cd) ->
  {port_settings, Cd};
expand_sh_flag({env, _EnvArg} = Env) ->
  {port_settings, Env}.
