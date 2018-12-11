%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Dec 2017 23:03
%%%-------------------------------------------------------------------
-module(barrel_lib).
-author("benoitc").

%% API
-export([
  to_atom/1,
  to_binary/1,
  to_list/1,
  make_uid/0, make_uid/1,
  uniqid/0, uniqid/1,
  to_hex/1,
  hex_to_binary/1,
  zeropad/1
]).

-export([
  validate_base64uri/1,
  derive_safe_string/2,
  encode_b64url/1,
  decode_b64url/1
]).

-export([parse_size_unit/1]).

-export([group_by/2]).

-export([do_exec/1]).

%% imported from attic
%% TODO: check if we really need such functions
-export([
  load_config/2,
  pmap/2, pmap/3, pmap/4,
  os_cmd/1
]).

-include("barrel.hrl").
-include_lib("syntax_tools/include/merl.hrl").

%% "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
-define(GENERATED_UID_CHARS,
        {65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,
         84,85,86,87,88,89,90,48,49,50,51,52,53,54,55,56,57}).
-define(UID_CHARS, "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890").
-define(UID_LENGTH, 12).
-define(BASE64_URI_CHARS,
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "abcdefghijklmnopqrstuvwxyz"
  "0123456789_-=").


-spec to_atom(atom() | list() | binary()) -> atom().
to_atom(V) when is_atom(V) -> V;
to_atom(V) when is_list(V) -> list_to_atom(V);
to_atom(V) when is_binary(V) -> list_to_atom(binary_to_list(V));
to_atom(_) -> error(badarg).

-spec to_binary(atom() | binary() | list() | integer()) -> binary().
to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> list_to_binary(V);
to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_binary(V) when is_integer(V) -> integer_to_binary(V);
to_binary(_) -> error(badarg).

-spec to_list(atom() | binary() | list() | integer()) -> list().
to_list(V) when is_list(V) -> V;
to_list(V) when is_binary(V) -> binary_to_list(V);
to_list(V) when is_atom(V) -> atom_to_list(V);
to_list(_) -> error(badarg).

-spec make_uid() -> binary().
make_uid() ->
    make_uid(<<>>).

-spec make_uid(atom() | binary() | string()) -> binary().
make_uid(Prefix0) ->
    ChrsSize = size(?GENERATED_UID_CHARS),
    F = fun(_, R) ->
                [element(rand:uniform(ChrsSize), ?GENERATED_UID_CHARS) | R]
        end,
    Prefix = to_binary(Prefix0),
    B = list_to_binary(lists:foldl(F, "", lists:seq(1, ?UID_LENGTH))),
    <<Prefix/binary, B/binary>>.


-spec validate_base64uri(string()) -> boolean().
validate_base64uri(Str) ->
  catch
    begin
      [begin
         case lists:member(C, ?BASE64_URI_CHARS) of
           true -> ok;
           false -> throw(false)
         end
       end || C <- string:to_graphemes(Str)],
      string:is_empty(Str) == false
    end.


derive_safe_string(S, Num) ->
  F = fun Take([], Acc) ->
    string:reverse(Acc);
    Take([G | Rem], Acc) ->
      case lists:member(G, ?BASE64_URI_CHARS) of
        true ->
          Take(string:next_grapheme(Rem), [G | Acc]);
        false ->
          Take(string:next_grapheme(Rem), Acc)
      end
      end,
  string:slice(F(string:next_grapheme(S), []), 0, Num).


encode_b64url(Bin) when is_binary(Bin) ->
  << << (urlencode_digit(D)) >> || <<D>> <= base64:encode(Bin), D =/= $= >>;
encode_b64url(L) when is_list(L) ->
  encode_b64url(iolist_to_binary(L)).

decode_b64url(Bin) when is_binary(Bin) ->
  Bin2 = case byte_size(Bin) rem 4 of
           % 1 -> << Bin/binary, "===" >>;
           2 -> << Bin/binary, "==" >>;
           3 -> << Bin/binary, "=" >>;
           _ -> Bin
         end,
  base64:decode(<< << (urldecode_digit(D)) >> || <<D>> <= Bin2 >>);
decode_b64url(L) when is_list(L) ->
  decode_b64url(iolist_to_binary(L)).


urlencode_digit($/) -> $_;
urlencode_digit($+) -> $-;
urlencode_digit(D)  -> D.

urldecode_digit($_) -> $/;
urldecode_digit($-) -> $+;
urldecode_digit(D)  -> D.

uniqid() -> uniqid(binary).

uniqid(string)    -> uuid:uuid_to_string(uuid:get_v4(), standard);
uniqid(binary)    -> uuid:uuid_to_string(uuid:get_v4(), binary_standard);
uniqid(integer)   -> <<Id:128>> = uuid:get_v4(), Id;
uniqid(float)     -> <<Id:128>> = uuid:get_v4(), Id * 1.0;
uniqid(_) -> error(badarg).


zeropad(Int) when is_integer(Int) ->
  Bin = integer_to_binary(Int),
  zeropad(Bin, 16 - (byte_size(Bin) rem 16)).

zeropad(Bin, 0) ->
  Bin;
zeropad(Bin, 1) ->
  << "0",  Bin/binary >>;
zeropad(Bin, 2) ->
  << "0", "0", Bin/binary >>;
zeropad(Bin, 3) ->
  << "0", "0", "0",  Bin/binary >>;
zeropad(Bin, 4) ->
  << "0", "0", "0", "0", Bin/binary >>;
zeropad(Bin, 5) ->
  << "0", "0", "0", "0", "0",  Bin/binary >>;
zeropad(Bin, 6) ->
  << "0", "0", "0", "0", "0", "0",  Bin/binary >>;
zeropad(Bin, 7) ->
  << "0", "0", "0", "0", "0", "0", "0", Bin/binary >>;
zeropad(Bin, 8) ->
  << "0", "0", "0", "0", "0", "0", "0", "0",  Bin/binary >>;
zeropad(Bin, 9) ->
  << "0", "0", "0", "0", "0", "0", "0", "0", "0",  Bin/binary >>;
zeropad(Bin, 10) ->
  << "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", Bin/binary >>;
zeropad(Bin, 11) ->
  << "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", Bin/binary >>;
zeropad(Bin, 12) ->
  << "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", Bin/binary >>;
zeropad(Bin, 13) ->
  << "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", Bin/binary >>;
zeropad(Bin, 14) ->
  << "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", Bin/binary >>;
zeropad(Bin, 15) ->
  << "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", Bin/binary >>.


to_hex([]) -> [];
to_hex(Bin) when is_binary(Bin) ->
  << <<(to_digit(H)),(to_digit(L))>> || <<H:4,L:4>> <= Bin >>;
to_hex([H|T]) ->
  [to_digit(H div 16), to_digit(H rem 16) | to_hex(T)].

to_digit(N) when N < 10 -> $0 + N;
to_digit(N)             -> $a + N-10.

hex_to_binary(Bin) when is_binary(Bin) ->
  << <<(binary_to_integer( <<H, L>>, 16))>> || << H, L >> <= Bin >>.


-spec parse_size_unit(integer() | string()) ->
  {ok, integer()} | {error, parse_error}.

parse_size_unit(Value) when is_integer(Value) -> {ok, Value};
parse_size_unit(Value) when is_list(Value) ->
  case re:run(Value,
    "^(?<VAL>[0-9]+)(?<UNIT>kB|KB|MB|GB|kb|mb|gb|Kb|Mb|Gb|kiB|KiB|MiB|GiB|kib|mib|gib|KIB|MIB|GIB|k|K|m|M|g|G)?$",
    [{capture, all_but_first, list}]) of
    {match, [[], _]} ->
      {ok, list_to_integer(Value)};
    {match, [Num]} ->
      {ok, list_to_integer(Num)};
    {match, [Num, Unit]} ->
      Multiplier = case Unit of
                     KiB when KiB =:= "k";  KiB =:= "kiB"; KiB =:= "K"; KiB =:= "KIB"; KiB =:= "kib" -> 1024;
                     MiB when MiB =:= "m";  MiB =:= "MiB"; MiB =:= "M"; MiB =:= "MIB"; MiB =:= "mib" -> 1024*1024;
                     GiB when GiB =:= "g";  GiB =:= "GiB"; GiB =:= "G"; GiB =:= "GIB"; GiB =:= "gib" -> 1024*1024*1024;
                     KB  when KB  =:= "KB"; KB  =:= "kB"; KB =:= "kb"; KB =:= "Kb"  -> 1000;
                     MB  when MB  =:= "MB"; MB  =:= "mB"; MB =:= "mb"; MB =:= "Mb"  -> 1000000;
                     GB  when GB  =:= "GB"; GB  =:= "gB"; GB =:= "gb"; GB =:= "Gb"  -> 1000000000
                   end,
      {ok, list_to_integer(Num) * Multiplier};
    nomatch ->
      % log error
      {error, parse_error}
  end.

%% @doc Utility that converts a given property list into a module that provides
%% constant time access to the various key/value pairs.
%%
%% Example:
%%
%%   load_config(store_config, [{backends, [{rocksdb_ram, barrel_rocksdb},
%%                                          {rocksdb_disk, barrel_rocksdb}]},
%%                              {data_dir, "/path/to_datadir"}]).
%%
%% creates the module store_config:
%%   store_config:backends(). => [{rocksdb_ram,barrel_rocksdb},{rocksdb_disk,barrel_rocksdb}]
%%   store_config:data_dir => "/path/to_datadir"
%%
-spec load_config(atom(), [{atom(), any()}]) -> ok.
load_config(Resource, Config) when is_atom(Resource), is_list(Config) ->
  Module = ?Q("-module(" ++ atom_to_list(Resource) ++ ")."),
  Functions = lists:foldl(fun({K, V}, Acc) ->
    [make_function(K,
                   V)
      | Acc]
                          end,
                          [], Config),
  Exported = [?Q("-export([" ++ atom_to_list(K) ++ "/0]).") || {K, _V} <-
    Config],
  Forms = lists:flatten([Module, Exported, Functions]),
  merl:compile_and_load(Forms, [verbose]),
  ok.

make_function(K, V) ->
  Cs = [?Q("() -> _@V@")],
  F = erl_syntax:function(merl:term(K), Cs),
  ?Q("'@_F'() -> [].").

%% @doc parallel map implementation
-spec pmap(F, List1) -> List2 when
  F :: fun(),
  List1 :: list(),
  List2 :: list().
pmap(Fun, List) -> pmap(Fun, List, length(List)).

%% @doc parallel map implementation with default timeout to 5000
-spec pmap(F, List1, Workers) -> List2 when
  F :: fun(),
  List1 :: list(),
  Workers :: non_neg_integer(), %% number of workers
  List2 :: list().
pmap(Fun, List, Workers) ->
  pmap(Fun, List, Workers, 5000).


-spec pmap(F, List1, Workers, Timeout) -> List2 when
  F :: fun(),
  List1 :: list(),
  Workers :: non_neg_integer(), %% number of workers
  Timeout :: non_neg_integer(), %% timeout
  List2 :: list().
pmap(Fun, List, NWorkers0, Timeout) ->
  NWorkers1 = erlang:min(length(List), NWorkers0),
  Parent = self(),
  Workers = [
    spawn_monitor(fun() -> pmap_worker(Parent, Fun) end)
    || _ <- lists:seq(1, NWorkers1)
  ],
  {Running, _} = lists:foldr(
    fun(E, {R, [{Pid, _}=W | Rest]}) ->
      Ref = erlang:make_ref(),
      Pid ! {Ref, E},
      {[Ref | R], Rest ++ [W]}
    end,
    {[], Workers},
    List
  ),
  Res = collect(Running, Timeout),
  [erlang:demonitor(MRef, [flush]) || {_Pid, MRef} <- Workers],
  Res.

collect([], _Timeout) -> [];
collect([Ref | Next], Timeout) ->
  receive
    {Ref, Res} ->
      [Res | collect(Next, Timeout)];
    {'DOWN', _MRef, process, _Pid, Reason} ->
      exit(Reason)
  after Timeout ->
    exit(pmap_timeout)
  end.

pmap_worker(Parent, Fun) ->
  receive
    {Ref, E} ->
      Parent ! {Ref, Fun(E)},
      pmap_worker(Parent, Fun)
  end.

-spec os_cmd(string()) -> string().
os_cmd(Command) ->
  case os:type() of
    {win32, _} ->
      %% Clink workaround; see
      %% http://code.google.com/p/clink/issues/detail?id=141
      os:cmd(" " ++ Command);
    _ ->
      %% Don't just return "/bin/sh: <cmd>: not found" if not found
      Exec = hd(string:tokens(Command, " ")),
      case os:find_executable(Exec) of
        false -> throw({command_not_found, Exec});
        _     -> os:cmd(Command)
      end
  end.


-spec group_by(list(), function()) -> dict:dict().
group_by(L, F) ->
  lists:foldr(
    fun({K, V}, D) -> dict:append(K, V, D) end,
    dict:new(),
    [{F(X), X} || X <- L]
  ).

do_exec({F, A}) ->
  erlang:apply(F, A);
do_exec({M, F, A}) ->
  erlang:apply(M, F, A);
do_exec(F) when is_function(F) ->
  F();
do_exec(_) ->
  erlang:error(badarg).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


b64url_test() ->
  ?assertEqual(decode_b64url(encode_b64url(<<"foo">>)), <<"foo">>),
  ?assertEqual(decode_b64url(encode_b64url(<<"foo1">>)), <<"foo1">>),
  ?assertEqual(decode_b64url(encode_b64url(<<"foo12">>)), <<"foo12">>),
  ?assertEqual(decode_b64url(encode_b64url(<<"foo123">>)), <<"foo123">>),
  ?assertEqual(decode_b64url(encode_b64url("foo")), <<"foo">>),
  ?assertEqual(decode_b64url(encode_b64url(["fo", "o1"])), <<"foo1">>),
  ?assertEqual(decode_b64url(encode_b64url([255,127,254,252])), <<255,127,254,252>>),
  % vanilla base64 produce URL unsafe output
  ?assertNotEqual(
    binary:match(base64:encode([255,127,254,252]), [<<"=">>, <<"/">>, <<"+">>]),
    nomatch),
  % this codec produce URL safe output
  ?assertEqual(
    binary:match(encode_b64url([255,127,254,252]), [<<"=">>, <<"/">>, <<"+">>]),
    nomatch).

pmap_test() ->
  L = [ 1, 2, 3, 4 ],
  Expected = [ 2, 4, 6, 8 ],
  Result = pmap(
    fun(E) -> E * 2 end,
    L,
    4
  ),
  ?assertEqual(Expected, Result).

-endif.
