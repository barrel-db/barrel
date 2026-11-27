%% @doc Strict decoder for the CBOR a guest emits.
%%
%% Accepts unsigned and negative integers that fit 64 bits, UTF-8 texts,
%% definite-length arrays, false, true and null, nested at most 4 deep,
%% and requires the input to hold exactly one value. Byte strings, maps,
%% floats, tags and indefinite lengths are refused: an emitted key or
%% value must mean the same thing on every node.
-module(barrel_design_cbor).

-export([decode/1]).

-define(MAX_DEPTH, 4).
-define(MAX_ITEMS, 1024).

-spec decode(binary()) -> {ok, barrel_design_key:key()} | {error, invalid_cbor}.
decode(Bin) when is_binary(Bin) ->
    try value(Bin, 0) of
        {Value, <<>>} -> {ok, Value};
        {_, _Trailing} -> {error, invalid_cbor}
    catch
        throw:invalid -> {error, invalid_cbor};
        error:{badmatch, _} -> {error, invalid_cbor};
        error:function_clause -> {error, invalid_cbor}
    end.

value(<<0:3, Info:5, R/binary>>, _D) ->
    {N, R1} = argument(Info, R),
    int(N, R1);
value(<<1:3, Info:5, R/binary>>, _D) ->
    {N, R1} = argument(Info, R),
    int(-1 - N, R1);
value(<<3:3, Info:5, R/binary>>, _D) ->
    {Len, R1} = argument(Info, R),
    case R1 of
        <<Text:Len/binary, R2/binary>> ->
            case unicode:characters_to_binary(Text) of
                Text -> {Text, R2};
                _ -> throw(invalid)
            end;
        _ ->
            throw(invalid)
    end;
value(<<4:3, Info:5, R/binary>>, D) when D < ?MAX_DEPTH ->
    {Len, R1} = argument(Info, R),
    Len =< ?MAX_ITEMS orelse throw(invalid),
    items(Len, R1, D + 1, []);
value(<<16#F4, R/binary>>, _D) -> {false, R};
value(<<16#F5, R/binary>>, _D) -> {true, R};
value(<<16#F6, R/binary>>, _D) -> {null, R};
value(_, _D) ->
    throw(invalid).

int(N, R) when N >= -(1 bsl 63), N < (1 bsl 63) -> {N, R};
int(_, _) -> throw(invalid).

items(0, R, _D, Acc) -> {lists:reverse(Acc), R};
items(N, R, D, Acc) ->
    {V, R1} = value(R, D),
    items(N - 1, R1, D, [V | Acc]).

argument(Info, R) when Info < 24 -> {Info, R};
argument(24, <<N:8, R/binary>>) -> {N, R};
argument(25, <<N:16, R/binary>>) -> {N, R};
argument(26, <<N:32, R/binary>>) -> {N, R};
argument(27, <<N:64, R/binary>>) -> {N, R};
argument(_, _) -> throw(invalid).
