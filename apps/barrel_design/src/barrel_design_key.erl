%% @doc Order-preserving view keys (encoding version 1).
%%
%% A key is null, false, true, a signed 64-bit integer, a UTF-8 text or
%% an array of keys. Encoded keys compare as bytes in this order:
%% null < false < true < integers (numeric) < texts (UTF-8 byte order)
%% < arrays (element by element, a shorter prefix first). Each value is
%% self-delimiting, so an array prefix such as `[<<"Ada">>]' selects
%% exactly the keys whose first component is `<<"Ada">>'.
%%
%% Texts sort by their UTF-8 bytes, which is code point order, not a
%% locale collation.
-module(barrel_design_key).

-export([version/0, encode/1, decode/1, valid/1, prefix/1, after_prefix/1, text_prefix/1]).

-define(NULL, 16#01).
-define(FALSE, 16#02).
-define(TRUE, 16#03).
-define(INT, 16#20).
-define(TEXT, 16#40).
-define(ARRAY, 16#60).
-define(I64_MIN, -(1 bsl 63)).
-define(I64_MAX, (1 bsl 63) - 1).
-define(MAX_DEPTH, 4).

-type key() :: null | boolean() | integer() | binary() | [key()].
-export_type([key/0]).

-spec version() -> pos_integer().
version() -> 1.

%% @doc True when Term is an encodable key: UTF-8 texts, 64-bit
%% integers, booleans, null and arrays of those nested at most 4 deep.
-spec valid(term()) -> boolean().
valid(Term) -> valid(Term, 0).

valid(null, _) -> true;
valid(B, _) when is_boolean(B) -> true;
valid(I, _) when is_integer(I) -> I >= ?I64_MIN andalso I =< ?I64_MAX;
valid(T, _) when is_binary(T) -> unicode:characters_to_binary(T) =:= T;
valid(L, D) when is_list(L), D < ?MAX_DEPTH -> lists:all(fun(E) -> valid(E, D + 1) end, L);
valid(_, _) -> false.

-spec encode(key()) -> binary().
encode(null) -> <<?NULL>>;
encode(false) -> <<?FALSE>>;
encode(true) -> <<?TRUE>>;
encode(I) when is_integer(I), I >= ?I64_MIN, I =< ?I64_MAX ->
    <<?INT, (I - ?I64_MIN):64/big-unsigned>>;
encode(T) when is_binary(T) ->
    <<?TEXT, (escape(T))/binary, 0, 0>>;
encode(L) when is_list(L) ->
    <<?ARRAY, << <<(encode(E))/binary>> || E <- L >>/binary, 0>>.

%% @doc The bytes every key whose leading array components equal
%% Components starts with.
-spec prefix([key()]) -> binary().
prefix(Components) when is_list(Components) ->
    <<?ARRAY, << <<(encode(E))/binary>> || E <- Components >>/binary>>.

%% @doc The bytes every encoded text starting with Stem starts with.
-spec text_prefix(binary()) -> binary().
text_prefix(Stem) when is_binary(Stem) ->
    <<?TEXT, (escape(Stem))/binary>>.

%% @doc The smallest binary greater than every binary starting with
%% Prefix (an exclusive upper bound for a prefix scan).
-spec after_prefix(binary()) -> binary().
after_prefix(Prefix) ->
    successor(lists:reverse(binary_to_list(Prefix))).

successor([]) -> <<16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>;
successor([255 | Rest]) -> successor(Rest);
successor([B | Rest]) -> list_to_binary(lists:reverse([B + 1 | Rest])).

-spec decode(binary()) -> key().
decode(Bin) ->
    {Key, <<>>} = take(Bin),
    Key.

take(<<?NULL, R/binary>>) -> {null, R};
take(<<?FALSE, R/binary>>) -> {false, R};
take(<<?TRUE, R/binary>>) -> {true, R};
take(<<?INT, U:64/big-unsigned, R/binary>>) -> {U + ?I64_MIN, R};
take(<<?TEXT, R/binary>>) -> take_text(R, <<>>);
take(<<?ARRAY, R/binary>>) -> take_array(R, []).

take_text(<<0, 0, R/binary>>, Acc) -> {Acc, R};
take_text(<<0, 16#FF, R/binary>>, Acc) -> take_text(R, <<Acc/binary, 0>>);
take_text(<<B, R/binary>>, Acc) when B =/= 0 -> take_text(R, <<Acc/binary, B>>).

take_array(<<0, R/binary>>, Acc) -> {lists:reverse(Acc), R};
take_array(Bin, Acc) ->
    {E, R} = take(Bin),
    take_array(R, [E | Acc]).

escape(T) ->
    case binary:match(T, <<0>>) of
        nomatch -> T;
        _ -> binary:replace(T, <<0>>, <<0, 16#FF>>, [global])
    end.
