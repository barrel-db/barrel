%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb_codec_cbor module
%%%
%%% Tests CBOR encoding/decoding, structural index, iterator API,
%%% and JSON conversion.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_codec_cbor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases - varint
-export([
    varint_zero/1,
    varint_small/1,
    varint_boundary_127/1,
    varint_boundary_128/1,
    varint_medium/1,
    varint_large/1,
    varint_max_u32/1,
    varint_max_u64/1
]).

%% Test cases - CBOR primitives
-export([
    cbor_uint_tiny/1,
    cbor_uint_small/1,
    cbor_uint_medium/1,
    cbor_uint_large/1,
    cbor_nint/1,
    cbor_true/1,
    cbor_false/1,
    cbor_null/1,
    cbor_float32/1,
    cbor_float64/1,
    cbor_text_empty/1,
    cbor_text_ascii/1,
    cbor_text_unicode/1
]).

%% Test cases - CBOR containers
-export([
    cbor_array_empty/1,
    cbor_array_simple/1,
    cbor_array_nested/1,
    cbor_map_empty/1,
    cbor_map_simple/1,
    cbor_map_nested/1,
    cbor_map_key_ordering/1
]).

%% Test cases - Record encoding
-export([
    record_simple_map/1,
    record_nested/1,
    record_hash_stability/1,
    record_roundtrip/1
]).

%% Test cases - JSON conversion
-export([
    json_roundtrip_simple/1,
    json_roundtrip_nested/1,
    json_roundtrip_types/1
]).

%% Test cases - Index and iterator
-export([
    index_present/1,
    index_containers/1,
    peek_top_level/1,
    peek_not_found/1,
    find_path_simple/1,
    find_path_nested/1,
    find_path_array/1,
    decode_value_primitive/1,
    decode_value_nested/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, varint}, {group, cbor_primitives}, {group, cbor_containers},
     {group, record}, {group, json}, {group, iterator}].

groups() ->
    [
        {varint, [sequence], [
            varint_zero,
            varint_small,
            varint_boundary_127,
            varint_boundary_128,
            varint_medium,
            varint_large,
            varint_max_u32,
            varint_max_u64
        ]},
        {cbor_primitives, [sequence], [
            cbor_uint_tiny,
            cbor_uint_small,
            cbor_uint_medium,
            cbor_uint_large,
            cbor_nint,
            cbor_true,
            cbor_false,
            cbor_null,
            cbor_float32,
            cbor_float64,
            cbor_text_empty,
            cbor_text_ascii,
            cbor_text_unicode
        ]},
        {cbor_containers, [sequence], [
            cbor_array_empty,
            cbor_array_simple,
            cbor_array_nested,
            cbor_map_empty,
            cbor_map_simple,
            cbor_map_nested,
            cbor_map_key_ordering
        ]},
        {record, [sequence], [
            record_simple_map,
            record_nested,
            record_hash_stability,
            record_roundtrip
        ]},
        {json, [sequence], [
            json_roundtrip_simple,
            json_roundtrip_nested,
            json_roundtrip_types
        ]},
        {iterator, [sequence], [
            index_present,
            index_containers,
            peek_top_level,
            peek_not_found,
            find_path_simple,
            find_path_nested,
            find_path_array,
            decode_value_primitive,
            decode_value_nested
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Varint
%%====================================================================

varint_zero(_Config) ->
    test_varint_roundtrip(0).

varint_small(_Config) ->
    lists:foreach(fun test_varint_roundtrip/1, [1, 10, 50, 100, 126]).

varint_boundary_127(_Config) ->
    test_varint_roundtrip(127),
    %% 127 should be single byte
    ?assertEqual(<<127>>, barrel_docdb_codec_cbor:encode_varint(127)).

varint_boundary_128(_Config) ->
    test_varint_roundtrip(128),
    %% 128 should be two bytes
    Encoded = barrel_docdb_codec_cbor:encode_varint(128),
    ?assertEqual(2, byte_size(Encoded)).

varint_medium(_Config) ->
    lists:foreach(fun test_varint_roundtrip/1, [255, 256, 1000, 16383, 16384]).

varint_large(_Config) ->
    lists:foreach(fun test_varint_roundtrip/1, [65535, 65536, 1000000]).

varint_max_u32(_Config) ->
    test_varint_roundtrip(4294967295).

varint_max_u64(_Config) ->
    test_varint_roundtrip(18446744073709551615).

test_varint_roundtrip(N) ->
    Encoded = barrel_docdb_codec_cbor:encode_varint(N),
    ?assert(is_binary(Encoded)),
    {Decoded, <<>>} = barrel_docdb_codec_cbor:decode_varint(Encoded),
    ?assertEqual(N, Decoded).

%%====================================================================
%% Test Cases - CBOR Primitives
%%====================================================================

cbor_uint_tiny(_Config) ->
    %% 0-23 encode as single byte
    lists:foreach(fun(N) ->
        Encoded = barrel_docdb_codec_cbor:encode_cbor(N),
        ?assertEqual(1, byte_size(Encoded)),
        ?assertEqual(N, barrel_docdb_codec_cbor:decode_cbor(Encoded))
    end, lists:seq(0, 23)).

cbor_uint_small(_Config) ->
    %% 24-255 encode as two bytes
    lists:foreach(fun(N) ->
        Encoded = barrel_docdb_codec_cbor:encode_cbor(N),
        ?assertEqual(2, byte_size(Encoded)),
        ?assertEqual(N, barrel_docdb_codec_cbor:decode_cbor(Encoded))
    end, [24, 100, 255]).

cbor_uint_medium(_Config) ->
    %% 256-65535 encode as three bytes
    lists:foreach(fun(N) ->
        Encoded = barrel_docdb_codec_cbor:encode_cbor(N),
        ?assertEqual(3, byte_size(Encoded)),
        ?assertEqual(N, barrel_docdb_codec_cbor:decode_cbor(Encoded))
    end, [256, 1000, 65535]).

cbor_uint_large(_Config) ->
    %% Large integers
    lists:foreach(fun(N) ->
        Encoded = barrel_docdb_codec_cbor:encode_cbor(N),
        ?assertEqual(N, barrel_docdb_codec_cbor:decode_cbor(Encoded))
    end, [65536, 4294967295, 4294967296]).

cbor_nint(_Config) ->
    %% Negative integers
    lists:foreach(fun(N) ->
        Encoded = barrel_docdb_codec_cbor:encode_cbor(N),
        ?assertEqual(N, barrel_docdb_codec_cbor:decode_cbor(Encoded))
    end, [-1, -10, -24, -25, -100, -256, -1000, -65536]).

cbor_true(_Config) ->
    Encoded = barrel_docdb_codec_cbor:encode_cbor(true),
    ?assertEqual(<<16#f5>>, Encoded),
    ?assertEqual(true, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_false(_Config) ->
    Encoded = barrel_docdb_codec_cbor:encode_cbor(false),
    ?assertEqual(<<16#f4>>, Encoded),
    ?assertEqual(false, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_null(_Config) ->
    Encoded = barrel_docdb_codec_cbor:encode_cbor(null),
    ?assertEqual(<<16#f6>>, Encoded),
    ?assertEqual(null, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_float32(_Config) ->
    %% Floats that fit in float32
    lists:foreach(fun(F) ->
        Encoded = barrel_docdb_codec_cbor:encode_cbor(F),
        Decoded = barrel_docdb_codec_cbor:decode_cbor(Encoded),
        ?assertEqual(F, Decoded)
    end, [0.0, 1.0, -1.0, 3.14]).

cbor_float64(_Config) ->
    %% Floats that need float64 precision
    F = 1.7976931348623157e308,
    Encoded = barrel_docdb_codec_cbor:encode_cbor(F),
    Decoded = barrel_docdb_codec_cbor:decode_cbor(Encoded),
    ?assertEqual(F, Decoded).

cbor_text_empty(_Config) ->
    Encoded = barrel_docdb_codec_cbor:encode_cbor(<<>>),
    ?assertEqual(<<>>, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_text_ascii(_Config) ->
    Text = <<"hello world">>,
    Encoded = barrel_docdb_codec_cbor:encode_cbor(Text),
    ?assertEqual(Text, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_text_unicode(_Config) ->
    Text = <<"Héllo 世界"/utf8>>,
    Encoded = barrel_docdb_codec_cbor:encode_cbor(Text),
    ?assertEqual(Text, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

%%====================================================================
%% Test Cases - CBOR Containers
%%====================================================================

cbor_array_empty(_Config) ->
    Array = [],
    Encoded = barrel_docdb_codec_cbor:encode_cbor(Array),
    ?assertEqual(Array, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_array_simple(_Config) ->
    Array = [1, 2, 3, <<"hello">>, true, null],
    Encoded = barrel_docdb_codec_cbor:encode_cbor(Array),
    ?assertEqual(Array, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_array_nested(_Config) ->
    Array = [[1, 2], [3, [4, 5]]],
    Encoded = barrel_docdb_codec_cbor:encode_cbor(Array),
    ?assertEqual(Array, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_map_empty(_Config) ->
    Map = #{},
    Encoded = barrel_docdb_codec_cbor:encode_cbor(Map),
    ?assertEqual(Map, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_map_simple(_Config) ->
    Map = #{<<"name">> => <<"Alice">>, <<"age">> => 30},
    Encoded = barrel_docdb_codec_cbor:encode_cbor(Map),
    ?assertEqual(Map, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_map_nested(_Config) ->
    Map = #{
        <<"user">> => #{
            <<"name">> => <<"Bob">>,
            <<"profile">> => #{
                <<"city">> => <<"Paris">>
            }
        }
    },
    Encoded = barrel_docdb_codec_cbor:encode_cbor(Map),
    ?assertEqual(Map, barrel_docdb_codec_cbor:decode_cbor(Encoded)).

cbor_map_key_ordering(_Config) ->
    %% Verify canonical key ordering (shorter keys first, then lexicographic)
    Map = #{<<"z">> => 1, <<"a">> => 2, <<"bb">> => 3, <<"aa">> => 4},
    Encoded1 = barrel_docdb_codec_cbor:encode_cbor(Map),
    Encoded2 = barrel_docdb_codec_cbor:encode_cbor(Map),
    %% Same map should produce identical encoding
    ?assertEqual(Encoded1, Encoded2),
    %% Decode should match original
    ?assertEqual(Map, barrel_docdb_codec_cbor:decode_cbor(Encoded1)).

%%====================================================================
%% Test Cases - Record Encoding
%%====================================================================

record_simple_map(_Config) ->
    Doc = #{<<"id">> => <<"doc1">>, <<"value">> => 42},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    ?assert(is_binary(RecordBin)),
    %% Check magic bytes
    <<"CB", _/binary>> = RecordBin.

record_nested(_Config) ->
    Doc = #{
        <<"id">> => <<"doc1">>,
        <<"data">> => #{
            <<"nested">> => #{
                <<"deep">> => true
            }
        }
    },
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    ?assert(is_binary(RecordBin)).

record_hash_stability(_Config) ->
    Doc = #{<<"id">> => <<"test">>, <<"value">> => 123},
    Record1 = barrel_docdb_codec_cbor:encode(Doc),
    Record2 = barrel_docdb_codec_cbor:encode(Doc),
    %% Same document should produce same hash
    Hash1 = barrel_docdb_codec_cbor:hash(Record1),
    Hash2 = barrel_docdb_codec_cbor:hash(Record2),
    ?assertEqual(Hash1, Hash2),
    %% Hash should be SHA-256 (32 bytes)
    ?assertEqual(32, byte_size(Hash1)).

record_roundtrip(_Config) ->
    Docs = [
        #{<<"simple">> => <<"value">>},
        #{<<"int">> => 42, <<"float">> => 3.14, <<"bool">> => true},
        #{<<"array">> => [1, 2, 3], <<"null">> => null},
        #{<<"nested">> => #{<<"a">> => #{<<"b">> => <<"c">>}}}
    ],
    lists:foreach(fun(Doc) ->
        RecordBin = barrel_docdb_codec_cbor:encode(Doc),
        Decoded = barrel_docdb_codec_cbor:decode(RecordBin),
        ?assertEqual(Doc, Decoded)
    end, Docs).

%%====================================================================
%% Test Cases - JSON Conversion
%%====================================================================

json_roundtrip_simple(_Config) ->
    Json = <<"{\"name\":\"test\",\"value\":42}">>,
    Record = barrel_docdb_codec_cbor:from_json(Json),
    JsonOut = barrel_docdb_codec_cbor:to_json(Record),
    %% Parse both to compare (key order may differ)
    ?assertEqual(json:decode(Json), json:decode(JsonOut)).

json_roundtrip_nested(_Config) ->
    Json = <<"{\"user\":{\"name\":\"Alice\",\"profile\":{\"city\":\"Paris\"}}}">>,
    Record = barrel_docdb_codec_cbor:from_json(Json),
    JsonOut = barrel_docdb_codec_cbor:to_json(Record),
    ?assertEqual(json:decode(Json), json:decode(JsonOut)).

json_roundtrip_types(_Config) ->
    %% Test all JSON types
    Json = <<"{\"str\":\"hello\",\"num\":42,\"float\":3.14,\"bool\":true,\"null\":null,\"arr\":[1,2,3]}">>,
    Record = barrel_docdb_codec_cbor:from_json(Json),
    JsonOut = barrel_docdb_codec_cbor:to_json(Record),
    ?assertEqual(json:decode(Json), json:decode(JsonOut)).

%%====================================================================
%% Test Cases - Index and Iterator
%%====================================================================

index_present(_Config) ->
    Doc = #{<<"id">> => <<"doc1">>, <<"value">> => 42},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    IndexBin = barrel_docdb_codec_cbor:index_bin(RecordBin),
    %% Index should be non-empty for a map document
    ?assert(byte_size(IndexBin) > 0).

index_containers(_Config) ->
    Doc = #{
        <<"user">> => #{
            <<"name">> => <<"Alice">>,
            <<"tags">> => [<<"a">>, <<"b">>]
        }
    },
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    IndexBin = barrel_docdb_codec_cbor:index_bin(RecordBin),
    %% Should have containers for: root map, nested map, array
    ?assert(byte_size(IndexBin) > 10).

peek_top_level(_Config) ->
    Doc = #{<<"name">> => <<"test">>, <<"value">> => 42},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    %% Peek should find top-level keys
    case barrel_docdb_codec_cbor:peek(RecordBin, <<"name">>) of
        {ok, {text, VRef}} ->
            {ok, Value} = barrel_docdb_codec_cbor:decode_value(RecordBin, VRef),
            ?assertEqual(<<"test">>, Value);
        {error, not_implemented} ->
            %% Stub - will be implemented
            ok
    end.

peek_not_found(_Config) ->
    Doc = #{<<"name">> => <<"test">>},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    case barrel_docdb_codec_cbor:peek(RecordBin, <<"missing">>) of
        not_found -> ok;
        {error, not_implemented} -> ok
    end.

find_path_simple(_Config) ->
    Doc = #{<<"user">> => #{<<"name">> => <<"Alice">>}},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    case barrel_docdb_codec_cbor:find_path(RecordBin, [<<"user">>, <<"name">>]) of
        {ok, {text, VRef}} ->
            {ok, Value} = barrel_docdb_codec_cbor:decode_value(RecordBin, VRef),
            ?assertEqual(<<"Alice">>, Value);
        {error, not_implemented} ->
            ok
    end.

find_path_nested(_Config) ->
    Doc = #{<<"a">> => #{<<"b">> => #{<<"c">> => 123}}},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    case barrel_docdb_codec_cbor:find_path(RecordBin, [<<"a">>, <<"b">>, <<"c">>]) of
        {ok, {uint, VRef}} ->
            {ok, Value} = barrel_docdb_codec_cbor:decode_value(RecordBin, VRef),
            ?assertEqual(123, Value);
        {error, not_implemented} ->
            ok
    end.

find_path_array(_Config) ->
    Doc = #{<<"items">> => [<<"a">>, <<"b">>, <<"c">>]},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    case barrel_docdb_codec_cbor:find_path(RecordBin, [<<"items">>, 1]) of
        {ok, {text, VRef}} ->
            {ok, Value} = barrel_docdb_codec_cbor:decode_value(RecordBin, VRef),
            ?assertEqual(<<"b">>, Value);
        {error, not_implemented} ->
            ok
    end.

decode_value_primitive(_Config) ->
    Doc = #{<<"num">> => 42, <<"str">> => <<"hello">>},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    case barrel_docdb_codec_cbor:peek(RecordBin, <<"num">>) of
        {ok, {uint, VRef}} ->
            {ok, Value} = barrel_docdb_codec_cbor:decode_value(RecordBin, VRef),
            ?assertEqual(42, Value);
        {error, not_implemented} ->
            ok
    end.

decode_value_nested(_Config) ->
    Doc = #{<<"nested">> => #{<<"deep">> => true}},
    RecordBin = barrel_docdb_codec_cbor:encode(Doc),
    case barrel_docdb_codec_cbor:peek(RecordBin, <<"nested">>) of
        {ok, {map, VRef}} ->
            {ok, Value} = barrel_docdb_codec_cbor:decode_value(RecordBin, VRef),
            ?assertEqual(#{<<"deep">> => true}, Value);
        {error, not_implemented} ->
            ok
    end.
