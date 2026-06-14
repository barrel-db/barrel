%%%-------------------------------------------------------------------
%%% @doc CBOR Document Codec with Structural Index
%%%
%%% Implements a Cosmos-DB-like document storage codec using canonical
%%% CBOR (RFC 8949) with a structural index for BSON-like navigation
%%% without full document decoding.
%%%
%%% Record Format:
%%%   [ HEADER | INDEX | PAYLOAD ]
%%%
%%% Features:
%%% - Canonical CBOR encoding (deterministic, sorted map keys)
%%% - Structural index for O(1) peek and path navigation
%%% - SHA-256 hash of payload for revision IDs
%%% - Iterator API for streaming access
%%% - JSON import/export via OTP stdlib json module
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_codec_cbor).

%% Encoding API
-export([encode/1, encode/2]).

%% Decoding API
-export([decode/1, decode_any/1]).

%% Record Info
-export([hash/1, payload/1, index_bin/1]).

%% Iterator API
-export([new_iterator/1, next/1, peek/2, find_path/2, decode_value/2]).

%% Map-like API (lazy access without full decode)
-export([get/2, get/3, get_value/2, set/3, to_map/1]).
-export([keys/1, size/1, is_key/2]).

%% JSON Conversion
-export([to_json/1, to_json_iolist/1]).
-export([from_json/1]).

%% CBOR Manipulation
-export([merge_into_cbor/2]).

%% Internal exports for testing
-export([encode_varint/1, decode_varint/1]).
-export([encode_cbor/1, decode_cbor/1]).
-export([encode_bytes/1]).  %% For byte string encoding

%%====================================================================
%% Records
%%====================================================================

%% Value reference for partial decoding
-record(vref, {
    offset :: non_neg_integer(),
    length :: non_neg_integer(),
    type :: cbor_type(),
    container_id :: non_neg_integer() | undefined
}).

%% Iterator state
-record(iter, {
    record :: binary(),
    index :: parsed_index(),
    payload_start :: non_neg_integer(),
    container_id :: non_neg_integer(),
    remaining_entries :: [entry()]  %% Entries owned by current container
}).

%% Parsed index structure
-record(parsed_index, {
    containers :: #{non_neg_integer() => container()},
    entries :: tuple(),  %% tuple for O(1) access
    top_keys :: #{binary() => non_neg_integer()}
}).

%% Container info
-record(container, {
    id :: non_neg_integer(),
    kind :: map | array,
    start_off :: non_neg_integer(),
    end_off :: non_neg_integer(),
    parent_id :: non_neg_integer(),
    first_entry :: non_neg_integer(),
    entry_count :: non_neg_integer()
}).

%% Entry info
-record(entry, {
    key :: binary() | non_neg_integer(),
    value_off :: non_neg_integer(),
    value_len :: non_neg_integer(),
    value_type :: cbor_type(),
    container_id :: non_neg_integer() | undefined,  %% ID of nested container (if value is container)
    owner_id :: non_neg_integer()  %% ID of container that owns this entry
}).

%% Encoding state for building index during encode
-record(enc_state, {
    offset = 0 :: non_neg_integer(),           %% Current offset in payload
    next_container_id = 1 :: pos_integer(),    %% Next container ID to assign
    container_stack = [] :: [non_neg_integer()], %% Stack of parent container IDs
    containers = [] :: [container()],          %% Collected containers (reversed)
    entries = [] :: [entry()],                 %% Collected entries (reversed)
    top_keys = [] :: [{binary(), non_neg_integer()}] %% Top-level keys to entry indices
}).

-type container() :: #container{}.
-type entry() :: #entry{}.
-type parsed_index() :: #parsed_index{}.
-type enc_state() :: #enc_state{}.

%%====================================================================
%% Types
%%====================================================================

-type record_bin() :: binary().
-type iter() :: #iter{}.
-type vref() :: #vref{}.
-type path() :: [binary() | non_neg_integer()].
-type cbor_type() :: uint | nint | bytes | text | array | map |
                     tag | simple | float_type | true | false | null.

-export_type([record_bin/0, iter/0, vref/0, path/0, cbor_type/0]).
-export_type([container/0, entry/0, parsed_index/0, enc_state/0]).

%%====================================================================
%% Constants
%%====================================================================

-define(MAGIC, <<"CB">>).
-define(VERSION, 1).
-define(FLAG_EXTENDED_LEN, 1).

%% CBOR major types
-define(CBOR_UINT, 0).
-define(CBOR_NINT, 1).
-define(CBOR_BYTES, 2).
-define(CBOR_TEXT, 3).
-define(CBOR_ARRAY, 4).
-define(CBOR_MAP, 5).
-define(CBOR_TAG, 6).
-define(CBOR_SIMPLE, 7).

%% CBOR simple values
-define(CBOR_FALSE, 16#f4).
-define(CBOR_TRUE, 16#f5).
-define(CBOR_NULL, 16#f6).
-define(CBOR_FLOAT16, 16#f9).
-define(CBOR_FLOAT32, 16#fa).
-define(CBOR_FLOAT64, 16#fb).

%%====================================================================
%% Varint Encoding (LEB128 unsigned)
%%====================================================================

%% @doc Encode an unsigned integer as a varint (LEB128).
-spec encode_varint(non_neg_integer()) -> binary().
encode_varint(N) when N < 128 ->
    <<N>>;
encode_varint(N) ->
    <<1:1, (N band 127):7, (encode_varint(N bsr 7))/binary>>.

%% @doc Decode a varint from binary, returning {Value, Rest}.
-spec decode_varint(binary()) -> {non_neg_integer(), binary()}.
decode_varint(Bin) ->
    decode_varint(Bin, 0, 0).

decode_varint(<<0:1, Byte:7, Rest/binary>>, Acc, Shift) ->
    {Acc bor (Byte bsl Shift), Rest};
decode_varint(<<1:1, Byte:7, Rest/binary>>, Acc, Shift) ->
    decode_varint(Rest, Acc bor (Byte bsl Shift), Shift + 7).

%%====================================================================
%% CBOR Encoding (Canonical - RFC 8949)
%%====================================================================

%% @doc Encode an Erlang term to canonical CBOR.
-spec encode_cbor(term()) -> binary().
encode_cbor(N) when is_integer(N), N >= 0 ->
    encode_uint(?CBOR_UINT, N);
encode_cbor(N) when is_integer(N), N < 0 ->
    encode_uint(?CBOR_NINT, -1 - N);
encode_cbor(true) ->
    <<?CBOR_TRUE>>;
encode_cbor(false) ->
    <<?CBOR_FALSE>>;
encode_cbor(null) ->
    <<?CBOR_NULL>>;
encode_cbor(F) when is_float(F) ->
    encode_float(F);
encode_cbor(B) when is_binary(B) ->
    %% Always encode as UTF-8 text (type 3) per design decision
    encode_text(B);
encode_cbor(L) when is_list(L) ->
    encode_array(L);
encode_cbor(M) when is_map(M) ->
    encode_map(M);
encode_cbor(A) when is_atom(A) ->
    %% Atoms other than true/false/null encoded as text
    encode_text(atom_to_binary(A, utf8)).

%% Encode unsigned integer with major type
encode_uint(Major, N) when N < 24 ->
    <<(Major bsl 5 bor N)>>;
encode_uint(Major, N) when N < 256 ->
    <<(Major bsl 5 bor 24), N>>;
encode_uint(Major, N) when N < 65536 ->
    <<(Major bsl 5 bor 25), N:16/big>>;
encode_uint(Major, N) when N < 4294967296 ->
    <<(Major bsl 5 bor 26), N:32/big>>;
encode_uint(Major, N) ->
    <<(Major bsl 5 bor 27), N:64/big>>.

%% Encode text string (UTF-8)
encode_text(Bin) ->
    Len = byte_size(Bin),
    <<(encode_uint(?CBOR_TEXT, Len))/binary, Bin/binary>>.

%% Encode byte string
encode_bytes(Bin) ->
    Len = byte_size(Bin),
    <<(encode_uint(?CBOR_BYTES, Len))/binary, Bin/binary>>.

%% Encode array
encode_array(List) ->
    Len = length(List),
    Elements = << <<(encode_cbor(E))/binary>> || E <- List >>,
    <<(encode_uint(?CBOR_ARRAY, Len))/binary, Elements/binary>>.

%% Encode map with canonical key ordering
encode_map(Map) ->
    Pairs = maps:to_list(Map),
    %% Sort by encoded key representation (canonical CBOR requirement)
    SortedPairs = lists:sort(
        fun({K1, _}, {K2, _}) ->
            encode_cbor(K1) =< encode_cbor(K2)
        end,
        Pairs
    ),
    Len = length(SortedPairs),
    Elements = << <<(encode_cbor(K))/binary, (encode_cbor(V))/binary>>
                 || {K, V} <- SortedPairs >>,
    <<(encode_uint(?CBOR_MAP, Len))/binary, Elements/binary>>.

%% Encode float (prefer smaller representation when lossless)
encode_float(F) ->
    %% Try to encode as float32 if no precision loss
    %% For canonical CBOR, we prefer smallest encoding
    case can_use_float32(F) of
        true ->
            <<?CBOR_FLOAT32, F:32/float-big>>;
        false ->
            <<?CBOR_FLOAT64, F:64/float-big>>
    end.

%% Check if float can be represented in float32 without precision loss
can_use_float32(F) ->
    %% Float32 max is ~3.4e38, check range first to avoid infinity
    case abs(F) < 3.4e38 of
        false ->
            false;
        true ->
            %% Check if no precision loss
            Float32Bin = <<F:32/float-big>>,
            <<F32:32/float-big>> = Float32Bin,
            F32 == F
    end.

%%====================================================================
%% CBOR Decoding
%%====================================================================

%% @doc Decode CBOR binary to Erlang term.
-spec decode_cbor(binary()) -> term().
decode_cbor(Bin) ->
    {Term, <<>>} = decode_cbor_value(Bin),
    Term.

decode_cbor_value(<<MajorInfo, Rest/binary>>) ->
    Major = MajorInfo bsr 5,
    Info = MajorInfo band 31,
    decode_cbor_major(Major, Info, Rest).

%% Unsigned integer
decode_cbor_major(?CBOR_UINT, Info, Rest) ->
    decode_uint(Info, Rest);

%% Negative integer
decode_cbor_major(?CBOR_NINT, Info, Rest) ->
    {N, Rest2} = decode_uint(Info, Rest),
    {-1 - N, Rest2};

%% Byte string
decode_cbor_major(?CBOR_BYTES, Info, Rest) ->
    {Len, Rest2} = decode_uint(Info, Rest),
    <<Bytes:Len/binary, Rest3/binary>> = Rest2,
    {Bytes, Rest3};

%% Text string
decode_cbor_major(?CBOR_TEXT, Info, Rest) ->
    {Len, Rest2} = decode_uint(Info, Rest),
    <<Text:Len/binary, Rest3/binary>> = Rest2,
    {Text, Rest3};

%% Array
decode_cbor_major(?CBOR_ARRAY, Info, Rest) ->
    {Len, Rest2} = decode_uint(Info, Rest),
    decode_array(Len, Rest2, []);

%% Map
decode_cbor_major(?CBOR_MAP, Info, Rest) ->
    {Len, Rest2} = decode_uint(Info, Rest),
    decode_map(Len, Rest2, #{});

%% Simple values and floats
decode_cbor_major(?CBOR_SIMPLE, 20, Rest) ->
    {false, Rest};
decode_cbor_major(?CBOR_SIMPLE, 21, Rest) ->
    {true, Rest};
decode_cbor_major(?CBOR_SIMPLE, 22, Rest) ->
    {null, Rest};
decode_cbor_major(?CBOR_SIMPLE, 25, <<F:16/float-big, Rest/binary>>) ->
    {F, Rest};
decode_cbor_major(?CBOR_SIMPLE, 26, <<F:32/float-big, Rest/binary>>) ->
    {F, Rest};
decode_cbor_major(?CBOR_SIMPLE, 27, <<F:64/float-big, Rest/binary>>) ->
    {F, Rest}.

decode_uint(Info, Rest) when Info < 24 ->
    {Info, Rest};
decode_uint(24, <<N, Rest/binary>>) ->
    {N, Rest};
decode_uint(25, <<N:16/big, Rest/binary>>) ->
    {N, Rest};
decode_uint(26, <<N:32/big, Rest/binary>>) ->
    {N, Rest};
decode_uint(27, <<N:64/big, Rest/binary>>) ->
    {N, Rest}.

decode_array(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_array(N, Bin, Acc) ->
    {Value, Rest} = decode_cbor_value(Bin),
    decode_array(N - 1, Rest, [Value | Acc]).

decode_map(0, Rest, Acc) ->
    {Acc, Rest};
decode_map(N, Bin, Acc) ->
    {Key, Rest1} = decode_cbor_value(Bin),
    {Value, Rest2} = decode_cbor_value(Rest1),
    decode_map(N - 1, Rest2, Acc#{Key => Value}).

%%====================================================================
%% Record Encoding (with Index)
%%====================================================================

%% @doc Encode an Erlang term to a CBOR record with structural index.
-spec encode(term()) -> record_bin().
encode(Term) ->
    encode(Term, #{}).

%% @doc Encode with options.
-spec encode(term(), map()) -> record_bin().
encode(Term, _Opts) ->
    %% Build payload and index together
    {Payload, Containers, Entries, TopKeys} = encode_with_index(Term),
    Index = serialize_index(Containers, Entries, TopKeys),
    Hash = crypto:hash(sha256, Payload),
    HashLen = byte_size(Hash),
    PayloadLen = byte_size(Payload),
    IndexLen = byte_size(Index),
    Flags = 0,
    Header = <<?MAGIC/binary, ?VERSION, Flags, HashLen,
               PayloadLen:32/big, IndexLen:32/big,
               Hash/binary>>,
    <<Header/binary, Index/binary, Payload/binary>>.

%%====================================================================
%% Indexed Encoding (builds payload + index simultaneously)
%%====================================================================

%% @doc Encode term and return {Payload, Containers, Entries, TopKeys}
-spec encode_with_index(term()) -> {binary(), [container()], [entry()], [{binary(), non_neg_integer()}]}.
encode_with_index(Term) ->
    State0 = #enc_state{},
    {PayloadBin, State1} = encode_indexed(Term, State0),
    #enc_state{containers = Containers, entries = Entries, top_keys = TopKeys} = State1,
    {PayloadBin, lists:reverse(Containers), lists:reverse(Entries), lists:reverse(TopKeys)}.

%% Encode with index tracking
encode_indexed(N, State) when is_integer(N), N >= 0 ->
    Bin = encode_uint(?CBOR_UINT, N),
    {Bin, advance_offset(State, byte_size(Bin))};

encode_indexed(N, State) when is_integer(N), N < 0 ->
    Bin = encode_uint(?CBOR_NINT, -1 - N),
    {Bin, advance_offset(State, byte_size(Bin))};

encode_indexed(true, State) ->
    {<<?CBOR_TRUE>>, advance_offset(State, 1)};

encode_indexed(false, State) ->
    {<<?CBOR_FALSE>>, advance_offset(State, 1)};

encode_indexed(null, State) ->
    {<<?CBOR_NULL>>, advance_offset(State, 1)};

encode_indexed(F, State) when is_float(F) ->
    Bin = encode_float(F),
    {Bin, advance_offset(State, byte_size(Bin))};

encode_indexed(B, State) when is_binary(B) ->
    Bin = encode_text(B),
    {Bin, advance_offset(State, byte_size(Bin))};

encode_indexed(L, State) when is_list(L) ->
    encode_indexed_array(L, State);

encode_indexed(M, State) when is_map(M) ->
    encode_indexed_map(M, State);

encode_indexed(A, State) when is_atom(A) ->
    Bin = encode_text(atom_to_binary(A, utf8)),
    {Bin, advance_offset(State, byte_size(Bin))}.

%% Encode array with index tracking
encode_indexed_array(List, State0) ->
    %% Assign container ID and push onto stack
    ContainerId = State0#enc_state.next_container_id,
    ParentId = current_parent(State0),
    StartOff = State0#enc_state.offset,
    FirstEntry = length(State0#enc_state.entries),

    %% Encode array header
    Len = length(List),
    Header = encode_uint(?CBOR_ARRAY, Len),
    HeaderSize = byte_size(Header),

    State1 = State0#enc_state{
        offset = StartOff + HeaderSize,
        next_container_id = ContainerId + 1,
        container_stack = [ContainerId | State0#enc_state.container_stack]
    },

    %% Encode elements and track entries
    {ElementsBin, State2} = encode_array_elements(List, 0, State1),

    %% Pop container and record it
    EndOff = State2#enc_state.offset,
    EntryCount = length(State2#enc_state.entries) - FirstEntry,

    Container = #container{
        id = ContainerId,
        kind = array,
        start_off = StartOff,
        end_off = EndOff,
        parent_id = ParentId,
        first_entry = FirstEntry,
        entry_count = EntryCount
    },

    State3 = State2#enc_state{
        container_stack = tl(State2#enc_state.container_stack),
        containers = [Container | State2#enc_state.containers]
    },

    {<<Header/binary, ElementsBin/binary>>, State3}.

encode_array_elements([], _Idx, State) ->
    {<<>>, State};
encode_array_elements([Elem | Rest], Idx, State0) ->
    ElemOff = State0#enc_state.offset,
    %% Current container owns this entry
    OwnerId = current_container_id(State0),
    {ElemBin, State1} = encode_indexed(Elem, State0),
    ElemLen = byte_size(ElemBin),

    %% Create entry for this element
    Entry = #entry{
        key = Idx,
        value_off = ElemOff,
        value_len = ElemLen,
        value_type = term_to_cbor_type(Elem),
        container_id = if is_map(Elem); is_list(Elem) ->
                            %% Container ID was just assigned
                            State0#enc_state.next_container_id;
                          true -> undefined
                       end,
        owner_id = OwnerId
    },
    State2 = State1#enc_state{entries = [Entry | State1#enc_state.entries]},

    {RestBin, State3} = encode_array_elements(Rest, Idx + 1, State2),
    {<<ElemBin/binary, RestBin/binary>>, State3}.

%% Encode map with index tracking
encode_indexed_map(Map, State0) ->
    %% Assign container ID and push onto stack
    ContainerId = State0#enc_state.next_container_id,
    ParentId = current_parent(State0),
    StartOff = State0#enc_state.offset,
    FirstEntry = length(State0#enc_state.entries),
    IsTopLevel = State0#enc_state.container_stack == [],

    %% Sort pairs by encoded key (canonical CBOR)
    Pairs = maps:to_list(Map),
    SortedPairs = lists:sort(
        fun({K1, _}, {K2, _}) ->
            encode_cbor(K1) =< encode_cbor(K2)
        end,
        Pairs
    ),

    %% Encode map header
    Len = length(SortedPairs),
    Header = encode_uint(?CBOR_MAP, Len),
    HeaderSize = byte_size(Header),

    State1 = State0#enc_state{
        offset = StartOff + HeaderSize,
        next_container_id = ContainerId + 1,
        container_stack = [ContainerId | State0#enc_state.container_stack]
    },

    %% Encode key-value pairs and track entries
    {PairsBin, State2} = encode_map_pairs(SortedPairs, IsTopLevel, FirstEntry, State1),

    %% Pop container and record it
    EndOff = State2#enc_state.offset,
    EntryCount = length(State2#enc_state.entries) - FirstEntry,

    Container = #container{
        id = ContainerId,
        kind = map,
        start_off = StartOff,
        end_off = EndOff,
        parent_id = ParentId,
        first_entry = FirstEntry,
        entry_count = EntryCount
    },

    State3 = State2#enc_state{
        container_stack = tl(State2#enc_state.container_stack),
        containers = [Container | State2#enc_state.containers]
    },

    {<<Header/binary, PairsBin/binary>>, State3}.

encode_map_pairs([], _IsTopLevel, _FirstEntry, State) ->
    {<<>>, State};
encode_map_pairs([{Key, Value} | Rest], IsTopLevel, FirstEntry, State0) ->
    %% Current container owns this entry
    OwnerId = current_container_id(State0),

    %% Encode key (not tracked as entry, just part of pair)
    KeyBin = encode_cbor(Key),
    KeySize = byte_size(KeyBin),
    State1 = advance_offset(State0, KeySize),

    %% Track value offset
    ValueOff = State1#enc_state.offset,
    {ValueBin, State2} = encode_indexed(Value, State1),
    ValueLen = byte_size(ValueBin),

    %% Create entry for this value
    EntryIdx = length(State2#enc_state.entries),
    Entry = #entry{
        key = Key,
        value_off = ValueOff,
        value_len = ValueLen,
        value_type = term_to_cbor_type(Value),
        container_id = if is_map(Value); is_list(Value) ->
                            %% Get the container ID that was just assigned
                            %% It's the most recent container added
                            case State2#enc_state.containers of
                                [#container{id = CId} | _] -> CId;
                                [] -> undefined
                            end;
                          true -> undefined
                       end,
        owner_id = OwnerId
    },

    %% Track top-level keys for O(1) lookup
    State3 = case IsTopLevel andalso is_binary(Key) of
        true ->
            State2#enc_state{
                entries = [Entry | State2#enc_state.entries],
                top_keys = [{Key, EntryIdx} | State2#enc_state.top_keys]
            };
        false ->
            State2#enc_state{entries = [Entry | State2#enc_state.entries]}
    end,

    {RestBin, State4} = encode_map_pairs(Rest, IsTopLevel, FirstEntry, State3),
    {<<KeyBin/binary, ValueBin/binary, RestBin/binary>>, State4}.

%% Helper: advance offset
advance_offset(State, N) ->
    State#enc_state{offset = State#enc_state.offset + N}.

%% Helper: get current parent container ID (0 = root)
current_parent(#enc_state{container_stack = []}) -> 0;
current_parent(#enc_state{container_stack = [ParentId | _]}) -> ParentId.

%% Helper: get current container ID (top of stack)
current_container_id(#enc_state{container_stack = []}) -> 0;
current_container_id(#enc_state{container_stack = [Id | _]}) -> Id.

%% Helper: determine CBOR type from Erlang term
term_to_cbor_type(N) when is_integer(N), N >= 0 -> uint;
term_to_cbor_type(N) when is_integer(N), N < 0 -> nint;
term_to_cbor_type(true) -> true;
term_to_cbor_type(false) -> false;
term_to_cbor_type(null) -> null;
term_to_cbor_type(F) when is_float(F) -> float_type;
term_to_cbor_type(B) when is_binary(B) -> text;
term_to_cbor_type(L) when is_list(L) -> array;
term_to_cbor_type(M) when is_map(M) -> map;
term_to_cbor_type(A) when is_atom(A) -> text.

%%====================================================================
%% Index Serialization
%%====================================================================

%% Serialize index to compact binary format
serialize_index(Containers, Entries, TopKeys) ->
    ContainersBin = serialize_containers(Containers),
    EntriesBin = serialize_entries(Entries),
    TopKeysBin = serialize_top_keys(TopKeys),
    <<(encode_varint(length(Containers)))/binary, ContainersBin/binary,
      (encode_varint(length(Entries)))/binary, EntriesBin/binary,
      (encode_varint(length(TopKeys)))/binary, TopKeysBin/binary>>.

serialize_containers([]) ->
    <<>>;
serialize_containers([C | Rest]) ->
    Kind = case C#container.kind of map -> 0; array -> 1 end,
    <<(encode_varint(C#container.id))/binary,
      Kind,
      (encode_varint(C#container.start_off))/binary,
      (encode_varint(C#container.end_off))/binary,
      (encode_varint(C#container.parent_id))/binary,
      (encode_varint(C#container.first_entry))/binary,
      (encode_varint(C#container.entry_count))/binary,
      (serialize_containers(Rest))/binary>>.

serialize_entries([]) ->
    <<>>;
serialize_entries([E | Rest]) ->
    KeyBin = case E#entry.key of
        K when is_binary(K) ->
            <<(encode_varint(byte_size(K)))/binary, K/binary>>;
        _Idx ->
            %% Array index - encode as 0 length (implicit)
            <<0>>
    end,
    TypeByte = cbor_type_to_byte(E#entry.value_type),
    ContainerIdBin = case E#entry.container_id of
        undefined -> <<0>>;
        CId -> encode_varint(CId)
    end,
    <<KeyBin/binary,
      (encode_varint(E#entry.value_off))/binary,
      (encode_varint(E#entry.value_len))/binary,
      TypeByte,
      ContainerIdBin/binary,
      (encode_varint(E#entry.owner_id))/binary,
      (serialize_entries(Rest))/binary>>.

serialize_top_keys([]) ->
    <<>>;
serialize_top_keys([{Key, EntryIdx} | Rest]) ->
    <<(encode_varint(byte_size(Key)))/binary, Key/binary,
      (encode_varint(EntryIdx))/binary,
      (serialize_top_keys(Rest))/binary>>.

%% Convert cbor_type to single byte
cbor_type_to_byte(uint) -> 0;
cbor_type_to_byte(nint) -> 1;
cbor_type_to_byte(bytes) -> 2;
cbor_type_to_byte(text) -> 3;
cbor_type_to_byte(array) -> 4;
cbor_type_to_byte(map) -> 5;
cbor_type_to_byte(tag) -> 6;
cbor_type_to_byte(float_type) -> 7;
cbor_type_to_byte(true) -> 8;
cbor_type_to_byte(false) -> 9;
cbor_type_to_byte(null) -> 10.

%%====================================================================
%% Record Decoding
%%====================================================================

%% @doc Decode a CBOR record to Erlang term.
-spec decode(record_bin()) -> term().
decode(RecordBin) ->
    {_Header, _IndexBin, PayloadBin} = parse_record(RecordBin),
    decode_cbor(PayloadBin).

%% @doc Decode any CBOR binary (indexed or plain) to Erlang term.
%% Auto-detects format: indexed CBOR (starts with "CB") or plain CBOR.
-spec decode_any(binary()) -> term().
decode_any(<<"CB", _/binary>> = RecordBin) ->
    decode(RecordBin);
decode_any(CborBin) ->
    decode_cbor(CborBin).

%% Parse record into components
parse_record(<<"CB", Version, Flags, HashLen,
               PayloadLen:32/big, IndexLen:32/big,
               Rest/binary>>) when Version == ?VERSION,
                                   Flags band ?FLAG_EXTENDED_LEN == 0 ->
    <<Hash:HashLen/binary, IndexBin:IndexLen/binary,
      PayloadBin:PayloadLen/binary>> = Rest,
    Header = #{magic => ?MAGIC, version => Version, flags => Flags,
               hash => Hash, payload_len => PayloadLen, index_len => IndexLen},
    {Header, IndexBin, PayloadBin}.

%%====================================================================
%% Record Info
%%====================================================================

%% @doc Get the SHA-256 hash of the payload.
-spec hash(record_bin()) -> binary().
hash(RecordBin) ->
    {#{hash := Hash}, _, _} = parse_record(RecordBin),
    Hash.

%% @doc Get the raw payload binary.
-spec payload(record_bin()) -> binary().
payload(RecordBin) ->
    {_, _, PayloadBin} = parse_record(RecordBin),
    PayloadBin.

%% @doc Get the raw index binary.
-spec index_bin(record_bin()) -> binary().
index_bin(RecordBin) ->
    {_, IndexBin, _} = parse_record(RecordBin),
    IndexBin.

%%====================================================================
%% Index Parsing
%%====================================================================

%% Parse index from record
parse_index(RecordBin) ->
    {Header, IndexBin, _PayloadBin} = parse_record(RecordBin),
    PayloadStart = get_payload_start(Header),
    parse_index_bin(IndexBin, PayloadStart).

get_payload_start(#{hash := Hash, index_len := IndexLen}) ->
    %% Header: magic(2) + version(1) + flags(1) + hashlen(1) +
    %% payload_len(4) + index_len(4) + hash(hashlen) + index(index_len)
    13 + byte_size(Hash) + IndexLen.

parse_index_bin(<<>>, _PayloadStart) ->
    %% Empty index
    #parsed_index{containers = #{}, entries = {}, top_keys = #{}};
parse_index_bin(IndexBin, _PayloadStart) ->
    {ContainerCount, Rest1} = decode_varint(IndexBin),
    {Containers, Rest2} = parse_containers(ContainerCount, Rest1, #{}),
    {EntryCount, Rest3} = decode_varint(Rest2),
    {Entries, Rest4} = parse_entries(EntryCount, Rest3, []),
    {TopKeyCount, Rest5} = decode_varint(Rest4),
    {TopKeys, <<>>} = parse_top_keys(TopKeyCount, Rest5, #{}),
    #parsed_index{
        containers = Containers,
        entries = list_to_tuple(Entries),
        top_keys = TopKeys
    }.

parse_containers(0, Rest, Acc) ->
    {Acc, Rest};
parse_containers(N, Bin, Acc) ->
    {ContainerId, Rest0} = decode_varint(Bin),
    <<Kind, Rest1/binary>> = Rest0,
    {StartOff, Rest2} = decode_varint(Rest1),
    {EndOff, Rest3} = decode_varint(Rest2),
    {ParentId, Rest4} = decode_varint(Rest3),
    {FirstEntry, Rest5} = decode_varint(Rest4),
    {EntryCount, Rest6} = decode_varint(Rest5),
    Container = #container{
        id = ContainerId,
        kind = case Kind of 0 -> map; 1 -> array end,
        start_off = StartOff,
        end_off = EndOff,
        parent_id = ParentId,
        first_entry = FirstEntry,
        entry_count = EntryCount
    },
    parse_containers(N - 1, Rest6, Acc#{ContainerId => Container}).

parse_entries(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
parse_entries(N, Bin, Acc) ->
    {KeyLen, Rest1} = decode_varint(Bin),
    {Key, Rest2} = case KeyLen of
        0 -> {length(Acc), Rest1};  %% Array index (implicit)
        _ ->
            <<KeyBin:KeyLen/binary, R/binary>> = Rest1,
            {KeyBin, R}
    end,
    {ValueOff, Rest3} = decode_varint(Rest2),
    {ValueLen, Rest4} = decode_varint(Rest3),
    <<TypeByte, Rest5/binary>> = Rest4,
    {ContainerId, Rest6} = decode_varint(Rest5),
    {OwnerId, Rest7} = decode_varint(Rest6),
    Entry = #entry{
        key = Key,
        value_off = ValueOff,
        value_len = ValueLen,
        value_type = byte_to_cbor_type(TypeByte),
        container_id = case ContainerId of 0 -> undefined; _ -> ContainerId end,
        owner_id = OwnerId
    },
    parse_entries(N - 1, Rest7, [Entry | Acc]).

parse_top_keys(0, Rest, Acc) ->
    {Acc, Rest};
parse_top_keys(N, Bin, Acc) ->
    {KeyLen, Rest1} = decode_varint(Bin),
    <<Key:KeyLen/binary, Rest2/binary>> = Rest1,
    {EntryIdx, Rest3} = decode_varint(Rest2),
    parse_top_keys(N - 1, Rest3, Acc#{Key => EntryIdx}).

%% Convert byte to cbor_type
byte_to_cbor_type(0) -> uint;
byte_to_cbor_type(1) -> nint;
byte_to_cbor_type(2) -> bytes;
byte_to_cbor_type(3) -> text;
byte_to_cbor_type(4) -> array;
byte_to_cbor_type(5) -> map;
byte_to_cbor_type(6) -> tag;
byte_to_cbor_type(7) -> float_type;
byte_to_cbor_type(8) -> true;
byte_to_cbor_type(9) -> false;
byte_to_cbor_type(10) -> null.

%%====================================================================
%% Iterator API
%%====================================================================

%% @doc Create a new iterator over a CBOR record.
-spec new_iterator(record_bin()) -> {ok, iter()} | {error, term()}.
new_iterator(RecordBin) ->
    {Header, IndexBin, _PayloadBin} = parse_record(RecordBin),
    PayloadStart = get_payload_start(Header),
    Index = parse_index_bin(IndexBin, PayloadStart),
    %% Start at root container (ID 1)
    case maps:get(1, Index#parsed_index.containers, undefined) of
        undefined ->
            {error, no_root_container};
        #container{} ->
            %% Get entries owned by root container
            Entries = get_entries_by_owner(Index#parsed_index.entries, 1),
            {ok, #iter{
                record = RecordBin,
                index = Index,
                payload_start = PayloadStart,
                container_id = 1,
                remaining_entries = Entries
            }}
    end.

%% @doc Get the next entry from the iterator.
-spec next(iter()) -> {ok, {binary() | non_neg_integer(), cbor_type(), vref()}, iter()} | done.
next(#iter{remaining_entries = []}) ->
    done;
next(#iter{remaining_entries = [Entry | Rest]} = Iter) ->
    VRef = #vref{
        offset = Entry#entry.value_off,
        length = Entry#entry.value_len,
        type = Entry#entry.value_type,
        container_id = Entry#entry.container_id
    },
    {ok, {Entry#entry.key, Entry#entry.value_type, VRef},
     Iter#iter{remaining_entries = Rest}}.

%% Get entries owned by a specific container
get_entries_by_owner(Entries, OwnerId) ->
    [E || E <- tuple_to_list(Entries), E#entry.owner_id =:= OwnerId].

%% @doc Peek at a top-level key without iteration.
-spec peek(record_bin(), binary()) -> {ok, {cbor_type(), vref()}} | not_found | {error, term()}.
peek(RecordBin, Key) ->
    Index = parse_index(RecordBin),
    case maps:get(Key, Index#parsed_index.top_keys, undefined) of
        undefined ->
            not_found;
        EntryIdx ->
            Entry = element(EntryIdx + 1, Index#parsed_index.entries),
            VRef = #vref{
                offset = Entry#entry.value_off,
                length = Entry#entry.value_len,
                type = Entry#entry.value_type,
                container_id = Entry#entry.container_id
            },
            {ok, {Entry#entry.value_type, VRef}}
    end.

%% @doc Find a value by path.
-spec find_path(record_bin(), path()) -> {ok, {cbor_type(), vref()}} | not_found | {error, term()}.
find_path(_RecordBin, []) ->
    {error, empty_path};
find_path(RecordBin, Path) ->
    Index = parse_index(RecordBin),
    find_path_impl(Index, 1, Path).

find_path_impl(Index, ContainerId, [Token | Rest]) ->
    case maps:get(ContainerId, Index#parsed_index.containers, undefined) of
        undefined ->
            {error, invalid_container};
        #container{} ->
            %% Find entry with matching key owned by this container
            case find_entry_by_owner(Index#parsed_index.entries, ContainerId, Token) of
                {ok, Entry} ->
                    case Rest of
                        [] ->
                            %% Found the target
                            VRef = #vref{
                                offset = Entry#entry.value_off,
                                length = Entry#entry.value_len,
                                type = Entry#entry.value_type,
                                container_id = Entry#entry.container_id
                            },
                            {ok, {Entry#entry.value_type, VRef}};
                        _ ->
                            %% Need to go deeper
                            case Entry#entry.container_id of
                                undefined ->
                                    {error, not_container};
                                CId ->
                                    find_path_impl(Index, CId, Rest)
                            end
                    end;
                not_found ->
                    not_found
            end
    end.

%% Find entry by owner container and key
find_entry_by_owner(Entries, OwnerId, Key) ->
    find_entry_by_owner_impl(tuple_to_list(Entries), OwnerId, Key).

find_entry_by_owner_impl([], _OwnerId, _Key) ->
    not_found;
find_entry_by_owner_impl([Entry | Rest], OwnerId, Key) ->
    case Entry#entry.owner_id =:= OwnerId andalso Entry#entry.key =:= Key of
        true -> {ok, Entry};
        false -> find_entry_by_owner_impl(Rest, OwnerId, Key)
    end.

%% @doc Decode a value using a ValueRef.
-spec decode_value(record_bin(), vref()) -> {ok, term()} | {error, term()}.
decode_value(RecordBin, #vref{offset = Off, length = Len}) ->
    {Header, _IndexBin, PayloadBin} = parse_record(RecordBin),
    _PayloadStart = get_payload_start(Header),
    %% Extract the value bytes from payload
    ValueBin = binary:part(PayloadBin, Off, Len),
    {ok, decode_cbor(ValueBin)}.

%%====================================================================
%% JSON Conversion
%%====================================================================

%% @doc Convert CBOR record to JSON binary.
-spec to_json(record_bin()) -> binary().
to_json(RecordBin) ->
    iolist_to_binary(to_json_iolist(RecordBin)).

%% @doc Convert CBOR record to JSON iolist using iterator (no full decode).
%% This traverses the CBOR structure via the index and emits JSON directly.
-spec to_json_iolist(record_bin()) -> iolist().
to_json_iolist(RecordBin) ->
    {Header, IndexBin, PayloadBin} = parse_record(RecordBin),
    PayloadStart = get_payload_start(Header),
    Index = parse_index_bin(IndexBin, PayloadStart),
    %% Root container is ID 1
    emit_json_container(Index, 1, PayloadBin).

%% Emit JSON for a container (map or array)
emit_json_container(Index, ContainerId, PayloadBin) ->
    case maps:get(ContainerId, Index#parsed_index.containers, undefined) of
        undefined ->
            %% Empty document, should not happen for valid records
            <<"{}">>;
        #container{kind = Kind} ->
            case Kind of
                map -> emit_json_object(Index, ContainerId, PayloadBin);
                array -> emit_json_array(Index, ContainerId, PayloadBin)
            end
    end.

%% Emit JSON object from entries owned by this container
emit_json_object(Index, ContainerId, PayloadBin) ->
    Entries = Index#parsed_index.entries,
    %% Find entries owned by this container
    OwnedEntries = [E || E <- tuple_to_list(Entries), E#entry.owner_id =:= ContainerId],
    Members = emit_json_members(OwnedEntries, Index, PayloadBin, []),
    [${ | interleave_comma(Members)] ++ [$}].

emit_json_members([], _Index, _PayloadBin, Acc) ->
    lists:reverse(Acc);
emit_json_members([Entry | Rest], Index, PayloadBin, Acc) ->
    Key = Entry#entry.key,
    KeyJson = json:encode(Key),
    ValueJson = emit_json_value(Entry, Index, PayloadBin),
    Member = [KeyJson, $:, ValueJson],
    emit_json_members(Rest, Index, PayloadBin, [Member | Acc]).

%% Emit JSON array from entries owned by this container
emit_json_array(Index, ContainerId, PayloadBin) ->
    Entries = Index#parsed_index.entries,
    %% Find entries owned by this container
    OwnedEntries = [E || E <- tuple_to_list(Entries), E#entry.owner_id =:= ContainerId],
    Elements = emit_json_elements(OwnedEntries, Index, PayloadBin, []),
    [$[ | interleave_comma(Elements)] ++ [$]].

emit_json_elements([], _Index, _PayloadBin, Acc) ->
    lists:reverse(Acc);
emit_json_elements([Entry | Rest], Index, PayloadBin, Acc) ->
    ValueJson = emit_json_value(Entry, Index, PayloadBin),
    emit_json_elements(Rest, Index, PayloadBin, [ValueJson | Acc]).

%% Emit JSON value based on entry type
emit_json_value(#entry{value_off = Off, value_len = Len, value_type = Type,
                       container_id = ContainerId}, Index, PayloadBin) ->
    case Type of
        map ->
            emit_json_container(Index, ContainerId, PayloadBin);
        array ->
            emit_json_container(Index, ContainerId, PayloadBin);
        _ ->
            %% For primitives, extract and decode the value, then JSON encode
            ValueBin = binary:part(PayloadBin, Off, Len),
            Value = decode_cbor(ValueBin),
            json:encode(Value)
    end.

%% Interleave commas between elements
interleave_comma([]) -> [];
interleave_comma([H]) -> [H];
interleave_comma([H | T]) -> [H, $, | interleave_comma(T)].

%% @doc Parse JSON and encode to CBOR record using callbacks.
%% This builds CBOR directly during JSON parsing without intermediate terms.
-spec from_json(binary()) -> record_bin().
from_json(JsonBin) ->
    %% Use standard decode then encode - the callback approach requires
    %% more complex state management for building the index during parse.
    %% The current approach is straightforward and performs well.
    Term = json:decode(JsonBin),
    encode(Term).

%%====================================================================
%% Map-like API
%%====================================================================

%% @doc Get a value at path without full decode, returns undefined if not found.
%% Works like maps:get/2 but for CBOR records.
-spec get(record_bin(), path()) -> term() | undefined.
get(RecordBin, Path) ->
    get(RecordBin, Path, undefined).

%% @doc Get a value at path without full decode, returns Default if not found.
%% Works like maps:get/3 but for CBOR records.
-spec get(record_bin(), path(), term()) -> term().
get(RecordBin, Path, Default) ->
    case find_path(RecordBin, Path) of
        {ok, {_Type, VRef}} ->
            {ok, Value} = decode_value(RecordBin, VRef),
            Value;
        not_found ->
            Default;
        {error, _} ->
            Default
    end.

%% @doc Get and decode value at path in one call.
%% Returns {ok, Value} or not_found.
-spec get_value(record_bin(), path()) -> {ok, term()} | not_found | {error, term()}.
get_value(RecordBin, Path) ->
    case find_path(RecordBin, Path) of
        {ok, {_Type, VRef}} ->
            decode_value(RecordBin, VRef);
        not_found ->
            not_found;
        {error, _} = Err ->
            Err
    end.

%% @doc Set a value at path, returns new CBOR record.
%% Note: This requires decode → modify → re-encode, so it's not optimized.
%% Use for occasional updates, not bulk operations.
-spec set(record_bin(), path(), term()) -> record_bin().
set(RecordBin, Path, Value) ->
    Map = decode(RecordBin),
    NewMap = set_path(Map, Path, Value),
    encode(NewMap).

%% @doc Convert CBOR record to Erlang map (full decode).
%% Alias for decode/1.
-spec to_map(record_bin()) -> map().
to_map(RecordBin) ->
    decode(RecordBin).

%% @doc Get all top-level keys without full decode.
%% Uses the index for O(1) access to key list.
-spec keys(record_bin()) -> [binary()].
keys(RecordBin) ->
    Index = parse_index(RecordBin),
    maps:keys(Index#parsed_index.top_keys).

%% @doc Get number of top-level entries without full decode.
%% Uses the index for O(1) access.
-spec size(record_bin()) -> non_neg_integer().
size(RecordBin) ->
    Index = parse_index(RecordBin),
    maps:size(Index#parsed_index.top_keys).

%% @doc Check if a key exists at top level without full decode.
%% Uses the index for O(1) lookup.
-spec is_key(record_bin(), binary()) -> boolean().
is_key(RecordBin, Key) ->
    Index = parse_index(RecordBin),
    maps:is_key(Key, Index#parsed_index.top_keys).

%% Internal: set value at path in nested map
set_path(Map, [Key], Value) when is_map(Map) ->
    Map#{Key => Value};
set_path(Map, [Key | Rest], Value) when is_map(Map) ->
    SubMap = maps:get(Key, Map, #{}),
    Map#{Key => set_path(SubMap, Rest, Value)};
set_path(_, [], _Value) ->
    error(empty_path).

%%====================================================================
%% CBOR Manipulation
%%====================================================================

%% @doc Merge metadata map into raw CBOR document body.
%% This is used for zero-copy CBOR responses where we have raw CBOR body
%% and need to add metadata (id, rev, etc.) without full decode/re-encode.
%%
%% For plain CBOR (non-indexed), we decode, merge, and re-encode.
%% This is still efficient as it avoids the HTTP-layer overhead.
-spec merge_into_cbor(binary(), map()) -> binary().
merge_into_cbor(CborBin, MetaMap) ->
    %% Decode the body CBOR to map
    BodyMap = decode_any(CborBin),
    %% Merge metadata (metadata keys take precedence)
    MergedMap = maps:merge(BodyMap, MetaMap),
    %% Re-encode as plain CBOR
    encode_cbor(MergedMap).
