%%%-------------------------------------------------------------------
%%% @doc Document utilities for barrel_docdb
%%%
%%% Provides functions for document manipulation and CBOR document
%%% content access.
%%%
%%% The module supports two document representations:
%%% - Indexed binary: CBOR with structural index for O(1) path access
%%% - Erlang map: Standard map for manipulation
%%%
%%% All map-like functions work with both representations transparently.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc).

-include("barrel_docdb.hrl").

%% Document accessors (metadata)
-export([id/1, rev/1, id_rev/1, deleted/1]).

%% Document processing
-export([
    make_doc_record/1,
    doc_without_meta/1
]).

%% Embedding column codec (32-bit floats, matching the vector stores)
-export([encode_embedding/1, decode_embedding/1]).

%% Unique ID generation
-export([generate_docid/0]).

%% CBOR Document Construction & Import
-export([
    new/0, new/1,
    from_map/1,
    from_json/1,
    from_cbor/1
]).

%% CBOR Document Export
-export([
    to_map/1,
    to_json/1,
    to_cbor/1
]).

%% CBOR Map-like API
-export([
    get/2, get/3,
    set/3,
    remove/2,
    update/3,
    is_key/2,
    keys/1,
    size/1,
    find/2,
    values/1,
    to_list/1,
    take/2,
    merge/2,
    fold/3,
    map/2,
    filter/2
]).

%% Index utilities
-export([
    is_indexed/1,
    normalize/1,
    ensure_indexed/1
]).

%% Type definitions
-type cbor_doc() :: binary().
-type doc_input() :: map() | cbor_doc().
-type path() :: [binary() | integer()].

-export_type([cbor_doc/0, doc_input/0, path/0]).

%%====================================================================
%% Document Accessors
%%====================================================================

%% @doc Get document ID
-spec id(doc()) -> docid() | undefined.
id(#{<<"id">> := Id}) -> Id;
id(#{}) -> undefined;
id(_) -> erlang:error(bad_doc).

%% @doc Get document revision
-spec rev(doc()) -> revid().
rev(#{<<"_rev">> := Rev}) -> Rev;
rev(#{}) -> <<>>;
rev(_) -> error(bad_doc).

%% @doc Get document ID and revision
-spec id_rev(doc()) -> {docid() | undefined, revid()}.
id_rev(#{<<"id">> := Id, <<"_rev">> := Rev}) -> {Id, Rev};
id_rev(#{<<"id">> := Id}) -> {Id, <<>>};
id_rev(#{<<"_rev">> := _Rev}) -> erlang:error(bad_doc);
id_rev(#{}) -> {undefined, <<>>};
id_rev(_) -> erlang:error(bad_doc).

%% @doc Check if document is deleted
-spec deleted(doc()) -> boolean().
deleted(#{<<"_deleted">> := Del}) when is_boolean(Del) -> Del;
deleted(_) -> false.

%% Rev-tree revision helpers ("Gen-Hash" parsing, revision hashing,
%% history codecs) were removed with the version-vector pivot: `_rev'
%% is an opaque barrel_version token now.

%%====================================================================
%% Document Processing
%%====================================================================

%% @doc Remove metadata fields from document.
%% `_'-prefixed top-level fields are reserved metadata and are stripped
%% before storage. `<<"_embedding">>' is extracted separately (see
%% make_doc_record) and stored as a document entity column, never in the
%% body, so it does not affect the revision hash.
-spec doc_without_meta(doc()) -> doc().
doc_without_meta(Doc) ->
    maps:filter(
        fun
            (<<"_attachments">>, _) -> false;
            (<<"_", _/binary>>, _) -> false;
            (_, _) -> true
        end,
        Doc
    ).

%% @doc Create internal document record from user document.
%% The document's version identity is issued by the writer
%% (barrel_db_server); the record only carries the caller's expected
%% current version (`_rev' token) for the CAS check.
-spec make_doc_record(doc()) -> map().
make_doc_record(Doc0) ->
    Deleted = maps:get(<<"_deleted">>, Doc0, false),
    ExpectedVersion = case maps:get(<<"_rev">>, Doc0, <<>>) of
        <<>> -> undefined;
        Token -> Token
    end,
    Atts = maps:get(<<"_attachments">>, Doc0, #{}),
    Id = case maps:find(<<"id">>, Doc0) of
        {ok, DocId} -> DocId;
        error -> generate_docid()
    end,
    Doc1 = doc_without_meta(Doc0),
    Record = #{
        id => Id,
        ref => erlang:make_ref(),
        expected_version => ExpectedVersion,
        deleted => Deleted,
        attachments => Atts,
        doc => Doc1
    },
    %% A carried embedding is derived data stored as an entity column
    %% (never in the body). Accepted shapes: a bare vector
    %% (client-supplied shorthand) or an object
    %% #{<<"vector">> => [...], <<"source">> => <<"client">>|<<"computed">>}
    %% as returned by reads with include_embedding.
    case parse_embedding(maps:get(<<"_embedding">>, Doc0, undefined)) of
        {Vector, Src} ->
            Record#{embedding => Vector, embedding_src => Src};
        undefined ->
            Record
    end.

%% @private Parse the _embedding property (vector shorthand or object).
parse_embedding(Vector) when is_list(Vector) ->
    {Vector, <<"client">>};
parse_embedding(#{<<"vector">> := Vector} = Obj) when is_list(Vector) ->
    Src = case maps:get(<<"source">>, Obj, <<"client">>) of
        <<"computed">> -> <<"computed">>;
        _ -> <<"client">>
    end,
    {Vector, Src};
parse_embedding(_) ->
    undefined.

%%====================================================================
%% Embedding column codec
%%====================================================================

%% @doc Encode an embedding vector for entity-column storage.
%% 32-bit little-endian floats, the same width the vector stores use.
-spec encode_embedding([number()]) -> binary().
encode_embedding(Vector) when is_list(Vector) ->
    << <<F:32/float-little>> || F <- Vector >>.

%% @doc Decode an embedding entity column back to a vector.
-spec decode_embedding(binary()) -> [float()].
decode_embedding(Binary) when is_binary(Binary) ->
    [F || <<F:32/float-little>> <= Binary].

%%====================================================================
%% ID Generation
%%====================================================================

%% @doc Generate a unique document ID
-spec generate_docid() -> docid().
generate_docid() ->
    %% Use a combination of timestamp and random bytes
    Now = erlang:system_time(microsecond),
    Random = crypto:strong_rand_bytes(8),
    Data = <<Now:64, Random/binary>>,
    to_hex(crypto:hash(md5, Data)).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Convert binary to lowercase hex string
-spec to_hex(binary()) -> binary().
to_hex(Bin) ->
    << <<(hex_char(N))>> || <<N:4>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.

%%====================================================================
%% CBOR Document Construction & Import
%%====================================================================

%% @doc Create empty indexed document
-spec new() -> cbor_doc().
new() ->
    barrel_docdb_codec_cbor:encode(#{}).

%% @doc Create indexed document from Erlang map
-spec new(map()) -> cbor_doc().
new(Map) when is_map(Map) ->
    barrel_docdb_codec_cbor:encode(Map).

%% @doc Import from Erlang map (alias for new/1)
-spec from_map(map()) -> cbor_doc().
from_map(Map) ->
    new(Map).

%% @doc Import from JSON binary
-spec from_json(binary()) -> cbor_doc().
from_json(JsonBin) when is_binary(JsonBin) ->
    Map = json:decode(JsonBin),
    new(Map).

%% @doc Import from plain CBOR (adds index if missing)
-spec from_cbor(binary()) -> cbor_doc().
from_cbor(CborBin) when is_binary(CborBin) ->
    normalize(CborBin).

%%====================================================================
%% CBOR Document Export
%%====================================================================

%% @doc Export to Erlang map (full decode)
%% Handles both indexed CBOR and plain CBOR
-spec to_map(doc_input()) -> map().
to_map(Doc) when is_map(Doc) ->
    Doc;
to_map(Doc) when is_binary(Doc) ->
    barrel_docdb_codec_cbor:decode_any(Doc).

%% @doc Export to JSON binary
-spec to_json(doc_input()) -> binary().
to_json(Doc) when is_map(Doc) ->
    iolist_to_binary(json:encode(Doc));
to_json(<<"CB", _/binary>> = Doc) ->
    %% Indexed CBOR - use optimized to_json
    barrel_docdb_codec_cbor:to_json(Doc);
to_json(Doc) when is_binary(Doc) ->
    %% Plain CBOR - decode then encode as JSON
    iolist_to_binary(json:encode(barrel_docdb_codec_cbor:decode_cbor(Doc))).

%% @doc Export to plain CBOR (without index)
%% Use when sending to external clients
-spec to_cbor(doc_input()) -> binary().
to_cbor(Doc) when is_map(Doc) ->
    barrel_docdb_codec_cbor:encode_cbor(Doc);
to_cbor(<<"CB", _/binary>> = Doc) ->
    %% Indexed CBOR - extract payload
    barrel_docdb_codec_cbor:payload(Doc);
to_cbor(Doc) when is_binary(Doc) ->
    %% Already plain CBOR
    Doc.

%%====================================================================
%% CBOR Map-like API
%%====================================================================

%% @doc Get value at path (lazy - uses index when available)
-spec get(doc_input(), path()) -> term() | undefined.
get(Doc, Path) ->
    get(Doc, Path, undefined).

%% @doc Get value at path with default
-spec get(doc_input(), path(), term()) -> term().
get(Doc, Path, Default) when is_map(Doc) ->
    get_from_map(Doc, Path, Default);
get(Doc, Path, Default) when is_binary(Doc) ->
    barrel_docdb_codec_cbor:get(Doc, Path, Default).

%% @doc Set value at path (returns indexed binary)
-spec set(doc_input(), path(), term()) -> cbor_doc().
set(Doc, Path, Value) when is_map(Doc) ->
    NewMap = set_in_map(Doc, Path, Value),
    barrel_docdb_codec_cbor:encode(NewMap);
set(Doc, Path, Value) when is_binary(Doc) ->
    barrel_docdb_codec_cbor:set(Doc, Path, Value).

%% @doc Check if key exists at top level (uses index, no decode)
-spec is_key(doc_input(), binary()) -> boolean().
is_key(Doc, Key) when is_map(Doc) ->
    maps:is_key(Key, Doc);
is_key(Doc, Key) when is_binary(Doc) ->
    barrel_docdb_codec_cbor:is_key(Doc, Key).

%% @doc Get all top-level keys (uses index when available)
-spec keys(doc_input()) -> [binary()].
keys(Doc) when is_map(Doc) ->
    maps:keys(Doc);
keys(Doc) when is_binary(Doc) ->
    barrel_docdb_codec_cbor:keys(Doc).

%% @doc Get number of top-level entries
-spec size(doc_input()) -> non_neg_integer().
size(Doc) when is_map(Doc) ->
    maps:size(Doc);
size(Doc) when is_binary(Doc) ->
    barrel_docdb_codec_cbor:size(Doc).

%% @doc Remove a key from the document (returns indexed binary)
-spec remove(doc_input(), binary()) -> cbor_doc().
remove(Doc, Key) when is_map(Doc) ->
    barrel_docdb_codec_cbor:encode(maps:remove(Key, Doc));
remove(Doc, Key) when is_binary(Doc) ->
    Map = to_map(Doc),
    barrel_docdb_codec_cbor:encode(maps:remove(Key, Map)).

%% @doc Update value at path using a function (returns indexed binary)
%% Fun is called with current value (or undefined if not present)
-spec update(doc_input(), path(), fun((term()) -> term())) -> cbor_doc().
update(Doc, Path, Fun) when is_function(Fun, 1) ->
    CurrentValue = get(Doc, Path, undefined),
    NewValue = Fun(CurrentValue),
    set(Doc, Path, NewValue).

%% @doc Find a key (like maps:find/2)
%% Returns {ok, Value} if found, error if not
-spec find(doc_input(), binary()) -> {ok, term()} | error.
find(Doc, Key) when is_map(Doc) ->
    maps:find(Key, Doc);
find(Doc, Key) when is_binary(Doc) ->
    case get(Doc, [Key], '$barrel_not_found$') of
        '$barrel_not_found$' -> error;
        Value -> {ok, Value}
    end.

%% @doc Get all values (top-level only)
-spec values(doc_input()) -> [term()].
values(Doc) when is_map(Doc) ->
    maps:values(Doc);
values(Doc) when is_binary(Doc) ->
    maps:values(to_map(Doc)).

%% @doc Convert to list of {Key, Value} tuples (top-level only)
-spec to_list(doc_input()) -> [{binary(), term()}].
to_list(Doc) when is_map(Doc) ->
    maps:to_list(Doc);
to_list(Doc) when is_binary(Doc) ->
    maps:to_list(to_map(Doc)).

%% @doc Remove and return value at key
%% Returns {Value, UpdatedDoc} or error if key not found
-spec take(doc_input(), binary()) -> {term(), cbor_doc()} | error.
take(Doc, Key) ->
    Map = to_map(Doc),
    case maps:take(Key, Map) of
        {Value, NewMap} ->
            {Value, barrel_docdb_codec_cbor:encode(NewMap)};
        error ->
            error
    end.

%% @doc Merge two documents (second overwrites first)
%% Returns indexed binary
-spec merge(doc_input(), doc_input()) -> cbor_doc().
merge(Doc1, Doc2) ->
    Map1 = to_map(Doc1),
    Map2 = to_map(Doc2),
    barrel_docdb_codec_cbor:encode(maps:merge(Map1, Map2)).

%% @doc Fold over top-level key-value pairs
-spec fold(fun((binary(), term(), Acc) -> Acc), Acc, doc_input()) -> Acc.
fold(Fun, Init, Doc) when is_map(Doc) ->
    maps:fold(Fun, Init, Doc);
fold(Fun, Init, Doc) when is_binary(Doc) ->
    maps:fold(Fun, Init, to_map(Doc)).

%% @doc Map a function over all top-level values
%% Returns indexed binary
-spec map(fun((binary(), term()) -> term()), doc_input()) -> cbor_doc().
map(Fun, Doc) when is_function(Fun, 2) ->
    Map = to_map(Doc),
    NewMap = maps:map(Fun, Map),
    barrel_docdb_codec_cbor:encode(NewMap).

%% @doc Filter key-value pairs using a predicate
%% Returns indexed binary
-spec filter(fun((binary(), term()) -> boolean()), doc_input()) -> cbor_doc().
filter(Pred, Doc) when is_function(Pred, 2) ->
    Map = to_map(Doc),
    NewMap = maps:filter(Pred, Map),
    barrel_docdb_codec_cbor:encode(NewMap).

%%====================================================================
%% Index Utilities
%%====================================================================

%% @doc Check if binary has barrel index (starts with "CB" magic)
-spec is_indexed(binary() | map()) -> boolean().
is_indexed(<<"CB", _/binary>>) -> true;
is_indexed(_) -> false.

%% @doc Normalize any input to indexed binary (for storage)
%% Auto-detects: map | indexed binary | plain CBOR
-spec normalize(doc_input()) -> cbor_doc().
normalize(Doc) when is_map(Doc) ->
    barrel_docdb_codec_cbor:encode(Doc);
normalize(Doc) when is_binary(Doc) ->
    case is_indexed(Doc) of
        true -> Doc;
        false ->
            %% Plain CBOR -> decode and re-encode with index
            Map = barrel_docdb_codec_cbor:decode_cbor(Doc),
            barrel_docdb_codec_cbor:encode(Map)
    end.

%% @doc Alias for normalize/1
-spec ensure_indexed(doc_input()) -> cbor_doc().
ensure_indexed(Doc) ->
    normalize(Doc).

%%====================================================================
%% Internal: Map Path Operations
%%====================================================================

%% @private Get value from nested map by path
get_from_map(Map, [], _Default) ->
    Map;
get_from_map(Map, [Key | Rest], Default) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, Value} -> get_from_map(Value, Rest, Default);
        error -> Default
    end;
get_from_map(List, [Index | Rest], Default) when is_list(List), is_integer(Index) ->
    case Index >= 0 andalso Index < length(List) of
        true -> get_from_map(lists:nth(Index + 1, List), Rest, Default);
        false -> Default
    end;
get_from_map(_, _, Default) ->
    Default.

%% @private Set value in nested map by path
set_in_map(Map, [Key], Value) when is_map(Map) ->
    Map#{Key => Value};
set_in_map(Map, [Key | Rest], Value) when is_map(Map) ->
    SubMap = maps:get(Key, Map, #{}),
    Map#{Key => set_in_map(SubMap, Rest, Value)};
set_in_map(_, [], _Value) ->
    error(empty_path).
