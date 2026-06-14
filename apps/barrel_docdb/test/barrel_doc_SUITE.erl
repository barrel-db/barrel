%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb document core
%%%
%%% Tests barrel_doc module.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).

%% Test cases - barrel_doc metadata
-export([
    doc_id/1,
    doc_rev/1,
    doc_deleted/1,
    parse_revision/1,
    make_revision/1,
    revision_hash/1,
    compare_revisions/1,
    encode_revisions/1,
    parse_revisions/1,
    doc_without_meta/1,
    make_doc_record/1,
    generate_docid/1
]).

%% Test cases - CBOR document handling
-export([
    cbor_new/1,
    cbor_from_map/1,
    cbor_from_json/1,
    cbor_to_map/1,
    cbor_to_json/1,
    cbor_to_cbor/1,
    cbor_is_indexed/1,
    cbor_normalize/1
]).

%% Test cases - CBOR map-like API
-export([
    cbor_get/1,
    cbor_set/1,
    cbor_is_key/1,
    cbor_keys/1,
    cbor_size/1,
    cbor_remove/1,
    cbor_update/1,
    cbor_find/1,
    cbor_values/1,
    cbor_to_list/1,
    cbor_take/1,
    cbor_merge/1,
    cbor_fold/1,
    cbor_map/1,
    cbor_filter/1
]).


%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, doc}, {group, cbor}, {group, cbor_api}].

groups() ->
    [
        {doc, [sequence], [
            doc_id,
            doc_rev,
            doc_deleted,
            parse_revision,
            make_revision,
            revision_hash,
            compare_revisions,
            encode_revisions,
            parse_revisions,
            doc_without_meta,
            make_doc_record,
            generate_docid
        ]},
        {cbor, [sequence], [
            cbor_new,
            cbor_from_map,
            cbor_from_json,
            cbor_to_map,
            cbor_to_json,
            cbor_to_cbor,
            cbor_is_indexed,
            cbor_normalize
        ]},
        {cbor_api, [sequence], [
            cbor_get,
            cbor_set,
            cbor_is_key,
            cbor_keys,
            cbor_size,
            cbor_remove,
            cbor_update,
            cbor_find,
            cbor_values,
            cbor_to_list,
            cbor_take,
            cbor_merge,
            cbor_fold,
            cbor_map,
            cbor_filter
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%====================================================================
%% Test Cases - barrel_doc
%%====================================================================

doc_id(_Config) ->
    %% Document with id
    Doc1 = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>},
    ?assertEqual(<<"doc1">>, barrel_doc:id(Doc1)),

    %% Document without id
    Doc2 = #{<<"name">> => <<"test">>},
    ?assertEqual(undefined, barrel_doc:id(Doc2)),

    ok.

doc_rev(_Config) ->
    %% Document with rev
    Doc1 = #{<<"id">> => <<"doc1">>, <<"_rev">> => <<"1-abc">>},
    ?assertEqual(<<"1-abc">>, barrel_doc:rev(Doc1)),

    %% Document without rev
    Doc2 = #{<<"id">> => <<"doc1">>},
    ?assertEqual(<<>>, barrel_doc:rev(Doc2)),

    ok.

doc_deleted(_Config) ->
    %% Deleted document
    Doc1 = #{<<"id">> => <<"doc1">>, <<"_deleted">> => true},
    ?assertEqual(true, barrel_doc:deleted(Doc1)),

    %% Not deleted
    Doc2 = #{<<"id">> => <<"doc1">>, <<"_deleted">> => false},
    ?assertEqual(false, barrel_doc:deleted(Doc2)),

    %% No deleted flag
    Doc3 = #{<<"id">> => <<"doc1">>},
    ?assertEqual(false, barrel_doc:deleted(Doc3)),

    ok.

parse_revision(_Config) ->
    %% Normal revision
    ?assertEqual({10, <<"abc123">>}, barrel_doc:parse_revision(<<"10-abc123">>)),

    %% First generation
    ?assertEqual({1, <<"xyz">>}, barrel_doc:parse_revision(<<"1-xyz">>)),

    %% Empty revision
    ?assertEqual({0, <<>>}, barrel_doc:parse_revision(<<>>)),

    %% String revision
    ?assertEqual({5, <<"def">>}, barrel_doc:parse_revision("5-def")),

    ok.

make_revision(_Config) ->
    Rev = barrel_doc:make_revision(3, <<"abc123">>),
    ?assertEqual(<<"3-abc123">>, Rev),

    Rev2 = barrel_doc:make_revision(1, <<"xyz">>),
    ?assertEqual(<<"1-xyz">>, Rev2),

    ok.

revision_hash(_Config) ->
    Doc = #{<<"name">> => <<"test">>},

    %% Hash is deterministic
    Hash1 = barrel_doc:revision_hash(Doc, <<>>, false),
    Hash2 = barrel_doc:revision_hash(Doc, <<>>, false),
    ?assertEqual(Hash1, Hash2),

    %% Different content produces different hash
    Doc2 = #{<<"name">> => <<"other">>},
    Hash3 = barrel_doc:revision_hash(Doc2, <<>>, false),
    ?assertNotEqual(Hash1, Hash3),

    %% Deleted flag affects hash
    Hash4 = barrel_doc:revision_hash(Doc, <<>>, true),
    ?assertNotEqual(Hash1, Hash4),

    %% Hash is hex encoded
    ?assert(is_binary(Hash1)),
    ?assertEqual(64, byte_size(Hash1)),  % SHA-256 = 32 bytes = 64 hex chars

    ok.

compare_revisions(_Config) ->
    %% Higher generation wins
    ?assertEqual(1, barrel_doc:compare_revisions(<<"2-abc">>, <<"1-abc">>)),
    ?assertEqual(-1, barrel_doc:compare_revisions(<<"1-abc">>, <<"2-abc">>)),

    %% Same generation, compare hash
    ?assertEqual(1, barrel_doc:compare_revisions(<<"2-xyz">>, <<"2-abc">>)),
    ?assertEqual(-1, barrel_doc:compare_revisions(<<"2-abc">>, <<"2-xyz">>)),

    %% Equal
    ?assertEqual(0, barrel_doc:compare_revisions(<<"1-abc">>, <<"1-abc">>)),

    ok.

encode_revisions(_Config) ->
    Revs = [<<"3-ccc">>, <<"2-bbb">>, <<"1-aaa">>],
    Encoded = barrel_doc:encode_revisions(Revs),

    ?assertEqual(3, maps:get(<<"start">>, Encoded)),
    ?assertEqual([<<"ccc">>, <<"bbb">>, <<"aaa">>], maps:get(<<"ids">>, Encoded)),

    %% Empty list
    Empty = barrel_doc:encode_revisions([]),
    ?assertEqual(0, maps:get(<<"start">>, Empty)),
    ?assertEqual([], maps:get(<<"ids">>, Empty)),

    ok.

parse_revisions(_Config) ->
    %% With revisions format
    Doc1 = #{
        <<"revisions">> => #{
            <<"start">> => 3,
            <<"ids">> => [<<"ccc">>, <<"bbb">>, <<"aaa">>]
        }
    },
    Revs1 = barrel_doc:parse_revisions(Doc1),
    ?assertEqual([<<"3-ccc">>, <<"2-bbb">>, <<"1-aaa">>], Revs1),

    %% With just _rev
    Doc2 = #{<<"_rev">> => <<"2-xyz">>},
    Revs2 = barrel_doc:parse_revisions(Doc2),
    ?assertEqual([<<"2-xyz">>], Revs2),

    %% Empty
    Revs3 = barrel_doc:parse_revisions(#{}),
    ?assertEqual([], Revs3),

    ok.

doc_without_meta(_Config) ->
    Doc = #{
        <<"id">> => <<"doc1">>,
        <<"name">> => <<"test">>,
        <<"_rev">> => <<"1-abc">>,
        <<"_deleted">> => false,
        <<"_attachments">> => #{}
    },

    Clean = barrel_doc:doc_without_meta(Doc),

    %% id is kept (not metadata)
    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, Clean)),
    ?assertEqual(<<"test">>, maps:get(<<"name">>, Clean)),

    %% Meta fields removed
    ?assertEqual(error, maps:find(<<"_rev">>, Clean)),
    ?assertEqual(error, maps:find(<<"_deleted">>, Clean)),
    ?assertEqual(error, maps:find(<<"_attachments">>, Clean)),

    ok.

make_doc_record(_Config) ->
    %% New document
    Doc1 = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>},
    Record1 = barrel_doc:make_doc_record(Doc1),

    ?assertEqual(<<"doc1">>, maps:get(id, Record1)),
    ?assertEqual(false, maps:get(deleted, Record1)),
    ?assertEqual(#{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>}, maps:get(doc, Record1)),
    ?assert(is_reference(maps:get(ref, Record1))),

    %% First revision
    [Rev1] = maps:get(revs, Record1),
    {Gen, _Hash} = barrel_doc:parse_revision(Rev1),
    ?assertEqual(1, Gen),

    %% Update with existing rev
    Doc2 = #{<<"id">> => <<"doc1">>, <<"_rev">> => <<"1-abc">>, <<"name">> => <<"updated">>},
    Record2 = barrel_doc:make_doc_record(Doc2),

    [NewRev, OldRev] = maps:get(revs, Record2),
    ?assertEqual(<<"1-abc">>, OldRev),
    {Gen2, _} = barrel_doc:parse_revision(NewRev),
    ?assertEqual(2, Gen2),

    ok.

generate_docid(_Config) ->
    Id1 = barrel_doc:generate_docid(),
    Id2 = barrel_doc:generate_docid(),

    %% IDs are binary
    ?assert(is_binary(Id1)),
    ?assert(is_binary(Id2)),

    %% IDs are unique
    ?assertNotEqual(Id1, Id2),

    %% IDs are hex encoded
    ?assertEqual(32, byte_size(Id1)),  % MD5 = 16 bytes = 32 hex chars

    ok.

%%====================================================================
%% Test Cases - CBOR Document Handling
%%====================================================================

cbor_new(_Config) ->
    %% Empty document
    Doc1 = barrel_doc:new(),
    ?assert(is_binary(Doc1)),
    ?assert(barrel_doc:is_indexed(Doc1)),
    ?assertEqual(#{}, barrel_doc:to_map(Doc1)),

    %% From map
    Map = #{<<"name">> => <<"test">>, <<"value">> => 42},
    Doc2 = barrel_doc:new(Map),
    ?assert(is_binary(Doc2)),
    ?assert(barrel_doc:is_indexed(Doc2)),
    ?assertEqual(Map, barrel_doc:to_map(Doc2)),

    ok.

cbor_from_map(_Config) ->
    Map = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>},
    Doc = barrel_doc:from_map(Map),

    ?assert(is_binary(Doc)),
    ?assert(barrel_doc:is_indexed(Doc)),
    ?assertEqual(Map, barrel_doc:to_map(Doc)),

    ok.

cbor_from_json(_Config) ->
    Json = <<"{\"id\":\"doc1\",\"name\":\"test\",\"count\":123}">>,
    Doc = barrel_doc:from_json(Json),

    ?assert(is_binary(Doc)),
    ?assert(barrel_doc:is_indexed(Doc)),

    Map = barrel_doc:to_map(Doc),
    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, Map)),
    ?assertEqual(<<"test">>, maps:get(<<"name">>, Map)),
    ?assertEqual(123, maps:get(<<"count">>, Map)),

    ok.

cbor_to_map(_Config) ->
    %% From binary doc
    Map = #{<<"id">> => <<"doc1">>, <<"nested">> => #{<<"key">> => <<"value">>}},
    Doc = barrel_doc:from_map(Map),

    Result = barrel_doc:to_map(Doc),
    ?assertEqual(Map, Result),

    %% Passthrough for maps
    ?assertEqual(Map, barrel_doc:to_map(Map)),

    ok.

cbor_to_json(_Config) ->
    Map = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>},
    Doc = barrel_doc:from_map(Map),

    %% From binary doc
    Json1 = barrel_doc:to_json(Doc),
    ?assert(is_binary(Json1)),
    %% Decode to verify
    Decoded1 = json:decode(Json1),
    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, Decoded1)),
    ?assertEqual(<<"test">>, maps:get(<<"name">>, Decoded1)),

    %% From map directly
    Json2 = barrel_doc:to_json(Map),
    ?assert(is_binary(Json2)),

    ok.

cbor_to_cbor(_Config) ->
    Map = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>},
    Doc = barrel_doc:from_map(Map),

    %% Export to plain CBOR (no index)
    PlainCbor = barrel_doc:to_cbor(Doc),
    ?assert(is_binary(PlainCbor)),
    ?assertNot(barrel_doc:is_indexed(PlainCbor)),

    %% Plain CBOR should be smaller (no index overhead)
    ?assert(byte_size(PlainCbor) < byte_size(Doc)),

    %% From map directly
    PlainCbor2 = barrel_doc:to_cbor(Map),
    ?assert(is_binary(PlainCbor2)),
    ?assertNot(barrel_doc:is_indexed(PlainCbor2)),

    ok.

cbor_is_indexed(_Config) ->
    Map = #{<<"id">> => <<"doc1">>},

    %% Indexed binary
    IndexedDoc = barrel_doc:from_map(Map),
    ?assert(barrel_doc:is_indexed(IndexedDoc)),

    %% Plain CBOR
    PlainCbor = barrel_doc:to_cbor(Map),
    ?assertNot(barrel_doc:is_indexed(PlainCbor)),

    %% Map
    ?assertNot(barrel_doc:is_indexed(Map)),

    %% Random binary
    ?assertNot(barrel_doc:is_indexed(<<"random">>)),

    ok.

cbor_normalize(_Config) ->
    Map = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>},

    %% From map
    Doc1 = barrel_doc:normalize(Map),
    ?assert(is_binary(Doc1)),
    ?assert(barrel_doc:is_indexed(Doc1)),
    ?assertEqual(Map, barrel_doc:to_map(Doc1)),

    %% From already indexed binary
    Doc2 = barrel_doc:normalize(Doc1),
    ?assertEqual(Doc1, Doc2),

    %% From plain CBOR
    PlainCbor = barrel_doc:to_cbor(Map),
    Doc3 = barrel_doc:normalize(PlainCbor),
    ?assert(barrel_doc:is_indexed(Doc3)),
    ?assertEqual(Map, barrel_doc:to_map(Doc3)),

    %% ensure_indexed is alias
    Doc4 = barrel_doc:ensure_indexed(Map),
    ?assert(barrel_doc:is_indexed(Doc4)),

    ok.

%%====================================================================
%% Test Cases - CBOR Map-like API
%%====================================================================

cbor_get(_Config) ->
    Map = #{<<"name">> => <<"test">>, <<"nested">> => #{<<"key">> => <<"value">>}},
    Doc = barrel_doc:from_map(Map),

    %% Get with path from binary doc
    ?assertEqual(<<"test">>, barrel_doc:get(Doc, [<<"name">>])),
    ?assertEqual(<<"value">>, barrel_doc:get(Doc, [<<"nested">>, <<"key">>])),

    %% Get with default
    ?assertEqual(undefined, barrel_doc:get(Doc, [<<"missing">>])),
    ?assertEqual(42, barrel_doc:get(Doc, [<<"missing">>], 42)),

    %% Get from map
    ?assertEqual(<<"test">>, barrel_doc:get(Map, [<<"name">>])),
    ?assertEqual(<<"default">>, barrel_doc:get(Map, [<<"missing">>], <<"default">>)),

    ok.

cbor_set(_Config) ->
    Map = #{<<"name">> => <<"test">>},
    Doc = barrel_doc:from_map(Map),

    %% Set on binary doc
    Doc2 = barrel_doc:set(Doc, [<<"count">>], 42),
    ?assert(is_binary(Doc2)),
    ?assert(barrel_doc:is_indexed(Doc2)),
    ?assertEqual(42, barrel_doc:get(Doc2, [<<"count">>])),
    ?assertEqual(<<"test">>, barrel_doc:get(Doc2, [<<"name">>])),

    %% Set nested path
    Doc3 = barrel_doc:set(Doc, [<<"nested">>, <<"key">>], <<"value">>),
    ?assertEqual(<<"value">>, barrel_doc:get(Doc3, [<<"nested">>, <<"key">>])),

    %% Set on map
    Doc4 = barrel_doc:set(Map, [<<"new">>], <<"field">>),
    ?assert(is_binary(Doc4)),
    ?assertEqual(<<"field">>, barrel_doc:get(Doc4, [<<"new">>])),

    ok.

cbor_is_key(_Config) ->
    Map = #{<<"name">> => <<"test">>, <<"count">> => 42},
    Doc = barrel_doc:from_map(Map),

    %% On binary doc
    ?assert(barrel_doc:is_key(Doc, <<"name">>)),
    ?assert(barrel_doc:is_key(Doc, <<"count">>)),
    ?assertNot(barrel_doc:is_key(Doc, <<"missing">>)),

    %% On map
    ?assert(barrel_doc:is_key(Map, <<"name">>)),
    ?assertNot(barrel_doc:is_key(Map, <<"missing">>)),

    ok.

cbor_keys(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3},
    Doc = barrel_doc:from_map(Map),

    %% From binary doc
    Keys1 = lists:sort(barrel_doc:keys(Doc)),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], Keys1),

    %% From map
    Keys2 = lists:sort(barrel_doc:keys(Map)),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], Keys2),

    ok.

cbor_size(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3},
    Doc = barrel_doc:from_map(Map),

    %% From binary doc
    ?assertEqual(3, barrel_doc:size(Doc)),

    %% From map
    ?assertEqual(3, barrel_doc:size(Map)),

    %% Empty
    ?assertEqual(0, barrel_doc:size(barrel_doc:new())),
    ?assertEqual(0, barrel_doc:size(#{})),

    ok.

cbor_remove(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3},
    Doc = barrel_doc:from_map(Map),

    %% Remove from binary doc
    Doc2 = barrel_doc:remove(Doc, <<"b">>),
    ?assert(is_binary(Doc2)),
    ?assert(barrel_doc:is_indexed(Doc2)),
    ?assertEqual(2, barrel_doc:size(Doc2)),
    ?assertNot(barrel_doc:is_key(Doc2, <<"b">>)),
    ?assert(barrel_doc:is_key(Doc2, <<"a">>)),

    %% Remove from map
    Doc3 = barrel_doc:remove(Map, <<"a">>),
    ?assert(is_binary(Doc3)),
    ?assertNot(barrel_doc:is_key(Doc3, <<"a">>)),

    %% Remove non-existent key (no error)
    Doc4 = barrel_doc:remove(Doc, <<"missing">>),
    ?assertEqual(3, barrel_doc:size(Doc4)),

    ok.

cbor_update(_Config) ->
    Map = #{<<"count">> => 10},
    Doc = barrel_doc:from_map(Map),

    %% Update existing value
    Doc2 = barrel_doc:update(Doc, [<<"count">>], fun(V) -> V * 2 end),
    ?assertEqual(20, barrel_doc:get(Doc2, [<<"count">>])),

    %% Update non-existent (gets undefined)
    Doc3 = barrel_doc:update(Doc, [<<"missing">>], fun(undefined) -> <<"created">> end),
    ?assertEqual(<<"created">>, barrel_doc:get(Doc3, [<<"missing">>])),

    ok.

cbor_find(_Config) ->
    Map = #{<<"name">> => <<"test">>, <<"count">> => 42},
    Doc = barrel_doc:from_map(Map),

    %% Find existing key from binary doc
    ?assertEqual({ok, <<"test">>}, barrel_doc:find(Doc, <<"name">>)),
    ?assertEqual({ok, 42}, barrel_doc:find(Doc, <<"count">>)),

    %% Find missing key
    ?assertEqual(error, barrel_doc:find(Doc, <<"missing">>)),

    %% Find from map
    ?assertEqual({ok, <<"test">>}, barrel_doc:find(Map, <<"name">>)),
    ?assertEqual(error, barrel_doc:find(Map, <<"missing">>)),

    ok.

cbor_values(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3},
    Doc = barrel_doc:from_map(Map),

    %% From binary doc
    Values1 = lists:sort(barrel_doc:values(Doc)),
    ?assertEqual([1, 2, 3], Values1),

    %% From map
    Values2 = lists:sort(barrel_doc:values(Map)),
    ?assertEqual([1, 2, 3], Values2),

    ok.

cbor_to_list(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2},
    Doc = barrel_doc:from_map(Map),

    %% From binary doc
    List1 = lists:sort(barrel_doc:to_list(Doc)),
    ?assertEqual([{<<"a">>, 1}, {<<"b">>, 2}], List1),

    %% From map
    List2 = lists:sort(barrel_doc:to_list(Map)),
    ?assertEqual([{<<"a">>, 1}, {<<"b">>, 2}], List2),

    ok.

cbor_take(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3},
    Doc = barrel_doc:from_map(Map),

    %% Take existing key
    {Value, Doc2} = barrel_doc:take(Doc, <<"b">>),
    ?assertEqual(2, Value),
    ?assert(is_binary(Doc2)),
    ?assertEqual(2, barrel_doc:size(Doc2)),
    ?assertNot(barrel_doc:is_key(Doc2, <<"b">>)),

    %% Take non-existent key
    ?assertEqual(error, barrel_doc:take(Doc, <<"missing">>)),

    ok.

cbor_merge(_Config) ->
    Map1 = #{<<"a">> => 1, <<"b">> => 2},
    Map2 = #{<<"b">> => 20, <<"c">> => 3},
    Doc1 = barrel_doc:from_map(Map1),
    Doc2 = barrel_doc:from_map(Map2),

    %% Merge two binary docs
    Merged1 = barrel_doc:merge(Doc1, Doc2),
    ?assert(is_binary(Merged1)),
    ?assertEqual(1, barrel_doc:get(Merged1, [<<"a">>])),
    ?assertEqual(20, barrel_doc:get(Merged1, [<<"b">>])),  % Second wins
    ?assertEqual(3, barrel_doc:get(Merged1, [<<"c">>])),

    %% Merge map + binary
    Merged2 = barrel_doc:merge(Map1, Doc2),
    ?assert(is_binary(Merged2)),
    ?assertEqual(20, barrel_doc:get(Merged2, [<<"b">>])),

    %% Merge binary + map
    Merged3 = barrel_doc:merge(Doc1, Map2),
    ?assert(is_binary(Merged3)),
    ?assertEqual(20, barrel_doc:get(Merged3, [<<"b">>])),

    ok.

cbor_fold(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3},
    Doc = barrel_doc:from_map(Map),

    %% Sum all values from binary doc
    Sum1 = barrel_doc:fold(fun(_K, V, Acc) -> Acc + V end, 0, Doc),
    ?assertEqual(6, Sum1),

    %% Collect keys from map
    Keys = barrel_doc:fold(fun(K, _V, Acc) -> [K | Acc] end, [], Map),
    ?assertEqual(3, length(Keys)),

    ok.

cbor_map(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3},
    Doc = barrel_doc:from_map(Map),

    %% Double all values
    Doc2 = barrel_doc:map(fun(_K, V) -> V * 2 end, Doc),
    ?assert(is_binary(Doc2)),
    ?assertEqual(2, barrel_doc:get(Doc2, [<<"a">>])),
    ?assertEqual(4, barrel_doc:get(Doc2, [<<"b">>])),
    ?assertEqual(6, barrel_doc:get(Doc2, [<<"c">>])),

    %% Map over map input
    Doc3 = barrel_doc:map(fun(K, _V) -> K end, Map),
    ?assert(is_binary(Doc3)),
    ?assertEqual(<<"a">>, barrel_doc:get(Doc3, [<<"a">>])),

    ok.

cbor_filter(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3, <<"d">> => 4},
    Doc = barrel_doc:from_map(Map),

    %% Keep only even values
    Doc2 = barrel_doc:filter(fun(_K, V) -> V rem 2 == 0 end, Doc),
    ?assert(is_binary(Doc2)),
    ?assertEqual(2, barrel_doc:size(Doc2)),
    ?assertEqual(2, barrel_doc:get(Doc2, [<<"b">>])),
    ?assertEqual(4, barrel_doc:get(Doc2, [<<"d">>])),
    ?assertNot(barrel_doc:is_key(Doc2, <<"a">>)),

    %% Filter by key
    Doc3 = barrel_doc:filter(fun(K, _V) -> K < <<"c">> end, Map),
    ?assertEqual(2, barrel_doc:size(Doc3)),

    ok.

