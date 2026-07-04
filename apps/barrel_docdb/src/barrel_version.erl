%%%-------------------------------------------------------------------
%%% @doc Document versions: an HLC timestamp plus the id of the node
%%% that authored the write.
%%%
%%% Versions replace rev-tree revisions ("Gen-Hash") as the identity of
%%% a document write. The HLC gives causally meaningful last-write-wins
%%% ordering; the node id breaks ties deterministically and identifies
%%% the writer. The API `_rev' token is `<hex(hlc)>@<node>': the hex is
%%% fixed width, so lexicographic token order equals causal order.
%%%
%%% The winner among sibling versions is the maximum under
%%% {@link compare/2}, a commutative rule: any replica that sees the
%%% same version set picks the same winner.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_version).

-export([new/0, new/1, new/2]).
-export([encode/1, decode/1]).
-export([to_token/1, from_token/1]).
-export([compare/2, max/2]).
-export([hlc/1, node/1]).
-export([node_id/0, cache_node_id/0]).

-type version() :: {barrel_hlc:timestamp(), binary()}.
%% {WriteHlc, AuthorNodeId}

-type token() :: binary().
%% API form: <<"0000018abc...@myhost-AbCdEf12">>

-export_type([version/0, token/0]).

-define(NODE_ID_KEY, {barrel_docdb, node_id}).
-define(HLC_HEX_LEN, 24). %% 12 bytes, hex encoded

%%====================================================================
%% Construction
%%====================================================================

%% @doc A fresh local version: new HLC, this node as author.
-spec new() -> version().
new() ->
    {barrel_hlc:new_hlc(), node_id()}.

%% @doc A local version at a given HLC (the write path issues the HLC
%% once and uses it for both the version and the change sequence).
-spec new(barrel_hlc:timestamp()) -> version().
new(Hlc) ->
    {Hlc, node_id()}.

%% @doc A version at a given HLC authored by an explicit identity.
%% Databases author their writes with their own source id (per-replica,
%% not per-node): two databases on one node must still detect each
%% other's divergent writes as concurrent.
-spec new(barrel_hlc:timestamp(), binary()) -> version().
new(Hlc, Author) when is_binary(Author) ->
    {Hlc, Author}.

%%====================================================================
%% Codec
%%====================================================================

%% @doc Storage encoding: fixed-width HLC first, node id after, so the
%% version parses from the front and sorts by HLC.
-spec encode(version()) -> binary().
encode({Hlc, Node}) when is_binary(Node) ->
    <<(barrel_hlc:encode(Hlc))/binary, Node/binary>>.

%% @doc Decode a storage-encoded version.
-spec decode(binary()) -> version().
decode(<<HlcBin:12/binary, Node/binary>>) ->
    {barrel_hlc:decode(HlcBin), Node}.

%% @doc API token: `<hex(hlc)>@<node>'. Node ids (hostname + base64)
%% can never contain `@'.
-spec to_token(version()) -> token().
to_token({Hlc, Node}) ->
    <<(hex(barrel_hlc:encode(Hlc)))/binary, "@", Node/binary>>.

%% @doc Parse an API token back to a version.
-spec from_token(token()) -> version().
from_token(<<Hex:?HLC_HEX_LEN/binary, "@", Node/binary>>) when Node =/= <<>> ->
    {barrel_hlc:decode(unhex(Hex)), Node};
from_token(_) ->
    erlang:error(badarg).

%%====================================================================
%% Ordering
%%====================================================================

%% @doc Total order over versions: HLC first, node id as tie-break.
-spec compare(version(), version()) -> lt | eq | gt.
compare({H1, N1}, {H2, N2}) ->
    case barrel_hlc:compare(H1, H2) of
        eq when N1 < N2 -> lt;
        eq when N1 > N2 -> gt;
        Other -> Other
    end.

%% @doc The greater of two versions (the deterministic winner rule).
-spec max(version(), version()) -> version().
max(A, B) ->
    case compare(A, B) of
        lt -> B;
        _ -> A
    end.

%%====================================================================
%% Accessors
%%====================================================================

-spec hlc(version()) -> barrel_hlc:timestamp().
hlc({Hlc, _Node}) -> Hlc.

-spec node(version()) -> binary().
node({_Hlc, Node}) -> Node.

%%====================================================================
%% Node identity
%%====================================================================

%% @doc This node's stable id, cached in persistent_term. Falls back to
%% computing and caching it, so callers do not depend on start order.
-spec node_id() -> binary().
node_id() ->
    case persistent_term:get(?NODE_ID_KEY, undefined) of
        undefined -> cache_node_id();
        NodeId -> NodeId
    end.

%% @doc Compute and cache the node id (called at application start).
-spec cache_node_id() -> binary().
cache_node_id() ->
    NodeId = barrel_docdb:node_id(),
    persistent_term:put(?NODE_ID_KEY, NodeId),
    NodeId.

%%====================================================================
%% Internal
%%====================================================================

hex(Bin) ->
    binary:encode_hex(Bin, lowercase).

unhex(Hex) ->
    binary:decode_hex(Hex).
