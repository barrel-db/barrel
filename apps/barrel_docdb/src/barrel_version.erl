%%%-------------------------------------------------------------------
%%% @doc Document versions: an HLC timestamp plus the id of the
%%% database that authored the write.
%%%
%%% Versions replace rev-tree revisions ("Gen-Hash") as the identity of
%%% a document write. The HLC gives causally meaningful last-write-wins
%%% ordering; the author id breaks ties deterministically and
%%% identifies the writer. Authorship is per-database (each database
%%% carries a source id, see barrel_db_server), not per-node: two
%%% databases on one node share the HLC clock, so node-level authorship
%%% would totally order their writes and hide concurrency. The API
%%% `_rev' token is `<hex(hlc)>@<author>': the hex is fixed width, so
%%% lexicographic token order equals causal order.
%%%
%%% The winner among sibling versions is the maximum under
%%% {@link compare/2}, a commutative rule: any replica that sees the
%%% same version set picks the same winner.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_version).

-export([new/2]).
-export([encode/1, decode/1]).
-export([to_token/1, from_token/1]).
-export([compare/2, max/2]).
-export([hlc/1, author/1]).

-type version() :: {barrel_hlc:timestamp(), binary()}.
%% {WriteHlc, AuthorId}

-type token() :: binary().
%% API form: `<<"0000018abc...@f1e061a70714abcd">>'

-export_type([version/0, token/0]).

-define(HLC_HEX_LEN, 24). %% 12 bytes, hex encoded

%%====================================================================
%% Construction
%%====================================================================

%% @doc A version at a given HLC authored by an explicit identity
%% (the authoring database's source id).
-spec new(barrel_hlc:timestamp(), binary()) -> version().
new(Hlc, Author) when is_binary(Author) ->
    {Hlc, Author}.

%%====================================================================
%% Codec
%%====================================================================

%% @doc Storage encoding: fixed-width HLC first, author id after, so
%% the version parses from the front and sorts by HLC.
-spec encode(version()) -> binary().
encode({Hlc, Author}) when is_binary(Author) ->
    <<(barrel_hlc:encode(Hlc))/binary, Author/binary>>.

%% @doc Decode a storage-encoded version.
-spec decode(binary()) -> version().
decode(<<HlcBin:12/binary, Author/binary>>) ->
    {barrel_hlc:decode(HlcBin), Author}.

%% @doc API token: `<hex(hlc)>@<author>'. Author ids (lowercase hex)
%% can never contain `@'.
-spec to_token(version()) -> token().
to_token({Hlc, Author}) ->
    <<(hex(barrel_hlc:encode(Hlc)))/binary, "@", Author/binary>>.

%% @doc Parse an API token back to a version.
-spec from_token(token()) -> version().
from_token(<<Hex:?HLC_HEX_LEN/binary, "@", Author/binary>>)
        when Author =/= <<>> ->
    {barrel_hlc:decode(unhex(Hex)), Author};
from_token(_) ->
    erlang:error(badarg).

%%====================================================================
%% Ordering
%%====================================================================

%% @doc Total order over versions: HLC first, author id as tie-break.
-spec compare(version(), version()) -> lt | eq | gt.
compare({H1, A1}, {H2, A2}) ->
    case barrel_hlc:compare(H1, H2) of
        eq when A1 < A2 -> lt;
        eq when A1 > A2 -> gt;
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
hlc({Hlc, _Author}) -> Hlc.

-spec author(version()) -> binary().
author({_Hlc, Author}) -> Author.

%%====================================================================
%% Internal
%%====================================================================

hex(Bin) ->
    binary:encode_hex(Bin, lowercase).

unhex(Hex) ->
    binary:decode_hex(Hex).
