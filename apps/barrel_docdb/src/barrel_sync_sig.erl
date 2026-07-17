%%%-------------------------------------------------------------------
%%% @doc Ed25519 signed-request auth for the barrel sync wire (protocol).
%%%
%%% Shared, pure helpers used by both ends of the wire: the client signer
%%% ({@link barrel_rep_transport_http}, same app) and the server verifier
%%% (`barrel_server_auth', which depends on barrel_docdb). It lives here,
%%% the sync wire's home, so both sides share one canonical string.
%%%
%%% A request is authenticated by an Ed25519 signature over:
%%% ```
%%%   ts | keyId | METHOD | path | content_sha256_hex
%%% '''
%%% `ts' is milliseconds since the epoch (decimal ASCII); `content_sha256'
%%% is the lowercase hex SHA-256 of the request body, carried in the
%%% `x-barrel-content-sha256' header so the verifier never reads the body
%%% (streamed attachments can be large). The signature travels in an
%%% `Authorization: Signature keyId="..",ts="..",sig="base64"' header.
%%%
%%% This mirrors the retired barrel_memory peer-auth scheme, fixing its
%%% two weaknesses: the body is bound end to end (the handler re-hashes it
%%% against the signed header), and a missing/unknown key fails closed
%%% here (never open). Replay protection is a server-side concern
%%% (`barrel_server_sig_cache'); this module is pure.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_sync_sig).

-export([content_sha256/1,
         canonical/5,
         sign/6,
         parse_auth/1,
         verify/6]).

-type parsed() :: #{key_id := binary(), ts := integer(),
                    sig := binary()}.
-export_type([parsed/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Lowercase hex SHA-256 of a body (the value for
%% `x-barrel-content-sha256').
-spec content_sha256(iodata()) -> binary().
content_sha256(Body) ->
    binary:encode_hex(crypto:hash(sha256, Body), lowercase).

%% @doc The canonical signing string. `Method' is the uppercase verb,
%% `Path' the request path (no scheme/host/query), `ContentHashHex' the
%% value of `x-barrel-content-sha256'.
-spec canonical(binary(), binary(), binary(), binary(), binary()) ->
    binary().
canonical(TsBin, KeyId, Method, Path, ContentHashHex)
        when is_binary(TsBin) ->
    <<TsBin/binary, "|", KeyId/binary, "|", Method/binary, "|",
      Path/binary, "|", ContentHashHex/binary>>.

%% @doc Build the `Authorization: Signature ...' header value for a
%% request. `PrivKey' is a 32-byte raw Ed25519 private key.
-spec sign(binary(), binary(), binary(), binary(), binary(), integer()) ->
    binary().
sign(KeyId, PrivKey, Method, Path, ContentHashHex, TsMs) ->
    TsBin = integer_to_binary(TsMs),
    Canonical = canonical(TsBin, KeyId, Method, Path, ContentHashHex),
    Sig = crypto:sign(eddsa, none, Canonical, [PrivKey, ed25519]),
    SigB64 = base64:encode(Sig),
    <<"Signature keyId=\"", KeyId/binary, "\",ts=\"", TsBin/binary,
      "\",sig=\"", SigB64/binary, "\"">>.

%% @doc Parse an `Authorization' header value. Returns `not_signature'
%% for anything that is not the Signature scheme (e.g. Bearer), so the
%% caller can fall through to other auth. Malformed Signature headers
%% return `{error, malformed}'.
-spec parse_auth(binary() | undefined) ->
    {ok, parsed()} | not_signature | {error, malformed}.
parse_auth(<<"Signature ", Rest/binary>>) ->
    parse_params(Rest);
parse_auth(_Other) ->
    not_signature.

%% @doc Verify a parsed signature against the configured signers and a
%% skew window. Pure: replay is checked separately by the caller.
%% `Signers' maps keyId to a 32-byte raw Ed25519 public key.
-spec verify(binary(), binary(), binary(), parsed(), map(), integer()) ->
    ok | {error, unknown_key | bad_signature | stale}.
verify(Method, Path, ContentHashHex,
       #{key_id := KeyId, ts := TsMs, sig := Sig}, Signers, SkewMs) ->
    case maps:get(KeyId, Signers, undefined) of
        undefined ->
            {error, unknown_key};
        PubKey ->
            TsBin = integer_to_binary(TsMs),
            Canonical = canonical(TsBin, KeyId, Method, Path, ContentHashHex),
            case crypto:verify(eddsa, none, Canonical, Sig,
                               [PubKey, ed25519]) of
                true -> check_skew(TsMs, SkewMs);
                false -> {error, bad_signature}
            end
    end.

%%====================================================================
%% Internal
%%====================================================================

check_skew(TsMs, SkewMs) ->
    Now = erlang:system_time(millisecond),
    case abs(Now - TsMs) =< SkewMs of
        true -> ok;
        false -> {error, stale}
    end.

parse_params(Bin) ->
    try
        Pairs = [split_kv(P) || P <- binary:split(Bin, <<",">>, [global])],
        Map = maps:from_list(Pairs),
        KeyId = maps:get(<<"keyId">>, Map),
        TsMs = binary_to_integer(maps:get(<<"ts">>, Map)),
        Sig = base64:decode(maps:get(<<"sig">>, Map)),
        {ok, #{key_id => KeyId, ts => TsMs, sig => Sig}}
    catch
        _:_ -> {error, malformed}
    end.

%% `keyId="node1"' -> {<<"keyId">>, <<"node1">>}; tolerant of surrounding
%% whitespace, strict about the quoted value.
split_kv(Pair) ->
    Trimmed = string:trim(Pair),
    [K, QuotedV] = binary:split(Trimmed, <<"=">>),
    V = unquote(QuotedV),
    {K, V}.

unquote(<<"\"", Rest/binary>>) ->
    Size = byte_size(Rest) - 1,
    <<V:Size/binary, "\"">> = Rest,
    V;
unquote(V) ->
    V.
