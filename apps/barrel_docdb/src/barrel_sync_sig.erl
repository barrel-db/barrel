%%%-------------------------------------------------------------------
%%% @doc Ed25519 signed-request auth for the barrel sync wire (protocol).
%%%
%%% Shared, pure helpers used by both ends of the wire: the client signer
%%% ({@link barrel_rep_transport_http}, same app) and the server verifier
%%% (`barrel_server_auth', which depends on barrel_docdb). It lives here,
%%% the sync wire's home, so both sides share one canonical string.
%%%
%%% A request is authenticated by an Ed25519 signature over (v2):
%%% ```
%%%   ts | keyId | nonce | METHOD | target | content_sha256_hex
%%% '''
%%% `ts' is milliseconds since the epoch (decimal ASCII); `nonce' is 16
%%% random bytes (base64url, no padding) making every request distinct
%%% even inside one millisecond; `target' is the path plus `?' and the raw
%%% query string exactly as sent when there is one; `content_sha256' is
%%% the lowercase hex SHA-256 of the request body, carried in the
%%% `x-barrel-content-sha256' header so the verifier never reads the body
%%% (streamed attachments can be large). The signature travels in an
%%% `Authorization: Signature keyId="..",ts="..",nonce="..",sig="base64"'
%%% header.
%%%
%%% v1 (no nonce, path only: `ts | keyId | METHOD | path | hash') is still
%%% verified by verify/7 unless `require_nonce' is set, so a fleet rolls
%%% servers first, then clients; the same-millisecond replay and the
%%% unsigned query of v1 are why v2 exists.
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
         canonical/5, canonical_v2/6,
         target/2, new_nonce/0,
         sign/6, sign/7, sign/8,
         parse_auth/1,
         verify/6, verify/7]).

-type parsed() :: #{key_id := binary(), ts := integer(),
                    sig := binary(), nonce => binary()}.
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

%% @doc The v2 canonical signing string; `Target' comes from target/2.
-spec canonical_v2(binary(), binary(), binary(), binary(), binary(),
                   binary()) -> binary().
canonical_v2(TsBin, KeyId, Nonce, Method, Target, ContentHashHex)
        when is_binary(TsBin) ->
    <<TsBin/binary, "|", KeyId/binary, "|", Nonce/binary, "|",
      Method/binary, "|", Target/binary, "|", ContentHashHex/binary>>.

%% @doc The signed target: the path, plus `?' and the raw query bytes as
%% sent when the query is non-empty (`/p?' and `/p' sign the same).
-spec target(binary(), binary()) -> binary().
target(Path, <<>>) ->
    Path;
target(Path, Query) ->
    <<Path/binary, "?", Query/binary>>.

%% @doc A fresh per-request nonce (16 random bytes, base64url, no
%% padding: safe inside the comma-separated header).
-spec new_nonce() -> binary().
new_nonce() ->
    base64:encode(crypto:strong_rand_bytes(16),
                  #{mode => urlsafe, padding => false}).

%% @doc Build a v1 `Authorization: Signature ...' header value (no nonce,
%% path only). Kept for callers that must talk to pre-v2 verifiers;
%% new code uses sign/7. `PrivKey' is a 32-byte raw Ed25519 private key.
-spec sign(binary(), binary(), binary(), binary(), binary(), integer()) ->
    binary().
sign(KeyId, PrivKey, Method, Path, ContentHashHex, TsMs) ->
    TsBin = integer_to_binary(TsMs),
    Canonical = canonical(TsBin, KeyId, Method, Path, ContentHashHex),
    Sig = crypto:sign(eddsa, none, Canonical, [PrivKey, ed25519]),
    SigB64 = base64:encode(Sig),
    <<"Signature keyId=\"", KeyId/binary, "\",ts=\"", TsBin/binary,
      "\",sig=\"", SigB64/binary, "\"">>.

%% @doc Build a v2 header value with a fresh nonce. `Query' is the raw
%% query string (`<<>>' when none).
-spec sign(binary(), binary(), binary(), binary(), binary(), binary(),
           integer()) -> binary().
sign(KeyId, PrivKey, Method, Path, Query, ContentHashHex, TsMs) ->
    sign(KeyId, PrivKey, Method, Path, Query, ContentHashHex, TsMs,
         new_nonce()).

%% @doc Build a v2 header value with an explicit nonce (tests, replay
%% experiments).
-spec sign(binary(), binary(), binary(), binary(), binary(), binary(),
           integer(), binary()) -> binary().
sign(KeyId, PrivKey, Method, Path, Query, ContentHashHex, TsMs, Nonce) ->
    TsBin = integer_to_binary(TsMs),
    Canonical = canonical_v2(TsBin, KeyId, Nonce, Method,
                             target(Path, Query), ContentHashHex),
    Sig = crypto:sign(eddsa, none, Canonical, [PrivKey, ed25519]),
    SigB64 = base64:encode(Sig),
    <<"Signature keyId=\"", KeyId/binary, "\",ts=\"", TsBin/binary,
      "\",nonce=\"", Nonce/binary, "\",sig=\"", SigB64/binary, "\"">>.

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

%% @doc Verify a parsed v1 signature (path only) against the configured
%% signers and a skew window. Pure: replay is checked separately by the
%% caller. `Signers' maps keyId to a 32-byte raw Ed25519 public key.
-spec verify(binary(), binary(), binary(), parsed(), map(), integer()) ->
    ok | {error, unknown_key | bad_signature | stale}.
verify(Method, Path, ContentHashHex,
       #{key_id := KeyId, ts := TsMs, sig := Sig}, Signers, SkewMs) ->
    TsBin = integer_to_binary(TsMs),
    Canonical = canonical(TsBin, KeyId, Method, Path, ContentHashHex),
    verify_canonical(KeyId, Canonical, Sig, Signers, SkewMs, TsMs).

%% @doc Verify by version: a header with a nonce is checked over the v2
%% canonical (path plus raw query), one without over v1, unless
%% `require_nonce' rejects v1 before any crypto runs. Opts:
%% `#{skew_ms := integer(), require_nonce => boolean()}'.
-spec verify(binary(), binary(), binary(), binary(), parsed(), map(),
             map()) ->
    ok | {error, unknown_key | bad_signature | stale | nonce_required}.
verify(Method, Path, Query, ContentHashHex,
       #{nonce := Nonce, key_id := KeyId, ts := TsMs, sig := Sig},
       Signers, #{skew_ms := SkewMs}) ->
    TsBin = integer_to_binary(TsMs),
    Canonical = canonical_v2(TsBin, KeyId, Nonce, Method,
                             target(Path, Query), ContentHashHex),
    verify_canonical(KeyId, Canonical, Sig, Signers, SkewMs, TsMs);
verify(_Method, _Path, _Query, _ContentHashHex, _Parsed, _Signers,
       #{require_nonce := true}) ->
    {error, nonce_required};
verify(Method, Path, _Query, ContentHashHex, Parsed, Signers,
       #{skew_ms := SkewMs}) ->
    verify(Method, Path, ContentHashHex, Parsed, Signers, SkewMs).

verify_canonical(KeyId, Canonical, Sig, Signers, SkewMs, TsMs) ->
    case maps:get(KeyId, Signers, undefined) of
        undefined ->
            {error, unknown_key};
        PubKey ->
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
        {ok, with_nonce(Map, #{key_id => KeyId, ts => TsMs, sig => Sig})}
    catch
        _:_ -> {error, malformed}
    end.

%% A nonce must decode (base64url, no padding) to at least 8 bytes; the
%% signed value is the encoded form.
with_nonce(#{<<"nonce">> := Nonce}, Parsed) ->
    Decoded = base64:decode(Nonce, #{mode => urlsafe, padding => false}),
    true = byte_size(Decoded) >= 8,
    Parsed#{nonce => Nonce};
with_nonce(_Map, Parsed) ->
    Parsed.

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
