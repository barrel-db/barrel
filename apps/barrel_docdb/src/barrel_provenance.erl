%%%-------------------------------------------------------------------
%%% @doc Write provenance: who made a write. A validated, size-capped
%%% map of `actor', `session', and `source' binaries supplied through
%%% the `provenance' write option, persisted as a CBOR blob in the
%%% current entity (last writer) and in every retained history entry
%%% (the audit record). Local-only in v1: provenance does not ride the
%%% sync wire; replicated arrivals are attributed by their history
%%% cause and the origin's source id.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_provenance).

-export([validate/1, encode/1, decode/1]).

-define(MAX_VALUE_SIZE, 256).
-define(MAX_BLOB_SIZE, 1024).
-define(ALLOWED_KEYS, [actor, session, source]).

-type provenance() :: #{actor => binary(),
                        session => binary(),
                        source => binary()}.
-export_type([provenance/0]).

%% @doc Validate a caller-supplied provenance map and return its
%% encoded form. Keys are limited to actor/session/source, values are
%% binaries of at most 256 bytes, and the encoded blob is capped at
%% 1024 bytes. Error terms never echo the values.
-spec validate(term()) -> {ok, binary()} | {error, term()}.
validate(Prov) when is_map(Prov), map_size(Prov) > 0 ->
    case maps:keys(Prov) -- ?ALLOWED_KEYS of
        [] ->
            case bad_values(Prov) of
                [] ->
                    Enc = encode(Prov),
                    case byte_size(Enc) =< ?MAX_BLOB_SIZE of
                        true -> {ok, Enc};
                        false -> {error, provenance_too_large}
                    end;
                BadKeys ->
                    {error, {bad_provenance_values, BadKeys}}
            end;
        Unknown ->
            {error, {unknown_provenance_keys, Unknown}}
    end;
validate(Prov) when is_map(Prov) ->
    {error, empty_provenance};
validate(_Other) ->
    {error, bad_provenance}.

%% @doc Encode a provenance map to its stored CBOR form.
-spec encode(provenance()) -> binary().
encode(Prov) ->
    Bin = maps:fold(
        fun(K, V, Acc) -> Acc#{atom_to_binary(K, utf8) => V} end,
        #{}, Prov),
    barrel_docdb_codec_cbor:encode_cbor(Bin).

%% @doc Decode a stored provenance blob back to the API map shape.
-spec decode(binary()) -> provenance().
decode(Bin) ->
    Map = barrel_docdb_codec_cbor:decode_cbor(Bin),
    maps:fold(
        fun(<<"actor">>, V, Acc) -> Acc#{actor => V};
           (<<"session">>, V, Acc) -> Acc#{session => V};
           (<<"source">>, V, Acc) -> Acc#{source => V};
           (_K, _V, Acc) -> Acc
        end, #{}, Map).

%%====================================================================
%% Internal
%%====================================================================

bad_values(Prov) ->
    [K || {K, V} <- maps:to_list(Prov),
          not (is_binary(V) andalso byte_size(V) =< ?MAX_VALUE_SIZE)].
