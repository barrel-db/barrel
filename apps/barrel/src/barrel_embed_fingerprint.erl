%% @doc Embedding identity and fingerprint (pure). Two vector result
%% sets are score-comparable only when their fingerprints are equal.
-module(barrel_embed_fingerprint).

-export([identity/4, fingerprint/1, canonical_json/1]).

-define(VERSION, 1).

%% @doc Identity from barrel_embed:info/1, the dimension, the index
%% distance and the text preprocessing (`none' when callers pass text).
%% Unknown values are left out; no `fingerprint' when a model is unknown.
-spec identity(map(), pos_integer() | undefined, atom() | undefined,
               barrel_embedding_policy:policy() | none) -> map().
identity(EmbedInfo, Dim, Distance, Policy) ->
    Chain = chain(EmbedInfo),
    Pre = preprocessing(Policy),
    Base = #{dimensions => Dim, distance => Distance, preprocessing => Pre},
    Ident = case Chain of
        [#{provider := P, model := M} = First | _] ->
            Base#{provider => P, model => M,
                  revision => maps:get(revision, First, undefined)};
        [] ->
            Base#{provider => undefined, model => undefined}
    end,
    maps:filter(fun(_K, V) -> V =/= undefined end,
                Ident#{fingerprint => fingerprint_of(Chain, Base)}).

%% @doc `<<"sha256:", Hex>>' of the canonical JSON of `Term'.
-spec fingerprint(term()) -> binary().
fingerprint(Term) ->
    Hash = crypto:hash(sha256, canonical_json(Term)),
    <<"sha256:", (binary:encode_hex(Hash, lowercase))/binary>>.

%% @doc JSON with object keys sorted; atoms are strings, `undefined'
%% is null.
-spec canonical_json(term()) -> binary().
canonical_json(Term) ->
    iolist_to_binary(encode(Term)).

%%====================================================================
%% Internal
%%====================================================================

chain(#{configured := true, providers := Providers}) ->
    [provider(P) || P <- Providers];
chain(_NotConfigured) ->
    [].

provider(#{name := Name} = P) ->
    maps:merge(#{provider => Name, model => maps:get(model, P, undefined)},
               maps:with([revision], P)).

preprocessing(none) ->
    none;
preprocessing(#{fields := Fields, join := Join}) ->
    #{fields => Fields, join => Join}.

%% Every provider of a fallback chain is part of the identity: a
%% fallback with another model writes vectors from another space.
fingerprint_of([], _Base) ->
    undefined;
fingerprint_of(Chain, Base) ->
    case lists:all(fun(#{model := M}) -> is_binary(M) end, Chain) of
        true -> fingerprint(Base#{v => ?VERSION, embedder => Chain});
        false -> undefined
    end.

encode(Map) when is_map(Map) ->
    Pairs = lists:sort([{key(K), V} || {K, V} <- maps:to_list(Map)]),
    [${, lists:join($,, [[json:encode(K), $:, encode(V)] || {K, V} <- Pairs]),
     $}];
encode(List) when is_list(List) ->
    [$[, lists:join($,, [encode(E) || E <- List]), $]];
encode(undefined) ->
    <<"null">>;
encode(Atom) when is_atom(Atom), Atom =/= true, Atom =/= false,
                  Atom =/= null ->
    json:encode(atom_to_binary(Atom));
encode(Other) ->
    json:encode(Other).

key(K) when is_atom(K) -> atom_to_binary(K);
key(K) when is_binary(K) -> K.
