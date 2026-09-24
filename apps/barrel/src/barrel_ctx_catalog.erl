%%%-------------------------------------------------------------------
%%% @doc Local context catalog: context cards stored as documents in a
%%% docdb database (`_barrel_catalog'). Cards are advisory and never
%%% carry credentials.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_catalog).

-compile({no_auto_import, [get/1, unregister/1]}).

-export([register/1, get/1, list/1, discover/2, update/2, unregister/1,
         resolve_name/1, catalog_db/0, new_id/0, validate/1]).

-define(DEFAULT_DB, <<"_barrel_catalog">>).
-define(TYPE, <<"context_card">>).
-define(FORMAT, 1).
-define(MAX_CARD_BYTES, 65536).
-define(MAX_NAME_BYTES, 256).
%% Keys that name a secret. Matched case-insensitively, at any depth.
-define(SECRET_KEYS, [<<"token">>, <<"tokens">>, <<"password">>,
                      <<"passwd">>, <<"secret">>, <<"secret_key">>,
                      <<"api_key">>, <<"apikey">>, <<"access_key">>,
                      <<"private_key">>, <<"authorization">>,
                      <<"bearer">>, <<"credential">>, <<"credentials">>,
                      <<"cookie">>]).
%% Fields the catalog owns; update/2 ignores them.
-define(OWNED, [<<"id">>, <<"type">>, <<"format">>, <<"updated_at">>,
                <<"_rev">>]).

-type card() :: #{binary() => term()}.
-export_type([card/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Register a card. An absent `id' is minted (`ctx_' + 24 base32
%% chars); an explicit id must be unused.
-spec register(card()) -> {ok, card()} | {error, term()}.
register(Card0) when is_map(Card0) ->
    Card1 = case maps:find(<<"id">>, Card0) of
        {ok, _} -> Card0;
        error -> Card0#{<<"id">> => new_id()}
    end,
    case validate(Card1) of
        {ok, Card} ->
            Db = catalog_db(),
            #{<<"id">> := Id} = Card,
            case barrel_docdb:get_doc(Db, Id) of
                {ok, _} ->
                    {error, already_exists};
                {error, not_found} ->
                    store(Db, Card);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end;
register(_Other) ->
    {error, {invalid_card, not_an_object}}.

%% @doc The card registered under `Id'.
-spec get(binary()) -> {ok, card()} | {error, not_found | term()}.
get(Id) when is_binary(Id) ->
    case barrel_docdb:get_doc(catalog_db(), Id) of
        {ok, #{<<"type">> := ?TYPE} = Doc} -> {ok, strip(Doc)};
        {ok, _} -> {error, not_found};
        {error, _} = Err -> Err
    end.

%% @doc Cards sorted by name. Options: `include_unlisted' (default
%% false), `name_prefix' (binary).
-spec list(map()) -> {ok, [card()]}.
list(Opts) when is_map(Opts) ->
    Unlisted = maps:get(include_unlisted, Opts, false),
    Prefix = maps:get(name_prefix, Opts, <<>>),
    {ok, Cards} = barrel_docdb:fold_docs(
        catalog_db(),
        fun(#{<<"type">> := ?TYPE} = Doc, Acc) ->
                case listed(Doc, Unlisted) andalso has_prefix(Doc, Prefix) of
                    true -> {ok, [strip(Doc) | Acc]};
                    false -> {ok, Acc}
                end;
           (_Doc, Acc) ->
                {ok, Acc}
        end, [], #{id_prefix => <<"ctx_">>}),
    {ok, lists:sort(fun by_name/2, Cards)}.

%% @doc Listed cards where every word of `Query' appears in the name,
%% title, description or topics (case-insensitive; a plural or -ing
%% form also matches its stem). An empty query lists every card. A
%% filter: it ranks nothing and proves nothing about other contexts.
-spec discover(binary(), map()) -> {ok, [card()]}.
discover(Query, Opts) when is_binary(Query), is_map(Opts) ->
    {ok, Cards} = list(Opts),
    Words = string:lexemes(string:lowercase(Query), " \t\n,;"),
    {ok, [C || C <- Cards, matches(C, Words)]}.

matches(Card, Words) ->
    Topics = case maps:get(<<"topics">>, Card, []) of
        L when is_list(L) -> [T || T <- L, is_binary(T)];
        _ -> []
    end,
    Texts = [string:lowercase(V)
             || V <- [maps:get(K, Card, undefined)
                      || K <- [<<"name">>, <<"title">>, <<"description">>]]
                     ++ Topics, is_binary(V)],
    lists:all(fun(W) -> word_found(W, Texts) end, Words).

word_found(Word, Texts) ->
    lists:any(fun(Form) ->
                  lists:any(fun(T) -> string:find(T, Form) =/= nomatch end,
                            Texts)
              end, forms(iolist_to_binary(Word))).

%% The word and its stem without a common English suffix.
forms(Word) ->
    [Word | [binary:part(Word, 0, byte_size(Word) - byte_size(Suffix))
             || Suffix <- [<<"ing">>, <<"es">>, <<"s">>, <<"ed">>],
                byte_size(Word) - byte_size(Suffix) >= 3,
                binary:longest_common_suffix([Word, Suffix]) =:=
                    byte_size(Suffix)]].

%% @doc Merge `Changes' into a card; catalog-owned fields are ignored.
-spec update(binary(), card()) -> {ok, card()} | {error, term()}.
update(Id, Changes) when is_binary(Id), is_map(Changes) ->
    Db = catalog_db(),
    case barrel_docdb:get_doc(Db, Id) of
        {ok, #{<<"type">> := ?TYPE, <<"_rev">> := Rev} = Doc} ->
            Merged = maps:merge(strip(Doc), maps:without(?OWNED, Changes)),
            case validate(Merged) of
                {ok, Card} -> store(Db, Card#{<<"_rev">> => Rev});
                {error, _} = Err -> Err
            end;
        {ok, _} ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

%% @doc Remove a card. Idempotent.
-spec unregister(binary()) -> ok | {error, term()}.
unregister(Id) when is_binary(Id) ->
    case barrel_docdb:delete_doc(catalog_db(), Id) of
        {ok, _} -> ok;
        {error, not_found} -> ok;
        {error, _} = Err -> Err
    end.

%% @doc The id of the only card named `Name'.
-spec resolve_name(binary()) ->
    {ok, binary()} | {error, not_found | ambiguous}.
resolve_name(Name) when is_binary(Name) ->
    {ok, Cards} = list(#{include_unlisted => true}),
    case [Id || #{<<"id">> := Id, <<"name">> := N} <- Cards, N =:= Name] of
        [Id] -> {ok, Id};
        [] -> {error, not_found};
        [_, _ | _] -> {error, ambiguous}
    end.

%% @doc The catalog database name (app env `ctx_catalog_db'), created on
%% first use.
-spec catalog_db() -> binary().
catalog_db() ->
    Name = application:get_env(barrel, ctx_catalog_db, ?DEFAULT_DB),
    case barrel_docdb:open_db(Name) of
        {ok, _} ->
            Name;
        {error, _} ->
            case barrel_docdb:create_db(Name) of
                {ok, _} -> Name;
                {error, already_exists} -> Name
            end
    end.

%% @doc A fresh context id: `ctx_' + 24 lowercase base32 chars.
-spec new_id() -> binary().
new_id() ->
    <<"ctx_", (base32(crypto:strong_rand_bytes(15)))/binary>>.

%% @doc Check a card and fill its defaults.
-spec validate(term()) -> {ok, card()} | {error, {invalid_card, term()}}.
validate(Card) when is_map(Card) ->
    try
        ok = check_secrets(Card),
        ok = check_id(maps:get(<<"id">>, Card, undefined)),
        ok = check_name(maps:get(<<"name">>, Card, undefined)),
        ok = check_locations(maps:get(<<"locations">>, Card, undefined)),
        ok = check_discoverable(maps:get(<<"discoverable">>, Card,
                                         <<"listed">>)),
        ok = check_embedding(maps:get(<<"embedding">>, Card, undefined)),
        Filled = (maps:without([<<"_rev">>], Card))#{
            <<"type">> => ?TYPE,
            <<"format">> => ?FORMAT,
            <<"discoverable">> =>
                maps:get(<<"discoverable">>, Card, <<"listed">>),
            <<"updated_at">> => now_iso()},
        ok = check_size(Filled),
        {ok, Filled}
    catch
        throw:{invalid_card, _} = Invalid -> {error, Invalid}
    end;
validate(_Other) ->
    {error, {invalid_card, not_an_object}}.

%%====================================================================
%% Validation
%%====================================================================

check_secrets(Map) when is_map(Map) ->
    maps:foreach(
        fun(Key, Value) ->
            ok = check_key(Key),
            ok = check_secrets(Value)
        end, Map);
check_secrets(List) when is_list(List) ->
    lists:foreach(fun check_secrets/1, List);
check_secrets(<<"bsp_", _/binary>>) ->
    invalid(capability_token_value);
check_secrets(_Scalar) ->
    ok.

check_key(Key) when is_binary(Key) ->
    case lists:member(string:lowercase(Key), ?SECRET_KEYS) of
        true -> invalid({credential_field, Key});
        false -> ok
    end;
check_key(Key) ->
    invalid({invalid_key, Key}).

check_id(<<"ctx_", Rest/binary>>) when byte_size(Rest) >= 1,
                                        byte_size(Rest) =< 64 ->
    case re:run(Rest, "^[a-z0-9_]+$", [{capture, none}]) of
        match -> ok;
        nomatch -> invalid(id)
    end;
check_id(_Other) ->
    invalid(id).

check_name(Name) when is_binary(Name), byte_size(Name) > 0,
                      byte_size(Name) =< ?MAX_NAME_BYTES ->
    ok;
check_name(_Other) ->
    invalid(name).

check_locations([_ | _] = Locations) ->
    lists:foreach(fun check_location/1, Locations);
check_locations(_Other) ->
    invalid(locations).

check_location(#{<<"kind">> := <<"local">>, <<"db">> := Db}) ->
    check_db_name(Db);
check_location(#{<<"kind">> := <<"remote">>, <<"endpoint">> := Endpoint,
                 <<"db">> := Db}) ->
    ok = check_endpoint(Endpoint),
    check_db_name(Db);
check_location(#{<<"kind">> := <<"snapshot">>,
                 <<"publication">> := Pub}) when is_binary(Pub) ->
    ok;
check_location(#{<<"kind">> := Kind})
  when Kind =:= <<"local">>; Kind =:= <<"remote">>;
       Kind =:= <<"snapshot">> ->
    invalid({incomplete_location, Kind});
check_location(#{<<"kind">> := Kind}) ->
    invalid({unknown_location_kind, Kind});
check_location(_Other) ->
    invalid(location).

check_db_name(Db) when is_binary(Db), byte_size(Db) > 0,
                       byte_size(Db) =< 128 ->
    case re:run(Db, "^[A-Za-z0-9_-]+$", [{capture, none}]) of
        match -> ok;
        nomatch -> invalid({db, Db})
    end;
check_db_name(Db) ->
    invalid({db, Db}).

%% http(s) only, and no userinfo: a URL must not smuggle a password.
check_endpoint(Endpoint) when is_binary(Endpoint) ->
    case uri_string:parse(Endpoint) of
        #{userinfo := _} ->
            invalid({credential_field, <<"endpoint">>});
        #{scheme := Scheme, host := Host}
          when (Scheme =:= <<"http">> orelse Scheme =:= <<"https">>),
               Host =/= <<>> ->
            ok;
        _ ->
            invalid({endpoint, Endpoint})
    end;
check_endpoint(Endpoint) ->
    invalid({endpoint, Endpoint}).

check_discoverable(<<"listed">>) -> ok;
check_discoverable(<<"unlisted">>) -> ok;
check_discoverable(_Other) -> invalid(discoverable).

%% Optional, advisory: what the source reports for vector results.
check_embedding(undefined) ->
    ok;
check_embedding(#{} = Emb) ->
    ok = check_fingerprint(maps:get(<<"fingerprint">>, Emb, undefined)),
    ok = check_opt_int(<<"dimensions">>, maps:get(<<"dimensions">>, Emb,
                                                  undefined)),
    check_opt_bin(<<"distance">>, maps:get(<<"distance">>, Emb, undefined));
check_embedding(_Other) ->
    invalid(embedding).

check_fingerprint(undefined) -> ok;
check_fingerprint(<<"sha256:", Hex/binary>>) when byte_size(Hex) =:= 64 -> ok;
check_fingerprint(_Other) -> invalid({embedding, fingerprint}).

check_opt_int(_Key, undefined) -> ok;
check_opt_int(_Key, N) when is_integer(N), N > 0 -> ok;
check_opt_int(Key, _Other) -> invalid({embedding, Key}).

check_opt_bin(_Key, undefined) -> ok;
check_opt_bin(_Key, B) when is_binary(B) -> ok;
check_opt_bin(Key, _Other) -> invalid({embedding, Key}).

check_size(Card) ->
    case iolist_size(json:encode(Card)) =< ?MAX_CARD_BYTES of
        true -> ok;
        false -> invalid(too_large)
    end.

-spec invalid(term()) -> no_return().
invalid(Reason) ->
    throw({invalid_card, Reason}).

%%====================================================================
%% Internal
%%====================================================================

store(Db, Card) ->
    case barrel_docdb:put_doc(Db, Card) of
        {ok, _} -> {ok, strip(Card)};
        {error, conflict} -> {error, already_exists};
        {error, _} = Err -> Err
    end.

strip(Doc) ->
    maps:without([<<"_rev">>], Doc).

listed(_Doc, true) -> true;
listed(#{<<"discoverable">> := <<"unlisted">>}, false) -> false;
listed(_Doc, false) -> true.

has_prefix(_Doc, <<>>) ->
    true;
has_prefix(#{<<"name">> := Name}, Prefix) ->
    binary:longest_common_prefix([Name, Prefix]) =:= byte_size(Prefix);
has_prefix(_Doc, _Prefix) ->
    false.

by_name(#{<<"name">> := A, <<"id">> := IA},
        #{<<"name">> := B, <<"id">> := IB}) ->
    {A, IA} =< {B, IB}.

now_iso() ->
    list_to_binary(calendar:system_time_to_rfc3339(
                     erlang:system_time(second),
                     [{offset, "Z"}, {unit, second}])).

%% RFC 4648 base32, lowercase, no padding (15 bytes -> 24 chars).
base32(Bin) ->
    << <<(b32_char(C))>> || <<C:5>> <= Bin >>.

b32_char(C) when C < 26 -> $a + C;
b32_char(C) -> $2 + C - 26.
