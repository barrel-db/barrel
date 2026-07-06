%% @doc Key provider behaviour and dispatcher for Barrel encryption at rest.
%%
%% A provider maps a database KEYSPACE to a 256-bit key. The keyspace is the
%% identity data is stored under: a timeline branch shares its parent's
%% keyspace, so it resolves the parent's key and can read the checkpointed
%% ciphertext. One logical database resolves one key across all of its
%% stores.
%%
%% Callers carry an encryption spec, runtime config supplied at each open:
%% <ul>
%% <li>`disabled': no encryption (the default)</li>
%% <li>`default': the built-in {@link barrel_keyprovider_env} provider
%%     (`BARREL_ENCRYPTION_KEY')</li>
%% <li>`#{provider := Mod}': a module implementing this behaviour, for KMS
%%     or token-derived keys</li>
%% </ul>
%% Resolution is fail closed: a provider error fails the database open.
%% @end
-module(barrel_keyprovider).

-callback key_for_db(Keyspace :: binary()) -> {ok, binary()} | {error, term()}.

-export([key_for_db/2]).

-type spec() :: disabled | default | #{provider := module(), _ => _}.
-export_type([spec/0]).

%% @doc Resolve the key for a keyspace under the given spec. `disabled'
%% resolves to the atom `plaintext'; providers must return a 32-byte key.
%% Error terms never carry key material.
-spec key_for_db(spec(), binary()) ->
    {ok, plaintext} | {ok, binary()} | {error, term()}.
key_for_db(disabled, _Keyspace) ->
    {ok, plaintext};
key_for_db(default, Keyspace) when is_binary(Keyspace) ->
    validate(barrel_keyprovider_env,
             barrel_keyprovider_env:key_for_db(Keyspace));
key_for_db(#{provider := Mod}, Keyspace)
  when is_atom(Mod), is_binary(Keyspace) ->
    try Mod:key_for_db(Keyspace) of
        Result -> validate(Mod, Result)
    catch
        Class:Reason ->
            {error, {key_provider_error, Mod, {Class, Reason}}}
    end;
key_for_db(Spec, _Keyspace) ->
    {error, {bad_encryption_spec, Spec}}.

%%====================================================================
%% Internal
%%====================================================================

validate(_Mod, {ok, Key}) when is_binary(Key), byte_size(Key) =:= 32 ->
    {ok, Key};
validate(Mod, {ok, _Other}) ->
    {error, {bad_key_size, Mod}};
validate(Mod, {error, Reason}) ->
    {error, {key_provider_error, Mod, Reason}};
validate(Mod, _Other) ->
    {error, {bad_provider_return, Mod}}.
