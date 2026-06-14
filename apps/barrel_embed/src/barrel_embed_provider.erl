%%%-------------------------------------------------------------------
%%% @doc Embedding provider behaviour definition
%%%
%%% Implement this behaviour to add new embedding providers.
%%%
%%% == Example Implementation ==
%%% ```
%%% -module(my_custom_embedder).
%%% -behaviour(barrel_embed_provider).
%%%
%%% -export([embed/2, embed_batch/2, dimension/1, name/0]).
%%%
%%% embed(Text, Config) ->
%%%     %% Your embedding logic here
%%%     {ok, Vector}.
%%%
%%% embed_batch(Texts, Config) ->
%%%     {ok, [embed(T, Config) || T <- Texts]}.
%%%
%%% dimension(_Config) -> 768.
%%%
%%% name() -> my_custom.
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_provider).

%% Behaviour callbacks

%% Generate embedding for a single text.
%% Returns `{ok, Vector}' on success, `{error, Reason}' on failure.
-callback embed(Text :: binary(), Config :: map()) ->
    {ok, Vector :: [float()]} | {error, term()}.

%% Generate embeddings for multiple texts.
%% Returns `{ok, Vectors}' on success, `{error, Reason}' on failure.
-callback embed_batch(Texts :: [binary()], Config :: map()) ->
    {ok, Vectors :: [[float()]]} | {error, term()}.

%% Get the dimension of vectors produced by this provider.
%% Returns the dimension (e.g., 768 for many models).
-callback dimension(Config :: map()) -> pos_integer().

%% Get the provider name.
%% Returns atom identifying this provider.
-callback name() -> atom().

%% Optional callbacks

%% Initialize the provider with configuration.
%% Called once when the provider is first used.
-callback init(Config :: map()) -> {ok, NewConfig :: map()} | {error, term()}.

%% Check if the provider is currently available.
%% Used to skip unavailable providers in fallback chains.
-callback available(Config :: map()) -> boolean().

-optional_callbacks([init/1, available/1]).

%% Utility exports
-export([
    call_embed/3,
    call_embed_batch/3,
    check_available/2
]).

%%====================================================================
%% Utility Functions
%%====================================================================

%% @doc Call embed on a provider module.
%% Wraps the call with error handling.
-spec call_embed(module(), binary(), map()) -> {ok, [float()]} | {error, term()}.
call_embed(Module, Text, Config) ->
    try
        Module:embed(Text, Config)
    catch
        error:undef ->
            {error, {provider_not_found, Module}};
        Class:Reason:Stack ->
            {error, {provider_error, Module, {Class, Reason, Stack}}}
    end.

%% @doc Call embed_batch on a provider module.
%% Wraps the call with error handling.
-spec call_embed_batch(module(), [binary()], map()) -> {ok, [[float()]]} | {error, term()}.
call_embed_batch(Module, Texts, Config) ->
    try
        Module:embed_batch(Texts, Config)
    catch
        error:undef ->
            {error, {provider_not_found, Module}};
        Class:Reason:Stack ->
            {error, {provider_error, Module, {Class, Reason, Stack}}}
    end.

%% @doc Check if a provider is available.
%% Returns true if the provider doesn't implement available/1.
-spec check_available(module(), map()) -> boolean().
check_available(Module, Config) ->
    try
        case erlang:function_exported(Module, available, 1) of
            true -> Module:available(Config);
            false -> true  %% Assume available if not implemented
        end
    catch
        _:_ -> false
    end.
