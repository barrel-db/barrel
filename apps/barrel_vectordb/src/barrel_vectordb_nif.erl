%%%-------------------------------------------------------------------
%%% @doc Unified NIF wrapper for barrel_vectordb SIMD operations.
%%%
%%% Provides access to all NIF-accelerated functions including:
%%% - Distance computations (dot product, cosine, euclidean)
%%% - TurboQuant ADC distance
%%% - Subspace-TurboQuant ADC distance
%%% - Binary/list conversions
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_nif).

-on_load(init/0).

-export([
    %% Distance functions
    dot_product/2,
    cosine_distance/2,
    cosine_distance_normalized/2,
    euclidean_distance/2,
    %% Conversion functions
    to_binary/1,
    from_binary/1,
    %% TurboQuant ADC
    tq_adc_distance/3,
    tq_batch_adc_distance/3,
    %% Subspace TurboQuant ADC
    tqs_adc_distance/4,
    tqs_batch_adc_distance/4,
    %% Info
    simd_info/0
]).

%%====================================================================
%% NIF Loading
%%====================================================================

init() ->
    PrivDir = case code:priv_dir(barrel_vectordb) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            filename:join(filename:dirname(EbinDir), "priv");
        Dir -> Dir
    end,
    erlang:load_nif(filename:join(PrivDir, "barrel_vectordb_nif"), 0).

%%====================================================================
%% Distance Functions
%%====================================================================

%% @doc Compute dot product of two binary vectors.
-spec dot_product(binary(), binary()) -> float().
dot_product(_Bin1, _Bin2) ->
    erlang:nif_error(not_loaded).

%% @doc Compute cosine distance: 1 - cos(a, b).
-spec cosine_distance(binary(), binary()) -> float().
cosine_distance(_Bin1, _Bin2) ->
    erlang:nif_error(not_loaded).

%% @doc Compute cosine distance for pre-normalized vectors.
-spec cosine_distance_normalized(binary(), binary()) -> float().
cosine_distance_normalized(_Bin1, _Bin2) ->
    erlang:nif_error(not_loaded).

%% @doc Compute Euclidean distance.
-spec euclidean_distance(binary(), binary()) -> float().
euclidean_distance(_Bin1, _Bin2) ->
    erlang:nif_error(not_loaded).

%%====================================================================
%% Conversion Functions
%%====================================================================

%% @doc Convert list of floats to binary vector (32-bit floats).
-spec to_binary([float()]) -> binary().
to_binary(_List) ->
    erlang:nif_error(not_loaded).

%% @doc Convert binary vector to list of floats.
-spec from_binary(binary()) -> [float()].
from_binary(_Bin) ->
    erlang:nif_error(not_loaded).

%%====================================================================
%% TurboQuant ADC Functions
%%====================================================================

%% @doc Compute TurboQuant ADC distance.
-spec tq_adc_distance(binary(), binary(), 2..4) -> float().
tq_adc_distance(_Tables, _Code, _Bits) ->
    erlang:nif_error(not_loaded).

%% @doc Compute TurboQuant ADC distance for multiple codes.
-spec tq_batch_adc_distance(binary(), [binary()], 2..4) -> [float()].
tq_batch_adc_distance(_Tables, _Codes, _Bits) ->
    erlang:nif_error(not_loaded).

%%====================================================================
%% Subspace TurboQuant ADC Functions
%%====================================================================

%% @doc Compute Subspace-TurboQuant ADC distance.
-spec tqs_adc_distance(binary(), binary(), 2..4, pos_integer()) -> float().
tqs_adc_distance(_Tables, _Code, _Bits, _M) ->
    erlang:nif_error(not_loaded).

%% @doc Compute Subspace-TurboQuant ADC distance for multiple codes.
-spec tqs_batch_adc_distance(binary(), [binary()], 2..4, pos_integer()) -> [float()].
tqs_batch_adc_distance(_Tables, _Codes, _Bits, _M) ->
    erlang:nif_error(not_loaded).

%%====================================================================
%% Info Functions
%%====================================================================

%% @doc Return SIMD backend info.
-spec simd_info() -> avx2 | neon | scalar.
simd_info() ->
    erlang:nif_error(not_loaded).
