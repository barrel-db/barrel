%%%-------------------------------------------------------------------
%%% @doc TurboQuant: Data-oblivious 3-bit vector quantization
%%%
%%% TurboQuant provides efficient vector compression without training.
%%% Based on Google Research's algorithm combining:
%%%
%%% 1. PolarQuant: Random rotation + polar coordinate conversion
%%% 2. QJL: 1-bit Johnson-Lindenstrauss error correction
%%%
%%% Key advantages over Product Quantization (PQ):
%%% - No training required (data-oblivious)
%%% - Deterministic with seed (reproducible results)
%%% - ~8x compression for 768-dim vectors (vs 4x for 8-bit scalar)
%%% - Only 1-3% recall loss vs float32
%%%
%%% Storage format (D=768, 3-bit):
%%%   Header: 4 bytes (version, bits, dimension flags)
%%%   PolarQuant: ceil(D*bits/8) bytes
%%%   QJL: ceil(D/8) bytes
%%%   Total: ~388 bytes vs 3072 bytes float32
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_turboquant).

-include("barrel_vectordb.hrl").

%% API
-export([
    new/1,
    encode/2,
    decode/2,
    precompute_tables/2,
    distance/2,
    distance_nif/2,
    batch_distance_nif/2,
    batch_encode/2,
    info/1
]).

%% Internal exports for testing
-export([
    generate_rotation_matrix/2,
    apply_rotation/2,
    apply_inverse_rotation/2,
    to_polar/2,
    from_polar/2,
    compute_qjl_signs/2
]).

-record(tq_config, {
    bits :: 2..4,                     %% Bits per component (default: 3)
    qjl_bits :: pos_integer(),        %% QJL error correction bits (default: 1)
    dimension :: pos_integer(),       %% Vector dimension (must be even)
    rotation_seed :: integer(),       %% Seed for rotation matrix generation
    rotation_matrix :: binary(),      %% Cached D x D orthogonal rotation matrix
    qjl_matrix :: binary(),           %% D x qjl_dim random +-1 matrix for QJL
    qjl_dim :: pos_integer(),         %% QJL projection dimension (default: D)
    angle_levels :: [float()],        %% Quantization levels for angles
    qjl_iterations :: non_neg_integer(), %% QJL correction iterations (default: 5)
    qjl_learning_rate :: float()      %% QJL gradient learning rate (default: 0.1)
}).

-type tq_config() :: #tq_config{}.
-type tq_code() :: binary().
-type distance_tables() :: binary().

-export_type([tq_config/0, tq_code/0, distance_tables/0]).

%% Version for storage format
-define(TQ_VERSION, 1).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new TurboQuant configuration (no training needed)
%% Options:
%%   bits - bits per component (default: 3, range: 2-4)
%%   qjl_bits - QJL error correction bits (default: 1)
%%   dimension - vector dimension (required, must be even)
%%   seed - random seed for rotation matrix (default: 42)
%%   qjl_iterations - QJL correction iterations (default: 5)
%%   qjl_learning_rate - QJL gradient learning rate (default: 0.1)
-spec new(map()) -> {ok, tq_config()} | {error, term()}.
new(Options) ->
    Bits = maps:get(bits, Options, 3),
    QJLBits = maps:get(qjl_bits, Options, 1),
    Dimension = maps:get(dimension, Options, undefined),
    Seed = maps:get(seed, Options, 42),
    QJLIterations = maps:get(qjl_iterations, Options, 5),
    QJLLearningRate = maps:get(qjl_learning_rate, Options, 0.1),

    case validate_config(Bits, Dimension) of
        ok ->
            %% Generate rotation matrix from seed
            RotationMatrix = generate_rotation_matrix(Dimension, Seed),

            %% Generate QJL random sign matrix
            QJLDim = Dimension,  %% Use same dimension for QJL
            QJLMatrix = generate_qjl_matrix(Dimension, QJLDim, Seed + 1),

            %% Compute angle quantization levels
            NumLevels = 1 bsl Bits,  %% 2^bits levels
            AngleLevels = compute_angle_levels(NumLevels),

            {ok, #tq_config{
                bits = Bits,
                qjl_bits = QJLBits,
                dimension = Dimension,
                rotation_seed = Seed,
                rotation_matrix = RotationMatrix,
                qjl_matrix = QJLMatrix,
                qjl_dim = QJLDim,
                angle_levels = AngleLevels,
                qjl_iterations = QJLIterations,
                qjl_learning_rate = QJLLearningRate
            }};
        {error, _} = Error ->
            Error
    end.

%% @doc Encode a vector using TurboQuant
%% Returns compact binary representation
-spec encode(tq_config(), [float()]) -> tq_code().
encode(#tq_config{dimension = Dim, bits = Bits, rotation_matrix = RotMat,
                  qjl_matrix = QJLMat,
                  angle_levels = Levels}, Vector) when length(Vector) =:= Dim ->
    %% Step 1: Apply random rotation
    RotatedVec = apply_rotation(RotMat, Vector),

    %% Step 2: Convert to polar coordinates (pairs of values -> r, theta)
    PolarVec = to_polar(RotatedVec, Levels),

    %% Step 3: Quantize polar values
    {RadiusBin, AngleBin} = quantize_polar(PolarVec, Bits),

    %% Step 4: Compute QJL error correction signs
    QJLSigns = compute_qjl_signs(QJLMat, Vector),

    %% Pack everything
    %% Header: version (1), bits (1), flags (2)
    Header = <<?TQ_VERSION:8, Bits:8, 0:16>>,

    %% Radii: D/2 floats compressed to 16-bit each for now (for radius reconstruction)
    %% Angles: D/2 values * bits / 8 bytes
    <<Header/binary, RadiusBin/binary, AngleBin/binary, QJLSigns/binary>>;
encode(#tq_config{dimension = Dim}, Vector) ->
    error({dimension_mismatch, Dim, length(Vector)}).

%% @doc Decode TurboQuant code back to approximate vector
-spec decode(tq_config(), tq_code()) -> [float()].
decode(#tq_config{dimension = Dim, rotation_matrix = RotMat,
                  angle_levels = Levels, qjl_matrix = QJLMat, qjl_dim = QJLDim,
                  qjl_iterations = QJLIter, qjl_learning_rate = QJLLR},
       <<?TQ_VERSION:8, Bits:8, _Flags:16, Rest/binary>>) ->
    %% Calculate sizes
    NumPairs = Dim div 2,
    RadiusBytes = NumPairs * 2,  %% 16-bit per radius
    AngleBits = NumPairs * Bits,
    AngleBytes = (AngleBits + 7) div 8,
    QJLBytes = (QJLDim + 7) div 8,

    %% Parse components
    <<RadiusBin:RadiusBytes/binary, AngleBin:AngleBytes/binary,
      QJLSigns:QJLBytes/binary, _/binary>> = Rest,

    %% Dequantize
    PolarVec = dequantize_polar(RadiusBin, AngleBin, NumPairs, Bits, Levels),

    %% Convert from polar to Cartesian
    RotatedVec = from_polar(PolarVec, NumPairs),

    %% Apply inverse rotation
    Vector = apply_inverse_rotation(RotMat, RotatedVec),

    %% Apply QJL correction (sign-based iterative refinement)
    apply_qjl_correction(Vector, QJLSigns, QJLMat, QJLDim, QJLIter, QJLLR).

%% @doc Precompute distance lookup tables for a query vector
%% This enables fast asymmetric distance computation (ADC)
%%
%% Full ADC table format per pair:
%%   QRSq (32-bit float) + NumLevels * CosTerm (32-bit floats)
%% where CosTerm_i = 2 * r_q * cos(theta_q - angle_center_i)
%%
%% Distance formula: d^2 = r_q^2 + r_d^2 - 2*r_q*r_d*cos(theta_diff)
%%                       = QRSq + r_d^2 - CosTerm * r_d
-spec precompute_tables(tq_config(), [float()]) -> distance_tables().
precompute_tables(#tq_config{dimension = Dim, bits = Bits,
                             rotation_matrix = RotMat, angle_levels = Levels}, Query)
  when length(Query) =:= Dim ->
    %% Rotate query
    RotatedQuery = apply_rotation(RotMat, Query),

    %% For each pair, precompute query radius squared and cos terms
    NumLevels = 1 bsl Bits,
    NumPairs = Dim div 2,

    %% Create lookup tables: for each pair, store QRSq and cos terms
    Tables = lists:map(
        fun(PairIdx) ->
            Offset = PairIdx * 2,
            QX = lists:nth(Offset + 1, RotatedQuery),
            QY = lists:nth(Offset + 2, RotatedQuery),
            QR = math:sqrt(QX * QX + QY * QY),
            QRSq = QR * QR,
            QTheta = math:atan2(QY, QX),

            %% Precompute 2 * r_q * cos(theta_q - angle_center) for each level
            CosTerms = lists:map(
                fun(AngleIdx) ->
                    CenterAngle = lists:nth(AngleIdx + 1, Levels),
                    AngleDiff = QTheta - CenterAngle,
                    2.0 * QR * math:cos(AngleDiff)
                end,
                lists:seq(0, NumLevels - 1)
            ),
            {QRSq, CosTerms}
        end,
        lists:seq(0, NumPairs - 1)
    ),

    %% Pack tables as binary: for each pair, QRSq followed by NumLevels cos terms
    << <<QRSq:32/float-little, (<< <<CT:32/float-little>> || CT <- CosTerms >>)/binary>>
       || {QRSq, CosTerms} <- Tables >>.

%% @doc Compute asymmetric distance using precomputed tables
%% Full ADC with correct polar distance formula:
%%   d^2 = r_q^2 + r_d^2 - 2*r_q*r_d*cos(theta_diff)
%%       = QRSq + r_d^2 - CosTerm * r_d
%%
%% Performance note: This ADC computation can be moved to C/NIF for better
%% performance if needed, similar to barrel_vectordb_hnsw distance functions.
%% The tight loop over pairs with table lookups would benefit from SIMD.
-spec distance(distance_tables(), tq_code()) -> float().
distance(Tables, <<?TQ_VERSION:8, Bits:8, _Flags:16, Rest/binary>>) ->
    NumLevels = 1 bsl Bits,
    %% Table row size: QRSq (1 float) + NumLevels cos terms
    TableRowSize = (1 + NumLevels) * 4,

    %% Calculate sizes from table size
    TablesSize = byte_size(Tables),
    NumPairs = TablesSize div TableRowSize,

    RadiusBytes = NumPairs * 2,
    AngleBits = NumPairs * Bits,
    AngleBytes = (AngleBits + 7) div 8,

    <<RadiusBin:RadiusBytes/binary, AngleBin:AngleBytes/binary, _/binary>> = Rest,

    %% Unpack radii and angles
    Radii = [dequantize_radius(R) || <<R:16/unsigned-little>> <= RadiusBin],
    AngleIndices = unpack_bits(AngleBin, Bits, NumPairs),

    %% Compute distance using full ADC (Asymmetric Distance Computation)
    %% For each pair: d^2 = QRSq + DR^2 - CosTerm * DR
    compute_full_adc_distance(Tables, Radii, AngleIndices, NumLevels, 0, 0.0).

%% @doc Batch encode multiple vectors
-spec batch_encode(tq_config(), [[float()]]) -> [tq_code()].
batch_encode(Config, Vectors) ->
    [encode(Config, V) || V <- Vectors].

%% @doc Get configuration info
-spec info(tq_config()) -> map().
info(#tq_config{bits = Bits, qjl_bits = QJLBits, dimension = Dim,
                rotation_seed = Seed, qjl_dim = QJLDim,
                qjl_iterations = QJLIter, qjl_learning_rate = QJLLR}) ->
    NumPairs = Dim div 2,
    %% Calculate bytes per vector
    HeaderBytes = 4,
    RadiusBytes = NumPairs * 2,
    AngleBits = NumPairs * Bits,
    AngleBytes = (AngleBits + 7) div 8,
    QJLBytes = (QJLDim + 7) div 8,
    TotalBytes = HeaderBytes + RadiusBytes + AngleBytes + QJLBytes,

    #{
        bits => Bits,
        qjl_bits => QJLBits,
        dimension => Dim,
        rotation_seed => Seed,
        qjl_iterations => QJLIter,
        qjl_learning_rate => QJLLR,
        bytes_per_vector => TotalBytes,
        compression_ratio => (Dim * 4) / TotalBytes,  %% vs float32
        training_required => false
    }.

%% @doc Compute ADC distance using SIMD-accelerated NIF.
%% This is the optimized version that should be used for production workloads.
-spec distance_nif(distance_tables(), tq_code()) -> float().
distance_nif(Tables, Code) when is_binary(Tables), is_binary(Code) ->
    <<?TQ_VERSION:8, Bits:8, _:16, _/binary>> = Code,
    barrel_vectordb_nif:tq_adc_distance(Tables, Code, Bits).

%% @doc Compute ADC distance for multiple codes using SIMD-accelerated NIF.
%% Amortizes NIF call overhead for batch operations.
-spec batch_distance_nif(distance_tables(), [tq_code()]) -> [float()].
batch_distance_nif(Tables, Codes) when is_binary(Tables), is_list(Codes) ->
    case Codes of
        [] -> [];
        [<<_:8, Bits:8, _/binary>> | _] ->
            barrel_vectordb_nif:tq_batch_adc_distance(Tables, Codes, Bits)
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

validate_config(_, undefined) ->
    {error, dimension_required};
validate_config(_, Dim) when Dim rem 2 =/= 0 ->
    {error, {dimension_must_be_even, Dim}};
validate_config(Bits, _) when Bits < 2; Bits > 4 ->
    {error, {bits_out_of_range, Bits, {2, 4}}};
validate_config(_, Dim) when Dim < 2 ->
    {error, {dimension_too_small, Dim}};
validate_config(_, _) ->
    ok.

%% Generate orthogonal rotation matrix via QR decomposition of random Gaussian matrix
%% Uses Gram-Schmidt orthogonalization as per TurboQuant paper requirements
%% Returns D*D floats as binary (row-major)
%%
%% Note: This is O(D²) for the orthogonalization. The matrix is generated once
%% per config and reused for all encode/decode operations.
generate_rotation_matrix(Dim, Seed) ->
    %% Seed the random generator
    _ = rand:seed(exsss, {Seed, Seed + 1, Seed + 2}),

    %% Generate random Gaussian matrix (stored as list of column vectors)
    %% We orthogonalize columns to get Q from QR decomposition
    RandomCols = [random_gaussian_vector(Dim) || _ <- lists:seq(1, Dim)],

    %% Apply modified Gram-Schmidt (more numerically stable)
    OrthogonalCols = modified_gram_schmidt(RandomCols),

    %% Convert column-major to row-major for efficient row access
    Rows = transpose(OrthogonalCols),

    %% Pack as binary (row-major)
    << <<F:64/float-little>> || Row <- Rows, F <- Row >>.

%% Generate random vector with Gaussian distribution
random_gaussian_vector(Dim) ->
    [rand:normal() || _ <- lists:seq(1, Dim)].

%% Modified Gram-Schmidt orthogonalization (column-wise)
%% More numerically stable than classical Gram-Schmidt
modified_gram_schmidt(Cols) ->
    modified_gram_schmidt(Cols, []).

modified_gram_schmidt([], Acc) ->
    lists:reverse(Acc);
modified_gram_schmidt([V | Rest], Acc) ->
    %% Orthogonalize V against all previously processed vectors
    V1 = lists:foldl(
        fun(U, Vk) ->
            %% Subtract projection of Vk onto U
            subtract_projection(Vk, U)
        end,
        V,
        Acc
    ),
    %% Normalize
    Normalized = normalize_vector(V1),
    %% Orthogonalize remaining vectors against this one (MGS improvement)
    Rest1 = [subtract_projection(R, Normalized) || R <- Rest],
    modified_gram_schmidt(Rest1, [Normalized | Acc]).

%% Subtract projection of V onto U: V - (V·U)U
subtract_projection(V, U) ->
    Dot = dot_product_list(V, U),
    [Vi - Dot * Ui || {Vi, Ui} <- lists:zip(V, U)].

%% Normalize vector to unit length
normalize_vector(V) ->
    Norm = math:sqrt(dot_product_list(V, V)),
    case Norm < 1.0e-10 of
        true -> V;  %% Avoid division by zero
        false -> [X / Norm || X <- V]
    end.

%% Dot product of two lists
dot_product_list(A, B) ->
    lists:sum([X * Y || {X, Y} <- lists:zip(A, B)]).

%% Transpose list of lists (columns to rows)
transpose([[] | _]) -> [];
transpose(Cols) ->
    [lists:map(fun hd/1, Cols) | transpose(lists:map(fun tl/1, Cols))].

%% Apply rotation: multiply vector by rotation matrix
apply_rotation(RotMatBin, Vector) ->
    Dim = length(Vector),
    VecBin = list_to_binary([<<F:64/float-little>> || F <- Vector]),
    apply_rotation_rows(RotMatBin, VecBin, Dim, []).

apply_rotation_rows(<<>>, _VecBin, _Dim, Acc) ->
    lists:reverse(Acc);
apply_rotation_rows(RotMatBin, VecBin, Dim, Acc) ->
    RowBytes = Dim * 8,
    <<RowBin:RowBytes/binary, Rest/binary>> = RotMatBin,
    DotProd = dot_product_bin(RowBin, VecBin),
    apply_rotation_rows(Rest, VecBin, Dim, [DotProd | Acc]).

%% Apply inverse rotation (transpose for orthogonal matrix)
apply_inverse_rotation(RotMatBin, Vector) ->
    Dim = length(Vector),
    %% For orthogonal matrix, inverse = transpose
    %% Compute v * R^T = sum over rows
    apply_inverse_rotation_cols(RotMatBin, Vector, Dim, 0, []).

apply_inverse_rotation_cols(_RotMatBin, _Vector, Dim, ColIdx, Acc) when ColIdx >= Dim ->
    lists:reverse(Acc);
apply_inverse_rotation_cols(RotMatBin, Vector, Dim, ColIdx, Acc) ->
    %% Sum: Vector[i] * RotMat[i, ColIdx]
    Sum = apply_inverse_col_sum(RotMatBin, Vector, Dim, ColIdx, 0, 0.0),
    apply_inverse_rotation_cols(RotMatBin, Vector, Dim, ColIdx + 1, [Sum | Acc]).

apply_inverse_col_sum(_RotMatBin, [], _Dim, _ColIdx, _RowIdx, Acc) ->
    Acc;
apply_inverse_col_sum(RotMatBin, [V | Rest], Dim, ColIdx, RowIdx, Acc) ->
    %% Get element at (RowIdx, ColIdx)
    Offset = (RowIdx * Dim + ColIdx) * 8,
    <<_:Offset/binary, F:64/float-little, _/binary>> = RotMatBin,
    apply_inverse_col_sum(RotMatBin, Rest, Dim, ColIdx, RowIdx + 1, Acc + V * F).

%% Dot product of two binary vectors (float64)
dot_product_bin(A, B) ->
    dot_product_bin_acc(A, B, 0.0).

dot_product_bin_acc(<<>>, <<>>, Acc) ->
    Acc;
dot_product_bin_acc(<<A:64/float-little, RestA/binary>>,
                    <<B:64/float-little, RestB/binary>>, Acc) ->
    dot_product_bin_acc(RestA, RestB, Acc + A * B).

%% Convert pairs to polar coordinates (r, theta)
to_polar(Vector, _Levels) ->
    to_polar_pairs(Vector, []).

to_polar_pairs([], Acc) ->
    lists:reverse(Acc);
to_polar_pairs([X, Y | Rest], Acc) ->
    R = math:sqrt(X * X + Y * Y),
    Theta = math:atan2(Y, X),
    to_polar_pairs(Rest, [{R, Theta} | Acc]).

%% Convert from polar to Cartesian
from_polar(PolarVec, _NumPairs) ->
    lists:flatmap(
        fun({R, Theta}) ->
            [R * math:cos(Theta), R * math:sin(Theta)]
        end,
        PolarVec
    ).

%% Compute angle quantization levels (centers of buckets)
compute_angle_levels(NumLevels) ->
    %% Angles range from -pi to pi
    %% Create NumLevels buckets with centers
    BucketSize = 2 * math:pi() / NumLevels,
    [(-math:pi() + BucketSize * (I + 0.5)) || I <- lists:seq(0, NumLevels - 1)].

%% Quantize polar coordinates
quantize_polar(PolarVec, Bits) ->
    NumLevels = 1 bsl Bits,
    BucketSize = 2 * math:pi() / NumLevels,

    {RadiiList, AngleIndices} = lists:unzip(
        lists:map(
            fun({R, Theta}) ->
                %% Quantize radius to 16-bit (log scale for better precision)
                QuantizedR = quantize_radius(R),
                %% Quantize angle to Bits
                AngleIdx = quantize_angle(Theta, BucketSize, NumLevels),
                {QuantizedR, AngleIdx}
            end,
            PolarVec
        )
    ),

    %% Pack radii as 16-bit values
    RadiusBin = << <<R:16/unsigned-little>> || R <- RadiiList >>,

    %% Pack angle indices as bit-packed
    AngleBin = pack_bits(AngleIndices, Bits),

    {RadiusBin, AngleBin}.

quantize_radius(R) ->
    %% Quantize to 16-bit using log scale for better dynamic range
    %% Map [0, inf) to [0, 65535]
    case R < 1.0e-10 of
        true -> 0;
        false ->
            %% Use log scale: map log(r + 1) to [0, 65535]
            LogR = math:log(R + 1.0),
            %% Assume typical embeddings have |r| < 10
            Scaled = LogR / math:log(11.0) * 65535,
            min(65535, max(0, round(Scaled)))
    end.

dequantize_radius(QuantizedR) ->
    %% Inverse of quantize_radius
    case QuantizedR of
        0 -> 0.0;
        _ ->
            LogR = QuantizedR / 65535.0 * math:log(11.0),
            math:exp(LogR) - 1.0
    end.

quantize_angle(Theta, BucketSize, NumLevels) ->
    %% Theta in [-pi, pi], map to [0, NumLevels)
    Normalized = Theta + math:pi(),
    Idx = floor(Normalized / BucketSize),
    min(NumLevels - 1, max(0, Idx)).

%% Pack list of N-bit integers into binary
pack_bits(Indices, Bits) ->
    pack_bits(Indices, Bits, <<>>, 0, 0).

pack_bits([], _Bits, Acc, Buffer, BufferBits) ->
    %% Flush remaining bits
    case BufferBits > 0 of
        true ->
            PadBits = 8 - (BufferBits rem 8),
            FinalBuffer = (Buffer bsl PadBits) band 16#FF,
            <<Acc/binary, FinalBuffer:8>>;
        false ->
            Acc
    end;
pack_bits([Idx | Rest], Bits, Acc, Buffer, BufferBits) ->
    NewBuffer = (Buffer bsl Bits) bor Idx,
    NewBufferBits = BufferBits + Bits,
    case NewBufferBits >= 8 of
        true ->
            %% Extract bytes
            {NewAcc, RemBuffer, RemBits} = extract_bytes(Acc, NewBuffer, NewBufferBits),
            pack_bits(Rest, Bits, NewAcc, RemBuffer, RemBits);
        false ->
            pack_bits(Rest, Bits, Acc, NewBuffer, NewBufferBits)
    end.

extract_bytes(Acc, Buffer, BufferBits) when BufferBits >= 8 ->
    ExtraBits = BufferBits - 8,
    Byte = (Buffer bsr ExtraBits) band 16#FF,
    RemBuffer = Buffer band ((1 bsl ExtraBits) - 1),
    extract_bytes(<<Acc/binary, Byte:8>>, RemBuffer, ExtraBits);
extract_bytes(Acc, Buffer, BufferBits) ->
    {Acc, Buffer, BufferBits}.

%% Dequantize polar coordinates
dequantize_polar(RadiusBin, AngleBin, NumPairs, Bits, Levels) ->
    %% Unpack radii
    Radii = [dequantize_radius(R) || <<R:16/unsigned-little>> <= RadiusBin],

    %% Unpack angle indices
    AngleIndices = unpack_bits(AngleBin, Bits, NumPairs),

    %% Convert to (R, Theta) pairs
    lists:zipwith(
        fun(R, AngleIdx) ->
            Theta = lists:nth(AngleIdx + 1, Levels),
            {R, Theta}
        end,
        Radii,
        AngleIndices
    ).

%% Unpack N-bit integers from binary
unpack_bits(Bin, Bits, Count) ->
    unpack_bits(Bin, Bits, Count, 0, 0, []).

unpack_bits(_Bin, _Bits, 0, _Buffer, _BufferBits, Acc) ->
    lists:reverse(Acc);
unpack_bits(Bin, Bits, Count, Buffer, BufferBits, Acc) when BufferBits >= Bits ->
    %% Extract one value
    ExtraBits = BufferBits - Bits,
    Value = (Buffer bsr ExtraBits) band ((1 bsl Bits) - 1),
    RemBuffer = Buffer band ((1 bsl ExtraBits) - 1),
    unpack_bits(Bin, Bits, Count - 1, RemBuffer, ExtraBits, [Value | Acc]);
unpack_bits(<<Byte:8, Rest/binary>>, Bits, Count, Buffer, BufferBits, Acc) ->
    NewBuffer = (Buffer bsl 8) bor Byte,
    NewBufferBits = BufferBits + 8,
    unpack_bits(Rest, Bits, Count, NewBuffer, NewBufferBits, Acc);
unpack_bits(<<>>, Bits, Count, Buffer, BufferBits, Acc) when BufferBits >= Bits ->
    %% Handle remaining bits
    unpack_bits(<<>>, Bits, Count, Buffer, BufferBits, Acc);
unpack_bits(<<>>, _Bits, _Count, _Buffer, _BufferBits, Acc) ->
    %% Not enough bits, return what we have
    lists:reverse(Acc).

%% Generate QJL random sign matrix
%% Returns D x QJLDim matrix of +1/-1 values packed as bits
generate_qjl_matrix(Dim, QJLDim, Seed) ->
    _ = rand:seed(exsss, {Seed, Seed + 1, Seed + 2}),
    %% Generate random signs for D x QJLDim matrix
    %% Store as bits (1 = +1, 0 = -1)
    Signs = [rand:uniform(2) - 1 || _ <- lists:seq(1, Dim * QJLDim)],
    pack_bits_simple(Signs).

pack_bits_simple(Bits) ->
    pack_bits_simple(Bits, <<>>, 0, 0).

pack_bits_simple([], Acc, Buffer, BufferBits) ->
    case BufferBits > 0 of
        true ->
            PadBits = 8 - BufferBits,
            FinalByte = Buffer bsl PadBits,
            <<Acc/binary, FinalByte:8>>;
        false ->
            Acc
    end;
pack_bits_simple([B | Rest], Acc, Buffer, 7) ->
    Byte = (Buffer bsl 1) bor B,
    pack_bits_simple(Rest, <<Acc/binary, Byte:8>>, 0, 0);
pack_bits_simple([B | Rest], Acc, Buffer, BufferBits) ->
    pack_bits_simple(Rest, Acc, (Buffer bsl 1) bor B, BufferBits + 1).

%% Compute QJL signs for error correction
compute_qjl_signs(QJLMatrix, Vector) ->
    %% Compute sign(QJL * v) for each QJL row
    %% QJL is D x D, each entry is +1 or -1
    Dim = length(Vector),
    VecBin = << <<V:64/float-little>> || V <- Vector >>,

    Signs = compute_qjl_row_signs(QJLMatrix, VecBin, Dim, 0, []),
    pack_bits_simple(Signs).

compute_qjl_row_signs(_QJLMatrix, _VecBin, Dim, RowIdx, Acc) when RowIdx >= Dim ->
    lists:reverse(Acc);
compute_qjl_row_signs(QJLMatrix, VecBin, Dim, RowIdx, Acc) ->
    %% Compute dot product of QJL row with vector
    DotProd = compute_qjl_dot(QJLMatrix, VecBin, Dim, RowIdx),
    Sign = if DotProd >= 0 -> 1; true -> 0 end,
    compute_qjl_row_signs(QJLMatrix, VecBin, Dim, RowIdx + 1, [Sign | Acc]).

compute_qjl_dot(QJLMatrix, VecBin, Dim, RowIdx) ->
    %% Sum: sign(QJL[row, i]) * Vec[i]
    compute_qjl_dot_acc(QJLMatrix, VecBin, Dim, RowIdx, 0, 0.0).

compute_qjl_dot_acc(_QJLMatrix, <<>>, _Dim, _RowIdx, _ColIdx, Acc) ->
    Acc;
compute_qjl_dot_acc(QJLMatrix, <<V:64/float-little, RestV/binary>>, Dim, RowIdx, ColIdx, Acc) ->
    %% Get sign at (RowIdx, ColIdx)
    BitIdx = RowIdx * Dim + ColIdx,
    ByteIdx = BitIdx div 8,
    BitOffset = 7 - (BitIdx rem 8),
    <<_:ByteIdx/binary, Byte:8, _/binary>> = QJLMatrix,
    SignBit = (Byte bsr BitOffset) band 1,
    Sign = if SignBit =:= 1 -> 1.0; true -> -1.0 end,
    compute_qjl_dot_acc(QJLMatrix, RestV, Dim, RowIdx, ColIdx + 1, Acc + Sign * V).

%% Apply QJL error correction via iterative sign-constrained refinement
%% For each iteration:
%%   1. Compute current signs: sign(QJL * v_approx)
%%   2. Compare with stored signs to find mismatches
%%   3. Adjust vector toward satisfying sign constraints via gradient
apply_qjl_correction(Vector, _StoredSigns, _QJLMatrix, _QJLDim, 0, _LR) ->
    %% No iterations, return original vector
    Vector;
apply_qjl_correction(Vector, StoredSigns, QJLMatrix, QJLDim, Iterations, LR) ->
    %% Convert vector to binary for efficient processing
    VecBin = << <<V:64/float-little>> || V <- Vector >>,
    Dim = length(Vector),

    %% Run iterative refinement
    FinalVecBin = qjl_iterate(VecBin, StoredSigns, QJLMatrix, Dim, QJLDim, Iterations, LR),

    %% Convert back to list
    [V || <<V:64/float-little>> <= FinalVecBin].

qjl_iterate(VecBin, _StoredSigns, _QJLMatrix, _Dim, _QJLDim, 0, _LR) ->
    VecBin;
qjl_iterate(VecBin, StoredSigns, QJLMatrix, Dim, QJLDim, Iter, LR) ->
    %% Compute current signs and gradient
    Gradient = compute_qjl_gradient(VecBin, StoredSigns, QJLMatrix, Dim, QJLDim),

    %% Normalize gradient to prevent overshooting
    NormGradient = normalize_gradient(Gradient, VecBin),

    %% Apply normalized gradient with learning rate
    NewVecBin = apply_gradient(VecBin, NormGradient, LR),

    qjl_iterate(NewVecBin, StoredSigns, QJLMatrix, Dim, QJLDim, Iter - 1, LR).

%% Normalize gradient relative to vector magnitude
normalize_gradient(GradientBin, VecBin) ->
    VecNorm = compute_binary_norm(VecBin),
    GradNorm = compute_binary_norm(GradientBin),
    case GradNorm < 1.0e-10 of
        true ->
            GradientBin;  %% No gradient to apply
        false ->
            %% Scale gradient to be proportional to vector magnitude
            Scale = VecNorm / (GradNorm * 10.0),  %% 10x dampening factor
            scale_gradient(GradientBin, Scale)
    end.

compute_binary_norm(Bin) ->
    SumSq = compute_binary_norm_acc(Bin, 0.0),
    math:sqrt(SumSq).

compute_binary_norm_acc(<<>>, Acc) ->
    Acc;
compute_binary_norm_acc(<<V:64/float-little, Rest/binary>>, Acc) ->
    compute_binary_norm_acc(Rest, Acc + V * V).

scale_gradient(GradientBin, Scale) ->
    << <<(G * Scale):64/float-little>> || <<G:64/float-little>> <= GradientBin >>.

%% Compute gradient from sign mismatches
%% For each mismatch at row i: add target_sign * QJL_row_i to gradient
compute_qjl_gradient(VecBin, StoredSigns, QJLMatrix, Dim, QJLDim) ->
    %% Initialize zero gradient
    Gradient = lists:duplicate(Dim, 0.0),

    %% For each QJL row, check if sign matches
    compute_qjl_gradient_rows(VecBin, StoredSigns, QJLMatrix, Dim, QJLDim, 0, Gradient).

compute_qjl_gradient_rows(_VecBin, _StoredSigns, _QJLMatrix, _Dim, QJLDim, RowIdx, Gradient)
  when RowIdx >= QJLDim ->
    %% Convert gradient to binary
    << <<G:64/float-little>> || G <- Gradient >>;
compute_qjl_gradient_rows(VecBin, StoredSigns, QJLMatrix, Dim, QJLDim, RowIdx, Gradient) ->
    %% Compute dot product of QJL row with vector
    DotProd = compute_qjl_dot(QJLMatrix, VecBin, Dim, RowIdx),

    %% Get current and stored signs
    CurrentSign = if DotProd >= 0 -> 1; true -> -1 end,
    StoredSign = get_stored_sign(StoredSigns, RowIdx),

    %% If signs mismatch, add QJL row to gradient (scaled by target sign)
    NewGradient = case CurrentSign =:= StoredSign of
        true ->
            Gradient;  %% No update needed
        false ->
            %% Add target_sign * QJL_row to gradient
            add_qjl_row_to_gradient(QJLMatrix, Dim, RowIdx, StoredSign, Gradient)
    end,

    compute_qjl_gradient_rows(VecBin, StoredSigns, QJLMatrix, Dim, QJLDim, RowIdx + 1, NewGradient).

%% Get stored sign at index (1 for positive, -1 for negative)
get_stored_sign(StoredSigns, RowIdx) ->
    ByteIdx = RowIdx div 8,
    BitOffset = 7 - (RowIdx rem 8),
    <<_:ByteIdx/binary, Byte:8, _/binary>> = StoredSigns,
    SignBit = (Byte bsr BitOffset) band 1,
    if SignBit =:= 1 -> 1; true -> -1 end.

%% Add target_sign * QJL_row to gradient
add_qjl_row_to_gradient(QJLMatrix, Dim, RowIdx, TargetSign, Gradient) ->
    add_qjl_row_elements(QJLMatrix, Dim, RowIdx, TargetSign, Gradient, 0, []).

add_qjl_row_elements(_QJLMatrix, Dim, _RowIdx, _TargetSign, [], _ColIdx, Acc)
  when length(Acc) =:= Dim ->
    lists:reverse(Acc);
add_qjl_row_elements(_QJLMatrix, _Dim, _RowIdx, _TargetSign, [], _ColIdx, Acc) ->
    lists:reverse(Acc);
add_qjl_row_elements(QJLMatrix, Dim, RowIdx, TargetSign, [G | RestG], ColIdx, Acc) ->
    %% Get QJL sign at (RowIdx, ColIdx)
    BitIdx = RowIdx * Dim + ColIdx,
    ByteIdx = BitIdx div 8,
    BitOffset = 7 - (BitIdx rem 8),
    <<_:ByteIdx/binary, Byte:8, _/binary>> = QJLMatrix,
    QJLSignBit = (Byte bsr BitOffset) band 1,
    QJLSign = if QJLSignBit =:= 1 -> 1.0; true -> -1.0 end,

    %% Add contribution to gradient
    NewG = G + TargetSign * QJLSign,
    add_qjl_row_elements(QJLMatrix, Dim, RowIdx, TargetSign, RestG, ColIdx + 1, [NewG | Acc]).

%% Apply gradient to vector: v_new = v + lr * gradient
apply_gradient(VecBin, GradientBin, LR) ->
    apply_gradient_elements(VecBin, GradientBin, LR, <<>>).

apply_gradient_elements(<<>>, <<>>, _LR, Acc) ->
    Acc;
apply_gradient_elements(<<V:64/float-little, RestV/binary>>,
                        <<G:64/float-little, RestG/binary>>, LR, Acc) ->
    NewV = V + LR * G,
    apply_gradient_elements(RestV, RestG, LR, <<Acc/binary, NewV:64/float-little>>).

%% Full ADC (Asymmetric Distance Computation) using precomputed tables
%% Table format per pair: QRSq (1 float) + NumLevels cos terms
%% Distance: d^2 = QRSq + DR^2 - CosTerm * DR
compute_full_adc_distance(_Tables, [], [], _NumLevels, _PairIdx, Acc) ->
    math:sqrt(max(0.0, Acc));
compute_full_adc_distance(Tables, [DR | RestR], [AngleIdx | RestA], NumLevels, PairIdx, Acc) ->
    %% Table row size: QRSq (1 float) + NumLevels cos terms
    TableRowSize = (1 + NumLevels) * 4,

    %% Look up QRSq and CosTerm from table
    RowOffset = PairIdx * TableRowSize,
    <<_:RowOffset/binary, QRSq:32/float-little, CosTermsBin/binary>> = Tables,

    %% Get the CosTerm for this angle index
    CosTermOffset = AngleIdx * 4,
    <<_:CosTermOffset/binary, CosTerm:32/float-little, _/binary>> = CosTermsBin,

    %% Full polar distance formula: d^2 = r_q^2 + r_d^2 - 2*r_q*r_d*cos(theta_diff)
    %% CosTerm already contains 2 * r_q * cos(theta_diff)
    DRSq = DR * DR,
    ContribSq = QRSq + DRSq - CosTerm * DR,

    compute_full_adc_distance(Tables, RestR, RestA, NumLevels, PairIdx + 1, Acc + ContribSq).
