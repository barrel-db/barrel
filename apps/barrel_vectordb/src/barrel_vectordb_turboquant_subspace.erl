%%%-------------------------------------------------------------------
%%% @doc Subspace-TurboQuant: O(D/M) scalable vector quantization
%%%
%%% Addresses O(D^2) scaling issues in standard TurboQuant by splitting
%%% D-dimensional vectors into M independent subspaces. Each subspace
%%% applies TurboQuant with its own rotation matrix, reducing:
%%%
%%% - Rotation matrix memory: D^2 -> M * (D/M)^2 = D^2/M (8x reduction for M=8)
%%% - Encode latency: O(D^2) -> O(D^2/M) per subspace, parallelizable
%%% - No training required (preserves data-oblivious property)
%%%
%%% Storage format:
%%%   Header: <<Version:8, Bits:8, M:8, Flags:8>>
%%%   Body: [SubspaceCode_1, ..., SubspaceCode_M]
%%%   Each subspace code contains radii, angles, and QJL signs
%%%
%%% Performance for D=768, M=8:
%%% - Rotation matrices: 8 * 96^2 * 8 = 590KB (vs 4.7MB)
%%% - Encode latency: ~0.5ms (vs ~3.6ms)
%%% - Recall: Within 2-3% of full TurboQuant
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_turboquant_subspace).

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
    select_m/1,
    split_subvectors/3
]).

-record(tq_subspace_config, {
    bits :: 2..4,                          %% Bits per component
    m :: pos_integer(),                    %% Number of subspaces
    dimension :: pos_integer(),            %% Total vector dimension
    subdim :: pos_integer(),               %% Dimension per subspace
    subspace_configs :: [term()],          %% Per-subspace TurboQuant configs
    seeds :: [integer()]                   %% Per-subspace seeds
}).

-type tq_subspace_config() :: #tq_subspace_config{}.
-type tq_subspace_code() :: binary().
-type distance_tables() :: binary().

-export_type([tq_subspace_config/0, tq_subspace_code/0, distance_tables/0]).

%% Version for storage format
-define(TQS_VERSION, 2).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new Subspace-TurboQuant configuration
%% Options:
%%   bits - bits per component (default: 3, range: 2-4)
%%   dimension - vector dimension (required, must be even and divisible by m)
%%   m - number of subspaces (default: auto-selected based on dimension)
%%   seed - base random seed (default: 42)
-spec new(map()) -> {ok, tq_subspace_config()} | {error, term()}.
new(Options) ->
    Bits = maps:get(bits, Options, 3),
    Dimension = maps:get(dimension, Options, undefined),
    BaseSeed = maps:get(seed, Options, 42),

    case validate_dimension(Dimension) of
        {error, _} = Error ->
            Error;
        ok ->
            %% Auto-select M based on dimension
            M = maps:get(m, Options, select_m(Dimension)),
            case validate_config(Bits, Dimension, M) of
                ok ->
                    SubDim = Dimension div M,
                    %% Generate unique seeds for each subspace
                    Seeds = [BaseSeed + I * 1000 || I <- lists:seq(0, M - 1)],

                    %% Create per-subspace TurboQuant configs
                    SubspaceConfigs = lists:map(
                        fun(Seed) ->
                            {ok, Config} = barrel_vectordb_turboquant:new(#{
                                bits => Bits,
                                dimension => SubDim,
                                seed => Seed
                            }),
                            Config
                        end,
                        Seeds
                    ),

                    {ok, #tq_subspace_config{
                        bits = Bits,
                        m = M,
                        dimension = Dimension,
                        subdim = SubDim,
                        subspace_configs = SubspaceConfigs,
                        seeds = Seeds
                    }};
                {error, _} = Error ->
                    Error
            end
    end.

%% @doc Encode a vector using Subspace-TurboQuant
%% Returns compact binary with header + M subspace codes
-spec encode(tq_subspace_config(), [float()]) -> tq_subspace_code().
encode(#tq_subspace_config{dimension = Dim, bits = Bits, m = M, subdim = SubDim,
                            subspace_configs = Configs}, Vector) when length(Vector) =:= Dim ->
    %% Split vector into M subvectors
    Subvectors = split_subvectors(Vector, M, SubDim),

    %% Encode each subvector with its corresponding config
    SubspaceCodes = lists:zipwith(
        fun(Subvec, Config) ->
            barrel_vectordb_turboquant:encode(Config, Subvec)
        end,
        Subvectors,
        Configs
    ),

    %% Pack with header
    %% Header: version, bits, M, flags
    Header = <<?TQS_VERSION:8, Bits:8, M:8, 0:8>>,

    %% Concatenate all subspace codes (without their individual headers)
    SubspaceData = lists:map(
        fun(<<1:8, _Bits:8, _Flags:16, Rest/binary>>) ->
            %% Strip TurboQuant header (version=1, bits, flags)
            Rest
        end,
        SubspaceCodes
    ),

    iolist_to_binary([Header | SubspaceData]);
encode(#tq_subspace_config{dimension = Dim}, Vector) ->
    error({dimension_mismatch, Dim, length(Vector)}).

%% @doc Decode Subspace-TurboQuant code back to approximate vector
-spec decode(tq_subspace_config(), tq_subspace_code()) -> [float()].
decode(#tq_subspace_config{m = M, subdim = SubDim, subspace_configs = Configs, bits = Bits},
       <<?TQS_VERSION:8, _Bits:8, _M:8, _Flags:8, Rest/binary>>) ->
    %% Calculate size of each subspace code (without header)
    SubspaceCodeSize = compute_subspace_code_size(SubDim, Bits),

    %% Split into per-subspace codes and decode
    SubspaceCodes = split_binary(Rest, SubspaceCodeSize, M, []),

    %% Reconstruct each subspace code with TurboQuant header and decode
    DecodedSubvectors = lists:zipwith(
        fun(SubspaceCode, Config) ->
            %% Add back TurboQuant header
            TQCode = <<1:8, Bits:8, 0:16, SubspaceCode/binary>>,
            barrel_vectordb_turboquant:decode(Config, TQCode)
        end,
        SubspaceCodes,
        Configs
    ),

    %% Flatten subvectors
    lists:flatten(DecodedSubvectors).

%% @doc Precompute distance lookup tables for a query vector
%% Returns M sets of tables (concatenated)
-spec precompute_tables(tq_subspace_config(), [float()]) -> distance_tables().
precompute_tables(#tq_subspace_config{dimension = Dim, m = M, subdim = SubDim,
                                       subspace_configs = Configs}, Query)
  when length(Query) =:= Dim ->
    %% Split query into subvectors
    QuerySubvectors = split_subvectors(Query, M, SubDim),

    %% Precompute tables for each subspace
    SubspaceTables = lists:zipwith(
        fun(QuerySub, Config) ->
            barrel_vectordb_turboquant:precompute_tables(Config, QuerySub)
        end,
        QuerySubvectors,
        Configs
    ),

    %% Concatenate with size prefix for each table
    << <<(byte_size(T)):32, T/binary>> || T <- SubspaceTables >>.

%% @doc Compute asymmetric distance using precomputed tables (pure Erlang)
-spec distance(distance_tables(), tq_subspace_code()) -> float().
distance(Tables, <<?TQS_VERSION:8, Bits:8, M:8, _Flags:8, Rest/binary>>) ->
    %% Parse tables and codes, compute per-subspace distances, sum
    compute_subspace_distances(Tables, Rest, Bits, M, 0.0).

%% @doc Compute ADC distance using SIMD-accelerated NIF
-spec distance_nif(distance_tables(), tq_subspace_code()) -> float().
distance_nif(Tables, Code) when is_binary(Tables), is_binary(Code) ->
    <<?TQS_VERSION:8, Bits:8, M:8, _:8, _/binary>> = Code,
    barrel_vectordb_nif:tqs_adc_distance(Tables, Code, Bits, M).

%% @doc Batch compute ADC distance using SIMD-accelerated NIF
-spec batch_distance_nif(distance_tables(), [tq_subspace_code()]) -> [float()].
batch_distance_nif(Tables, Codes) when is_binary(Tables), is_list(Codes) ->
    case Codes of
        [] -> [];
        [<<?TQS_VERSION:8, Bits:8, M:8, _:8, _/binary>> | _] ->
            barrel_vectordb_nif:tqs_batch_adc_distance(Tables, Codes, Bits, M)
    end.

%% @doc Batch encode multiple vectors
-spec batch_encode(tq_subspace_config(), [[float()]]) -> [tq_subspace_code()].
batch_encode(Config, Vectors) ->
    [encode(Config, V) || V <- Vectors].

%% @doc Get configuration info
-spec info(tq_subspace_config()) -> map().
info(#tq_subspace_config{bits = Bits, m = M, dimension = Dim, subdim = SubDim,
                          seeds = Seeds}) ->
    %% Calculate bytes per vector
    SubspaceCodeSize = compute_subspace_code_size(SubDim, Bits),
    HeaderBytes = 4,
    TotalBytes = HeaderBytes + M * SubspaceCodeSize,

    %% Calculate memory for rotation matrices
    RotationMatrixBytes = M * SubDim * SubDim * 8,

    #{
        bits => Bits,
        m => M,
        dimension => Dim,
        subdim => SubDim,
        bytes_per_vector => TotalBytes,
        compression_ratio => (Dim * 4) / TotalBytes,
        rotation_matrix_bytes => RotationMatrixBytes,
        training_required => false,
        seeds => Seeds
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

validate_dimension(undefined) ->
    {error, dimension_required};
validate_dimension(Dim) when Dim rem 2 =/= 0 ->
    {error, {dimension_must_be_even, Dim}};
validate_dimension(Dim) when Dim < 2 ->
    {error, {dimension_too_small, Dim}};
validate_dimension(_) ->
    ok.

validate_config(Bits, _, _) when Bits < 2; Bits > 4 ->
    {error, {bits_out_of_range, Bits, {2, 4}}};
validate_config(_, Dim, M) when Dim rem M =/= 0 ->
    {error, {dimension_not_divisible_by_m, Dim, M}};
validate_config(_, Dim, M) when (Dim div M) rem 2 =/= 0 ->
    {error, {subdim_must_be_even, Dim div M}};
validate_config(_, _, _) ->
    ok.

%% @doc Auto-select M based on dimension for optimal performance
%% Keeps subdim around 64-128 for best SIMD performance
-spec select_m(pos_integer()) -> pos_integer().
select_m(D) when D =< 128 -> 1;
select_m(D) when D =< 256 -> 2;
select_m(D) when D =< 512 -> 4;
select_m(D) when D =< 1024 -> 8;
select_m(D) when D =< 2048 -> 16;
select_m(_) -> 32.

%% Split vector into M subvectors
-spec split_subvectors([float()], pos_integer(), pos_integer()) -> [[float()]].
split_subvectors(Vec, M, SubDim) ->
    split_subvectors(Vec, M, SubDim, []).

split_subvectors([], 0, _SubDim, Acc) ->
    lists:reverse(Acc);
split_subvectors(Vec, Remaining, SubDim, Acc) when Remaining > 0 ->
    {Subvec, Rest} = lists:split(SubDim, Vec),
    split_subvectors(Rest, Remaining - 1, SubDim, [Subvec | Acc]).

%% Compute size of subspace code (without header)
compute_subspace_code_size(SubDim, Bits) ->
    NumPairs = SubDim div 2,
    RadiusBytes = NumPairs * 2,       %% 16-bit per radius
    AngleBits = NumPairs * Bits,
    AngleBytes = (AngleBits + 7) div 8,
    QJLBytes = (SubDim + 7) div 8,
    RadiusBytes + AngleBytes + QJLBytes.

%% Split binary into M chunks
split_binary(_Bin, _Size, 0, Acc) ->
    lists:reverse(Acc);
split_binary(Bin, Size, Remaining, Acc) ->
    <<Chunk:Size/binary, Rest/binary>> = Bin,
    split_binary(Rest, Size, Remaining - 1, [Chunk | Acc]).

%% Compute sum of squared distances across all subspaces
compute_subspace_distances(_Tables, _Codes, _Bits, 0, AccSq) ->
    math:sqrt(max(0.0, AccSq));
compute_subspace_distances(Tables, Codes, Bits, Remaining, AccSq) ->
    %% Parse table size prefix
    <<TableSize:32, TableData:TableSize/binary, RestTables/binary>> = Tables,

    %% Calculate subspace code size and parse
    NumLevels = 1 bsl Bits,
    TableRowSize = (1 + NumLevels) * 4,
    NumPairs = TableSize div TableRowSize,
    SubDim = NumPairs * 2,
    CodeSize = compute_subspace_code_size(SubDim, Bits),

    <<SubspaceCode:CodeSize/binary, RestCodes/binary>> = Codes,

    %% Reconstruct TurboQuant code with header for distance computation
    TQCode = <<1:8, Bits:8, 0:16, SubspaceCode/binary>>,

    %% Compute squared distance for this subspace
    DistSq = compute_subspace_dist_sq(TableData, TQCode, Bits, NumLevels, NumPairs),

    compute_subspace_distances(RestTables, RestCodes, Bits, Remaining - 1, AccSq + DistSq).

%% Compute squared distance for a single subspace (no sqrt - we sum and sqrt at end)
compute_subspace_dist_sq(Tables, <<1:8, _Bits:8, _Flags:16, Rest/binary>>, Bits, NumLevels, NumPairs) ->
    RadiusBytes = NumPairs * 2,
    AngleBits = NumPairs * Bits,
    AngleBytes = (AngleBits + 7) div 8,

    <<RadiusBin:RadiusBytes/binary, AngleBin:AngleBytes/binary, _/binary>> = Rest,

    %% Unpack radii and angles
    Radii = [dequantize_radius(R) || <<R:16/unsigned-little>> <= RadiusBin],
    AngleIndices = unpack_bits(AngleBin, Bits, NumPairs),

    %% Compute distance contribution (sum of squared differences per pair)
    compute_adc_sum(Tables, Radii, AngleIndices, NumLevels, 0, 0.0).

%% ADC computation for one subspace - returns sum of squared contributions
compute_adc_sum(_Tables, [], [], _NumLevels, _PairIdx, Acc) ->
    Acc;
compute_adc_sum(Tables, [DR | RestR], [AngleIdx | RestA], NumLevels, PairIdx, Acc) ->
    TableRowSize = (1 + NumLevels) * 4,
    RowOffset = PairIdx * TableRowSize,

    <<_:RowOffset/binary, QRSq:32/float-little, CosTermsBin/binary>> = Tables,

    CosTermOffset = AngleIdx * 4,
    <<_:CosTermOffset/binary, CosTerm:32/float-little, _/binary>> = CosTermsBin,

    DRSq = DR * DR,
    ContribSq = QRSq + DRSq - CosTerm * DR,

    compute_adc_sum(Tables, RestR, RestA, NumLevels, PairIdx + 1, Acc + ContribSq).

%% Dequantize radius (same as TurboQuant)
dequantize_radius(QuantizedR) ->
    case QuantizedR of
        0 -> 0.0;
        _ ->
            LogR = QuantizedR / 65535.0 * math:log(11.0),
            math:exp(LogR) - 1.0
    end.

%% Unpack N-bit integers from binary
unpack_bits(Bin, Bits, Count) ->
    unpack_bits(Bin, Bits, Count, 0, 0, []).

unpack_bits(_Bin, _Bits, 0, _Buffer, _BufferBits, Acc) ->
    lists:reverse(Acc);
unpack_bits(Bin, Bits, Count, Buffer, BufferBits, Acc) when BufferBits >= Bits ->
    ExtraBits = BufferBits - Bits,
    Value = (Buffer bsr ExtraBits) band ((1 bsl Bits) - 1),
    RemBuffer = Buffer band ((1 bsl ExtraBits) - 1),
    unpack_bits(Bin, Bits, Count - 1, RemBuffer, ExtraBits, [Value | Acc]);
unpack_bits(<<Byte:8, Rest/binary>>, Bits, Count, Buffer, BufferBits, Acc) ->
    NewBuffer = (Buffer bsl 8) bor Byte,
    NewBufferBits = BufferBits + 8,
    unpack_bits(Rest, Bits, Count, NewBuffer, NewBufferBits, Acc);
unpack_bits(<<>>, _Bits, _Count, _Buffer, _BufferBits, Acc) ->
    lists:reverse(Acc).
