%%%-------------------------------------------------------------------
%%% @doc Embedding policy for record-mode databases.
%%%
%%% A policy declares which document fields embed, how they are joined
%%% into the embedded text, and how documents project into search
%%% metadata. It is configured per database at `barrel:open/2' via the
%%% `embedding' option and evaluated on every write by the record
%%% indexer, and on every read by the read-through docstore adapter.
%%%
%%% This module is pure: validation, matching, text rendering, and
%%% metadata projection only. Embedding itself (barrel_embed) and
%%% persistence of the policy are barrel's concern.
%%%
%%% Policy map:
%%% ```
%%% #{
%%%     fields          => [[<<"title">>], [<<"body">>, <<"text">>]],
%%%     join            => <<"\n">>,          %% default
%%%     mode            => async | sync,       %% default async
%%%     dimensions      => pos_integer(),      %% optional, checked at open
%%%     embedder        => term(),             %% barrel_embed config, opaque here
%%%     metadata_fields => [binary()]          %% optional projection
%%% }
%%% '''
%%% A field is a path of binary keys into the document (a bare binary is
%%% shorthand for a top-level field). Only binary values embed; missing
%%% fields and non-binary values are skipped. Without `metadata_fields',
%%% metadata is the document minus `id' and `_'-prefixed keys.
%%%
%%% `fields' is optional: a policy without fields never auto-embeds and
%%% relies on client-supplied vectors (the `<<"_embedding">>' document
%%% field or the `vector' put option).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embedding_policy).

-export([validate/1, matches/2, text/2, metadata/2, mode/1]).

-type path() :: [binary()].
-type policy() :: #{
    fields := [path()],
    join := binary(),
    mode := async | sync,
    dimensions => pos_integer(),
    embedder => term(),
    metadata_fields => [binary()]
}.
-export_type([policy/0]).

-define(ALLOWED_KEYS,
        [fields, join, mode, dimensions, embedder, metadata_fields]).

%%====================================================================
%% API
%%====================================================================

%% @doc Validate and normalize a policy map.
%% Bare-binary fields become single-segment paths; defaults are filled
%% in (`join', `mode'). Unknown keys are rejected so config typos fail
%% loudly at open time.
-spec validate(map()) -> {ok, policy()} | {error, term()}.
validate(Map) when is_map(Map) ->
    case maps:keys(Map) -- ?ALLOWED_KEYS of
        [] ->
            try
                {ok, normalize(Map)}
            catch
                throw:Reason -> {error, Reason}
            end;
        Unknown ->
            {error, {unknown_options, Unknown}}
    end;
validate(Other) ->
    {error, {invalid_policy, Other}}.

%% @doc Does the document carry at least one embeddable policy field?
%% Documents that match nothing produce no vector (and any existing
%% vector is removed by the indexer).
-spec matches(policy(), map()) -> boolean().
matches(#{fields := Fields}, Doc) ->
    lists:any(fun(Path) -> field_text(Path, Doc) =/= skip end, Fields).

%% @doc Render the text to embed: policy fields in order, missing and
%% non-binary values skipped, joined with the policy separator.
-spec text(policy(), map()) -> binary().
text(#{fields := Fields, join := Join}, Doc) ->
    Parts = lists:filtermap(
        fun(Path) ->
            case field_text(Path, Doc) of
                skip -> false;
                Text -> {true, Text}
            end
        end, Fields),
    join_binaries(Parts, Join).

%% @doc Project a document into search metadata.
%% With `metadata_fields', keep only those top-level fields; otherwise
%% the document minus `id' and `_'-prefixed keys.
-spec metadata(policy(), map()) -> map().
metadata(#{metadata_fields := Fields}, Doc) ->
    maps:with(Fields, Doc);
metadata(_Policy, Doc) ->
    maps:filter(fun(K, _V) -> not reserved_key(K) end, Doc).

%% @doc The policy's write mode (async | sync).
-spec mode(policy()) -> async | sync.
mode(#{mode := Mode}) ->
    Mode.

%%====================================================================
%% Internal
%%====================================================================

normalize(Map) ->
    %% fields is optional: a policy without fields never auto-embeds
    %% (bring-your-own-embeddings via the _embedding document field or
    %% the vector put option).
    Fields = case maps:get(fields, Map, []) of
        Fs when is_list(Fs) -> [normalize_field(F) || F <- Fs];
        Other -> throw({invalid_option, fields, Other})
    end,
    Join = case maps:get(join, Map, <<"\n">>) of
        J when is_binary(J) -> J;
        BadJoin -> throw({invalid_option, join, BadJoin})
    end,
    Mode = case maps:get(mode, Map, async) of
        async -> async;
        sync -> sync;
        BadMode -> throw({invalid_option, mode, BadMode})
    end,
    Policy0 = #{fields => Fields, join => Join, mode => Mode},
    Policy1 = case maps:get(dimensions, Map, undefined) of
        undefined -> Policy0;
        Dim when is_integer(Dim), Dim > 0 -> Policy0#{dimensions => Dim};
        BadDim -> throw({invalid_option, dimensions, BadDim})
    end,
    Policy2 = case maps:get(embedder, Map, undefined) of
        undefined -> Policy1;
        Embedder -> Policy1#{embedder => Embedder}
    end,
    case maps:get(metadata_fields, Map, undefined) of
        undefined ->
            Policy2;
        MFs when is_list(MFs) ->
            case lists:all(fun is_binary/1, MFs) of
                true -> Policy2#{metadata_fields => MFs};
                false -> throw({invalid_option, metadata_fields, MFs})
            end;
        BadMFs ->
            throw({invalid_option, metadata_fields, BadMFs})
    end.

normalize_field(F) when is_binary(F) ->
    [F];
normalize_field(Path) when is_list(Path), Path =/= [] ->
    case lists:all(fun is_binary/1, Path) of
        true -> Path;
        false -> throw({invalid_field, Path})
    end;
normalize_field(Other) ->
    throw({invalid_field, Other}).

%% @private Resolve a path to embeddable text: non-empty binaries only.
field_text(Path, Doc) ->
    case get_path(Path, Doc) of
        Bin when is_binary(Bin), Bin =/= <<>> -> Bin;
        _ -> skip
    end.

get_path([], Value) ->
    Value;
get_path([Key | Rest], Doc) when is_map(Doc) ->
    case maps:find(Key, Doc) of
        {ok, Value} -> get_path(Rest, Value);
        error -> undefined
    end;
get_path(_Path, _NonMap) ->
    undefined.

join_binaries([], _Join) ->
    <<>>;
join_binaries([Single], _Join) ->
    Single;
join_binaries([First | Rest], Join) ->
    lists:foldl(
        fun(Part, Acc) -> <<Acc/binary, Join/binary, Part/binary>> end,
        First, Rest).

reserved_key(<<"id">>) -> true;
reserved_key(<<"_", _/binary>>) -> true;
reserved_key(_) -> false.
