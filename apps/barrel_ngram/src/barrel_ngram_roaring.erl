%%%-------------------------------------------------------------------
%%% @doc Roaring bitmap set operations over integer ordinals (NIF).
%%%
%%% A self-contained intersection primitive for posting lists, backed by
%%% the vendored CRoaring library. The set operations return a serialized
%%% bitmap so they compose (the regex query tree ANDs and ORs); `decode'
%%% materializes the ordinals only at the end.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_roaring).

-export([encode/1, decode/1, intersect_all/1, union_all/1]).

-on_load(init/0).

-define(APPNAME, barrel_ngram).
-define(LIBNAME, "barrel_ngram_nif").

%% @doc Serialize a set of ordinals to a roaring bitmap binary.
-spec encode([non_neg_integer()]) -> binary().
encode(_Ordinals) ->
    not_loaded(?LINE).

%% @doc The ascending ordinals in a roaring bitmap binary.
-spec decode(binary()) -> [non_neg_integer()].
decode(_Bin) ->
    not_loaded(?LINE).

%% @doc Intersect several roaring bitmap binaries; returns a bitmap binary.
-spec intersect_all([binary()]) -> binary().
intersect_all(_Bins) ->
    not_loaded(?LINE).

%% @doc Union several roaring bitmap binaries; returns a bitmap binary.
-spec union_all([binary()]) -> binary().
union_all(_Bins) ->
    not_loaded(?LINE).

%%====================================================================
%% NIF loading
%%====================================================================

init() ->
    SoName = case code:priv_dir(?APPNAME) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true -> filename:join(["..", priv, ?LIBNAME]);
                _ -> filename:join([priv, ?LIBNAME])
            end;
        Dir ->
            filename:join(Dir, ?LIBNAME)
    end,
    erlang:load_nif(SoName, 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
