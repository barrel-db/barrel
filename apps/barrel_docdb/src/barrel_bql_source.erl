%% @doc Registered BQL FROM sources.
%%
%% An application that serves rows of its own (a materialized view, for
%% example) registers a module under a name, and BQL reaches it as
%% `FROM name(args...) AS v'. docdb never depends on that application:
%% the registry maps a binary name to the module, and the executor
%% calls the callbacks below.
%%
%% `run/4' receives the WHERE conditions in `barrel_query:match/2'
%% shape. It may use any of them to narrow what it reads, but the
%% executor re-applies all of them to the rows it returns, so a source
%% that narrows too little only costs time. A source that returns rows
%% in a known order says so through `ordered/2'; the executor then
%% streams `ORDER BY' on that order instead of sorting.
-module(barrel_bql_source).

-export([register/2, unregister/1, lookup/1, names/0]).

-callback init_args(Positional :: [term()], Named :: #{binary() => term()}) ->
    {ok, Args :: map()} | {error, Reason :: term()}.

-callback run(Db :: binary() | pid(), Args :: map(),
              Query :: #{where := [term()],
                         order := undefined | {[binary() | integer()], asc | desc}
                                | term(),
                         limit := undefined | non_neg_integer(),
                         offset := non_neg_integer(),
                         chunk_size := pos_integer(),
                         continuation => term()},
              Opts :: map()) ->
    {ok, Rows :: [map()], Meta :: map()} | {error, term()}.

-callback ordered(Args :: map(), Order :: {[binary() | integer()], asc | desc}) ->
    boolean().

-callback explain(Args :: map(), Query :: map()) -> map().

-optional_callbacks([ordered/2, explain/2]).

-define(BUILTIN, [<<"vector_top_k">>, <<"bm25_top_k">>, <<"hybrid_top_k">>]).

%% @doc Register Module as the FROM source Name. Built-in table
%% functions cannot be replaced.
-spec register(binary(), module()) -> ok | {error, builtin | invalid_name}.
register(Name, Module) when is_binary(Name), is_atom(Module) ->
    case lists:member(Name, ?BUILTIN) of
        true -> {error, builtin};
        false ->
            case valid_name(Name) of
                true -> persistent_term:put({?MODULE, Name}, Module);
                false -> {error, invalid_name}
            end
    end.

-spec unregister(binary()) -> ok.
unregister(Name) when is_binary(Name) ->
    _ = persistent_term:erase({?MODULE, Name}),
    ok.

-spec lookup(binary()) -> {ok, module()} | error.
lookup(Name) when is_binary(Name) ->
    case persistent_term:get({?MODULE, Name}, undefined) of
        undefined -> error;
        Module -> {ok, Module}
    end.

-spec names() -> [binary()].
names() ->
    lists:sort([Name || {{?MODULE, Name}, _} <- persistent_term:get()]).

valid_name(<<C, Rest/binary>>) when C >= $a, C =< $z ->
    lists:all(fun(B) -> (B >= $a andalso B =< $z) orelse (B >= $0 andalso B =< $9)
                        orelse B =:= $_ end, binary_to_list(Rest));
valid_name(_) ->
    false.
