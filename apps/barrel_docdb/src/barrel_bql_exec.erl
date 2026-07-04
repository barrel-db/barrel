%% @doc BQL post-stage executor for collection sources: drives
%% barrel_docdb:find/3 with a compiled plan and applies the per-row
%% stages (unnest expansion, frame-residual predicates, order,
%% offset/limit, projection).
%%
%% The frame helpers (frames_for/2, residual_match/2, finalize/3) are
%% also the reuse surface for the barrel facade, which feeds them
%% table-function hits instead of find rows.
%%
%% A frame is #{doc := map() with id merged in, elem := term() |
%% undefined, score := map()}; score carries the synthetic _score /
%% _distance columns of table-function sources (empty here).
-module(barrel_bql_exec).

-export([
    run/3,
    fold/5
]).

%% Frame surface reused by the barrel facade for table functions.
-export([
    frames_for/2,
    residual_match/2,
    finalize/3
]).

-type frame() :: #{doc := map(), elem := term(), score := map()}.

%%====================================================================
%% Entry points (collection sources)
%%====================================================================

%% @doc Run a compiled plan. Streamable plans (no ORDER BY, no UNNEST)
%% keep find/3's chunk semantics: one chunk per call, meta carries the
%% continuation. Materializing plans paginate internally and return
%% everything.
-spec run(binary(), barrel_bql_lower:plan(), map()) ->
    {ok, [map()], map()} | {error, term()}.
run(_DbName, #{post := #{empty := true}}, _Opts) ->
    {ok, [], #{has_more => false, count => 0}};
run(DbName, #{streamable := true} = Plan, Opts) ->
    #{spec := Spec, post := #{project := Project}} = Plan,
    case barrel_docdb:find(DbName, Spec, find_opts(Opts)) of
        {ok, Rows, Meta} ->
            {ok, project_rows(Rows, Project), Meta};
        {error, _} = Error ->
            Error
    end;
run(DbName, Plan, Opts) ->
    case maps:is_key(continuation, Opts) of
        true ->
            {error, {unsupported, continuation}};
        false ->
            #{post := Post, unnest := Unnest} = Plan,
            case collect_frames(DbName, Plan, Opts) of
                {ok, Frames, LastSeq} ->
                    Rows = finalize(Frames, Post, Unnest),
                    {ok, Rows, #{has_more => false,
                                 count => length(Rows),
                                 last_seq => LastSeq}};
                {error, _} = Error ->
                    Error
            end
    end.

%% @doc Fold rows without materializing the full result (streamable
%% plans paginate internally; materializing plans fold the built list).
%% Fun(Row, Acc) -> {ok, Acc} | {stop, Acc}.
-spec fold(binary(), barrel_bql_lower:plan(), map(),
           fun((map(), term()) -> {ok, term()} | {stop, term()}),
           term()) ->
    {ok, term(), map()} | {error, term()}.
fold(_DbName, #{post := #{empty := true}}, _Opts, _Fun, Acc) ->
    {ok, Acc, #{has_more => false, count => 0}};
fold(DbName, #{streamable := true} = Plan, Opts, Fun, Acc0) ->
    #{spec := Spec, post := #{project := Project}} = Plan,
    ChunkSize = maps:get(chunk_size, Opts, 100),
    fold_chunks(DbName, Spec, Project,
                maps:get(continuation, Opts, undefined),
                ChunkSize, Fun, Acc0);
fold(DbName, Plan, Opts, Fun, Acc0) ->
    case run(DbName, Plan, Opts) of
        {ok, Rows, Meta} ->
            {_, Acc} = fold_rows(Rows, Fun, Acc0),
            {ok, Acc, Meta};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Frame surface (shared with the facade)
%%====================================================================

%% @doc Expand a document into frames: one per unnested array element,
%% or a single element-less frame. Missing / non-array unnest values
%% produce no frames (inner-join UNNEST).
-spec frames_for(map(), undefined | #{path := list(), alias := binary()}) ->
    [frame()].
frames_for(Doc, undefined) ->
    [#{doc => Doc, elem => undefined, score => #{}}];
frames_for(Doc, #{path := Path}) ->
    case get_path(Doc, Path) of
        {ok, Elements} when is_list(Elements) ->
            [#{doc => Doc, elem => Element, score => #{}}
             || Element <- Elements];
        _ ->
            []
    end.

%% @doc Evaluate the frame-residual conjuncts (tagged paths) against a
%% frame. Mirrors the engine's exists-and-satisfies leaf semantics.
-spec residual_match([term()], frame()) -> boolean().
residual_match(Conds, Frame) ->
    lists:all(fun(Cond) -> eval_cond(Cond, Frame) end, Conds).

%% @doc Order, offset/limit and project a list of frames.
-spec finalize([frame()], map(),
               undefined | #{path := list(), alias := binary()}) ->
    [map()].
finalize(Frames0, Post, Unnest) ->
    #{order := Order, offset := Offset, limit := Limit,
      project := Project} = Post,
    Frames1 = sort_frames(Frames0, Order),
    Frames2 = case Offset of
        0 -> Frames1;
        _ when Offset >= length(Frames1) -> [];
        _ -> lists:nthtail(Offset, Frames1)
    end,
    Frames = case Limit of
        undefined -> Frames2;
        _ -> lists:sublist(Frames2, Limit)
    end,
    [project_frame(Project, Frame, Unnest) || Frame <- Frames].

%%====================================================================
%% Find pagination
%%====================================================================

find_opts(Opts) ->
    maps:with([chunk_size, continuation], Opts).

collect_frames(DbName, Plan, Opts) ->
    #{spec := Spec, unnest := Unnest,
      post := #{residual := Residual, order := Order, offset := Offset,
                limit := Limit}} = Plan,
    ChunkSize = maps:get(chunk_size, Opts, 1000),
    %% Bounded queries without ORDER BY can stop scanning once enough
    %% surviving frames are collected; a sort needs everything.
    Need = case {Order, Limit} of
        {undefined, undefined} -> infinity;
        {undefined, _} -> Offset + Limit;
        {_, _} -> infinity
    end,
    collect_frames_loop(DbName, Spec, Unnest, Residual, ChunkSize,
                        Need, undefined, []).

collect_frames_loop(DbName, Spec, Unnest, Residual, ChunkSize, Need,
                    Cont, Acc) ->
    FindOpts = case Cont of
        undefined -> #{chunk_size => ChunkSize};
        _ -> #{chunk_size => ChunkSize, continuation => Cont}
    end,
    case barrel_docdb:find(DbName, Spec, FindOpts) of
        {ok, Rows, Meta} ->
            Frames = [Frame || Row <- Rows,
                               Frame <- row_frames(Row, Unnest),
                               residual_match(Residual, Frame)],
            Acc1 = Acc ++ Frames,
            Done = Need =/= infinity andalso length(Acc1) >= Need,
            case Meta of
                #{has_more := true, continuation := Cont1} when not Done ->
                    collect_frames_loop(DbName, Spec, Unnest, Residual,
                                        ChunkSize, Need, Cont1, Acc1);
                _ ->
                    {ok, Acc1, maps:get(last_seq, Meta, first)}
            end;
        {error, _} = Error ->
            Error
    end.

row_frames(#{<<"id">> := Id, <<"doc">> := Doc}, Unnest) ->
    frames_for(Doc#{<<"id">> => Id}, Unnest);
row_frames(_DeletedRow, _Unnest) ->
    [].

project_rows(Rows, Project) ->
    [project_frame(Project, Frame, undefined)
     || Row <- Rows, Frame <- row_frames(Row, undefined)].

fold_chunks(DbName, Spec, Project, Cont, ChunkSize, Fun, Acc0) ->
    FindOpts = case Cont of
        undefined -> #{chunk_size => ChunkSize};
        _ -> #{chunk_size => ChunkSize, continuation => Cont}
    end,
    case barrel_docdb:find(DbName, Spec, FindOpts) of
        {ok, Rows, Meta} ->
            ProjRows = project_rows(Rows, Project),
            case fold_rows(ProjRows, Fun, Acc0) of
                {stopped, Acc} ->
                    {ok, Acc, Meta};
                {ok, Acc} ->
                    case Meta of
                        #{has_more := true, continuation := Cont1} ->
                            fold_chunks(DbName, Spec, Project, Cont1,
                                        ChunkSize, Fun, Acc);
                        _ ->
                            {ok, Acc, Meta}
                    end
            end;
        {error, _} = Error ->
            Error
    end.

fold_rows([], _Fun, Acc) ->
    {ok, Acc};
fold_rows([Row | Rest], Fun, Acc0) ->
    case Fun(Row, Acc0) of
        {ok, Acc} -> fold_rows(Rest, Fun, Acc);
        {stop, Acc} -> {stopped, Acc}
    end.

%%====================================================================
%% Residual evaluation (tagged paths, engine leaf semantics)
%%====================================================================

eval_cond({path, TaggedPath, Value}, Frame) ->
    case frame_value(TaggedPath, Frame) of
        {ok, Found} -> Found =:= Value;
        not_found -> false
    end;
eval_cond({compare, TaggedPath, Op, Value}, Frame) ->
    case frame_value(TaggedPath, Frame) of
        {ok, Found} -> compare_values(Found, Op, Value);
        not_found -> false
    end;
eval_cond({exists, TaggedPath}, Frame) ->
    frame_value(TaggedPath, Frame) =/= not_found;
eval_cond({missing, TaggedPath}, Frame) ->
    frame_value(TaggedPath, Frame) =:= not_found;
eval_cond({in, TaggedPath, Values}, Frame) ->
    case frame_value(TaggedPath, Frame) of
        {ok, Found} -> lists:member(Found, Values);
        not_found -> false
    end;
eval_cond({contains, TaggedPath, Value}, Frame) ->
    case frame_value(TaggedPath, Frame) of
        {ok, List} when is_list(List) -> lists:member(Value, List);
        _ -> false
    end;
eval_cond({prefix, TaggedPath, Prefix}, Frame) ->
    case frame_value(TaggedPath, Frame) of
        {ok, Found} when is_binary(Found) ->
            Size = byte_size(Prefix),
            case Found of
                <<Prefix:Size/binary, _/binary>> -> true;
                _ -> false
            end;
        _ ->
            false
    end;
eval_cond({regex, TaggedPath, Pattern}, Frame) ->
    case frame_value(TaggedPath, Frame) of
        {ok, Found} when is_binary(Found) ->
            re:run(Found, Pattern, [{capture, none}]) =:= match;
        _ ->
            false
    end;
eval_cond({'and', Conds}, Frame) ->
    lists:all(fun(Cond) -> eval_cond(Cond, Frame) end, Conds);
eval_cond({'or', Conds}, Frame) ->
    lists:any(fun(Cond) -> eval_cond(Cond, Frame) end, Conds);
eval_cond({'not', Cond}, Frame) ->
    not eval_cond(Cond, Frame).

%% Same-type-only comparisons, mirroring barrel_query:compare_values/3.
compare_values(A, '=/=', B) -> A =/= B;
compare_values(A, '==', B) -> A =:= B;
compare_values(A, Op, B) when is_number(A), is_number(B);
                              is_binary(A), is_binary(B) ->
    case Op of
        '<' -> A < B;
        '=<' -> A =< B;
        '>' -> A > B;
        '>=' -> A >= B
    end;
compare_values(_, _, _) ->
    false.

frame_value({b, Comps}, #{doc := Doc}) ->
    get_path(Doc, Comps);
frame_value({u, Comps}, #{elem := Elem}) ->
    case Elem of
        undefined -> not_found;
        _ -> get_path(Elem, Comps)
    end;
frame_value({score, Key}, #{score := Score}) ->
    case maps:find(Key, Score) of
        {ok, Value} -> {ok, Value};
        error -> not_found
    end.

get_path(Value, []) ->
    {ok, Value};
get_path(Map, [Key | Rest]) when is_map(Map), is_binary(Key) ->
    case maps:find(Key, Map) of
        {ok, Value} -> get_path(Value, Rest);
        error -> not_found
    end;
get_path(List, [Index | Rest]) when is_list(List), is_integer(Index),
                                    Index >= 0 ->
    case Index < length(List) of
        true -> get_path(lists:nth(Index + 1, List), Rest);
        false -> not_found
    end;
get_path(_Value, _Path) ->
    not_found.

%%====================================================================
%% Ordering (single key, stable, missing sorts as null)
%%====================================================================

sort_frames(Frames, undefined) ->
    Frames;
sort_frames(Frames, {Key, Direction}) ->
    Decorated = decorate(Frames, Key, 0, []),
    Sorted = lists:sort(
        fun({KA, IA, _}, {KB, IB, _}) ->
            case KA =:= KB of
                true -> IA =< IB;
                false when Direction =:= asc -> KA < KB;
                false -> KB < KA
            end
        end,
        Decorated),
    [Frame || {_, _, Frame} <- Sorted].

decorate([], _Key, _Index, Acc) ->
    lists:reverse(Acc);
decorate([Frame | Rest], Key, Index, Acc) ->
    SortValue = case frame_value(Key, Frame) of
        {ok, Value} -> Value;
        not_found -> null
    end,
    decorate(Rest, Key, Index + 1, [{SortValue, Index, Frame} | Acc]).

%%====================================================================
%% Projection
%%====================================================================

project_frame(star, #{doc := Doc, elem := Elem, score := Score},
              Unnest) ->
    Row0 = maps:merge(maps:remove(<<"_rev">>, Doc), Score),
    case Unnest of
        undefined -> Row0;
        #{alias := Alias} -> Row0#{Alias => Elem}
    end;
project_frame(Projections, #{doc := Doc} = Frame, _Unnest) ->
    Row0 = case maps:find(<<"id">>, Doc) of
        {ok, Id} -> #{<<"id">> => Id};
        error -> #{}
    end,
    lists:foldl(
        fun({Tag, Key, Name}, Row) ->
            case frame_value({Tag, Key}, Frame) of
                {ok, Value} -> Row#{Name => Value};
                not_found -> Row
            end
        end,
        Row0,
        Projections).
