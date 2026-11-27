%% @doc Row layout and reads of a view index.
%%
%% In the derived store of a database, with V the view's 8-byte id and
%% G a 32-bit generation:
%% <ul>
%% <li>`r V G Key DocId Ord' a row; its value is `{Value, ExpiresAt}'.
%%     Key and DocId use barrel_design_key's encoding, Ord is the
%%     emission ordinal, so a document can emit the same key twice;</li>
%% <li>`b V G DocId' the document's current rows and revision;</li>
%% <li>`e V G DocId' the error of the document's last map, if any;</li>
%% <li>`s V' the view state: generations, definitions, checkpoints.</li>
%% </ul>
%% The indexer replaces a document's rows, its reverse entry, its error
%% and the state's checkpoint in one write batch.
-module(barrel_design_index).

-export([view_id/2, state_key/1, read_state/2, read_state/3, state_op/2]).
-export([doc_ops/6, reverse_keys/3, generation_ranges/2]).
-export([query/6, bounds_for/2]).

-define(MAX_LIMIT, 10000).
-define(DEFAULT_LIMIT, 100).
-define(DEFAULT_SCAN, 100000).
-define(I64_MIN, -(1 bsl 63)).
-define(I64_MAX, (1 bsl 63) - 1).

%%====================================================================
%% Keys and state
%%====================================================================

-spec view_id(binary(), binary()) -> binary().
view_id(Design, View) ->
    binary:part(crypto:hash(sha256, <<Design/binary, 0, View/binary>>), 0, 8).

state_key(V) -> <<"s", V/binary>>.

-spec read_state(barrel_design_store:store(), binary()) -> {ok, map()} | not_found.
read_state(Store, V) ->
    case barrel_design_store:get(Store, state_key(V)) of
        {ok, Bin} -> {ok, binary_to_term(Bin)};
        not_found -> not_found
    end.

-spec read_state(barrel_design_store:store(), binary(), rocksdb:snapshot_handle()) ->
    {ok, map()} | not_found.
read_state(Store, V, Snap) ->
    case barrel_design_store:get(Store, state_key(V), Snap) of
        {ok, Bin} -> {ok, binary_to_term(Bin)};
        not_found -> not_found
    end.

state_op(V, State) -> {put, state_key(V), term_to_binary(State)}.

row_prefix(V, G) -> <<"r", V/binary, G:32>>.
rev_key(V, G, Id) -> <<"b", V/binary, G:32, Id/binary>>.
err_key(V, G, Id) -> <<"e", V/binary, G:32, Id/binary>>.

%% @doc Key ranges holding a generation's rows, reverse entries and errors.
-spec generation_ranges(binary(), non_neg_integer()) -> [{binary(), binary()}].
generation_ranges(V, G) ->
    [{P, barrel_design_key:after_prefix(P)}
     || P <- [row_prefix(V, G), <<"b", V/binary, G:32>>, <<"e", V/binary, G:32>>]].

%% @doc Reverse entry keys for a batch of document ids.
-spec reverse_keys(binary(), non_neg_integer(), [binary()]) -> [binary()].
reverse_keys(V, G, Ids) -> [rev_key(V, G, Id) || Id <- Ids].

%% @doc Operations replacing one document's contributions. Old is its
%% reverse entry (or none); Result is the map's outcome for the current
%% revision (`skip' when the document is gone or not selected).
-spec doc_ops(binary(), non_neg_integer(), binary(), {binary(), non_neg_integer()},
              not_found | {ok, binary()} | {error, term()},
              {ok, [term()]} | {error, term()} | skip) -> list().
doc_ops(V, G, Id, {Rev, Expires}, Old, Result) ->
    Prefix = row_prefix(V, G),
    EncId = barrel_design_key:encode(Id),
    Drop = case Old of
        {ok, Bin} ->
            #{rows := OldRows} = binary_to_term(Bin),
            [{delete, <<Prefix/binary, Suffix/binary>>} || Suffix <- OldRows];
        _ -> []
    end,
    case Result of
        {ok, []} ->
            Drop ++ [{delete, rev_key(V, G, Id)}, {delete, err_key(V, G, Id)}];
        skip ->
            Drop ++ [{delete, rev_key(V, G, Id)}, {delete, err_key(V, G, Id)}];
        {ok, Rows} ->
            {Puts, Suffixes, _} = lists:foldl(fun({Key, Value}, {P, S, Ord}) ->
                Suffix = <<(barrel_design_key:encode(Key))/binary, EncId/binary, Ord:32>>,
                {[{put, <<Prefix/binary, Suffix/binary>>, term_to_binary({Value, Expires})} | P],
                 [Suffix | S], Ord + 1}
            end, {[], [], 0}, Rows),
            Drop ++ lists:reverse(Puts)
                ++ [{put, rev_key(V, G, Id), term_to_binary(#{rev => Rev, rows => lists:reverse(Suffixes)})},
                    {delete, err_key(V, G, Id)}];
        {error, Why} ->
            Drop ++ [{delete, rev_key(V, G, Id)},
                     {put, err_key(V, G, Id), term_to_binary(#{rev => Rev, class => Why})}]
    end.

%%====================================================================
%% Queries
%%====================================================================

%% @doc Raw key bounds [Lower, Upper) for the selection options:
%% `key', `key_prefix', `start_key'/`end_key' with `inclusive_end'
%% (default true), and `bounds' (already encoded).
-spec bounds_for(map(), map()) -> {ok, {binary(), binary() | infinity}} | {error, term()}.
bounds_for(_View, Opts) ->
    try
        All = {<<>>, infinity},
        B0 = case maps:find(bounds, Opts) of
            {ok, {L, U}} when is_binary(L), (is_binary(U) orelse U =:= infinity) -> {L, U};
            {ok, Other} -> throw({invalid_bounds, Other});
            error -> All
        end,
        B1 = case maps:find(key, Opts) of
            {ok, K} -> intersect(B0, exact(enc(K)));
            error -> B0
        end,
        B2 = case maps:find(key_prefix, Opts) of
            {ok, P} when is_list(P) ->
                Pre = barrel_design_key:prefix(check_key(P)),
                intersect(B1, {Pre, barrel_design_key:after_prefix(Pre)});
            {ok, P} -> throw({invalid_key_prefix, P});
            error -> B1
        end,
        B3 = case maps:find(start_key, Opts) of
            {ok, S} -> intersect(B2, {enc(S), infinity});
            error -> B2
        end,
        B4 = case maps:find(end_key, Opts) of
            {ok, E} ->
                case maps:get(inclusive_end, Opts, true) of
                    true -> intersect(B3, {<<>>, barrel_design_key:after_prefix(enc(E))});
                    false -> intersect(B3, {<<>>, enc(E)})
                end;
            error -> B3
        end,
        {ok, B4}
    catch
        throw:Why -> {error, Why}
    end.

exact(Enc) -> {Enc, barrel_design_key:after_prefix(Enc)}.

enc(K) -> barrel_design_key:encode(check_key(K)).

check_key(K) ->
    case barrel_design_key:valid(K) of
        true -> K;
        false -> throw({invalid_key, K})
    end.

intersect({L1, U1}, {L2, U2}) -> {max(L1, L2), min_upper(U1, U2)}.

min_upper(infinity, U) -> U;
min_upper(U, infinity) -> U;
min_upper(A, B) -> min(A, B).

%% @doc Read rows or grouped totals of the published generation G under
%% Snap, the snapshot the caller read the view state with. `reduce'
%% totals with the view's reducer, grouped by the first `group_level'
%% key components (`exact' for whole keys); `limit' rows or groups per
%% call; a continuation resumes in the same generation.
-spec query(barrel_design_store:store(), rocksdb:snapshot_handle(), binary(),
            non_neg_integer(), map(), map()) ->
    {ok, [map()], map()} | {error, term()}.
query(Store, Snap, V, G, View, Opts) ->
    case decode_cont(maps:get(continuation, Opts, undefined), G) of
        {error, _} = Error -> Error;
        {ok, Cont} ->
            Now = case Cont of
                #{now := N} -> N;
                _ -> erlang:system_time(millisecond)
            end,
            Limit = case maps:get(limit, Opts, ?DEFAULT_LIMIT) of
                L when is_integer(L), L >= 1 -> min(L, ?MAX_LIMIT);
                _ -> invalid
            end,
            Reduce = reduce_mode(View, Opts),
            case {Reduce, bounds_for(View, Opts)} of
                _ when Limit =:= invalid -> {error, {invalid_limit, maps:get(limit, Opts)}};
                {{error, _} = E, _} -> E;
                {_, {error, _} = E} -> E;
                {_, {ok, {Lower, Upper}}} ->
                    Prefix = row_prefix(V, G),
                    PLow = <<Prefix/binary, Lower/binary>>,
                    PHigh = case Upper of
                        infinity -> barrel_design_key:after_prefix(Prefix);
                        _ -> <<Prefix/binary, Upper/binary>>
                    end,
                    %% the byte successor of a key is the key followed by 0
                    Start = case Cont of
                        #{start := S} -> max(PLow, S);
                        #{after_key := AK} -> max(PLow, <<AK/binary, 0>>);
                        _ -> PLow
                    end,
                    case Start >= PHigh of
                        true -> {ok, [], #{}};
                        false ->
                            Ctx = #{prefix => Prefix, now => Now, limit => Limit, g => G,
                                    scan => maps:get(max_scan, Opts, ?DEFAULT_SCAN)},
                            case Reduce of
                                false -> read_rows(Store, Start, PHigh, Ctx, Snap);
                                {true, Fun, Level} ->
                                    read_groups(Store, Start, PHigh, Ctx#{fn => Fun, level => Level},
                                                maps:get(group, Cont, undefined), Snap)
                            end
                    end
            end
    end.

reduce_mode(#{reduce := R}, Opts) ->
    case maps:get(reduce, Opts, R =/= none) of
        false -> false;
        true when R =:= none -> {error, no_reducer};
        true ->
            case maps:get(group_level, Opts, 0) of
                exact -> {true, R, exact};
                N when is_integer(N), N >= 0 -> {true, R, N};
                Other -> {error, {invalid_group_level, Other}}
            end
    end.

read_rows(Store, Start, Upper, #{prefix := Prefix, now := Now, limit := Limit, g := G}, Snap) ->
    PLen = byte_size(Prefix),
    Fold = fun(K, Val, {N, Acc, _Last}) ->
        case binary_to_term(Val) of
            {_, Exp} when is_integer(Exp), Exp > 0, Exp =< Now ->
                {ok, {N, Acc, K}};
            {Value, _} ->
                <<_:PLen/binary, Rest/binary>> = K,
                {Key, Id} = split_row(Rest),
                Row = #{id => Id, key => Key, value => Value},
                case N + 1 of
                    Limit -> {stop, {Limit, [Row | Acc], K}};
                    N1 -> {ok, {N1, [Row | Acc], K}}
                end
        end
    end,
    {Count, Rows, Last} = barrel_design_store:fold(Store, Start, Upper, Fold, {0, [], undefined}, Snap),
    Meta = case Count of
        Limit when Limit > 0 -> #{continuation => encode_cont(#{g => G, now => Now, after_key => Last})};
        _ -> #{}
    end,
    {ok, lists:reverse(Rows), Meta}.

%% Groups are emitted only when complete. A call that reaches its scan
%% budget inside a group carries the partial total in its continuation.
read_groups(Store, Start, Upper, #{prefix := Prefix, now := Now, limit := Limit, g := G,
                                   scan := Scan, fn := Fn, level := Level}, Group0, Snap) ->
    PLen = byte_size(Prefix),
    Fold = fun(K, Val, {Scanned, Out, NOut, Group, _Last}) ->
        case binary_to_term(Val) of
            {_, Exp} when is_integer(Exp), Exp > 0, Exp =< Now ->
                {ok, {Scanned + 1, Out, NOut, Group, K}};
            {Value, _} ->
                <<_:PLen/binary, Rest/binary>> = K,
                {Key, _Id} = split_row(Rest),
                GKey = group_key(Key, Level),
                {Out1, NOut1, Group1} = case Group of
                    {GKey, Acc} -> {Out, NOut, {GKey, step(Fn, Acc, Value)}};
                    undefined -> {Out, NOut, {GKey, step(Fn, init(Fn), Value)}};
                    {Done, Acc} -> {[{Done, Acc} | Out], NOut + 1, {GKey, step(Fn, init(Fn), Value)}}
                end,
                Scanned1 = Scanned + 1,
                if
                    NOut1 >= Limit -> {stop, {Scanned1, Out1, NOut1, Group1, K, row_started}};
                    Scanned1 >= Scan -> {stop, {Scanned1, Out1, NOut1, Group1, K, scan}};
                    true -> {ok, {Scanned1, Out1, NOut1, Group1, K}}
                end
        end
    end,
    case barrel_design_store:fold(Store, Start, Upper, Fold, {0, [], 0, Group0, undefined}, Snap) of
        {_, Out, _, Group, _Last} ->
            %% the range ended: the open group is complete
            All = lists:reverse(case Group of undefined -> Out; _ -> [Group | Out] end),
            finish_groups(All, #{});
        {_, Out, _, _Open, K, row_started} ->
            %% Limit groups done; the row at K began the next group, so
            %% the next call starts at K itself
            Cont = encode_cont(#{g => G, now => Now, start => K}),
            finish_groups(lists:reverse(Out), #{continuation => Cont});
        {_, Out, _, Group, K, scan} ->
            Cont = encode_cont(#{g => G, now => Now, after_key => K, group => Group}),
            finish_groups(lists:reverse(Out), #{continuation => Cont})
    end.

finish_groups(Groups, Meta) ->
    try
        {ok, [#{key => K, value => final(Acc)} || {K, Acc} <- Groups], Meta}
    catch
        throw:{sum_overflow, _} = E -> {error, E}
    end.

group_key(Key, exact) -> Key;
group_key(_Key, 0) -> null;
group_key(Key, N) when is_list(Key) -> lists:sublist(Key, N);
group_key(Key, _N) -> Key.

init('_sum') -> {sum, 0};
init('_count') -> {count, 0}.

step('_sum', {sum, S}, V) when is_integer(V) -> {sum, S + V};
step('_sum', Acc, _V) -> Acc;
step('_count', {count, N}, _V) -> {count, N + 1}.

final({count, N}) -> N;
final({sum, S}) when S >= ?I64_MIN, S =< ?I64_MAX -> S;
final({sum, S}) -> throw({sum_overflow, S}).

%% Row suffix: Key, DocId (both self-delimiting) and a 32-bit ordinal.
split_row(Bin) ->
    {Key, R1} = take_key(Bin),
    {Id, <<_Ord:32>>} = take_key(R1),
    {Key, Id}.

take_key(Bin) ->
    Len = key_len(Bin, 0),
    <<K:Len/binary, R/binary>> = Bin,
    {barrel_design_key:decode(K), R}.

%% Length of the encoded key at the head of Bin.
key_len(Bin, Off) ->
    case binary:at(Bin, Off) of
        T when T =:= 16#01; T =:= 16#02; T =:= 16#03 -> Off + 1;
        16#20 -> Off + 9;
        16#40 -> text_end(Bin, Off + 1);
        16#60 -> array_end(Bin, Off + 1)
    end.

text_end(Bin, I) ->
    case Bin of
        <<_:I/binary, 0, 0, _/binary>> -> I + 2;
        <<_:I/binary, 0, 16#FF, _/binary>> -> text_end(Bin, I + 2);
        _ -> text_end(Bin, I + 1)
    end.

array_end(Bin, I) ->
    case binary:at(Bin, I) of
        0 -> I + 1;
        _ -> array_end(Bin, key_len(Bin, I))
    end.

%%====================================================================
%% Continuations
%%====================================================================

encode_cont(Map) -> term_to_binary(Map).

decode_cont(undefined, _G) -> {ok, #{}};
decode_cont(Bin, G) when is_binary(Bin) ->
    try binary_to_term(Bin, [safe]) of
        #{g := G} = C -> {ok, C};
        #{g := _} -> {error, stale_cursor};
        _ -> {error, invalid_continuation}
    catch
        error:badarg -> {error, invalid_continuation}
    end;
decode_cont(_, _G) -> {error, invalid_continuation}.
