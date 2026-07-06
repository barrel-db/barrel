%% @doc Write-time channels: named subsets of a database materialized
%% into per-channel change feeds (prefix 0x1E) as documents are
%% written, so partial sync reads one bounded range scan instead of
%% filtering the global feed.
%%
%% A channel is a set of MQTT-style patterns over the document's ars
%% topics (`field/value', `+' matches one segment, a trailing `#'
%% matches the rest). Channels are db create-time config:
%% `channels => #{Name => [Pattern]}', compiled at init into
%% `persistent_term {barrel_channels, DbName}'. Immutable in v1; only
%% writes made after creation are indexed.
%%
%% Feed semantics: one member row per doc per channel, moved to the
%% doc's current change HLC on every write (the forget-time cleanup
%% relies on row HLC == doc COL_HLC). Deletes land member rows carrying
%% the tombstone in the channels the doc was in. A doc that stops
%% matching gets a leave row (an event, swept by retention); readers
%% skip leave rows unless asked, and replicas simply stop receiving
%% the departed doc (no target-side delete).
-module(barrel_channel).

-export([
    compile/1,
    write_ops_compiled/7,
    install/2,
    uninstall/1,
    names/1,
    channels/1,
    topics/1,
    match/2,
    write_ops/6,
    move_ops/5,
    fold/5
]).

%% Row codec (used by the read side and tests)
-export([encode_row/2, decode_row/1]).

-export_type([config/0, compiled/0, row/0]).

-type pattern_segments() :: [binary() | plus | hash].
-type config() :: #{binary() => [binary()]}.
-type compiled() :: #{binary() => [pattern_segments()]}.
-type row() :: #{flag := member | leave,
                 id := binary(),
                 rev := binary(),
                 deleted := boolean(),
                 num_conflicts := non_neg_integer()}.

-define(FLAG_LEAVE, 0).
-define(FLAG_MEMBER, 1).

%%====================================================================
%% Config
%%====================================================================

%% @doc Validate and compile a channels config map.
-spec compile(term()) -> {ok, compiled()} | {error, term()}.
compile(Config) when is_map(Config) ->
    try
        {ok, maps:fold(
            fun(Name, Patterns, Acc) ->
                ok = validate_name(Name),
                Acc#{Name => [compile_pattern(Name, P) || P <- patterns(Name, Patterns)]}
            end,
            #{},
            Config)}
    catch
        throw:Reason -> {error, Reason}
    end;
compile(Other) ->
    {error, {invalid_channels, Other}}.

validate_name(Name) when is_binary(Name), byte_size(Name) > 0 ->
    case binary:match(Name, [<<0>>, <<16#FF>>]) of
        nomatch -> ok;
        _ -> throw({invalid_channel_name, Name})
    end;
validate_name(Name) ->
    throw({invalid_channel_name, Name}).

patterns(_Name, Patterns) when is_list(Patterns), Patterns =/= [] ->
    Patterns;
patterns(Name, Other) ->
    throw({invalid_channel_patterns, Name, Other}).

compile_pattern(Name, Pattern) when is_binary(Pattern), byte_size(Pattern) > 0 ->
    Segments = binary:split(Pattern, <<"/">>, [global]),
    compile_segments(Name, Pattern, Segments, []);
compile_pattern(Name, Pattern) ->
    throw({invalid_channel_pattern, Name, Pattern}).

compile_segments(_Name, _Pattern, [], Acc) ->
    lists:reverse(Acc);
compile_segments(_Name, _Pattern, [<<"#">>], Acc) ->
    lists:reverse([hash | Acc]);
compile_segments(Name, Pattern, [<<"#">> | _More], _Acc) ->
    throw({invalid_channel_pattern, Name, Pattern});
compile_segments(Name, Pattern, [<<"+">> | Rest], Acc) ->
    compile_segments(Name, Pattern, Rest, [plus | Acc]);
compile_segments(Name, Pattern, [<<>> | _], _Acc) ->
    throw({invalid_channel_pattern, Name, Pattern});
compile_segments(Name, Pattern, [Seg | Rest], Acc) ->
    compile_segments(Name, Pattern, Rest, [Seg | Acc]).

%% @doc Publish a compiled config for a database.
-spec install(binary(), compiled()) -> ok.
install(DbName, Compiled) ->
    persistent_term:put({barrel_channels, DbName}, Compiled).

-spec uninstall(binary()) -> ok.
uninstall(DbName) ->
    _ = persistent_term:erase({barrel_channels, DbName}),
    ok.

%% @doc The configured channels of a database (empty when none).
-spec channels(binary()) -> compiled().
channels(DbName) ->
    persistent_term:get({barrel_channels, DbName}, #{}).

-spec names(binary()) -> [binary()].
names(DbName) ->
    maps:keys(channels(DbName)).

%% @doc The ars topics of a document body ([] for tombstones).
-spec topics(map() | undefined) -> [binary()].
topics(undefined) ->
    [];
topics(Body) when is_map(Body) ->
    barrel_ars:paths_to_topics(barrel_ars:analyze(Body)).

%%====================================================================
%% Matching
%%====================================================================

%% @doc Channels whose pattern set matches any of the given topics.
-spec match(compiled(), [binary()]) -> [binary()].
match(Compiled, _Topics) when map_size(Compiled) =:= 0 ->
    [];
match(Compiled, Topics) ->
    Split = [binary:split(T, <<"/">>, [global]) || T <- Topics],
    [Name || {Name, Patterns} <- maps:to_list(Compiled),
             any_topic_matches(Patterns, Split)].

any_topic_matches(Patterns, SplitTopics) ->
    lists:any(
        fun(TopicSegs) ->
            lists:any(fun(P) -> segments_match(P, TopicSegs) end, Patterns)
        end,
        SplitTopics).

segments_match([hash], _Rest) -> true;
segments_match([], []) -> true;
segments_match([], _Rest) -> false;
segments_match(_Pattern, []) -> false;
segments_match([plus | PRest], [_ | TRest]) ->
    segments_match(PRest, TRest);
segments_match([Seg | PRest], [Seg | TRest]) ->
    segments_match(PRest, TRest);
segments_match(_, _) ->
    false.

%%====================================================================
%% Write ops
%%====================================================================

%% @doc Batch ops for one applied write. DocInfo carries id, rev,
%% deleted, num_conflicts. Old rows (at OldHlc) are deleted for ALL
%% configured channels: deleting an absent key is a RocksDB no-op, and
%% it removes any old-membership bookkeeping.
-spec write_ops(binary(), barrel_hlc:timestamp(), map(),
                [binary()], barrel_hlc:timestamp() | undefined,
                [binary()]) -> [term()].
write_ops(DbName, NewHlc, DocInfo, NewTopics, OldHlc, OldTopics) ->
    %% config by the LOGICAL name, keys by the keyspace
    write_ops_compiled(channels(DbName), barrel_keyspace:resolve(DbName),
                       NewHlc, DocInfo, NewTopics, OldHlc, OldTopics).

%% @doc Pure variant of write_ops/6: the compiled channel set and the
%% key name are explicit arguments (no persistent_term reads), so it
%% is callable before a database is registered (timeline rewind).
-spec write_ops_compiled(compiled(), binary(), barrel_hlc:timestamp(),
                         map(), [binary()],
                         barrel_hlc:timestamp() | undefined,
                         [binary()]) -> [term()].
write_ops_compiled(Compiled0, Ks, NewHlc, DocInfo, NewTopics, OldHlc,
                   OldTopics) ->
    case Compiled0 of
        Compiled when map_size(Compiled) =:= 0 ->
            [];
        Compiled ->
            DeleteOps = case OldHlc of
                undefined -> [];
                _ -> [{delete,
                       barrel_store_keys:channel_key(Ks, C, OldHlc)}
                      || C <- maps:keys(Compiled)]
            end,
            Deleted = maps:get(deleted, DocInfo, false),
            RowOps = case Deleted of
                true ->
                    %% the tombstone lands where the doc was
                    [{put,
                      barrel_store_keys:channel_key(Ks, C, NewHlc),
                      encode_row(member, DocInfo)}
                     || C <- match(Compiled, OldTopics)];
                false ->
                    InNew = match(Compiled, NewTopics),
                    InOld = match(Compiled, OldTopics),
                    [{put,
                      barrel_store_keys:channel_key(Ks, C, NewHlc),
                      encode_row(member, DocInfo)}
                     || C <- InNew]
                    ++
                    [{put,
                      barrel_store_keys:channel_key(Ks, C, NewHlc),
                      encode_row(leave, DocInfo)}
                     || C <- InOld -- InNew]
            end,
            DeleteOps ++ RowOps
    end.

%% @doc Batch ops moving a doc's channel rows from OldHlc to NewHlc
%% without recomputing membership: point-read each configured channel
%% at OldHlc and rewrite what exists. This is the concurrent-loser
%% path, where the winner's feed row moves but its body (possibly a
%% tombstone, whose membership cannot be derived) is unchanged. Member
%% rows are re-encoded from DocInfo (fresh rev and conflict count);
%% leave rows move verbatim.
-spec move_ops(fun((binary()) -> {ok, binary()} | not_found |
                                 {error, term()}),
               binary(), barrel_hlc:timestamp(), barrel_hlc:timestamp(),
               map()) -> [term()].
move_ops(Get, DbName, OldHlc, NewHlc, DocInfo) ->
    %% config by the LOGICAL name, keys by the keyspace
    Ks = barrel_keyspace:resolve(DbName),
    lists:flatmap(
        fun(Channel) ->
            OldKey = barrel_store_keys:channel_key(Ks, Channel, OldHlc),
            case Get(OldKey) of
                {ok, Value} ->
                    NewValue = case decode_row(Value) of
                        #{flag := member} -> encode_row(member, DocInfo);
                        #{flag := leave} -> Value
                    end,
                    [{delete, OldKey},
                     {put,
                      barrel_store_keys:channel_key(Ks, Channel, NewHlc),
                      NewValue}];
                _ ->
                    []
            end
        end,
        names(DbName)).

%%====================================================================
%% Read side
%%====================================================================

%% @doc Fold one channel's feed since an HLC (exclusive), returning
%% change maps in feed order. Leave rows are skipped unless
%% `include_leaves' is set (they surface tagged `left => true'); the
%% returned LastHlc is the last VISITED row, so pagination never
%% re-reads skipped leaves. Options: limit, include_leaves.
-spec fold(barrel_store_rocksdb:db_ref(), binary(), binary(),
           barrel_hlc:timestamp() | first, map()) ->
    {ok, [map()], barrel_hlc:timestamp()}.
fold(StoreRef, DbName0, Channel, Since, Opts) ->
    DbName = barrel_keyspace:resolve(DbName0),
    {StartHlc, StartKey} = case Since of
        first ->
            Min = barrel_hlc:min(),
            {Min, barrel_store_keys:channel_key(DbName, Channel, Min)};
        SinceHlc ->
            {SinceHlc,
             barrel_store_keys:channel_key(DbName, Channel, SinceHlc)}
    end,
    EndKey = barrel_store_keys:channel_end(DbName, Channel),
    Limit = maps:get(limit, Opts, infinity),
    IncludeLeaves = maps:get(include_leaves, Opts, false),
    FoldFun = fun(Key, Value, {LastHlc, Count, Acc}) ->
        {_Chan, Hlc} = barrel_store_keys:decode_channel_key(DbName, Key),
        case Since =/= first andalso barrel_hlc:equal(Hlc, Since) of
            true ->
                {ok, {LastHlc, Count, Acc}};
            false ->
                Row = decode_row(Value),
                case maps:get(flag, Row) of
                    leave when not IncludeLeaves ->
                        {ok, {Hlc, Count, Acc}};
                    Flag ->
                        Change = row_to_change(Row, Hlc, Flag),
                        Count1 = Count + 1,
                        Acc1 = [Change | Acc],
                        case is_integer(Limit) andalso Count1 >= Limit of
                            true -> {stop, {Hlc, Count1, Acc1}};
                            false -> {ok, {Hlc, Count1, Acc1}}
                        end
                end
        end
    end,
    {LastHlc, _N, RevChanges} = barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey, FoldFun, {StartHlc, 0, []}),
    {ok, lists:reverse(RevChanges), LastHlc}.

row_to_change(#{id := Id, rev := Rev, deleted := Deleted,
                num_conflicts := NumConflicts}, Hlc, Flag) ->
    Change0 = #{id => Id, hlc => Hlc, rev => Rev,
                changes => [#{rev => Rev}],
                num_conflicts => NumConflicts},
    Change1 = case Deleted of
        true -> Change0#{deleted => true};
        false -> Change0
    end,
    case Flag of
        leave -> Change1#{left => true};
        member -> Change1
    end.

%%====================================================================
%% Row codec
%%====================================================================

-spec encode_row(member | leave, map()) -> binary().
encode_row(Flag, DocInfo) ->
    FlagByte = case Flag of
        member -> ?FLAG_MEMBER;
        leave -> ?FLAG_LEAVE
    end,
    Change = barrel_changes:encode_change(maps:remove(doc, DocInfo)),
    <<FlagByte:8, Change/binary>>.

-spec decode_row(binary()) -> row().
decode_row(<<FlagByte:8, DocIdLen:16, DocId:DocIdLen/binary,
             RevLen:16, Rev:RevLen/binary, Deleted:8, NumConflicts:16,
             _HasDoc:8, _Rest/binary>>) ->
    #{flag => case FlagByte of
                  ?FLAG_MEMBER -> member;
                  ?FLAG_LEAVE -> leave
              end,
      id => DocId,
      rev => Rev,
      deleted => Deleted =:= 1,
      num_conflicts => NumConflicts}.
