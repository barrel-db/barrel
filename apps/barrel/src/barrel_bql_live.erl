%% @doc A live BQL query: one process per SUBSCRIBE statement, pushing
%% the initial snapshot and then add/change/remove deltas to its owner.
%%
%% Two signal sources funnel into one idempotent refresh:
%% - barrel_query_sub fires when a commit's NEW body matches the query
%%   (trie-prefiltered centrally). It never fires on deletes and never
%%   for rows that STOP matching, so it can only drive add/change.
%% - a push-mode changes stream (from the snapshot's last_seq) covers
%%   the rest: deletes and unmatches for ids in the matched set. When
%%   the plan has no engine where (unconstrained or id-only
%%   subscriptions), query_sub has nothing to match on and the stream
%%   drives adds too.
%%
%% refresh re-reads the document and re-checks it with
%% barrel_query:match/2 (plus the plan's id bounds and residual
%% conditions), which also neutralizes false positives from
%% query_sub's own matcher.
%%
%% Messages to the owner (Ref is the subscription reference):
%%   {bql_rows, Ref, [Row]}                       snapshot chunks
%%   {bql_ready, Ref, #{count, last_seq}}
%%   {bql_change, Ref, #{action := add | change, id, rev, row}}
%%   {bql_change, Ref, #{action := remove, id}}
%%   {bql_error, Ref, Reason}                      then the process exits
%%
%% LIMIT caps the initial snapshot only; deltas are unbounded events.
%% Owner death tears the query down; callers that need crash signals
%% monitor the returned pid.
-module(barrel_bql_live).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         handle_continue/2, terminate/2]).

-define(SNAPSHOT_CHUNK, 100).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%====================================================================
%% gen_server
%%====================================================================

init(#{db := Db, plan := Plan, owner := Owner, ref := Ref}) ->
    OwnerRef = erlang:monitor(process, Owner),
    #{docdb := DbBin} = Db,
    #{spec := Spec, post := Post} = Plan,
    SpecWhere = maps:get(where, Spec, []),
    WherePlan = case SpecWhere of
        [] ->
            none;
        _ ->
            {ok, Compiled} = barrel_query:compile(#{where => SpecWhere}),
            Compiled
    end,
    %% no engine where => query_sub has nothing to match on: the
    %% changes stream drives adds as well
    SubRef = case SpecWhere of
        [] ->
            undefined;
        _ ->
            {ok, QSubRef} = barrel_query_sub:subscribe(
                DbBin, #{where => SpecWhere}, self()),
            QSubRef
    end,
    DeltaPost = Post#{limit => undefined, offset => 0,
                      order => undefined, empty => false},
    State = #{
        ref => Ref,
        owner => Owner,
        owner_mref => OwnerRef,
        db_bin => DbBin,
        plan => Plan,
        where_plan => WherePlan,
        stream_adds => SpecWhere =:= [],
        id_check => id_check_fun(Spec),
        residual => maps:get(residual, Post),
        delta_post => DeltaPost,
        sub_ref => SubRef,
        stream => undefined,
        matched => #{}
    },
    {ok, State, {continue, snapshot}}.

handle_continue(snapshot, State) ->
    #{ref := Ref, owner := Owner, db_bin := DbBin, plan := Plan} = State,
    Send = fun(Rows) -> Owner ! {bql_rows, Ref, Rows} end,
    Fold = fun(Row, {Buf, Matched}) ->
        Buf1 = [Row | Buf],
        Matched1 = case Row of
            #{<<"id">> := Id} -> Matched#{Id => undefined};
            _ -> Matched
        end,
        case length(Buf1) >= ?SNAPSHOT_CHUNK of
            true -> Send(lists:reverse(Buf1)), {ok, {[], Matched1}};
            false -> {ok, {Buf1, Matched1}}
        end
    end,
    case barrel_bql_exec:fold(DbBin, Plan,
                              #{chunk_size => ?SNAPSHOT_CHUNK},
                              Fold, {[], #{}}) of
        {ok, {Buf, Matched}, Meta} ->
            case Buf of
                [] -> ok;
                _ -> Send(lists:reverse(Buf))
            end,
            LastSeq = maps:get(last_seq, Meta, first),
            case barrel_docdb:subscribe_changes(
                     DbBin, since(LastSeq),
                     #{mode => push, owner => self()}) of
                {ok, Stream} ->
                    Owner ! {bql_ready, Ref,
                             #{count => map_size(Matched),
                               last_seq => LastSeq}},
                    {noreply, State#{matched => Matched,
                                     stream => Stream}};
                {error, Reason} ->
                    Owner ! {bql_error, Ref, Reason},
                    {stop, normal, State}
            end;
        {error, Reason} ->
            Owner ! {bql_error, Ref, Reason},
            {stop, normal, State}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({barrel_query_change, _DbName, #{id := Id, rev := Rev}},
            #{matched := Matched} = State) ->
    case maps:find(Id, Matched) of
        {ok, Rev} -> {noreply, State};
        _ -> {noreply, refresh(Id, State)}
    end;
handle_info({changes, ReqId, Changes}, #{stream := Stream} = State0) ->
    State = lists:foldl(fun handle_change/2, State0, Changes),
    ok = barrel_changes_stream:ack(Stream, ReqId),
    {noreply, State};
handle_info({'DOWN', MRef, process, _Pid, _Reason},
            #{owner_mref := MRef} = State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State of
        #{sub_ref := SubRef} when SubRef =/= undefined ->
            _ = try barrel_query_sub:unsubscribe(SubRef)
                catch _:_ -> ok end,
            ok;
        _ ->
            ok
    end,
    case State of
        #{stream := Stream} when is_pid(Stream) ->
            _ = try barrel_changes_stream:stop(Stream)
                catch _:_ -> ok end,
            ok;
        _ ->
            ok
    end.

%%====================================================================
%% Deltas
%%====================================================================

handle_change(Change, State) ->
    #{matched := Matched, stream_adds := StreamAdds} = State,
    Id = maps:get(id, Change),
    Deleted = maps:get(deleted, Change, false),
    Rev = maps:get(rev, Change, undefined),
    InSet = maps:is_key(Id, Matched),
    case {Deleted, InSet} of
        {true, true} ->
            emit_remove(Id, State);
        {true, false} ->
            State;
        {false, true} ->
            case maps:get(Id, Matched) of
                Rev -> State;
                _ -> refresh(Id, State)
            end;
        {false, false} when StreamAdds ->
            refresh(Id, State);
        {false, false} ->
            %% adds are query_sub's job when an engine where exists
            State
    end.

refresh(Id, State) ->
    #{db_bin := DbBin, matched := Matched} = State,
    case barrel_docdb:get_doc(DbBin, Id) of
        {ok, Doc} ->
            Rev = maps:get(<<"_rev">>, Doc, undefined),
            case {matches(Id, Doc, State), maps:find(Id, Matched)} of
                {true, error} ->
                    emit_row(add, Id, Rev, Doc, State);
                {true, {ok, Rev}} ->
                    State;
                {true, {ok, _Old}} ->
                    emit_row(change, Id, Rev, Doc, State);
                {false, {ok, _}} ->
                    emit_remove(Id, State);
                {false, error} ->
                    State
            end;
        {error, not_found} ->
            case maps:is_key(Id, Matched) of
                true -> emit_remove(Id, State);
                false -> State
            end;
        {error, _Other} ->
            State
    end.

matches(Id, Doc, State) ->
    #{id_check := IdCheck, where_plan := WherePlan,
      residual := Residual} = State,
    IdCheck(Id)
        andalso where_matches(WherePlan, Doc)
        andalso barrel_bql_exec:residual_match(
                    Residual, #{doc => Doc#{<<"id">> => Id},
                                elem => undefined, score => #{}}).

where_matches(none, _Doc) -> true;
where_matches(Plan, Doc) -> barrel_query:match(Plan, Doc).

emit_row(Action, Id, Rev, Doc, State) ->
    #{ref := Ref, owner := Owner, matched := Matched,
      delta_post := DeltaPost} = State,
    Frame = #{doc => Doc#{<<"id">> => Id}, elem => undefined,
              score => #{}},
    [Row] = barrel_bql_exec:finalize([Frame], DeltaPost, undefined),
    Owner ! {bql_change, Ref, #{action => Action, id => Id, rev => Rev,
                                row => Row}},
    State#{matched => Matched#{Id => Rev}}.

emit_remove(Id, State) ->
    #{ref := Ref, owner := Owner, matched := Matched} = State,
    Owner ! {bql_change, Ref, #{action => remove, id => Id}},
    State#{matched => maps:remove(Id, Matched)}.

%% find meta carries the change sequence as an encoded HLC binary;
%% the changes stream wants the decoded timestamp.
since(first) -> first;
since(Seq) when is_binary(Seq) -> barrel_hlc:decode(Seq);
since(Seq) -> Seq.

%%====================================================================
%% Id bounds from the plan's spec
%%====================================================================

id_check_fun(Spec) ->
    case {maps:get(id_prefix, Spec, undefined),
          maps:get(id_range, Spec, undefined)} of
        {undefined, undefined} ->
            fun(_Id) -> true end;
        {Prefix, undefined} ->
            Size = byte_size(Prefix),
            fun(Id) ->
                case Id of
                    <<Prefix:Size/binary, _/binary>> -> true;
                    _ -> false
                end
            end;
        {undefined, {Start, End}} ->
            fun(Id) ->
                (Start =:= undefined orelse Id >= Start)
                    andalso (End =:= undefined orelse Id < End)
            end
    end.
