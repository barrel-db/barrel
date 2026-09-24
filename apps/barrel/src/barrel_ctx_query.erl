%%%-------------------------------------------------------------------
%%% @doc Federated query executor: one BQL statement over several
%%% contexts or the members of a working set. Classifies the statement,
%%% decides per member what can answer (offline coverage), fans out to
%%% local copies and remote locations under budgets and deadlines, merges
%%% (`grouped', `ordered', `interleave', `score'), and reports per-source
%%% provenance and coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_query).

-export([run/1]).

-define(MAX_CONTEXTS, 8).
-define(MAX_PARALLEL, 8).
-define(MAX_PARALLEL_CAP, 16).
-define(MEMBER_TIMEOUT, 4000).
-define(DEADLINE, 5000).
-define(DEADLINE_CAP, 60000).
%% Extra time a remote worker gets to report its own timeout before the
%% coordinator stops waiting for it.
-define(REMOTE_GRACE, 100).

-type request() :: #{query := binary(),
                     contexts => [binary()],
                     working_set => binary(),
                     offline => boolean(),
                     merge => grouped | ordered | interleave | score
                            | default | atom(),
                     params => map(),
                     deadline_ms => pos_integer(),
                     per_context_timeout_ms => pos_integer(),
                     max_parallel => pos_integer(),
                     authorize => fun((binary()) -> ok | {error, term()}),
                     open_opts => map(),
                     continuation => term()}.
-export_type([request/0]).

%% `decision' is what coverage decides from before any work; `extra'
%% the membership, version and note a successful source reports; `emb'
%% the card's advertised embedding (fallback for remote fingerprints).
-record(member, {idx :: pos_integer(),
                 ctx :: binary(),
                 loc :: map() | none,
                 open_opts :: map() | undefined,
                 decision = #{} :: map(),
                 extra = #{} :: map(),
                 emb :: map() | undefined,
                 status = pending :: atom(),
                 result :: term(),
                 elapsed = 0 :: non_neg_integer()}).

%%====================================================================
%% API
%%====================================================================

-spec run(request()) -> {ok, map()} | {error, term()}.
run(Req) when is_map(Req) ->
    Start = now_ms(),
    case prepare(Req) of
        {ok, Shape, Members0, Budget, Scope} ->
            Members = fanout(Members0, Req, Shape, Budget),
            respond(Members, Shape, Scope, now_ms() - Start);
        {error, _} = Err ->
            Err
    end;
run(_Other) ->
    {error, {bad_request, request}}.

%%====================================================================
%% Preparation
%%====================================================================

%% Rejections come before any member is contacted.
prepare(Req) ->
    case {field_query(Req), field_scope(Req), field_merge(Req),
          field_params(Req), field_offline(Req)} of
        {{ok, Bql}, {ok, Scope}, {ok, Merge}, {ok, Params}, {ok, Offline}} ->
            case compile(Bql, Params) of
                {ok, Plan} ->
                    case barrel_ctx_shape:classify(Plan, Merge, Req) of
                        {ok, Shape} ->
                            prepare_members(Req, Shape, Scope, Offline);
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        Fields ->
            first_error(tuple_to_list(Fields))
    end.

prepare_members(Req, Shape, Scope, Offline) ->
    case resolve(Scope) of
        {ok, Members0, Defaults} ->
            case budget(Req, Defaults) of
                {ok, Budget} ->
                    Members = [decide(M, Offline) || M <- Members0],
                    {ok, Shape, Members, Budget, Scope};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

first_error([{error, _} = Err | _]) -> Err;
first_error([_ | Rest]) -> first_error(Rest).

field_query(#{query := Bql}) when is_binary(Bql), Bql =/= <<>> -> {ok, Bql};
field_query(_Req) -> {error, {bad_request, query}}.

field_scope(#{working_set := _, contexts := _}) ->
    {error, {bad_request, scope}};
field_scope(#{working_set := Ws}) when is_binary(Ws), Ws =/= <<>> ->
    {ok, {working_set, Ws}};
field_scope(#{working_set := _}) ->
    {error, {bad_request, working_set}};
field_scope(Req) ->
    case field_contexts(Req) of
        {ok, Ctxs} -> {ok, {contexts, Ctxs}};
        {error, _} = Err -> Err
    end.

field_contexts(#{contexts := [_ | _] = Ctxs}) ->
    Max = env(ctx_max_contexts, ?MAX_CONTEXTS),
    case {lists:all(fun is_binary/1, Ctxs),
          length(lists:usort(Ctxs)) =:= length(Ctxs),
          length(Ctxs) =< Max} of
        {false, _, _} -> {error, {bad_request, contexts}};
        {true, false, _} -> {error, {bad_request, duplicate_context}};
        {true, true, false} ->
            {error, {unsupported_federated_query, too_many_contexts}};
        {true, true, true} -> {ok, Ctxs}
    end;
field_contexts(_Req) ->
    {error, {bad_request, contexts}}.

field_merge(Req) ->
    case maps:get(merge, Req, default) of
        M when is_atom(M) -> {ok, M};
        <<"grouped">> -> {ok, grouped};
        <<"ordered">> -> {ok, ordered};
        <<"score">> -> {ok, score};
        <<"rerank">> -> {ok, rerank};
        <<"interleave">> -> {ok, interleave};
        <<"rrf">> -> {ok, rrf};
        _ -> {error, {bad_request, merge}}
    end.

%% Offline: the request's flag, else the node's (`ctx_offline').
field_offline(Req) ->
    case maps:get(offline, Req, env(ctx_offline, false)) of
        B when is_boolean(B) -> {ok, B};
        _ -> {error, {bad_request, offline}}
    end.

field_params(Req) ->
    case maps:get(params, Req, #{}) of
        P when is_map(P) -> {ok, P};
        _ -> {error, {bad_request, params}}
    end.

%% A working set's budget gives defaults a request may override.
budget(Req, Defaults) ->
    Deadline0 = maps:get(deadline_ms, Defaults,
                         env(ctx_deadline_ms, ?DEADLINE)),
    Parallel0 = maps:get(remote_parallel, Defaults,
                         env(ctx_max_parallel, ?MAX_PARALLEL)),
    case {pos_int(Req, deadline_ms, Deadline0, ?DEADLINE_CAP),
          pos_int(Req, per_context_timeout_ms,
                  env(ctx_member_timeout_ms, ?MEMBER_TIMEOUT), ?DEADLINE_CAP),
          pos_int(Req, max_parallel, Parallel0, ?MAX_PARALLEL_CAP)} of
        {{ok, Deadline}, {ok, Member}, {ok, Parallel}} ->
            {ok, #{deadline_ms => Deadline,
                   member_timeout_ms => min(Member, Deadline),
                   max_parallel => Parallel}};
        Fields ->
            first_error(tuple_to_list(Fields))
    end.

pos_int(Req, Key, Default, Cap) ->
    case maps:get(Key, Req, Default) of
        N when is_integer(N), N > 0 -> {ok, min(N, Cap)};
        _ -> {error, {bad_request, Key}}
    end.

compile(Bql, Params) ->
    case barrel_bql:compile(Bql, #{params => Params}) of
        {ok, Plan} -> {ok, Plan};
        {error, Reason} -> {error, {invalid_query, Reason}}
    end.

resolve({contexts, Ctxs}) ->
    case resolve(Ctxs, 1, []) of
        {ok, Members} -> {ok, Members, #{}};
        {error, _} = Err -> Err
    end;
resolve({working_set, WsId}) ->
    case {barrel_ctx_ws:get(WsId), barrel_ctx_ws:members(WsId)} of
        {{ok, #{budget := Budget}}, {ok, Resolved}} ->
            Indexed = lists:zip(lists:seq(1, length(Resolved)), Resolved),
            {ok, [ws_member(Idx, R) || {Idx, R} <- Indexed], Budget};
        _ ->
            {error, {unknown_working_set, WsId}}
    end.

resolve([], _Idx, Acc) ->
    {ok, lists:reverse(Acc)};
resolve([Ctx | Rest], Idx, Acc) ->
    case barrel_ctx_catalog:get(Ctx) of
        {ok, Card} ->
            Loc = pick_location(Card),
            Member = #member{idx = Idx, ctx = Ctx, loc = Loc,
                             decision = #{mode => card_mode(Loc)},
                             emb = card_embedding(Card)},
            resolve(Rest, Idx + 1, [Member | Acc]);
        {error, not_found} ->
            {error, {unknown_context, Ctx}};
        {error, _} = Err ->
            Err
    end.

card_mode(#{<<"kind">> := <<"remote">>}) -> remote;
card_mode(_LocalOrNone) -> local.

card_embedding(#{<<"embedding">> := #{} = Emb}) -> Emb;
card_embedding(_Card) -> undefined.

%% A working-set member resolved by barrel_ctx_ws: local copies carry
%% their own open options (read-only imports and slices).
ws_member(Idx, #{context := Ctx, mode := remote, location := Loc} = R) ->
    Base = #{<<"kind">> => <<"remote">>,
             <<"endpoint">> => maps:get(endpoint, Loc),
             <<"db">> => maps:get(db, Loc)},
    Remote = case maps:get(credential_ref, R,
                           maps:get(credential_ref, Loc, undefined)) of
        Ref when is_binary(Ref) -> Base#{<<"credential_ref">> => Ref};
        _ -> Base
    end,
    #member{idx = Idx, ctx = Ctx, loc = Remote, decision = R,
            emb = known_embedding(Ctx)};
ws_member(Idx, #{context := Ctx, local_db := Db} = R) ->
    #member{idx = Idx, ctx = Ctx,
            loc = #{<<"kind">> => <<"local">>, <<"db">> => Db},
            open_opts = maps:get(open_opts, R, undefined), decision = R,
            emb = known_embedding(Ctx)}.

known_embedding(Ctx) ->
    case barrel_ctx_catalog:get(Ctx) of
        {ok, Card} -> card_embedding(Card);
        {error, _} -> undefined
    end.

%% Coverage verdict before any work: offline remote members are skipped
%% (never replaced by a download), missing local copies fail.
decide(#member{decision = D, ctx = Ctx} = M, Offline) ->
    Facts = #{offline => Offline, available => maps:get(available, D, true)},
    case barrel_ctx_coverage:member(D#{context => Ctx}, Facts) of
        #{status := skipped_offline, error := Error} ->
            M#member{status = skipped_offline, result = Error};
        #{status := error, error := Error} ->
            M#member{status = error, result = Error};
        Verdict ->
            M#member{extra = maps:with([membership, version, note], Verdict)}
    end.

%% A local copy wins over a remote one; snapshots are not queryable yet.
pick_location(#{<<"locations">> := Locs}) ->
    Local = [L || #{<<"kind">> := <<"local">>} = L <- Locs],
    Remote = [L || #{<<"kind">> := <<"remote">>} = L <- Locs],
    case Local ++ Remote of
        [Loc | _] -> Loc;
        [] -> none
    end.

%%====================================================================
%% Fanout
%%====================================================================

fanout(Members0, Req, Shape, Budget) ->
    #{deadline_ms := DeadlineMs} = Budget,
    Deadline = now_ms() + DeadlineMs,
    Members = [admit(M, Req) || M <- Members0],
    {Runnable, Done} = lists:partition(
        fun(#member{status = S}) -> S =:= pending end, Members),
    Ctx = #{req => Req, shape => Shape, budget => Budget,
            deadline => Deadline},
    Finished = schedule(Runnable, #{}, Ctx, []),
    lists:keysort(#member.idx, Done ++ Finished).

%% Members refused before any work: decided already, no location, not
%% authorized.
admit(#member{status = Status} = M, _Req) when Status =/= pending ->
    M;
admit(#member{loc = none} = M, _Req) ->
    M#member{status = error, result = no_queryable_location};
admit(#member{loc = #{<<"kind">> := <<"local">>, <<"db">> := Db}} = M,
      Req) ->
    Authorize = maps:get(authorize, Req, fun(_) -> ok end),
    case Authorize(Db) of
        ok -> M;
        {error, Reason} -> M#member{status = unauthorized, result = Reason}
    end;
admit(M, _Req) ->
    M.

%% Keep at most max_parallel workers running; collect results until
%% every member is done or the global deadline passes.
schedule([], Running, _Ctx, Acc) when map_size(Running) =:= 0 ->
    Acc;
schedule([M | Rest] = Pending, Running,
         #{budget := #{max_parallel := Max}, deadline := Deadline} = Ctx, Acc)
  when map_size(Running) < Max ->
    %% past the global deadline a queued member is not started with a
    %% leftover budget: it is reported as never started
    case now_ms() of
        Now when Now < Deadline -> start_next(M, Now, Rest, Running, Ctx, Acc);
        _ -> expire(Pending, Running, Ctx, Acc)
    end;
schedule(Pending, Running, #{deadline := Deadline} = Ctx, Acc) ->
    Now = now_ms(),
    Wait = next_wait(Running, Deadline, Now),
    receive
        {ctx_member, Mon, Result} when is_map_key(Mon, Running) ->
            erlang:demonitor(Mon, [flush]),
            {Info, Running1} = maps:take(Mon, Running),
            schedule(Pending, Running1, Ctx, [finish(Info, Result) | Acc]);
        {ctx_lease, Mon, Db, OpenOpts} when is_map_key(Mon, Running) ->
            Info = lease_for(maps:get(Mon, Running), Db, OpenOpts),
            schedule(Pending, Running#{Mon := Info}, Ctx, Acc);
        {'DOWN', Mon, process, _Pid, Reason} when is_map_key(Mon, Running) ->
            {Info, Running1} = maps:take(Mon, Running),
            Failure = #{status => error, reason => {crashed, Reason}},
            schedule(Pending, Running1, Ctx, [finish(Info, {error, Failure})
                                              | Acc])
    after Wait ->
        expire(Pending, Running, Ctx, Acc)
    end.

%% The coordinator holds a local member's lease, so it is released
%% synchronously whatever happens to the worker. The worker has already
%% done the (possibly slow) open.
lease_for(#{pid := Pid} = Info, Db, OpenOpts) ->
    case barrel_dbs:lease(Db, OpenOpts) of
        {ok, Handle, Lease} ->
            Pid ! {ctx_leased, {ok, Handle}},
            Info#{lease => Lease};
        {error, _} = Err ->
            Pid ! {ctx_leased, Err},
            Info
    end.

start_next(M, Now, Rest, Running, Ctx, Acc) ->
    case start(M, Now, Ctx) of
        {started, Mon, Info} ->
            schedule(Rest, Running#{Mon => Info}, Ctx, Acc);
        {done, M1} ->
            schedule(Rest, Running, Ctx, [M1 | Acc])
    end.

%% Stop workers past their own timeout; at the global deadline, every
%% worker and every member not started yet times out.
expire(Pending, Running, #{deadline := Deadline} = Ctx, Acc) ->
    Now = now_ms(),
    Global = Now >= Deadline,
    {Expired, Alive} = maps:fold(
        fun(Mon, #{kill_at := KillAt} = Info, {E, A}) ->
            case Global orelse Now >= KillAt of
                true -> {[{Mon, Info} | E], A};
                false -> {E, A#{Mon => Info}}
            end
        end, {[], #{}}, Running),
    Killed = [begin
                  stop_worker(Mon, Info),
                  finish(Info, {error, timeout_failure(Info)})
              end || {Mon, Info} <- Expired],
    case Global of
        true ->
            NotStarted = [M#member{status = timeout,
                                   result = #{reason => deadline_before_start}}
                          || M <- Pending],
            Killed ++ NotStarted ++ drain_all(Alive) ++ Acc;
        false ->
            schedule(Pending, Alive, Ctx, Killed ++ Acc)
    end.

drain_all(Running) ->
    [begin
         stop_worker(Mon, Info),
         finish(Info, {error, timeout_failure(Info)})
     end || {Mon, Info} <- maps:to_list(Running)].

next_wait(Running, Deadline, Now) ->
    KillAts = [K || #{kill_at := K} <- maps:values(Running)],
    max(0, lists:min([Deadline | KillAts]) - Now).

start(#member{loc = #{<<"kind">> := <<"remote">>}} = M, Now, Ctx) ->
    case barrel_ctx_remote:acquire_slot() of
        ok ->
            spawn_member(M, Now, Ctx, remote);
        {error, busy} ->
            {done, M#member{status = skipped_budget,
                            result = node_remote_limit}}
    end;
start(M, Now, Ctx) ->
    spawn_member(M, Now, Ctx, local).

%% Now is before the global deadline: the member's budget is at least 1.
spawn_member(M, Now, #{deadline := Deadline, budget := Budget} = Ctx, Kind) ->
    Timeout = min(maps:get(member_timeout_ms, Budget), Deadline - Now),
    Loc = M#member.loc,
    MCtx = Ctx#{member_open_opts => M#member.open_opts},
    Pid = spawn(fun() ->
                    MyMon = member_mon(),
                    Result = member_work(Loc, Timeout, MCtx#{alias => MyMon}),
                    MyMon ! {ctx_member, MyMon, Result}
                end),
    %% the monitor is also the alias results are sent to: once
    %% demonitored, a late result is dropped
    Mon = monitor(process, Pid, [{alias, demonitor}]),
    Pid ! {mon, Mon},
    KillAt = Now + Timeout + grace(Kind),
    {started, Mon, #{member => M, pid => Pid, kind => Kind, started => Now,
                     timeout => Timeout, kill_at => KillAt}}.

%% The worker learns its monitor ref so its result message is matched.
member_mon() ->
    receive {mon, Mon} -> Mon end.

grace(remote) -> ?REMOTE_GRACE;
grace(local) -> 0.

%% A local worker is killed (finish/2 then drops its lease). A remote
%% worker is left to its own deadline so its client closes the
%% connection; its result, if any, is dropped.
stop_worker(Mon, #{kind := local, pid := Pid}) ->
    exit(Pid, kill),
    receive {'DOWN', Mon, process, Pid, _} -> ok end,
    flush_worker(Mon);
stop_worker(Mon, #{kind := remote}) ->
    erlang:demonitor(Mon, [flush]),
    flush_worker(Mon).

flush_worker(Mon) ->
    receive
        {ctx_member, Mon, _} -> flush_worker(Mon);
        {ctx_lease, Mon, _, _} -> flush_worker(Mon)
    after 0 ->
        ok
    end.

release(#{kind := remote}) -> barrel_ctx_remote:release_slot();
release(#{kind := local, lease := Lease}) -> barrel_dbs:release(Lease);
release(#{kind := local}) -> ok.

finish(#{member := M, started := Started} = Info, Result) ->
    release(Info),
    Elapsed = now_ms() - Started,
    case Result of
        {ok, Rows, Meta} ->
            M#member{status = ok, result = {Rows, Meta}, elapsed = Elapsed};
        {error, #{status := Status} = Failure} ->
            M#member{status = Status, result = Failure, elapsed = Elapsed}
    end.

timeout_failure(#{timeout := Timeout}) ->
    #{status => timeout, reason => deadline, after_ms => Timeout}.

%%====================================================================
%% Member work (runs in the worker process)
%%====================================================================

%% Local members open existing databases only: a typo in a card never
%% creates an empty database. The worker opens, the coordinator leases.
member_work(#{<<"kind">> := <<"local">>, <<"db">> := Db}, _Timeout,
            #{req := Req, shape := Shape, alias := Alias} = Ctx) ->
    OpenOpts = case maps:get(member_open_opts, Ctx, undefined) of
        undefined -> maps:get(open_opts, Req, #{});
        Own -> Own
    end,
    Opts = OpenOpts#{must_exist => true},
    case barrel_dbs:ensure(Db, Opts) of
        {ok, _} ->
            Alias ! {ctx_lease, Alias, Db, Opts},
            receive {ctx_leased, Leased} -> leased_query(Leased, Req, Shape) end;
        {error, _} = Err ->
            leased_query(Err, Req, Shape)
    end;
member_work(#{<<"kind">> := <<"remote">>, <<"endpoint">> := Endpoint,
              <<"db">> := Db} = Loc, Timeout,
            #{req := Req, shape := #{bound := Bound}}) ->
    Opts0 = #{timeout => Timeout, max_rows => Bound,
              params => maps:get(params, Req, #{})},
    Opts = case maps:get(<<"credential_ref">>, Loc, undefined) of
        Ref when is_binary(Ref) -> Opts0#{credential_ref => Ref};
        _ -> Opts0
    end,
    Opts1 = case application:get_env(barrel, ctx_max_response_bytes) of
        {ok, Max} -> Opts#{max_bytes => Max};
        undefined -> Opts
    end,
    case barrel_ctx_remote:query(#{endpoint => Endpoint, db => Db},
                                 maps:get(query, Req), Opts1) of
        %% a server that ignores max_rows never widens the answer
        {ok, Rows, Meta} -> {ok, lists:sublist(Rows, Bound), Meta};
        {error, _} = Err -> Err
    end.

leased_query({ok, Handle}, Req, Shape) ->
    local_query(Handle, Req, Shape);
leased_query({error, not_found}, _Req, _Shape) ->
    {error, #{status => error, reason => db_not_found}};
leased_query({error, Reason}, _Req, _Shape) ->
    {error, #{status => error, reason => Reason}}.

local_query(Handle, Req, Shape) ->
    QOpts = #{params => maps:get(params, Req, #{})},
    case barrel:'query'(Handle, maps:get(query, Req), QOpts) of
        {ok, Rows, Meta} ->
            Base = #{has_more => maps:get(has_more, Meta, false),
                     bound => undefined,
                     instance_id => maps:get(instance_id, Meta, undefined),
                     last_seq => encode_seq(maps:get(last_seq, Meta,
                                                     undefined))},
            {ok, Rows, with_embedding(Handle, Shape, Base)};
        {error, Reason} ->
            {error, #{status => error, reason => Reason}}
    end.

%% Vector results carry the embedding identity a score merge checks.
with_embedding(Handle, #{fn := vector_top_k}, Meta) ->
    try barrel:embedder_info(Handle) of
        {ok, Info} ->
            Meta#{embedding => maps:with([fingerprint, distance], Info)}
    catch
        _:_ -> Meta
    end;
with_embedding(_Handle, _Shape, Meta) ->
    Meta.

encode_seq(Seq) when is_binary(Seq) -> base64:encode(Seq, #{mode => urlsafe});
encode_seq(_Other) -> undefined.

%%====================================================================
%% Response
%%====================================================================

respond(Members, #{merge := Merge} = Shape, Scope, ElapsedMs) ->
    Ok = [M || #member{status = ok} = M <- Members],
    Skipped = [M || #member{status = S} = M <- Members,
                    S =:= skipped_budget orelse S =:= skipped_offline],
    Requested = length(Members),
    Answered = length(Ok),
    Execution = case Answered of
        Requested -> succeeded;
        0 -> failed;
        _ -> partial
    end,
    Base = #{execution => Execution,
             merge => Merge,
             sources => [source(M, Shape) || M <- Members],
             coverage => #{requested => Requested,
                           answered => Answered,
                           failed => Requested - Answered - length(Skipped),
                           skipped => length(Skipped),
                           missing => [C || #member{ctx = C, status = S}
                                                <- Members, S =/= ok],
                           scope_origin => explicit},
             elapsed_ms => ElapsedMs},
    merge(Merge, Ok, Shape, with_scope(Scope, Base)).

with_scope({working_set, WsId}, Base) -> Base#{working_set => WsId};
with_scope({contexts, _}, Base) -> Base.

merge(grouped, Ok, Shape, Base) ->
    Groups = [#{context => Ctx, rows => tag(Ctx, Rows)}
              || #member{ctx = Ctx, result = {Rows, _}} <- Ok],
    {ok, with_relevance(Shape, false, Base#{groups => Groups})};
merge(ordered, Ok, #{order := {_Key, Dir}, bound := Bound} = Shape, Base) ->
    Keyed = [{barrel_ctx_shape:order_value(Row, Shape), Ctx, row_id(Row), Row}
             || #member{ctx = Ctx, result = {Rows, _}} <- Ok,
                Row <- tag(Ctx, Rows)],
    Sorted = lists:sort(fun(A, B) -> before(A, B, Dir) end, Keyed),
    {ok, Base#{rows => [Row || {_, _, _, Row} <- lists:sublist(Sorted, Bound)]}};
merge(interleave, Ok, #{bound := Bound}, Base) ->
    {ok, Rows, #{relevance := Rel}} =
        barrel_ctx_merge:interleave(merge_members(Ok), Bound),
    {ok, Base#{rows => Rows, relevance => Rel}};
merge(score, Ok, #{bound := Bound} = Shape, Base) ->
    case barrel_ctx_merge:score(merge_members(Ok), Bound, #{}) of
        {ok, Rows, #{relevance := Rel}} ->
            {ok, Base#{rows => Rows, relevance => Rel}};
        {error, Reason} ->
            score_refused(Reason, Ok, Shape, Base)
    end.

%% An automatic score merge falls back to grouped and says why; a
%% requested one is refused.
score_refused(Reason, Ok, #{score_mode := auto} = Shape, Base) ->
    {ok, Grouped} = merge(grouped, Ok, Shape, Base#{merge => grouped}),
    {ok, Grouped#{merge_fallback => (fallback_reason(Reason))#{
                                        requested => score}}};
score_refused(Reason, _Ok, _Shape, _Base) ->
    {error, {unsupported_federated_query, Reason}}.

fallback_reason({Tag, Ctx}) -> #{reason => Tag, context => Ctx}.

with_relevance(#{kind := retrieval}, Rel, Base) -> Base#{relevance => Rel};
with_relevance(_Shape, _Rel, Base) -> Base.

merge_members(Ok) ->
    [maps:merge(#{ctx => Ctx, rows => Rows}, embedding_of(M))
     || #member{ctx = Ctx, result = {Rows, _}} = M <- Ok].

%% The member's own report wins over its card's advertisement.
embedding_of(#member{result = {_, #{embedding := Emb}}}) ->
    embedding_fields(Emb);
embedding_of(#member{emb = #{} = Emb}) ->
    embedding_fields(Emb);
embedding_of(_M) ->
    #{}.

embedding_fields(Emb) ->
    Fields = #{fingerprint => get_any([fingerprint, <<"fingerprint">>], Emb),
               distance => distance(get_any([distance, <<"distance">>], Emb))},
    maps:filter(fun(_K, V) -> V =/= undefined end, Fields).

get_any([], _Map) -> undefined;
get_any([K | Rest], Map) ->
    case maps:find(K, Map) of
        {ok, V} -> V;
        error -> get_any(Rest, Map)
    end.

distance(<<"cosine">>) -> cosine;
distance(D) -> D.

%% Term order on the ORDER BY value, then context id, then document id.
before({VA, CA, IA, _}, {VB, CB, IB, _}, asc) ->
    if VA < VB -> true;
       VB < VA -> false;
       true -> {CA, IA} =< {CB, IB}
    end;
before({VA, CA, IA, _}, {VB, CB, IB, _}, desc) ->
    if VB < VA -> true;
       VA < VB -> false;
       true -> {CA, IA} =< {CB, IB}
    end.

tag(Ctx, Rows) ->
    [Row#{<<"_ctx">> => Ctx} || Row <- Rows].

row_id(#{<<"id">> := Id}) -> Id;
row_id(_Row) -> null.

source(#member{ctx = Ctx, loc = Loc, status = ok, elapsed = Elapsed,
               result = {Rows, Meta}, extra = Extra} = M,
       #{bound := Bound} = Shape) ->
    N = length(Rows),
    Base = #{context => Ctx,
             location => location(Loc),
             status => ok,
             rows => N,
             bound => bound(N, Bound, Meta),
             retrieval => maps:get(retrieval, Shape),
             version => version(Meta, Extra),
             elapsed_ms => Elapsed},
    Base1 = maps:merge(Base, maps:with([membership, note], Extra)),
    Base2 = case maps:find(bytes, Meta) of
        {ok, Bytes} -> Base1#{bytes => Bytes};
        error -> Base1
    end,
    with_source_embedding(M, Shape, Base2);
source(#member{ctx = Ctx, loc = Loc, status = Status, elapsed = Elapsed,
               result = Failure}, _Shape) ->
    #{context => Ctx,
      location => location(Loc),
      status => Status,
      rows => 0,
      error => failure(Failure),
      elapsed_ms => Elapsed}.

bound(N, Bound, _Meta) when N >= Bound -> limit_reached;
bound(_N, _Bound, #{has_more := true}) -> limit_reached;
bound(_N, _Bound, #{bound := <<"limit_reached">>}) -> limit_reached;
bound(_N, _Bound, _Meta) -> exhausted.

%% Imported generations and slices report their own version kind; live
%% members report what the query observed.
version(_Meta, #{version := #{kind := generation} = V}) -> V;
version(_Meta, #{version := #{kind := retrieved_set} = V}) -> V;
version(Meta, _Extra) -> version(Meta).

with_source_embedding(M, #{fn := vector_top_k}, Source) ->
    case embedding_of(M) of
        #{fingerprint := _} = Emb -> Source#{embedding => Emb};
        _ -> Source
    end;
with_source_embedding(_M, _Shape, Source) ->
    Source.

version(#{instance_id := Id, last_seq := Seq}) when is_binary(Id),
                                                     is_binary(Seq) ->
    #{kind => live, observed => #{instance_id => Id, last_seq => Seq}};
version(#{instance_id := Id}) when is_binary(Id) ->
    #{kind => live, observed => #{instance_id => Id, last_seq => null}};
version(_Meta) ->
    #{kind => unknown}.

location(none) -> null;
location(#{<<"kind">> := <<"local">>, <<"db">> := Db}) ->
    #{kind => local, db => Db};
location(#{<<"kind">> := <<"remote">>, <<"endpoint">> := E, <<"db">> := Db}) ->
    #{kind => remote, endpoint => E, db => Db}.

%% A JSON-ready error block: reason tag, optional detail and counters.
failure(#{reason := Reason} = F) ->
    Extra = maps:with([after_ms], F),
    Received = case maps:find(rows_received, F) of
        {ok, N} -> #{rows_received_before_failure => N};
        error -> #{}
    end,
    maps:merge(maps:merge(reason(Reason), Extra), Received);
failure(Reason) ->
    reason(Reason).

reason(#{reason := _} = R) -> R;
reason(R) when is_atom(R) -> #{reason => R};
reason(R) when is_binary(R) -> #{reason => R};
%% A remote error named by a code reads like the same local error.
reason({remote_error, Detail}) ->
    Text = detail(Detail),
    case re:run(Text, "^[a-z][a-z0-9_]*$", [{capture, none}]) of
        match -> #{reason => Text, origin => remote};
        nomatch -> #{reason => remote_error, detail => Text}
    end;
reason({http_status, Status, Detail}) ->
    #{reason => http_status, http_status => Status, detail => detail(Detail)};
reason({Tag, Detail}) when is_atom(Tag) ->
    #{reason => Tag, detail => detail(Detail)};
reason(Other) ->
    #{reason => error, detail => detail(Other)}.

detail(B) when is_binary(B) -> B;
detail(A) when is_atom(A) -> atom_to_binary(A, utf8);
detail(Other) -> iolist_to_binary(io_lib:format("~0p", [Other])).

%%====================================================================
%% Helpers
%%====================================================================

env(Key, Default) ->
    application:get_env(barrel, Key, Default).

now_ms() ->
    erlang:monotonic_time(millisecond).
