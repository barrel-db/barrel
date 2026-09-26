%%%-------------------------------------------------------------------
%%% @doc Federated executor (B6): union oracle for ordered merges,
%%% grouped and retrieval shapes, rejected shapes, member failures
%%% (unreachable, stall, error mid-stream), deadlines, budgets, leases.
%%% Data: a small OTP module corpus, one application per context.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_query_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([ordered_union_oracle/1,
         ordered_generated/1,
         ordered_ties_deterministic/1,
         grouped_rows/1,
         retrieval_grouped/1,
         rejected_shapes/1,
         remote_member_ok/1,
         member_failures/1,
         global_deadline/1,
         local_stall/1,
         unauthorized_member/1,
         node_budget_skips/1,
         no_location/1,
         no_leases_left/1]).

-define(APPS, [<<"et">>, <<"ftp">>, <<"os_mon">>]).

all() ->
    [ordered_union_oracle, ordered_generated, ordered_ties_deterministic,
     grouped_rows, retrieval_grouped, rejected_shapes, remote_member_ok,
     member_failures, global_deadline, local_stall, unauthorized_member,
     node_budget_skips, no_location, no_leases_left].

%%====================================================================
%% Setup
%%====================================================================

init_per_suite(Config) ->
    Dir = ?config(priv_dir, Config),
    application:load(barrel_docdb),
    application:set_env(barrel_docdb, data_dir, Dir),
    {ok, _} = application:ensure_all_started(barrel),
    application:set_env(barrel, ctx_catalog_db, <<"_ctxq_catalog">>),
    mock_embed(),
    Docs = load_corpus(filename:join(?config(data_dir, Config),
                                     "otp_small.jsonl")),
    Ctxs = [begin
                Db = <<"ctxq_", App/binary>>,
                ok = seed(Db, Dir, [D || #{<<"app">> := A} = D <- Docs,
                                         A =:= App]),
                {ok, #{<<"id">> := Id}} = barrel_ctx:register(
                    #{<<"name">> => <<"otp/", App/binary>>,
                      <<"locations">> => [#{<<"kind">> => <<"local">>,
                                            <<"db">> => Db}]}),
                {App, Id, Db}
            end || App <- ?APPS],
    ok = seed(<<"ctxq_union">>, Dir,
              [D || #{<<"app">> := A} = D <- Docs, lists:member(A, ?APPS)]),
    [{ctxs, Ctxs} | Config].

end_per_suite(_Config) ->
    %% suites share the VM: leave barrel_dbs as found
    [ok = barrel_dbs:close(Db) || <<"ctxq_", _/binary>> = Db <- barrel_dbs:list()],
    application:unset_env(barrel, ctx_catalog_db),
    try meck:unload(barrel_embed) catch _:_ -> ok end,
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    application:unset_env(barrel, ctx_node_remote_max),
    ?assertEqual(#{}, barrel_dbs:leases()),
    ok.

mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed,
                fun(Text, _State) -> {ok, mock_vec(Text)} end),
    meck:expect(barrel_embed, embed_batch,
                fun(Texts, _State) -> {ok, [mock_vec(T) || T <- Texts]} end).

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].

load_corpus(Path) ->
    {ok, Bin} = file:read_file(Path),
    [begin
         Doc = json:decode(L),
         Doc#{<<"bucket">> => maps:get(<<"lines">>, Doc) rem 3}
     end || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>].

%% Open through barrel_dbs (record mode, mock embedder) so later leases
%% find the handle, then load the docs.
seed(Db, Dir, Docs) ->
    {ok, Handle} = barrel_dbs:ensure(Db, #{
        embedding => #{fields => [<<"moduledoc">>], mode => sync},
        vectordb => #{dimension => 3,
                      db_path => filename:join(Dir, "vec_" ++
                                                   binary_to_list(Db))}}),
    ok = barrel_dbs:pin(Db),
    lists:foreach(fun(D) -> {ok, _} = barrel:put_doc(Handle, D) end, Docs).

ctx_ids(Config) ->
    [Id || {_App, Id, _Db} <- ?config(ctxs, Config)].

union() ->
    {ok, Db} = barrel_dbs:ensure(<<"ctxq_union">>),
    Db.

query(Config, Bql) ->
    query(Config, Bql, #{}).

query(Config, Bql, Extra) ->
    barrel_ctx:query(maps:merge(#{query => Bql, contexts => ctx_ids(Config)},
                                Extra)).

%%====================================================================
%% Cases
%%====================================================================

ordered_union_oracle(Config) ->
    Stmts = [<<"SELECT id, path, lines FROM c WHERE lines > 100 "
               "ORDER BY lines DESC LIMIT 10">>,
             <<"SELECT * FROM c ORDER BY lines LIMIT 7">>,
             <<"SELECT path AS p, size FROM c ORDER BY size DESC LIMIT 12">>,
             <<"SELECT id, path FROM c WHERE size < 20000 "
               "ORDER BY path LIMIT 5">>],
    lists:foreach(fun(Stmt) -> assert_oracle(Config, Stmt) end, Stmts).

%% Fixed-seed generated statements (property-style).
ordered_generated(Config) ->
    rand:seed(exsss, {7, 11, 13}),
    lists:foreach(
        fun(_) ->
            Field = pick([<<"lines">>, <<"size">>, <<"path">>]),
            Dir = pick([<<"ASC">>, <<"DESC">>]),
            Threshold = rand:uniform(900),
            Limit = rand:uniform(20),
            Stmt = iolist_to_binary(
                     ["SELECT id, ", Field, " FROM c WHERE lines > ",
                      integer_to_binary(Threshold), " ORDER BY ", Field, " ",
                      Dir, " LIMIT ", integer_to_binary(Limit)]),
            assert_oracle(Config, Stmt)
        end, lists:seq(1, 25)).

pick(List) ->
    lists:nth(rand:uniform(length(List)), List).

assert_oracle(Config, Stmt) ->
    {ok, #{execution := succeeded, merge := ordered, rows := Rows,
           sources := Sources}} = query(Config, Stmt),
    {ok, Expected, _} = barrel:query(union(), Stmt),
    Strip = fun(R) -> maps:without([<<"_ctx">>, <<"_ctx_name">>], R) end,
    ?assertEqual({Stmt, Expected}, {Stmt, [Strip(R) || R <- Rows]}),
    %% every row names the context that holds it
    ByCtx = maps:from_list([{Id, Db} || {_, Id, Db} <- ?config(ctxs, Config)]),
    lists:foreach(
        fun(#{<<"_ctx">> := Ctx, <<"id">> := Id}) ->
            {ok, H} = barrel_dbs:ensure(maps:get(Ctx, ByCtx)),
            ?assertMatch({ok, _}, barrel:get_doc(H, Id))
        end, Rows),
    ?assertEqual(3, length(Sources)).

%% Ties at the order key break by context id then doc id; the merge is
%% the same on every run.
ordered_ties_deterministic(Config) ->
    Stmt = <<"SELECT id, bucket FROM c ORDER BY bucket LIMIT 15">>,
    {ok, #{rows := Rows}} = query(Config, Stmt),
    Keys = [{B, C, I} || #{<<"bucket">> := B, <<"_ctx">> := C,
                           <<"id">> := I} <- Rows],
    ?assertEqual(lists:sort(Keys), Keys),
    ?assertEqual(15, length(Rows)),
    [?assertEqual({ok, Rows}, element_rows(query(Config, Stmt)))
     || _ <- lists:seq(1, 3)],
    %% bound: every member filled its LIMIT (each holds at least 6 docs)
    {ok, #{sources := Sources}} =
        query(Config, <<"SELECT id, bucket FROM c ORDER BY bucket LIMIT 4">>),
    ?assert(lists:all(fun(#{bound := B}) -> B =:= limit_reached end,
                      Sources)).

element_rows({ok, #{rows := Rows}}) -> {ok, Rows}.

grouped_rows(Config) ->
    {ok, R} = query(Config, <<"SELECT id, app FROM c WHERE lines > 50 "
                              "LIMIT 3">>),
    #{execution := succeeded, merge := grouped, groups := Groups,
      coverage := Coverage} = R,
    ?assertEqual(ctx_ids(Config), [C || #{context := C} <- Groups]),
    lists:foreach(
        fun(#{context := C, rows := Rows}) ->
            ?assert(length(Rows) =< 3),
            [?assertEqual(C, maps:get(<<"_ctx">>, Row)) || Row <- Rows]
        end, Groups),
    ?assertMatch(#{requested := 3, answered := 3, failed := 0, skipped := 0,
                   missing := [], scope_origin := explicit}, Coverage),
    %% an unordered row query cannot be merged in order
    ?assertMatch({error, {unsupported_federated_query,
                          #{reason := order_by_required}}},
                 query(Config, <<"SELECT id FROM c LIMIT 3">>,
                       #{merge => ordered})),
    %% LIMIT larger than the data: exhausted
    {ok, #{sources := Sources}} =
        query(Config, <<"SELECT id FROM c LIMIT 500">>),
    ?assert(lists:all(fun(#{bound := B}) -> B =:= exhausted end, Sources)).

retrieval_grouped(Config) ->
    {ok, #{groups := Groups, sources := Sources}} =
        query(Config, <<"SELECT b.id, b._score FROM "
                        "bm25_top_k('event trace', k => 3) AS b">>),
    ?assertEqual(3, length(Groups)),
    lists:foreach(
        fun(#{status := ok, retrieval := exact, rows := N, bound := Bound,
              version := #{kind := live,
                           observed := #{instance_id := Iid,
                                         last_seq := Seq}},
              location := #{kind := local, db := Db}}) ->
            ?assert(N =< 3),
            ?assertEqual(Bound, case N of 3 -> limit_reached;
                                          _ -> exhausted end),
            {ok, H} = barrel_dbs:ensure(Db),
            {ok, #{instance_id := Iid, last_seq := Raw}} =
                barrel_docdb:db_observed_version(maps:get(docdb, H)),
            ?assertEqual(base64:encode(Raw, #{mode => urlsafe}), Seq)
        end, Sources),
    {ok, #{sources := HSources}} =
        query(Config, <<"SELECT * FROM hybrid_top_k('trace', k => 2) AS h">>),
    ?assert(lists:all(fun(#{retrieval := R}) -> R =:= approximate end,
                      HSources)),
    %% scores are not comparable across contexts: no ordered merge
    ?assertMatch({error, {merge_not_allowed, #{merge := ordered}}},
                 query(Config, <<"SELECT * FROM vector_top_k('x', k => 2) "
                                 "AS v ORDER BY v._score DESC">>,
                       #{merge => ordered})).

rejected_shapes(_Config) ->
    %% a remote member that records any contact
    Server = barrel_ctx_fake_server:start({chunks, 200, []}),
    {ok, #{<<"id">> := Remote}} = barrel_ctx:register(
        #{<<"name">> => <<"probe">>,
          <<"locations">> => [#{<<"kind">> => <<"remote">>,
                                <<"endpoint">> => endpoint(Server),
                                <<"db">> => <<"x">>}]}),
    Q = fun(Bql, Extra) ->
            barrel_ctx:query(maps:merge(#{query => Bql,
                                          contexts => [Remote]}, Extra))
        end,
    Cases = [
        {<<"SELECT * FROM c WHERE a = 1 SUBSCRIBE">>, #{}, subscribe},
        {<<"SELECT * FROM c ORDER BY path LIMIT 5 OFFSET 2">>, #{}, offset},
        {<<"SELECT t FROM c AS c, UNNEST(c.tags) AS t LIMIT 5">>, #{}, unnest},
        {<<"SELECT * FROM c ORDER BY path">>, #{}, limit_required},
        {<<"SELECT * FROM c">>, #{}, limit_required},
        {<<"SELECT * FROM c LIMIT 1001">>, #{}, limit_too_large},
        {<<"SELECT * FROM bm25_top_k('x', k => 1001) AS b">>, #{},
         limit_too_large},
        {<<"SELECT id FROM c ORDER BY path LIMIT 5">>, #{},
         order_key_not_projected},
        {<<"SELECT id FROM c LIMIT 5">>, #{merge => ordered},
         order_by_required},
        {<<"SELECT * FROM bm25_top_k('x', k => 3) AS b">>,
         #{merge => ordered}, {merge_not_allowed, ordered}},
        {<<"SELECT * FROM bm25_top_k('x', k => 3) AS b">>,
         #{merge => score}, {merge_not_allowed, score}},
        {<<"SELECT * FROM c ORDER BY path LIMIT 3">>,
         #{merge => score}, {merge_not_allowed, score}},
        {<<"SELECT * FROM hybrid_top_k('x', k => 3) AS h">>,
         #{merge => rerank}, {merge_not_supported, rerank}},
        {<<"SELECT * FROM hybrid_top_k('x', k => 3) AS h">>,
         #{merge => <<"rrf">>}, {merge_not_supported, rrf}},
        {<<"SELECT * FROM c LIMIT 5">>, #{continuation => <<"abc">>},
         continuation}],
    lists:foreach(
        fun({Bql, Extra, Reason}) ->
            {Code, Sub} = expected(Reason),
            {error, {Got, Details}} = Q(Bql, Extra),
            ?assertEqual({Bql, Code, Sub},
                         {Bql, Got, maps:with(maps:keys(Sub), Details)})
        end, Cases),
    Nine = [<<"ctx_", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 9)],
    ?assertMatch({error, {too_many_contexts, #{requested := 9,
                                               max_contexts := 8}}},
                 barrel_ctx:query(#{query => <<"SELECT * FROM c LIMIT 1">>,
                                    contexts => Nine})),
    ?assertMatch({error, {invalid_query, #{bql_error := _}}},
                 Q(<<"SELECT FROM">>, #{})),
    ?assertMatch({error, {unknown_context, #{context := <<"ctx_missing">>}}},
                 barrel_ctx:query(#{query => <<"SELECT * FROM c LIMIT 1">>,
                                    contexts => [<<"ctx_missing">>]})),
    ?assertMatch({error, {invalid_argument, #{field := contexts}}},
                 barrel_ctx:query(#{query => <<"SELECT * FROM c LIMIT 1">>,
                                    contexts => []})),
    ?assertMatch({error, {duplicate_context, #{context := Remote}}},
                 Q(<<"SELECT * FROM c LIMIT 1">>,
                   #{contexts => [Remote, Remote]})),
    %% nothing reached the member
    receive {fake_request, _, _} -> ct:fail(member_contacted)
    after 100 -> ok
    end,
    barrel_ctx_fake_server:stop(Server).

remote_member_ok(Config) ->
    Row = fun(Id, Lines) ->
              [json:encode(#{row => #{id => Id, lines => Lines}}), $\n]
          end,
    Meta = [json:encode(#{meta => #{has_more => false,
                                    instance_id => <<"feed">>,
                                    last_seq => <<"AAAAAAAAAAAAAAAA">>}}),
            $\n],
    Server = barrel_ctx_fake_server:start(
        {chunks, 200, [Row(<<"zz_remote">>, 100000), Meta]}),
    Remote = register_remote(<<"remote/ok">>, endpoint(Server)),
    [Et | _] = ctx_ids(Config),
    {ok, R} = barrel_ctx:query(
        #{query => <<"SELECT id, lines FROM c ORDER BY lines DESC LIMIT 2">>,
          contexts => [Et, Remote]}),
    #{execution := succeeded, rows := [First | _], sources := [_, RSrc]} = R,
    ?assertEqual(#{<<"id">> => <<"zz_remote">>, <<"lines">> => 100000,
                   <<"_ctx">> => Remote, <<"_ctx_name">> => <<"remote/ok">>},
                 First),
    ?assertMatch(#{status := ok, rows := 1, bound := exhausted,
                   location := #{kind := remote},
                   version := #{kind := live,
                                observed := #{instance_id := <<"feed">>}},
                   bytes := _}, RSrc),
    %% the member was asked for at most the statement's bound
    receive
        {fake_request, _, Body} ->
            ?assertMatch(#{<<"max_rows">> := 2, <<"deadline_ms">> := _},
                         json:decode(Body))
    after 1000 -> ct:fail(no_request)
    end,
    barrel_ctx_fake_server:stop(Server).

%% One local member answers; the closed, stalled and failing remotes are
%% reported with their status and contribute no rows. The stalled server
%% never answers: the query returning proves it was not waited for.
member_failures(Config) ->
    Stall = barrel_ctx_fake_server:start(stall),
    Err = barrel_ctx_fake_server:start(
        {chunks, 200, [[json:encode(#{row => #{id => <<"leak">>}}), $\n],
                       [json:encode(#{error => <<"boom">>}), $\n]]}),
    Closed = register_remote(<<"r/closed">>, endpoint(
                 barrel_ctx_fake_server:closed_port())),
    Stalled = register_remote(<<"r/stall">>, endpoint(Stall)),
    Failing = register_remote(<<"r/err">>, endpoint(Err)),
    [Et | _] = ctx_ids(Config),
    Conns = live_conns(),
    %% the member budget must cover the members that do answer; the
    %% global deadline is far enough not to cut it
    Budget = 1000,
    {ok, R} = barrel_ctx:query(
        #{query => <<"SELECT id FROM c LIMIT 5">>,
          contexts => [Et, Closed, Stalled, Failing],
          per_context_timeout_ms => Budget, deadline_ms => 30000}),
    ok = no_leases([Et]),
    #{execution := partial, groups := Groups, sources := Sources,
      coverage := Coverage, elapsed_ms := Total} = R,
    ?assertEqual([Et], [C || #{context := C} <- Groups]),
    ?assertMatch([#{status := ok},
                  #{status := unreachable, rows := 0},
                  #{status := timeout, rows := 0,
                    error := #{reason := deadline, after_ms := Budget}},
                  #{status := error, rows := 0,
                    error := #{reason := <<"boom">>, origin := remote,
                               rows_received_before_failure := 1}}],
                 Sources),
    %% the stalled member got its whole budget
    #{elapsed_ms := StallMs} = lists:nth(3, Sources),
    ?assert(StallMs >= Budget),
    ?assert(Total >= Budget),
    ?assertMatch(#{requested := 4, answered := 1, failed := 3,
                   skipped := 0}, Coverage),
    ?assertEqual([Closed, Stalled, Failing], maps:get(missing, Coverage)),
    AllRows = lists:append([Rs || #{rows := Rs} <- Groups]),
    ?assertEqual([], [X || #{<<"id">> := <<"leak">>} = X <- AllRows]),
    ?assertEqual(0, barrel_ctx_remote:slots_in_use()),
    %% no remote connection outlives its worker, no worker message is left
    ok = await_conns_gone(Conns),
    ok = no_worker_messages(),
    barrel_ctx_fake_server:stop(Stall),
    barrel_ctx_fake_server:stop(Err).

%% The global deadline cuts the running member and reports the queued
%% one as never started. The server never answers, so returning at all
%% proves the deadline fired.
global_deadline(_Config) ->
    S1 = barrel_ctx_fake_server:start(stall),
    A = register_remote(<<"r/a">>, endpoint(S1)),
    B = register_remote(<<"r/b">>, endpoint(S1)),
    Conns = live_conns(),
    Deadline = 400,
    {ok, R} = barrel_ctx:query(
        #{query => <<"SELECT id FROM c LIMIT 5">>, contexts => [A, B],
          max_parallel => 1, per_context_timeout_ms => 5000,
          deadline_ms => Deadline}),
    ok = no_leases([A, B]),
    #{execution := failed, elapsed_ms := Total,
      sources := [#{status := timeout, rows := 0,
                    error := #{reason := deadline, after_ms := AfterMs}},
                  #{status := timeout, rows := 0,
                    error := #{reason := deadline_before_start} = E2}]} = R,
    %% the running member's budget is what the global deadline left it
    ?assert(AfterMs > 0 andalso AfterMs =< Deadline),
    ?assertNot(maps:is_key(after_ms, E2)),
    ?assert(Total >= Deadline),
    ?assertEqual(0, barrel_ctx_remote:slots_in_use()),
    %% the killed worker's connection goes with it
    ok = await_conns_gone(Conns),
    ok = no_worker_messages(),
    barrel_ctx_fake_server:stop(S1).

%% Local members that never answer: killed at their own timeout, then at
%% the global deadline. Their leases are gone when query/1 returns.
local_stall(Config) ->
    [Et, Ftp, OsMon] = Ctxs = ctx_ids(Config),
    stall_local_queries(),
    try
        {ok, R1} = barrel_ctx:query(
            #{query => <<"SELECT id FROM c LIMIT 5">>, contexts => [Et, Ftp],
              params => #{<<"stall">> => true},
              per_context_timeout_ms => 300, deadline_ms => 30000}),
        ok = no_leases(Ctxs),
        ?assertMatch(#{execution := failed,
                       sources := [#{status := timeout,
                                     error := #{after_ms := 300}},
                                   #{status := timeout,
                                     error := #{after_ms := 300}}]}, R1),
        {ok, R2} = barrel_ctx:query(
            #{query => <<"SELECT id FROM c LIMIT 5">>,
              contexts => [Et, Ftp, OsMon],
              params => #{<<"stall">> => true}, max_parallel => 1,
              per_context_timeout_ms => 5000, deadline_ms => 300}),
        ok = no_leases(Ctxs),
        ?assertMatch(#{execution := failed,
                       sources := [#{status := timeout,
                                     error := #{reason := deadline}},
                                   #{status := timeout,
                                     error := #{reason :=
                                                    deadline_before_start}},
                                   #{status := timeout,
                                     error := #{reason :=
                                                    deadline_before_start}}]},
                     R2),
        ok = no_worker_messages()
    after
        meck:unload(barrel)
    end.

unauthorized_member(Config) ->
    [{_, Et, EtDb}, {_, Ftp, _} | _] = ?config(ctxs, Config),
    Authorize = fun(Db) when Db =:= EtDb -> {error, forbidden};
                   (_) -> ok
                end,
    {ok, R} = barrel_ctx:query(#{query => <<"SELECT id FROM c LIMIT 2">>,
                                 contexts => [Et, Ftp],
                                 authorize => Authorize}),
    ?assertMatch(#{execution := partial,
                   sources := [#{context := Et, status := unauthorized,
                                 error := #{reason := forbidden}},
                               #{context := Ftp, status := ok}]}, R).

node_budget_skips(Config) ->
    application:set_env(barrel, ctx_node_remote_max, 0),
    Server = barrel_ctx_fake_server:start(stall),
    Remote = register_remote(<<"r/budget">>, endpoint(Server)),
    [Et | _] = ctx_ids(Config),
    {ok, R} = barrel_ctx:query(#{query => <<"SELECT id FROM c LIMIT 2">>,
                                 contexts => [Et, Remote]}),
    ?assertMatch(#{execution := partial,
                   sources := [#{status := ok},
                               #{status := skipped_budget}],
                   coverage := #{skipped := 1, failed := 0}}, R),
    barrel_ctx_fake_server:stop(Server).

no_location(_Config) ->
    {ok, #{<<"id">> := Id}} = barrel_ctx:register(
        #{<<"name">> => <<"snap/only">>,
          <<"locations">> => [#{<<"kind">> => <<"snapshot">>,
                                <<"publication">> => <<"s3://b/x/">>}]}),
    ?assertMatch({ok, #{execution := failed,
                        sources := [#{status := error,
                                      error := #{reason :=
                                                     no_queryable_location}}]}},
                 barrel_ctx:query(#{query => <<"SELECT * FROM c LIMIT 1">>,
                                    contexts => [Id]})).

%% Concurrent federated queries leave no lease and no remote slot held.
no_leases_left(Config) ->
    Self = self(),
    Pids = [spawn(fun() ->
                      {ok, _} = query(Config, <<"SELECT id, lines FROM c "
                                                "ORDER BY lines LIMIT 5">>),
                      Self ! {done, self()}
                  end) || _ <- lists:seq(1, 8)],
    [receive {done, P} -> ok after 10000 -> ct:fail(timeout) end
     || P <- Pids],
    ?assertEqual(#{}, barrel_dbs:leases()),
    ?assertEqual(0, barrel_ctx_remote:slots_in_use()).

%% The catalog code (and details subset) of an executor rejection.
expected(limit_required) -> {limit_required, #{max_limit => 1000}};
expected(limit_too_large) -> {limit_too_large, #{max_limit => 1000}};
expected({merge_not_allowed, M}) -> {merge_not_allowed, #{merge => M}};
expected({merge_not_supported, M}) -> {merge_not_supported, #{merge => M}};
expected(Reason) -> {unsupported_federated_query, #{reason => Reason}}.

%%====================================================================
%% Helpers
%%====================================================================

endpoint(Server) when is_tuple(Server) ->
    endpoint(barrel_ctx_fake_server:port(Server));
endpoint(Port) when is_integer(Port) ->
    <<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>.

live_conns() ->
    [P || {_, P, _, _} <- supervisor:which_children(hackney_conn_sup),
          is_pid(P), is_process_alive(P)].

%% Connections opened since Before stop with their owner (a worker that
%% was killed or has returned); the bound only guards against a hang.
await_conns_gone(Before) ->
    Mons = [monitor(process, P) || P <- live_conns() -- Before],
    await_down(Mons).

await_down([]) ->
    ok;
await_down([Mon | Rest]) ->
    receive {'DOWN', Mon, process, _, _} -> await_down(Rest)
    after 10000 -> {error, connection_left_open}
    end.

%% A query with the `stall' param never answers.
stall_local_queries() ->
    meck:new(barrel, [passthrough, no_link]),
    meck:expect(barrel, 'query',
                fun(_Db, _Bql, #{params := #{<<"stall">> := true}}) ->
                        receive after infinity -> ok end;
                   (Db, Bql, Opts) ->
                        meck:passthrough([Db, Bql, Opts])
                end).

%% Read right after query/1 returned: no lease is released later.
no_leases(Ctxs) ->
    Dbs = [Db || Ctx <- Ctxs, {ok, #{<<"locations">> := Locs}}
                                  <- [barrel_ctx_catalog:get(Ctx)],
                 #{<<"kind">> := <<"local">>, <<"db">> := Db} <- Locs],
    case maps:with(Dbs, barrel_dbs:leases()) of
        Held when map_size(Held) =:= 0 -> ok;
        Held -> {error, {leases_held, Held}}
    end.

%% Workers report to the caller's process: nothing may be left behind.
no_worker_messages() ->
    receive
        {ctx_member, _, _} = M -> {error, {leftover, M}};
        {'DOWN', _, process, _, _} = M -> {error, {leftover, M}}
    after 0 ->
        ok
    end.

register_remote(Name, Endpoint) ->
    {ok, #{<<"id">> := Id}} = barrel_ctx:register(
        #{<<"name">> => Name,
          <<"locations">> => [#{<<"kind">> => <<"remote">>,
                                <<"endpoint">> => Endpoint,
                                <<"db">> => <<"src">>}]}),
    Id.
