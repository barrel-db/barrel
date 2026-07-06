%%%-------------------------------------------------------------------
%%% @doc Record-mode timeline: branching rebuilds the branch's vector
%%% index from the embeddings stored in doc bodies (zero embedder
%%% calls for indexed docs), PITR-rewound docs re-embed from their
%%% restored text, pending outbox work is inherited, and merges
%%% re-index the parent through its own outbox.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_record_timeline_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    backfill_stored_vectors_no_embed_calls/1,
    backfill_byo_client_vectors/1,
    backfill_pitr_reembeds/1,
    backfill_opt_none_and_report/1,
    backfill_inherits_pending_outbox/1,
    branch_write_isolation/1,
    merge_reindexes_parent/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [backfill_stored_vectors_no_embed_calls, backfill_byo_client_vectors,
     backfill_pitr_reembeds, backfill_opt_none_and_report,
     backfill_inherits_pending_outbox, branch_write_isolation,
     merge_reindexes_parent].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    Dir = "/tmp/barrel_record_timeline_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    mock_embed(),
    Config.

end_per_testcase(_TC, _Config) ->
    try meck:unload(barrel_embed) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Helpers (mock embedder mirrors barrel_record_SUITE)
%%====================================================================

mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed, fun(Text, _State) ->
        case binary:match(Text, <<"poison">>) of
            nomatch -> {ok, mock_vec(Text)};
            _ -> {error, poison}
        end
    end),
    meck:expect(barrel_embed, embed_batch, fun(Texts, _State) ->
        case lists:any(fun(T) ->
                           binary:match(T, <<"poison">>) =/= nomatch
                       end, Texts) of
            true -> {error, poison};
            false -> {ok, [mock_vec(T) || T <- Texts]}
        end
    end).

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].

open_record(Name, Config) ->
    Dir = ?config(dir, Config),
    barrel:open(Name, #{
        embedding => #{fields => [<<"title">>]},
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3,
                      db_path => Dir ++ "/" ++ atom_to_list(Name)
                          ++ "_vec"}}).

branch_opts(Name, Config) ->
    Dir = ?config(dir, Config),
    #{vectordb => #{dimension => 3,
                    db_path => Dir ++ "/" ++ atom_to_list(Name)
                        ++ "_vec"}}.

wait_until(Fun) ->
    wait_until(Fun, 100).

wait_until(_Fun, 0) ->
    ct:fail(condition_never_met);
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Fun, N - 1)
    end.

hit(Db, Text, Id) ->
    case barrel:search_vector(Db, mock_vec(Text), #{k => 1}) of
        {ok, [#{key := Id} | _]} -> true;
        _ -> false
    end.

%% The precise "this write is indexed" signal: its outbox entry was
%% acked (nearest-neighbor hits alone cannot distinguish versions).
drained(#{docdb := DbBin}) ->
    [] =:= barrel_docdb:outbox_fold(
        DbBin, <<"embed">>, fun(E, Acc) -> {ok, [E | Acc]} end, []).

embed_calls() ->
    meck:num_calls(barrel_embed, embed, '_')
        + meck:num_calls(barrel_embed, embed_batch, '_').

cursor(#{docdb := DbBin}) ->
    {ok, _, Last} = barrel_docdb:get_changes(DbBin, first),
    Last.

%%====================================================================
%% Cases
%%====================================================================

backfill_stored_vectors_no_embed_calls(Config) ->
    {ok, Db} = open_record(rtl_stored, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"alpha doc">>}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"b">>,
                                   <<"title">> => <<"beta doc">>}),
    ok = wait_until(fun() -> drained(Db) end),
    ?assert(hit(Db, <<"alpha doc">>, <<"a">>)),
    ?assert(hit(Db, <<"beta doc">>, <<"b">>)),
    Calls0 = embed_calls(),
    {ok, Branch} = barrel:branch(Db, rtl_stored_b,
                                 branch_opts(rtl_stored_b, Config)),
    try
        %% zero embedder calls: everything came from stored vectors
        ?assertEqual(Calls0, embed_calls()),
        ?assert(hit(Branch, <<"alpha doc">>, <<"a">>)),
        %% BM25 backfilled too, so hybrid search answers on the branch
        {ok, [_ | _]} = barrel:search_bm25(Branch, <<"beta">>,
                                           #{k => 2})
    after
        ok = barrel:delete(Branch)
    end,
    ok = barrel:close(Db).

backfill_byo_client_vectors(Config) ->
    {ok, Db} = open_record(rtl_byo, Config),
    V = [0.1, 0.2, 0.3],
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"note">> => <<"no policy field">>,
                                   <<"_embedding">> => V}),
    ok = wait_until(fun() ->
        case barrel:search_vector(Db, V, #{k => 1}) of
            {ok, [#{key := <<"a">>}]} -> true;
            _ -> false
        end
    end),
    Calls0 = embed_calls(),
    {ok, Branch} = barrel:branch(Db, rtl_byo_b,
                                 branch_opts(rtl_byo_b, Config)),
    try
        ?assertEqual(Calls0, embed_calls()),
        {ok, [#{key := <<"a">>}]} = barrel:search_vector(Branch, V,
                                                         #{k => 1})
    after
        ok = barrel:delete(Branch)
    end,
    ok = barrel:close(Db).

backfill_pitr_reembeds(Config) ->
    {ok, Db} = open_record(rtl_pitr, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"version one">>}),
    ok = wait_until(fun() -> drained(Db) end),
    T = cursor(Db),
    {ok, #{<<"_rev">> := R}} = barrel:get_doc(Db, <<"a">>),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"version two">>,
                                   <<"_rev">> => R}),
    ok = wait_until(fun() -> drained(Db) end),
    Calls0 = embed_calls(),
    {ok, Branch} = barrel:branch(
        Db, rtl_pitr_b, (branch_opts(rtl_pitr_b, Config))#{at => T}),
    try
        %% the rewound doc lost its embedding column: the backfill
        %% re-embedded its restored text, exactly once
        ?assertEqual(Calls0 + 1, embed_calls()),
        ?assert(hit(Branch, <<"version one">>, <<"a">>)),
        {ok, #{<<"title">> := <<"version one">>}} =
            barrel:get_doc(Branch, <<"a">>)
    after
        ok = barrel:delete(Branch)
    end,
    ok = barrel:close(Db).

backfill_opt_none_and_report(Config) ->
    {ok, Db} = open_record(rtl_none, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"lazy doc">>}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"skipme">>,
                                   <<"other">> => <<"field">>}),
    ok = wait_until(fun() -> drained(Db) end),
    {ok, #{docdb := BranchBin, vstore := VStore, embedding := Policy,
           embed := Embed, dimensions := Dim} = Branch} =
        barrel:branch(Db, rtl_none_b,
                      (branch_opts(rtl_none_b, Config))
                          #{backfill => none}),
    try
        %% nothing indexed without the backfill
        {ok, []} = barrel:search_vector(Branch,
                                        mock_vec(<<"lazy doc">>),
                                        #{k => 1}),
        %% the exported run reports what it did
        {ok, Report} = barrel_record_backfill:run(#{
            db => BranchBin, vstore => VStore, policy => Policy,
            embed => Embed, dimensions => Dim}),
        ?assertMatch(#{indexed := 1, embedded := 0, failed := 0},
                     Report),
        ?assert(maps:get(skipped, Report) >= 1),
        ?assert(hit(Branch, <<"lazy doc">>, <<"a">>))
    after
        ok = barrel:delete(Branch)
    end,
    ok = barrel:close(Db).

backfill_inherits_pending_outbox(Config) ->
    {ok, Db} = open_record(rtl_poison, Config),
    %% the embedder rejects this text: the outbox entry stays pending
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"p">>,
                                   <<"title">> => <<"poison pill">>}),
    timer:sleep(300),
    Pending = fun(DbBin) ->
        lists:reverse(barrel_docdb:outbox_fold(
            DbBin, <<"embed">>,
            fun(E, Acc) -> {ok, [E | Acc]} end, []))
    end,
    ?assertMatch([_], Pending(<<"rtl_poison">>)),
    {ok, #{docdb := BranchBin} = Branch} =
        barrel:branch(Db, rtl_poison_b,
                      branch_opts(rtl_poison_b, Config)),
    try
        %% the fork copied the pending entry; the backfill could not
        %% embed it either and says so
        ?assertMatch([_], Pending(BranchBin)),
        {ok, Report} = barrel_record_backfill:run(#{
            db => BranchBin,
            vstore => maps:get(vstore, Branch),
            policy => maps:get(embedding, Branch),
            embed => maps:get(embed, Branch),
            dimensions => maps:get(dimensions, Branch)}),
        ?assertMatch(#{failed := 1}, Report)
    after
        ok = barrel:delete(Branch)
    end,
    ok = barrel:close(Db).

branch_write_isolation(Config) ->
    {ok, Db} = open_record(rtl_iso, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"parent doc">>}),
    ok = wait_until(fun() -> drained(Db) end),
    {ok, Branch} = barrel:branch(Db, rtl_iso_b,
                                 branch_opts(rtl_iso_b, Config)),
    try
        %% a branch write embeds through the BRANCH indexer only
        {ok, _} = barrel:put_doc(Branch, #{<<"id">> => <<"b">>,
                                           <<"title">> => <<"branch doc">>}),
        ok = wait_until(fun() -> drained(Branch) end),
        ?assert(hit(Branch, <<"branch doc">>, <<"b">>)),
        %% nearest-neighbor search always answers; the branch doc must
        %% not be in the PARENT's store
        {ok, ParentHits} = barrel:search_vector(
            Db, mock_vec(<<"branch doc">>), #{k => 5}),
        ?assertNot(lists:any(fun(#{key := K}) -> K =:= <<"b">> end,
                             ParentHits))
    after
        ok = barrel:delete(Branch)
    end,
    ok = barrel:close(Db).

merge_reindexes_parent(Config) ->
    {ok, Db} = open_record(rtl_merge, Config),
    {ok, Branch} = barrel:branch(Db, rtl_merge_b,
                                 branch_opts(rtl_merge_b, Config)),
    try
        {ok, _} = barrel:put_doc(Branch, #{<<"id">> => <<"m">>,
                                           <<"title">> => <<"merged doc">>}),
        ok = wait_until(fun() -> drained(Branch) end),
        {ok, #{docs_written := 1}} = barrel:merge(Branch),
        %% the merge applied through put_version, which tags the
        %% parent's embed outbox: its indexer picks the doc up
        ok = wait_until(fun() -> drained(Db) end),
        ?assert(hit(Db, <<"merged doc">>, <<"m">>))
    after
        ok = barrel:delete(Branch)
    end,
    ok = barrel:close(Db).
