%%%-------------------------------------------------------------------
%%% @doc Working sets (B13), retrieved-set slices (B16) and offline
%%% coverage over resolved members (B17): attach opens nothing, budgets
%%% refuse before writing, slices answer like their source restricted to
%%% their ids (local and remote), provenance survives a restart.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_ws_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([attach_opens_nothing/1,
         attach_rules/1,
         snapshot_over_budget_writes_nothing/1,
         slice_plain_local/1,
         slice_record_local/1,
         slice_remote/1,
         remote_limits/1,
         slice_over_budget_writes_nothing/1,
         slice_frozen/1,
         provenance_survives_restart/1,
         store_follows_ctx_dir/1,
         offline_coverage/1]).

-define(APPS, [<<"tools">>, <<"sasl">>, <<"eunit">>]).
-define(WS_STORE, <<"_barrel_worksets">>).
-define(VQ, ["cover analysis of modules", "release handling upgrade",
             "unit testing framework", "system alarms"]).
-define(BQ, ["cover", "release", "test", "module", "server"]).

all() ->
    [attach_opens_nothing, attach_rules, snapshot_over_budget_writes_nothing,
     slice_plain_local, slice_record_local, slice_remote, remote_limits,
     slice_over_budget_writes_nothing, slice_frozen,
     provenance_survives_restart, store_follows_ctx_dir, offline_coverage].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    %% suites share the VM: never reuse a store another suite left open
    _ = barrel_docdb:close_db(?WS_STORE),
    Priv = ?config(priv_dir, Config),
    ok = application:set_env(barrel, ctx_dir, filename:join(Priv, "ctx")),
    [{docs, barrel_ctx_test_corpus:docs(?APPS)} | Config].

end_per_suite(_Config) ->
    _ = barrel_docdb:close_db(?WS_STORE),
    application:unset_env(barrel, ctx_dir),
    ok.

init_per_testcase(TC, Config) ->
    ok = barrel_ctx_test_corpus:mock_embed(),
    Dir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    [{dir, Dir}, {src, <<"src_", (atom_to_binary(TC))/binary>>} | Config].

end_per_testcase(_TC, Config) ->
    [ok = barrel_ctx_ws:delete(Ws) || Ws <- barrel_ctx_ws:list()],
    [ok = barrel_ctx_export:remove_import(N)
     || N <- barrel_ctx_export:list_imports()],
    _ = barrel_dbs:destroy(?config(src, Config)),
    ok = barrel_ctx_test_corpus:unmock_embed(),
    ok.

%%====================================================================
%% Working sets
%%====================================================================

attach_opens_nothing(Config) ->
    {ok, Ws} = barrel_ctx_ws:create(#{owner => <<"session:t">>}),
    {Src, _} = seed_plain(Config),
    Snap = export_import(Config, Src),
    Open0 = barrel_dbs:list(),
    Dbs0 = lists:sort(barrel_docdb:list_dbs()),
    {ok, _} = barrel_ctx_ws:attach(Ws, #{context => <<"ctx_remote">>,
                                         mode => remote,
                                         location => #{endpoint =>
                                                           <<"http://h">>,
                                                       db => <<"d">>}}),
    {ok, _} = barrel_ctx_ws:attach(Ws, #{context => <<"ctx_snap">>,
                                         mode => snapshot, local_db => Snap,
                                         generation => 1}),
    {ok, _} = barrel_ctx_ws:attach(Ws, #{context => <<"ctx_local">>,
                                         mode => local, local_db => Src}),
    {ok, Members} = barrel_ctx_ws:members(Ws),
    ?assertEqual(Open0, barrel_dbs:list()),
    ?assertEqual(Dbs0, lists:sort(barrel_docdb:list_dbs())),
    [#{mode := remote, available := false, coverage := live},
     #{mode := snapshot, available := true, local_db := Snap,
       coverage := complete_generation,
       version := #{kind := generation, generation := 1},
       open_opts := #{read_only := true}},
     #{mode := local, coverage := live}] = Members,
    %% opening is explicit, per member
    {ok, SnapDb} = barrel_ctx_ws:open_member(lists:nth(2, Members)),
    ?assertEqual(true, maps:get(read_only, SnapDb)),
    ?assertEqual({error, remote_member},
                 barrel_ctx_ws:open_member(hd(Members))),
    %% detach is logical too, and the import stays
    {ok, #{members := [_, _]}} = barrel_ctx_ws:detach(Ws, <<"ctx_snap">>),
    ?assertEqual([Snap], barrel_ctx_export:list_imports()).

attach_rules(_Config) ->
    {ok, Ws} = barrel_ctx_ws:create(#{budget => #{contexts => 2}}),
    Remote = fun(C) -> #{context => C, mode => remote,
                         location => #{endpoint => <<"http://h">>,
                                       db => <<"d">>}} end,
    {ok, _} = barrel_ctx_ws:attach(Ws, Remote(<<"c1">>)),
    ?assertEqual({error, {already_attached, <<"c1">>}},
                 barrel_ctx_ws:attach(Ws, Remote(<<"c1">>))),
    {ok, _} = barrel_ctx_ws:attach(Ws, Remote(<<"c2">>)),
    ?assertEqual({error, {over_budget, contexts, 2}},
                 barrel_ctx_ws:attach(Ws, Remote(<<"c3">>))),
    ?assertMatch({error, {invalid_member, _}},
                 barrel_ctx_ws:attach(Ws, #{context => <<"c4">>,
                                            mode => snapshot})),
    ?assertEqual({error, {not_attached, <<"nope">>}},
                 barrel_ctx_ws:detach(Ws, <<"nope">>)),
    {ok, #{budget := #{contexts := 2, bytes := 1073741824},
           owner := null}} = barrel_ctx_ws:get(Ws).

snapshot_over_budget_writes_nothing(Config) ->
    {Src, _} = seed_plain(Config),
    Dest = filename:join(?config(dir, Config), "export"),
    {ok, _} = barrel_ctx_export:export(Src, Dest, #{}),
    {ok, Ws} = barrel_ctx_ws:create(#{budget => #{bytes => 1000}}),
    {error, {over_budget, bytes, #{available := 1000}}} =
        barrel_ctx_ws:import_snapshot(Ws, Dest, #{name => <<"snap_big">>}),
    ?assertEqual({error, not_imported},
                 barrel_ctx_export:import_info(<<"snap_big">>)),
    Entries = case file:list_dir(barrel_ctx_export:imports_dir()) of
        {ok, L} -> L;
        {error, enoent} -> []
    end,
    ?assertEqual([], [E || E <- Entries, string:find(E, "snap_big") =/= nomatch]),
    %% within budget it imports unopened and counts the bytes
    {ok, Ws2} = barrel_ctx_ws:create(#{}),
    {ok, #{usage := #{bytes := Used}, members := [#{mode := snapshot}]}} =
        barrel_ctx_ws:import_snapshot(Ws2, Dest, #{name => <<"snap_ok">>}),
    ?assert(Used > 0),
    ?assertNot(lists:member(<<"snap_ok">>, barrel_dbs:list())).

%%====================================================================
%% Slices
%%====================================================================

slice_plain_local(Config) ->
    {Src, _} = seed_plain(Config),
    slice_matches_source(Config, Src, {local, Src}, #{}).

slice_record_local(Config) ->
    {Src, _} = seed_record(Config),
    slice_matches_source(Config, Src, {local, Src}, #{}).

slice_remote(Config) ->
    {Src, _} = seed_record(Config),
    {ok, SrcDb} = barrel_dbs:ensure(Src),
    {Port, Stop} = stub_server(SrcDb),
    try
        Loc = #{endpoint => iolist_to_binary(
                              ["http://127.0.0.1:",
                               integer_to_list(Port)]),
                db => Src},
        slice_matches_source(Config, Src, {remote, Loc},
                             #{embedding => #{fields => [<<"moduledoc">>,
                                                         <<"path">>]}})
    after
        Stop()
    end.

%% The response cap and the deadline both fail the fetch before any
%% write; credentials come from node configuration.
remote_limits(Config) ->
    {Src, _} = seed_record(Config),
    {ok, SrcDb} = barrel_dbs:ensure(Src),
    {Port, Stop} = stub_server(SrcDb),
    {ok, Silent} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, SilentPort} = inet:port(Silent),
    Url = fun(P) -> iolist_to_binary(["http://127.0.0.1:",
                                      integer_to_list(P)]) end,
    {ok, Ws} = barrel_ctx_ws:create(#{}),
    try
        {error, {over_budget, max_bytes, #{limit := 512}}} =
            barrel_ctx_ws:materialize(Ws, #{
                context => <<"ctx_src">>,
                source => {remote, #{endpoint => Url(Port), db => Src}},
                ids => slice_ids(Config), max_bytes => 512}),
        {error, {source_unavailable, timeout}} =
            barrel_ctx_ws:materialize(Ws, #{
                context => <<"ctx_src">>,
                source => {remote, #{endpoint => Url(SilentPort), db => Src}},
                ids => slice_ids(Config), timeout => 300}),
        {ok, #{members := []}} = barrel_ctx_ws:get(Ws),
        ok = application:set_env(barrel, ctx_credentials,
                                 #{<<"ref1">> => <<"tok1">>,
                                   Url(Port) => <<"tok2">>}),
        ?assertEqual(<<"tok1">>,
                     barrel_ctx_remote:credential(
                       #{endpoint => Url(Port), db => Src,
                         credential_ref => <<"ref1">>}, #{})),
        ?assertEqual(<<"tok2">>,
                     barrel_ctx_remote:credential(
                       #{endpoint => Url(Port), db => Src}, #{}))
    after
        application:unset_env(barrel, ctx_credentials),
        gen_tcp:close(Silent),
        Stop()
    end.

slice_over_budget_writes_nothing(Config) ->
    {Src, _} = seed_plain(Config),
    Ids = slice_ids(Config),
    {ok, Ws} = barrel_ctx_ws:create(#{budget => #{bytes => 2000}}),
    {error, {over_budget, bytes, _}} =
        barrel_ctx_ws:materialize(Ws, #{context => <<"ctx_src">>,
                                        source => {local, Src}, ids => Ids}),
    {ok, Ws2} = barrel_ctx_ws:create(#{}),
    {error, {over_budget, transfer_bytes, #{limit := 100}}} =
        barrel_ctx_ws:materialize(Ws2, #{context => <<"ctx_src">>,
                                         source => {local, Src}, ids => Ids,
                                         max_bytes => 100}),
    ?assert(lists:member(file:list_dir(barrel_ctx_slice:slices_dir()),
                         [{ok, []}, {error, enoent}])),
    {ok, #{members := [], usage := #{bytes := 0}}} = barrel_ctx_ws:get(Ws),
    {ok, #{members := []}} = barrel_ctx_ws:get(Ws2).

slice_frozen(Config) ->
    {Src, _} = seed_plain(Config),
    {ok, Ws} = barrel_ctx_ws:create(#{}),
    Req = #{context => <<"ctx_src">>, source => {local, Src},
            ids => slice_ids(Config) ++ [<<"no_such_doc">>]},
    {ok, #{slices := [#{local_db := Name, docs := 20,
                        derived := #{<<"missing">> := [<<"no_such_doc">>]}}]}} =
        barrel_ctx_ws:materialize(Ws, Req),
    ?assertEqual({error, {already_attached, <<"ctx_src">>}},
                 barrel_ctx_ws:materialize(Ws, Req)),
    {ok, [M]} = barrel_ctx_ws:members(Ws),
    {ok, Db} = barrel_ctx_ws:open_member(M),
    ?assertEqual({error, read_only},
                 barrel:put_doc(Db, #{<<"id">> => <<"x">>})),
    %% detaching drops the slice it owns
    {ok, _} = barrel_ctx_ws:detach(Ws, <<"ctx_src">>),
    ?assertEqual({error, not_materialized}, barrel_ctx_slice:open_opts(Name)).

provenance_survives_restart(Config) ->
    {Src, _} = seed_plain(Config),
    {ok, Ws} = barrel_ctx_ws:create(#{}),
    {ok, #{slices := [#{local_db := Name, derived := Derived}]}} =
        barrel_ctx_ws:materialize(Ws, #{context => <<"ctx_src">>,
                                        source => {local, Src},
                                        ids => slice_ids(Config)}),
    {ok, SrcIid} = barrel_docdb:db_instance_id(Src),
    #{<<"observed">> := #{<<"instance_id">> := SrcIid,
                          <<"last_seq">> := Seq},
      <<"ids_hash">> := <<"sha256:", _/binary>>} = Derived,
    ?assert(is_binary(Seq)),
    ok = application:stop(barrel),
    ok = application:stop(barrel_vectordb),
    ok = application:stop(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel),
    {ok, #{members := [#{derived := Derived}]}} = barrel_ctx_ws:get(Ws),
    ?assertEqual({ok, Derived}, barrel_ctx_slice:provenance(Name)),
    {ok, [M]} = barrel_ctx_ws:members(Ws),
    {ok, _} = barrel_ctx_ws:open_member(M),
    ?assertEqual({ok, Derived},
                 barrel_docdb:get_local_doc(Name, <<"_ctx/slice">>)).

%% The store follows ctx_dir: one opened under another dir is not reused.
store_follows_ctx_dir(Config) ->
    {ok, Dir} = application:get_env(barrel, ctx_dir),
    {ok, A} = barrel_ctx_ws:create(#{}),
    Other = filename:join(?config(dir, Config), "other_ctx"),
    ok = application:set_env(barrel, ctx_dir, Other),
    try
        ?assertEqual([], barrel_ctx_ws:list()),
        {ok, B} = barrel_ctx_ws:create(#{}),
        ?assertEqual([B], barrel_ctx_ws:list()),
        ok = barrel_ctx_ws:delete(B)
    after
        ok = application:set_env(barrel, ctx_dir, Dir)
    end,
    ?assertEqual([A], barrel_ctx_ws:list()).

offline_coverage(Config) ->
    {Src, _} = seed_plain(Config),
    Snap = export_import(Config, Src),
    {ok, Ws} = barrel_ctx_ws:create(#{}),
    {ok, _} = barrel_ctx_ws:attach(Ws, #{context => <<"ctx_snap">>,
                                         mode => snapshot, local_db => Snap,
                                         generation => 1}),
    {ok, _} = barrel_ctx_ws:materialize(Ws, #{context => <<"ctx_slice">>,
                                              source => {local, Src},
                                              ids => slice_ids(Config)}),
    {ok, _} = barrel_ctx_ws:attach(Ws, #{context => <<"ctx_remote">>,
                                         mode => remote,
                                         location => #{endpoint =>
                                                           <<"http://h">>,
                                                       db => <<"d">>}}),
    {ok, Members} = barrel_ctx_ws:members(Ws),
    Sources = [barrel_ctx_coverage:member(M, #{offline => true,
                                               available =>
                                                   maps:get(available, M)})
               || M <- Members],
    [#{status := ok, membership := complete_generation},
     #{status := ok, membership := retrieved_set},
     #{status := skipped_offline}] = Sources,
    #{execution := partial,
      coverage := #{answered := 2, skipped := 1,
                    missing := [<<"ctx_remote">>]}} =
        barrel_ctx_coverage:summarize(Sources, explicit).

%%====================================================================
%% Helpers
%%====================================================================

slice_matches_source(Config, Src, Source, Extra) ->
    Ids = slice_ids(Config),
    {ok, Ws} = barrel_ctx_ws:create(#{}),
    {ok, #{slices := [#{local_db := Name, docs := N, status := complete}],
           usage := #{bytes := Used}}} =
        barrel_ctx_ws:materialize(Ws, maps:merge(#{context => <<"ctx_src">>,
                                                   source => Source,
                                                   ids => Ids}, Extra)),
    ?assertEqual(length(Ids), N),
    ?assert(Used > 0),
    {ok, [M]} = barrel_ctx_ws:members(Ws),
    {ok, Slice} = barrel_ctx_ws:open_member(M),
    {ok, SrcDb} = barrel_dbs:ensure(Src),
    Total = length(?config(docs, Config)),
    Set = sets:from_list(Ids),
    In = fun(Rows) -> [R || {Id, _} = R <- Rows, sets:is_element(Id, Set)] end,
    %% find: the source's rows restricted to the slice ids
    [?assertEqual({App, [I || I <- find_ids(SrcDb, App),
                              sets:is_element(I, Set)]},
                  {App, find_ids(Slice, App)}) || App <- ?APPS],
    %% vector: same vectors, so the source's full ranking restricted to
    %% the slice gives the slice's top k. Scores agree to float32
    %% precision: the source's in-memory index holds the vectors as
    %% embedded, the slice the stored float32 values.
    [same_ranking(Q, lists:sublist(In(top(SrcDb, "vector_top_k", Q, Total)),
                                   10),
                  top(Slice, "vector_top_k", Q, 10)) || Q <- ?VQ],
    %% bm25: same matching documents; scores are corpus-relative (IDF)
    [?assertEqual({Q, lists:sort(ids(In(top(SrcDb, "bm25_top_k", Q,
                                            Total))))},
                  {Q, lists:sort(ids(top(Slice, "bm25_top_k", Q, Total)))})
     || Q <- ?BQ],
    Name.

same_ranking(Q, Expected, Got) ->
    ?assertEqual({Q, ids(Expected)}, {Q, ids(Got)}),
    [?assert(abs(S1 - S2) < 1.0e-6)
     || {{_, S1}, {_, S2}} <- lists:zip(Expected, Got)],
    ok.

slice_ids(Config) ->
    Docs = ?config(docs, Config),
    %% every other doc, 20 of them
    Every = [Id || {I, #{<<"id">> := Id}} <- lists:zip(
                                                 lists:seq(1, length(Docs)),
                                                 Docs), I rem 2 =:= 0],
    lists:sublist(Every, 20).

export_import(Config, Src) ->
    Dest = filename:join(?config(dir, Config), "export"),
    {ok, _} = barrel_ctx_export:export(Src, Dest, #{}),
    {ok, #{name := Name}} = barrel_ctx_export:import(Dest,
                                                     #{name => <<"snap_t">>,
                                                       open => false}),
    Name.

seed_plain(Config) ->
    Src = ?config(src, Config),
    Docs = ?config(docs, Config),
    Dir = ?config(dir, Config),
    {ok, Db} = barrel_dbs:ensure(Src, #{
        docdb => #{data_dir => filename:join(Dir, "docdb")},
        vectordb => #{dimension => barrel_ctx_test_corpus:dim(),
                      db_path => filename:join(Dir, "vec"),
                      bm25_backend => memory}}),
    [{ok, _} = R || R <- barrel:put_docs(Db, Docs)],
    Batch = [{Id, T, #{}, barrel_ctx_test_corpus:vec(T)}
             || #{<<"id">> := Id} = D <- Docs,
                T <- [barrel_ctx_test_corpus:text(D)]],
    {ok, _} = barrel:vector_add_batch(Db, Batch),
    {Src, Db}.

seed_record(Config) ->
    Src = ?config(src, Config),
    Docs = ?config(docs, Config),
    Dir = ?config(dir, Config),
    {ok, Db} = barrel_dbs:ensure(Src, #{
        embedding => #{fields => [<<"moduledoc">>, <<"path">>], mode => sync},
        docdb => #{data_dir => filename:join(Dir, "docdb")},
        vectordb => #{dimension => barrel_ctx_test_corpus:dim(),
                      db_path => filename:join(Dir, "vec")}}),
    [{ok, _} = R || R <- barrel:put_docs(Db, Docs)],
    {Src, Db}.

find_ids(Db, App) ->
    {ok, Rows, _} = barrel:find(Db, #{where => [{path, [<<"app">>], App}]}),
    lists:sort([maps:get(<<"id">>, R) || R <- Rows]).

top(Db, Fn, Q, K) ->
    Bql = io_lib:format("SELECT * FROM ~s('~s', k => ~b) AS h", [Fn, Q, K]),
    {ok, Rows, _} = barrel:query(Db, lists:flatten(Bql)),
    lists:sort(fun({I1, S1}, {I2, S2}) -> {-S1, I1} =< {-S2, I2} end,
               [{maps:get(<<"id">>, R), maps:get(<<"_score">>, R)}
                || R <- Rows]).

ids(Rows) -> [Id || {Id, _} <- Rows].

%% A minimal HTTP/1.1 server answering _bulk_get and db info from a
%% local database, with the wire shape barrel_server uses.
stub_server(Db) ->
    {ok, L} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true},
                                 {packet, raw}]),
    {ok, Port} = inet:port(L),
    Pid = spawn(fun() -> accept_loop(L, Db) end),
    ok = gen_tcp:controlling_process(L, Pid),
    {Port, fun() -> exit(Pid, kill), gen_tcp:close(L) end}.

accept_loop(L, Db) ->
    case gen_tcp:accept(L) of
        {ok, S} ->
            serve(S, Db, <<>>),
            accept_loop(L, Db);
        {error, _} ->
            ok
    end.

serve(S, Db, Buf) ->
    case binary:split(Buf, <<"\r\n\r\n">>) of
        [Head, Rest] ->
            [ReqLine | Hdrs] = binary:split(Head, <<"\r\n">>, [global]),
            [Method, Path | _] = binary:split(ReqLine, <<" ">>, [global]),
            Len = content_length(Hdrs),
            Body = read_body(S, Rest, Len),
            Resp = route(Method, Path, Body, Db),
            ok = gen_tcp:send(S, [<<"HTTP/1.1 200 OK\r\n"
                                    "content-type: application/json\r\n"
                                    "connection: close\r\n"
                                    "content-length: ">>,
                                  integer_to_binary(byte_size(Resp)),
                                  <<"\r\n\r\n">>, Resp]),
            gen_tcp:close(S);
        [_] ->
            {ok, More} = gen_tcp:recv(S, 0, 5000),
            serve(S, Db, <<Buf/binary, More/binary>>)
    end.

content_length(Hdrs) ->
    case [binary_to_integer(string:trim(V))
          || H <- Hdrs,
             [K, V] <- [binary:split(H, <<":">>)],
             string:lowercase(K) =:= <<"content-length">>] of
        [N] -> N;
        [] -> 0
    end.

read_body(_S, Buf, Len) when byte_size(Buf) >= Len ->
    Buf;
read_body(S, Buf, Len) ->
    {ok, More} = gen_tcp:recv(S, 0, 5000),
    read_body(S, <<Buf/binary, More/binary>>, Len).

route(<<"POST">>, Path, Body, Db) ->
    case binary:match(Path, <<"/query">>) of
        nomatch -> bulk_get_route(Body, Db);
        _ -> query_route()
    end;
route(<<"GET">>, _Path, _Body, #{name := Name}) ->
    iolist_to_binary(json:encode(#{<<"name">> => Name})).

%% Only the observation query of a remote slice reaches this route.
query_route() ->
    Meta = #{<<"has_more">> => false, <<"instance_id">> => <<"stub">>,
             <<"last_seq">> => <<"AAAA">>},
    iolist_to_binary([json:encode(#{<<"meta">> => Meta}), $\n]).

bulk_get_route(Body, Db) ->
    #{<<"ids">> := Ids} = Req = json:decode(Body),
    Opts = #{include_embedding => maps:get(<<"include_embedding">>, Req,
                                           false)},
    Results = [wire(R) || R <- barrel:get_docs(Db, Ids, Opts)],
    iolist_to_binary(json:encode(#{<<"results">> => Results})).

wire({ok, #{<<"_embedding">> := #{<<"vector">> := V} = E} = Doc}) ->
    Doc#{<<"_embedding">> =>
             E#{<<"vector">> => base64:encode(barrel_doc:encode_embedding(V)),
                <<"dim">> => length(V)}};
wire({ok, Doc}) ->
    Doc;
wire({error, R}) ->
    #{<<"error">> => iolist_to_binary(io_lib:format("~p", [R]))}.
