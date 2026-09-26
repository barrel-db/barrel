%%%-------------------------------------------------------------------
%%% @doc Spike S1: cost of remote fanout. Starts a barrel_server in this
%%% VM, loads one corpus application per database, registers every
%%% database as a remote context reached through a delay proxy, and
%%% times federated queries for each members x RTT cell.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_bench).

-export([main/1]).

-define(QUERIES,
        [{ordered, <<"SELECT id, path, lines FROM c WHERE lines > 100 "
                     "ORDER BY lines DESC LIMIT 20">>},
         {grouped, <<"SELECT id, path FROM c LIMIT 20">>}]).

main(Args) ->
    Opts = opts(Args),
    #{corpus := Corpus, data_dir := DataDir, members := MembersList,
      rtts := Rtts, iterations := Iter, out := Out} = Opts,
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, DataDir),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Port = port(),
    MaxMembers = lists:max(MembersList),
    Apps = load(Corpus, MaxMembers, Port),
    io:format("loaded ~p apps: ~p~n", [length(Apps), [A || {A, _} <- Apps]]),
    Results = [cell(Port, Apps, M, Rtt, Name, Q, Iter)
               || M <- MembersList, Rtt <- Rtts, {Name, Q} <- ?QUERIES],
    print(Results),
    write(Out, Results),
    ok.

opts(Args) ->
    Env = fun(K, D) -> case os:getenv(K) of false -> D; V -> V end end,
    Ints = fun(S) -> [list_to_integer(X) || X <- string:tokens(S, ",")] end,
    Corpus = case Args of
        [C | _] -> C;
        [] -> Env("CORPUS", "")
    end,
    Corpus =/= "" orelse erlang:error(missing_corpus),
    #{corpus => Corpus,
      data_dir => Env("DATA_DIR", "/tmp/barrel_ctx_bench"),
      members => Ints(Env("MEMBERS", "1,3,8")),
      rtts => Ints(Env("RTTS", "0,20,100")),
      iterations => list_to_integer(Env("ITER", "30")),
      out => Env("OUT", "")}.

%% The largest applications first, one database each.
load(Corpus, N, Port) ->
    {ok, Bin} = file:read_file(Corpus),
    Docs = [json:decode(L) || L <- binary:split(Bin, <<"\n">>, [global]),
                              L =/= <<>>],
    ByApp = lists:foldl(
        fun(#{<<"app">> := App} = D, Acc) ->
            Doc = #{<<"id">> => maps:get(<<"id">>, D),
                    <<"app">> => App,
                    <<"path">> => maps:get(<<"path">>, D),
                    <<"lines">> => count_lines(maps:get(<<"body">>, D)),
                    <<"moduledoc">> => maps:get(<<"moduledoc">>, D, null)},
            maps:update_with(App, fun(L) -> [Doc | L] end, [Doc], Acc)
        end, #{}, Docs),
    Sorted = lists:sublist(
        lists:reverse(lists:keysort(2, [{A, length(L)}
                                        || {A, L} <- maps:to_list(ByApp)])),
        N),
    [begin
         Db = <<"bench_", App/binary>>,
         {ok, _} = barrel_server_dbs:ensure(Db),
         ok = barrel_dbs:pin(Db),
         Chunks = chunk(maps:get(App, ByApp), 200),
         [{201, _} = post(Port, <<"/db/", Db/binary, "/_bulk_docs">>,
                          #{<<"docs">> => C}) || C <- Chunks],
         {App, Db}
     end || {App, _Count} <- Sorted].

count_lines(Body) when is_binary(Body) ->
    length(binary:matches(Body, <<"\n">>));
count_lines(_) ->
    0.

chunk([], _N) -> [];
chunk(L, N) when length(L) =< N -> [L];
chunk(L, N) ->
    {H, T} = lists:split(N, L),
    [H | chunk(T, N)].

cell(Port, Apps, Members, Rtt, Name, Query, Iter) ->
    Proxy = barrel_ctx_delay_proxy:start(Port, {delay, Rtt div 2}),
    Endpoint = endpoint(barrel_ctx_delay_proxy:port(Proxy)),
    Ctxs = [register_ctx(Endpoint, Db, Rtt, I)
            || {I, {_App, Db}} <- lists:zip(lists:seq(1, Members),
                                             lists:sublist(Apps, Members))],
    Req = #{query => Query, contexts => Ctxs, deadline_ms => 20000,
            per_context_timeout_ms => 15000, max_parallel => 8},
    _ = barrel_ctx:query(Req),
    {Runtime0, _} = erlang:statistics(runtime),
    Samples = [sample(Req) || _ <- lists:seq(1, Iter)],
    {Runtime1, _} = erlang:statistics(runtime),
    [ok = barrel_ctx_catalog:unregister(C) || C <- Ctxs],
    barrel_ctx_delay_proxy:stop(Proxy),
    Lat = lists:sort([L || {L, _, _, _} <- Samples]),
    #{members => Members, rtt_ms => Rtt, query => Name, iterations => Iter,
      p50_ms => pct(Lat, 50), p95_ms => pct(Lat, 95),
      rows => avg([R || {_, R, _, _} <- Samples]),
      bytes => avg([B || {_, _, B, _} <- Samples]),
      ok_ratio => avg([O || {_, _, _, O} <- Samples]),
      vm_cpu_ms_per_query => (Runtime1 - Runtime0) / max(1, Iter)}.

sample(Req) ->
    T0 = erlang:monotonic_time(microsecond),
    {ok, R} = barrel_ctx:query(Req),
    Us = erlang:monotonic_time(microsecond) - T0,
    #{sources := Sources, execution := Exec} = R,
    Rows = case R of
        #{rows := Rs} -> length(Rs);
        #{groups := Gs} -> lists:sum([length(Rs) || #{rows := Rs} <- Gs])
    end,
    Bytes = lists:sum([maps:get(bytes, S, 0) || S <- Sources]),
    {Us / 1000, Rows, Bytes, case Exec of succeeded -> 1; _ -> 0 end}.

register_ctx(Endpoint, Db, Rtt, I) ->
    {ok, #{<<"id">> := Id}} = barrel_ctx:register(
        #{<<"name">> => iolist_to_binary(["bench/", integer_to_list(Rtt), "/",
                                          integer_to_list(I)]),
          <<"locations">> => [#{<<"kind">> => <<"remote">>,
                                <<"endpoint">> => Endpoint,
                                <<"db">> => Db}]}),
    Id.

pct([], _P) -> 0;
pct(Sorted, P) ->
    N = length(Sorted),
    lists:nth(max(1, min(N, round(P / 100 * N))), Sorted).

avg([]) -> 0;
avg(L) -> lists:sum(L) / length(L).

print(Results) ->
    io:format("~nmembers\trtt_ms\tquery\tp50_ms\tp95_ms\trows\tbytes\t"
              "ok_ratio\tvm_cpu_ms/q~n"),
    [io:format("~b\t~b\t~s\t~.1f\t~.1f\t~.1f\t~.1f\t~.2f\t~.2f~n",
               [M, Rtt, Q, P50 * 1.0, P95 * 1.0, Rows * 1.0, Bytes * 1.0,
                Ok * 1.0, Cpu * 1.0])
     || #{members := M, rtt_ms := Rtt, query := Q, p50_ms := P50,
          p95_ms := P95, rows := Rows, bytes := Bytes, ok_ratio := Ok,
          vm_cpu_ms_per_query := Cpu} <- Results],
    ok.

write("", _Results) ->
    ok;
write(Path, Results) ->
    ok = file:write_file(Path, json:encode(Results)),
    io:format("~nwrote ~s~n", [Path]).

port() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    maps:get(h1, maps:map(fun(_P, [Port | _]) -> Port; (_P, Port) -> Port end,
                          livery:which_listeners(Pid))).

endpoint(Port) ->
    <<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>.

post(Port, Path, Map) ->
    Url = <<(endpoint(Port))/binary, Path/binary>>,
    {ok, S, _H, B} = hackney:request(post, Url,
                                     [{<<"content-type">>,
                                       <<"application/json">>}],
                                     json:encode(Map), [with_body]),
    {S, B}.
