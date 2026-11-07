%%% @doc Delete stub size: bytes a deleted document keeps for good, as
%%% its history grows (writer ids from reopens, versions from old
%%% peers), and the time the retention sweep takes to turn deletes
%%% into stubs.
-module(barrel_bench_stubs).

-export([run/0, run/1]).

-define(DB, <<"barrel_bench_stubs">>).

run() -> run(#{}).

%% Options: docs (per case, default 1000), cases ([{Writers, OldPeers}]).
run(Config) ->
    Docs = maps:get(docs, Config, 1000),
    Cases = maps:get(cases, Config, [{1, 0}, {4, 0}, {16, 0}, {1, 4}, {4, 4}]),
    io:format("~n=== Delete stub size (~p docs per case) ===~n", [Docs]),
    io:format("~-9s ~-9s ~12s ~14s ~10s~n",
              [<<"writers">>, <<"old peers">>, <<"bytes/stub">>, <<"total bytes">>,
               <<"sweep ms">>]),
    Results = [run_case(Docs, W, P) || {W, P} <- Cases],
    io:format("~n"),
    Results.

run_case(Docs, Writers, OldPeers) ->
    _ = barrel_docdb:delete_db(?DB),
    Opts = #{retention_period => 1, retention_check_interval => 3600000},
    {ok, _} = barrel_docdb:create_db(?DB, Opts),
    Ids = [integer_to_binary(I) || I <- lists:seq(1, Docs)],
    [{ok, _} = barrel_docdb:put_doc(?DB, #{<<"id">> => Id, <<"v">> => 0}) || Id <- Ids],
    %% each reopen is a new writer id; edit every doc once per writer
    lists:foreach(
      fun(N) ->
          ok = barrel_docdb:close_db(?DB),
          {ok, _} = barrel_docdb:create_db(?DB, Opts),
          [edit(Id, N) || Id <- Ids]
      end, lists:seq(2, Writers)),
    %% versions from old peers, kept as conflicts then resolved
    [old_peers(Id, OldPeers) || OldPeers > 0, Id <- Ids],
    [delete(Id) || Id <- Ids],
    timer:sleep(1100),
    T0 = erlang:monotonic_time(millisecond),
    {ok, _} = barrel_docdb:sweep_retention(?DB),
    Ms = erlang:monotonic_time(millisecond) - T0,
    {ok, #{stubs := N, bytes := Bytes}} = barrel_docdb:stub_stats(?DB),
    Per = case N of 0 -> 0; _ -> Bytes div N end,
    io:format("~-9w ~-9w ~12w ~14w ~10w~n", [Writers, OldPeers, Per, Bytes, Ms]),
    _ = barrel_docdb:delete_db(?DB),
    #{writers => Writers, old_peers => OldPeers, stubs => N,
      bytes_per_stub => Per, bytes => Bytes, sweep_ms => Ms}.

edit(Id, V) ->
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(?DB, Id),
    {ok, _} = barrel_docdb:put_doc(?DB, #{<<"id">> => Id, <<"_rev">> => R, <<"v">> => V}).

old_peers(Id, K) ->
    lists:foreach(
      fun(P) ->
          Author = <<"oldpeer", (integer_to_binary(P))/binary>>,
          V = barrel_version:new(barrel_hlc:new_hlc(), Author),
          VV = barrel_vv:encode(barrel_vv:bump(barrel_vv:new(), V)),
          {ok, _, _} = barrel_docdb:put_version(?DB, #{<<"id">> => Id, <<"p">> => P},
                                                barrel_version:to_token(V), VV, false)
      end, lists:seq(1, K)),
    {ok, #{<<"_observed">> := Seen}} = barrel_docdb:get_doc(?DB, Id, #{conflicts => true}),
    {ok, _} = barrel_docdb:resolve_conflict(?DB, Id, Seen, {merge, #{<<"v">> => <<"r">>}}).

delete(Id) ->
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(?DB, Id),
    {ok, _} = barrel_docdb:delete_doc(?DB, Id, #{rev => R}).
