%%%-------------------------------------------------------------------
%%% @doc The Erlang side of the `barrel-backup' script. The script starts
%%% a short-lived hidden node with the running node's own distribution
%%% flags (TCP, TLS or QUIC), and main/1 drives the node over erpc: every
%%% backup, restore and prune runs on the node, against its own files.
%%%
%%%   create  Dir (Db... | --all)    Dir/Db/UTC-STAMP per database
%%%   list    Dir [Db]               backups, newest first
%%%   restore Backup Name [--retention Seconds]
%%%   prune   Dir --keep N           keep the N newest per database
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_backup_cli).

-export([main/1]).
%% run on the node, over erpc
-export([create/2, list/2, restore/3, prune/2]).

%% @doc Entry point: `[Node | Command]', from the script.
-spec main([string()]) -> no_return().
main([NodeStr | Command]) ->
    Node = list_to_atom(NodeStr),
    case net_kernel:connect_node(Node) of
        true ->
            halt(run(Node, Command));
        false ->
            fail("cannot reach ~s: is it running, and do its name, cookie and "
                 "distribution settings match this script's?", [Node])
    end;
main(_) ->
    fail(usage(), []).

run(Node, ["create", Dir | Dbs]) when Dbs =/= [] ->
    What = case Dbs of ["--all"] -> all; _ -> [list_to_binary(D) || D <- Dbs] end,
    report(remote(Node, create, [Dir, What]), fun print_created/1);
run(Node, ["list", Dir | Db]) ->
    report(remote(Node, list, [Dir, [list_to_binary(D) || D <- Db]]), fun print_list/1);
run(Node, ["restore", Backup, Name | Rest]) ->
    Opts = case Rest of
        ["--retention", Secs] -> #{retention_period => list_to_integer(Secs)};
        [] -> #{}
    end,
    report(remote(Node, restore, [Backup, list_to_binary(Name), Opts]),
           fun(Path) -> io:format("restored ~s in ~s~n", [Name, Path]) end);
run(Node, ["prune", Dir, "--keep", N]) ->
    report(remote(Node, prune, [Dir, list_to_integer(N)]),
           fun(Removed) -> [io:format("removed ~s~n", [R]) || R <- Removed] end);
run(_Node, _) ->
    io:format(standard_error, "~s~n", [usage()]),
    2.

remote(Node, F, Args) ->
    try erpc:call(Node, ?MODULE, F, Args, infinity)
    catch Class:Reason -> {error, {Class, Reason}}
    end.

report({ok, Result}, Print) -> Print(Result), 0;
report({error, Db, Reason}, _Print) ->
    io:format(standard_error, "~s: ~p~n", [Db, Reason]), 1;
report({error, Reason}, _Print) ->
    io:format(standard_error, "error: ~p~n", [Reason]), 1.

print_created(Done) ->
    [io:format("~s ~s~n", [Db, Dir]) || {Db, Dir} <- Done].

print_list(Rows) ->
    [io:format("~s ~s ~s ~b~n", [Db, Ts, Hlc, Size]) || {Db, Ts, Hlc, Size} <- Rows].

usage() ->
    "usage: barrel-backup create <dir> <db>... | --all\n"
    "       barrel-backup list <dir> [<db>]\n"
    "       barrel-backup restore <backup> <name> [--retention SECONDS]\n"
    "       barrel-backup prune <dir> --keep N".

fail(Fmt, Args) ->
    io:format(standard_error, Fmt ++ "~n", Args),
    halt(1).

%%====================================================================
%% On the node
%%====================================================================

%% @doc Back up the databases into Dir/Db/UTC-STAMP, stopping at
%% the first failure.
-spec create(string(), all | [binary()]) ->
    {ok, [{binary(), binary()}]} | {error, binary(), term()}.
create(Dir, all) ->
    create(Dir, barrel_db_backup:local_dbs());
create(Dir, Dbs) ->
    Stamp = stamp(),
    create(Dir, Stamp, Dbs, []).

create(_Dir, _Stamp, [], Done) ->
    {ok, lists:reverse(Done)};
create(Dir, Stamp, [Db | Rest], Done) ->
    Target = filename:join([Dir, binary_to_list(Db), Stamp]),
    case barrel_docdb:backup_db(Db, Target) of
        {ok, #{dir := D}} -> create(Dir, Stamp, Rest, [{Db, D} | Done]);
        {error, Reason} -> {error, Db, Reason}
    end.

%% @doc The complete backups under Dir (an interrupted one has no
%% manifest and is not listed), newest first.
-spec list(string(), [binary()]) -> {ok, [{binary(), string(), binary(), integer()}]}.
list(Dir, Only) ->
    Rows = [{Db, Ts, maps:get(<<"backup_hlc">>, M),
             lists:sum([S || #{<<"size">> := S} <- maps:get(<<"files">>, M)])}
            || Db <- dbs(Dir, Only), Ts <- stamps(Dir, Db),
               {ok, M} <- [barrel_db_backup:read_manifest(filename:join([Dir, Db, Ts]))]],
    {ok, lists:sort(fun({D1, T1, _, _}, {D2, T2, _, _}) -> {D1, T2} =< {D2, T1} end, Rows)}.

%% @doc Restore a backup, then open it the way the node opens its
%% databases (barrel_server's opener applies its runtime config).
-spec restore(string(), binary(), map()) -> {ok, string()} | {error, term()}.
restore(Backup, Name, Opts) ->
    case barrel_docdb:restore_db(Backup, Name, Opts#{open => false}) of
        {ok, Path} ->
            case application:get_env(barrel_docdb, task_db_opener) of
                {ok, {M, F}} -> _ = M:F(Name);
                undefined -> {ok, _} = barrel_docdb:create_db(Name, maps:without([open], Opts))
            end,
            {ok, Path};
        {error, _} = Err ->
            Err
    end.

%% @doc Keep the N newest complete backups of each database.
-spec prune(string(), pos_integer()) -> {ok, [string()]}.
prune(Dir, Keep) when Keep > 0 ->
    Removed = lists:append(
                [begin
                     Old = lists:nthtail(min(Keep, length(Ts)), Ts),
                     [begin
                          P = filename:join([Dir, Db, T]),
                          ok = file:del_dir_r(P),
                          P
                      end || T <- Old]
                 end || Db <- dbs(Dir, []), Ts <- [stamps(Dir, Db)]]),
    {ok, Removed}.

dbs(Dir, []) ->
    case file:list_dir(Dir) of
        {ok, L} -> lists:sort([D || D <- L, filelib:is_dir(filename:join(Dir, D))]);
        {error, _} -> []
    end;
dbs(_Dir, Only) ->
    [binary_to_list(D) || D <- Only].

%% Complete backups of one database, newest first.
stamps(Dir, Db) ->
    case file:list_dir(filename:join(Dir, Db)) of
        {ok, L} ->
            lists:reverse(lists:sort(
                [T || T <- L, string:find(T, ".partial-") =:= nomatch,
                      filelib:is_regular(filename:join([Dir, Db, T, "MANIFEST.json"]))]));
        {error, _} ->
            []
    end.

stamp() ->
    {{Y, Mo, D}, {H, Mi, S}} = calendar:universal_time(),
    lists:flatten(io_lib:format("~4..0b~2..0b~2..0bT~2..0b~2..0b~2..0bZ",
                                [Y, Mo, D, H, Mi, S])).
