%%%-------------------------------------------------------------------
%%% @doc Timeline over REST: branch (now and at a cursor), lineage,
%%% merge, purge, and error mapping. Branches are normal dbs, so the
%%% existing API and the _sync replication wire work on them
%%% unchanged; one case pins that.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_timeline_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_branch_now/1,
    t_branch_at_cursor/1,
    t_branch_isolation/1,
    t_timeline_info/1,
    t_merge/1,
    t_purge/1,
    t_branch_errors/1,
    t_merge_not_a_branch/1,
    t_branch_syncs_over_wire/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_branch_now, t_branch_at_cursor, t_branch_isolation,
     t_timeline_info, t_merge, t_purge, t_branch_errors,
     t_merge_not_a_branch, t_branch_syncs_over_wire].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_list(TC) ++ "_db",
    B = ?config(base, Config),
    {201, _} = req(put, B ++ "/db/" ++ Db, #{}),
    [{db, Db}, {branch, Db ++ "_b"} | Config].

end_per_testcase(_TC, Config) ->
    B = ?config(base, Config),
    _ = req(delete, B ++ "/db/" ++ ?config(branch, Config)
                      ++ "?purge=true", none),
    _ = req(delete, B ++ "/db/" ++ ?config(db, Config)
                      ++ "?purge=true", none),
    ok.

req(Method, Url, none) ->
    {ok, S, _H, Body} = hackney:request(Method, list_to_binary(Url),
                                        [], <<>>, [with_body]),
    {S, decode(Body)};
req(Method, Url, JsonTerm) ->
    {ok, S, _H, Body} = hackney:request(
        Method, list_to_binary(Url),
        [{<<"content-type">>, <<"application/json">>}],
        json:encode(JsonTerm), [with_body]),
    {S, decode(Body)}.

decode(<<>>) -> #{};
decode(Bin) ->
    try json:decode(Bin) catch _:_ -> Bin end.

put_doc(B, Db, Id, Doc) ->
    {201, _} = req(put, B ++ "/db/" ++ Db ++ "/doc/" ++ Id, Doc).

get_doc(B, Db, Id) ->
    req(get, B ++ "/db/" ++ Db ++ "/doc/" ++ Id, none).

changes_cursor(B, Db) ->
    {200, #{<<"last">> := Last}} = req(get, B ++ "/db/" ++ Db
                                            ++ "/changes", none),
    Last.

%%====================================================================
%% Cases
%%====================================================================

t_branch_now(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    put_doc(B, Db, "a", #{<<"v">> => 1}),
    {201, Resp} = req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
                      #{name => list_to_binary(Branch)}),
    ?assertMatch(#{<<"ok">> := true,
                   <<"branch">> := _,
                   <<"parent">> := _,
                   <<"fork_hlc">> := _}, Resp),
    {200, #{<<"v">> := 1}} = get_doc(B, Branch, "a"),
    ok.

t_branch_at_cursor(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    put_doc(B, Db, "a", #{<<"v">> => 1}),
    T = changes_cursor(B, Db),
    {200, #{<<"_rev">> := R}} = get_doc(B, Db, "a"),
    put_doc(B, Db, "a", #{<<"v">> => 2, <<"_rev">> => R}),
    {201, _} = req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
                   #{name => list_to_binary(Branch), at => T}),
    {200, #{<<"v">> := 1}} = get_doc(B, Branch, "a"),
    {200, #{<<"v">> := 2}} = get_doc(B, Db, "a"),
    ok.

t_branch_isolation(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {201, _} = req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
                   #{name => list_to_binary(Branch)}),
    put_doc(B, Branch, "bonly", #{}),
    {404, _} = get_doc(B, Db, "bonly"),
    put_doc(B, Db, "ponly", #{}),
    {404, _} = get_doc(B, Branch, "ponly"),
    ok.

t_timeline_info(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {201, _} = req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
                   #{name => list_to_binary(Branch)}),
    %% the parent lists its open branch, no lineage of its own
    {200, PInfo} = req(get, B ++ "/db/" ++ Db ++ "/_timeline", none),
    ?assertEqual([list_to_binary(Branch)],
                 maps:get(<<"branches">>, PInfo)),
    ?assertNot(maps:is_key(<<"parent">>, PInfo)),
    %% the branch shows parent + fork instant
    {200, BInfo} = req(get, B ++ "/db/" ++ Branch ++ "/_timeline",
                       none),
    ?assertEqual(list_to_binary(Db), maps:get(<<"parent">>, BInfo)),
    ?assert(maps:is_key(<<"fork_hlc">>, BInfo)),
    %% GET /db/:db carries the lineage too, as cursors
    {200, DbInfo} = req(get, B ++ "/db/" ++ Branch, none),
    ?assert(is_binary(maps:get(<<"fork_hlc">>, DbInfo))),
    ok.

t_merge(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {201, _} = req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
                   #{name => list_to_binary(Branch)}),
    put_doc(B, Branch, "feature", #{<<"done">> => true}),
    {200, Report} = req(post, B ++ "/db/" ++ Branch
                             ++ "/_timeline/merge", #{}),
    ?assertMatch(#{<<"docs_written">> := 1,
                   <<"last_merged">> := _}, Report),
    {200, #{<<"done">> := true}} = get_doc(B, Db, "feature"),
    %% idempotent rerun
    {200, #{<<"docs_written">> := 0}} =
        req(post, B ++ "/db/" ++ Branch ++ "/_timeline/merge", #{}),
    ok.

t_purge(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {201, _} = req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
                   #{name => list_to_binary(Branch)}),
    put_doc(B, Branch, "a", #{}),
    {200, #{<<"purged">> := true}} =
        req(delete, B ++ "/db/" ++ Branch ++ "?purge=true", none),
    %% recreated fresh: the branch data is gone (it opens as a NEW
    %% plain db under the same name)
    {200, Changes} = req(get, B ++ "/db/" ++ Branch ++ "/changes",
                         none),
    ?assertEqual([], maps:get(<<"changes">>, Changes)),
    ok.

t_branch_errors(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    %% bad names and bad cursors
    {400, #{<<"error">> := <<"invalid_name">>}} =
        req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
            #{name => <<"no spaces">>}),
    {400, #{<<"error">> := <<"bad_at">>}} =
        req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
            #{name => list_to_binary(Branch), at => <<"garbage">>}),
    %% duplicates and branch-of-branch
    {201, _} = req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
                   #{name => list_to_binary(Branch)}),
    {409, #{<<"error">> := <<"already_exists">>}} =
        req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
            #{name => list_to_binary(Branch)}),
    {409, #{<<"error">> := <<"cannot_branch_a_branch">>}} =
        req(post, B ++ "/db/" ++ Branch ++ "/_timeline/branch",
            #{name => <<"grandchild">>}),
    ok.

t_merge_not_a_branch(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    {409, #{<<"error">> := <<"not_a_branch">>}} =
        req(post, B ++ "/db/" ++ Db ++ "/_timeline/merge", #{}),
    ok.

t_branch_syncs_over_wire(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    put_doc(B, Db, "a", #{}),
    {201, _} = req(post, B ++ "/db/" ++ Db ++ "/_timeline/branch",
                   #{name => list_to_binary(Branch)}),
    put_doc(B, Branch, "b", #{}),
    %% a branch is a normal db: the existing _sync wire replicates it
    Local = <<"tl_wire_local">>,
    {ok, _} = barrel_docdb:create_db(Local, #{
        data_dir => ?config(priv_dir, Config)}),
    try
        Endpoint = barrel_rep_transport_http:endpoint(
            list_to_binary(B ++ "/db/" ++ Branch)),
        {ok, #{docs_written := 2}} = barrel_rep:replicate(
            Endpoint, Local,
            #{source_transport => barrel_rep_transport_http}),
        {ok, _} = barrel_docdb:get_doc(Local, <<"a">>),
        {ok, _} = barrel_docdb:get_doc(Local, <<"b">>)
    after
        _ = barrel_docdb:delete_db(Local)
    end,
    ok.
