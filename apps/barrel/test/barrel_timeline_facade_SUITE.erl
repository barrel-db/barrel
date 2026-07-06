%%%-------------------------------------------------------------------
%%% @doc Facade timeline: branch a composed database (docdb fork +
%%% fresh vector store), work in isolation, merge back, delete.
%%% Plain databases do not carry vectors across the fork (documented);
%%% record-mode branching is covered by the record timeline suite.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_timeline_facade_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_branch_isolation/1,
    t_branch_at_cursor/1,
    t_branch_lineage_info/1,
    t_branch_vectors_not_carried/1,
    t_merge_back/1,
    t_branch_delete/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DIM, 8).

all() ->
    [t_branch_isolation, t_branch_at_cursor, t_branch_lineage_info,
     t_branch_vectors_not_carried, t_merge_back, t_branch_delete].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_vectordb),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Priv = ?config(priv_dir, Config),
    VCfg = #{dimension => ?DIM,
             db_path => filename:join(Priv, atom_to_list(Case) ++ "_vec"),
             bm25_backend => memory},
    {ok, Db} = barrel:open(Case, #{vectordb => VCfg}),
    BranchName = list_to_atom(atom_to_list(Case) ++ "_b"),
    BranchVCfg = #{dimension => ?DIM,
                   db_path => filename:join(Priv,
                                            atom_to_list(Case) ++ "_bvec"),
                   bm25_backend => memory},
    [{db, Db}, {branch_name, BranchName},
     {branch_opts, #{vectordb => BranchVCfg}} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel:close(?config(db, Config)),
    ok.

cursor(#{docdb := DbBin}) ->
    {ok, _, Last} = barrel_docdb:get_changes(DbBin, first),
    Last.

%%====================================================================
%% Cases
%%====================================================================

t_branch_isolation(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"shared">>,
                                   <<"v">> => 1}),
    {ok, Branch} = barrel:branch(Db, ?config(branch_name, Config),
                                 ?config(branch_opts, Config)),
    try
        {ok, #{<<"v">> := 1}} = barrel:get_doc(Branch, <<"shared">>),
        {ok, _} = barrel:put_doc(Branch, #{<<"id">> => <<"bonly">>}),
        {error, not_found} = barrel:get_doc(Db, <<"bonly">>),
        {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"ponly">>}),
        {error, not_found} = barrel:get_doc(Branch, <<"ponly">>)
    after
        ok = barrel:delete(Branch)
    end,
    ok.

t_branch_at_cursor(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"d">>, <<"v">> => 1}),
    T = cursor(Db),
    {ok, #{<<"_rev">> := R}} = barrel:get_doc(Db, <<"d">>),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"d">>, <<"v">> => 2,
                                   <<"_rev">> => R}),
    {ok, Branch} = barrel:branch(Db, ?config(branch_name, Config),
                                 (?config(branch_opts, Config))#{at => T}),
    try
        {ok, #{<<"v">> := 1}} = barrel:get_doc(Branch, <<"d">>),
        {ok, #{<<"v">> := 2}} = barrel:get_doc(Db, <<"d">>)
    after
        ok = barrel:delete(Branch)
    end,
    ok.

t_branch_lineage_info(Config) ->
    #{docdb := ParentBin} = Db = ?config(db, Config),
    {ok, Branch} = barrel:branch(Db, ?config(branch_name, Config),
                                 ?config(branch_opts, Config)),
    try
        {ok, Info} = barrel:info(Branch),
        ?assertEqual(ParentBin, maps:get(parent, Info)),
        ?assertEqual(ParentBin, maps:get(keyspace, Info)),
        ?assert(maps:is_key(fork_hlc, Info))
    after
        ok = barrel:delete(Branch)
    end,
    ok.

t_branch_vectors_not_carried(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    ok = barrel:vector_add(Db, <<"a">>, <<"text">>, #{},
                           lists:duplicate(?DIM, 0.5)),
    {ok, Branch} = barrel:branch(Db, ?config(branch_name, Config),
                                 ?config(branch_opts, Config)),
    try
        %% documented: plain vectors are not carried; the store is
        %% fresh and writable
        {ok, []} = barrel:search_vector(Branch,
                                        lists:duplicate(?DIM, 0.5),
                                        #{k => 1}),
        ok = barrel:vector_add(Branch, <<"a">>, <<"text">>, #{},
                               lists:duplicate(?DIM, 0.5)),
        {ok, [_]} = barrel:search_vector(Branch,
                                         lists:duplicate(?DIM, 0.5),
                                         #{k => 1})
    after
        ok = barrel:delete(Branch)
    end,
    ok.

t_merge_back(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"base">>}),
    {ok, Branch} = barrel:branch(Db, ?config(branch_name, Config),
                                 ?config(branch_opts, Config)),
    try
        {ok, _} = barrel:put_doc(Branch, #{<<"id">> => <<"feature">>,
                                           <<"done">> => true}),
        {ok, Report} = barrel:merge(Branch),
        ?assertMatch(#{docs_written := 1}, Report),
        ?assert(maps:is_key(last_merged, Report)),
        {ok, #{<<"done">> := true}} = barrel:get_doc(Db, <<"feature">>),
        %% second merge with nothing new writes nothing
        {ok, #{docs_written := 0}} = barrel:merge(Branch)
    after
        ok = barrel:delete(Branch)
    end,
    ok.

t_branch_delete(Config) ->
    Priv = ?config(priv_dir, Config),
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, Branch} = barrel:branch(Db, ?config(branch_name, Config),
                                 ?config(branch_opts, Config)),
    #{docdb := BranchBin} = Branch,
    BranchDocPath = filename:join(Priv, binary_to_list(BranchBin)),
    ?assert(filelib:is_dir(BranchDocPath)),
    ok = barrel:delete(Branch),
    ?assertNot(filelib:is_dir(BranchDocPath)),
    %% the parent is untouched
    {ok, _} = barrel:get_doc(Db, <<"a">>),
    ok.
