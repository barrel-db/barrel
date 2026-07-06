%%%-------------------------------------------------------------------
%%% @doc Audit surface of the facade: the retained history log with
%%% provenance, per-document versions, version bodies, and the history
%%% floor. Answers "what did the agent know when" and "who wrote this"
%%% at database granularity.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_audit_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_history_with_provenance/1,
    t_delete_with_provenance/1,
    t_history_id_filter_and_limit/1,
    t_doc_versions_and_bodies/1,
    t_provenance_cleared_on_plain_write/1,
    t_history_floor/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_history_with_provenance, t_delete_with_provenance,
     t_history_id_filter_and_limit, t_doc_versions_and_bodies,
     t_provenance_cleared_on_plain_write, t_history_floor].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Priv = ?config(priv_dir, Config),
    Name = <<"audit_", (atom_to_binary(Case, utf8))/binary>>,
    VCfg = #{dimension => 3,
             db_path => filename:join(Priv, binary_to_list(Name) ++ "_vec"),
             bm25_backend => memory},
    {ok, Db} = barrel:open(Name, #{vectordb => VCfg}),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    ok = barrel:close(?config(db, Config)),
    ok.

-define(PROV, #{actor => <<"agent-7">>, session => <<"ses-1">>,
                source => <<"suite">>}).

%%====================================================================
%% Cases
%%====================================================================

t_history_with_provenance(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1},
                             #{provenance => ?PROV}),
    {ok, [Entry]} = barrel:history(Db),
    ?assertEqual(<<"a">>, maps:get(id, Entry)),
    ?assertEqual(local, maps:get(cause, Entry)),
    ?assertEqual(?PROV, maps:get(provenance, Entry)),
    ?assert(maps:is_key(hlc, Entry)),
    %% the current version carries the last writer too
    {ok, [Current | _]} = barrel:doc_versions(Db, <<"a">>),
    ?assertEqual(current, maps:get(status, Current)),
    ?assertEqual(?PROV, maps:get(provenance, Current)),
    ok.

t_delete_with_provenance(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel:delete_doc(Db, <<"a">>,
                                #{provenance => #{actor => <<"reaper">>}}),
    {ok, Entries} = barrel:history(Db, #{id => <<"a">>}),
    [_, DelEntry] = Entries,
    ?assert(maps:get(deleted, DelEntry)),
    ?assertEqual(#{actor => <<"reaper">>}, maps:get(provenance, DelEntry)),
    ok.

t_history_id_filter_and_limit(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"b">>, <<"v">> => 1}),
    {ok, #{<<"_rev">> := R}} = barrel:get_doc(Db, <<"a">>),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 2,
                                   <<"_rev">> => R}),
    {ok, All} = barrel:history(Db),
    ?assertEqual(3, length(All)),
    {ok, OnlyA} = barrel:history(Db, #{id => <<"a">>}),
    ?assertEqual([<<"a">>, <<"a">>], [maps:get(id, E) || E <- OnlyA]),
    %% limit bounds the scan, oldest first
    {ok, First2} = barrel:history(Db, #{limit => 2}),
    ?assertEqual(2, length(First2)),
    ok.

t_doc_versions_and_bodies(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} =
        barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} =
        barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 2,
                             <<"_rev">> => Rev1}),
    {ok, [Current, Superseded]} = barrel:doc_versions(Db, <<"a">>),
    ?assertEqual({current, Rev2},
                 {maps:get(status, Current), maps:get(version, Current)}),
    ?assertEqual({superseded, Rev1},
                 {maps:get(status, Superseded),
                  maps:get(version, Superseded)}),
    {ok, #{<<"v">> := 2}} = barrel:version_body(Db, <<"a">>, Rev2),
    {ok, #{<<"v">> := 1}} = barrel:version_body(Db, <<"a">>, Rev1),
    ?assertEqual({error, not_found},
                 barrel:version_body(Db, <<"a">>, <<"1-deadbeef">>)),
    ok.

t_provenance_cleared_on_plain_write(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := R1}} =
        barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1},
                       #{provenance => ?PROV}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 2,
                                   <<"_rev">> => R1}),
    %% the current entity no longer names a writer, the audit log does
    {ok, [Current | _]} = barrel:doc_versions(Db, <<"a">>),
    ?assertNot(maps:is_key(provenance, Current)),
    {ok, [E1, E2]} = barrel:history(Db, #{id => <<"a">>}),
    ?assertEqual(?PROV, maps:get(provenance, E1)),
    ?assertNot(maps:is_key(provenance, E2)),
    ok.

t_history_floor(Config) ->
    Db = ?config(db, Config),
    ?assertEqual(undefined, barrel:history_floor(Db)),
    ok.
