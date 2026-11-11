%%% @doc A client-supplied vector travels with its version: replication
%%% carries it, state writes keep it, resolutions that pick an input's
%%% body keep that input's vector.
-module(barrel_rep_vector_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([state_replication_carries_client_vector/1,
         computed_vector_not_replicated/1,
         version_fallback_carries_client_vector/1,
         losing_sibling_keeps_winner_vector/1,
         late_vector_reaches_replica/1,
         resolver_choose_keeps_vector/1,
         resolver_merge_same_body_keeps_vector/1,
         resolver_merge_new_body_drops_vector/1]).
-export([choose_keep/1, merge_keep/1, merge_new/1]).

-define(VEC, [0.5, 0.25]).

all() ->
    [state_replication_carries_client_vector,
     computed_vector_not_replicated,
     version_fallback_carries_client_vector,
     losing_sibling_keeps_winner_vector,
     late_vector_reaches_replica,
     resolver_choose_keeps_vector,
     resolver_merge_same_body_keeps_vector,
     resolver_merge_new_body_drops_vector].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    A = <<(atom_to_binary(TC))/binary, "_a">>,
    B = <<(atom_to_binary(TC))/binary, "_b">>,
    Opts = case TC of
        resolver_choose_keeps_vector -> #{conflict_resolver => {?MODULE, choose_keep}};
        resolver_merge_same_body_keeps_vector -> #{conflict_resolver => {?MODULE, merge_keep}};
        resolver_merge_new_body_drops_vector -> #{conflict_resolver => {?MODULE, merge_new}};
        _ -> #{}
    end,
    [begin
         _ = barrel_docdb:delete_db(D),
         {ok, _} = barrel_docdb:create_db(D, Opts)
     end || D <- [A, B]],
    [{a, A}, {b, B} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_docdb:delete_db(?config(a, Config)),
    _ = barrel_docdb:delete_db(?config(b, Config)),
    ok.

%%====================================================================
%% Replication
%%====================================================================

state_replication_carries_client_vector(Config) ->
    {A, B} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(A, doc(<<"m">>, <<"secret">>, ?VEC)),
    {ok, _} = replicate(A, B),
    ?assertEqual({client, ?VEC}, vector(B, <<"m">>)).

computed_vector_not_replicated(Config) ->
    {A, B} = dbs(Config),
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(A, #{<<"id">> => <<"c">>,
                                                          <<"text">> => <<"t">>}),
    ok = barrel_docdb:set_doc_embedding(A, <<"c">>, Rev, ?VEC),
    {ok, _} = replicate(A, B),
    ?assertEqual(none, vector(B, <<"c">>)).

%% The one-version path: the replication read carries the vector in the
%% document, and the target stores it as the client's.
version_fallback_carries_client_vector(Config) ->
    {A, B} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(A, doc(<<"m">>, <<"secret">>, ?VEC)),
    {ok, #{doc := Doc, version := Tok, vv := VV, deleted := Del}} =
        barrel_docdb:get_doc_for_replication(A, <<"m">>),
    {ok, <<"m">>, _} = barrel_docdb:put_version(B, Doc, Tok, VV, Del),
    ?assertEqual({client, ?VEC}, vector(B, <<"m">>)),
    {ok, Body} = barrel_docdb:get_doc(B, <<"m">>),
    ?assertNot(maps:is_key(<<"_embedding">>, Body)).

%% A replicated version that loses rewrites the state, not the winner:
%% the winner's vector stays.
losing_sibling_keeps_winner_vector(Config) ->
    {A, B} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(B, #{<<"id">> => <<"m">>, <<"text">> => <<"old">>}),
    timer:sleep(2),
    {ok, _} = barrel_docdb:put_doc(A, doc(<<"m">>, <<"new">>, ?VEC)),
    {ok, _} = replicate(B, A),
    {ok, Doc} = barrel_docdb:get_doc(A, <<"m">>),
    ?assertEqual(<<"new">>, maps:get(<<"text">>, Doc)),
    {ok, [_]} = barrel_docdb:get_conflicts(A, <<"m">>),
    ?assertEqual({client, ?VEC}, vector(A, <<"m">>)).

%% A replica that holds the version without its vector (an older peer
%% sent it) gets the vector when a peer that has it replicates again.
late_vector_reaches_replica(Config) ->
    {A, B} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(A, doc(<<"m">>, <<"secret">>, ?VEC)),
    {ok, #{state := State, bodies := Bodies}} = barrel_docdb:get_doc_state(A, <<"m">>),
    {ok, _, _} = barrel_docdb:put_doc_state(B, <<"m">>, State, Bodies),
    ?assertEqual(none, vector(B, <<"m">>)),
    {ok, _} = replicate(A, B),
    ?assertEqual({client, ?VEC}, vector(B, <<"m">>)).

%%====================================================================
%% Automatic resolution
%%====================================================================

resolver_choose_keeps_vector(Config) ->
    conflict(Config),
    ?assertEqual({client, ?VEC}, vector(?config(a, Config), <<"m">>)).

resolver_merge_same_body_keeps_vector(Config) ->
    conflict(Config),
    ?assertEqual({client, ?VEC}, vector(?config(a, Config), <<"m">>)).

resolver_merge_new_body_drops_vector(Config) ->
    conflict(Config),
    {ok, Doc} = barrel_docdb:get_doc(?config(a, Config), <<"m">>),
    ?assertEqual(true, maps:get(<<"merged">>, Doc)),
    ?assertEqual(none, vector(?config(a, Config), <<"m">>)).

%% Concurrent versions of `m': A's carries a vector, B's does not. The
%% resolver runs on A when B's version arrives.
conflict(Config) ->
    {A, B} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(A, (doc(<<"m">>, <<"keep">>, ?VEC))#{<<"keep">> => true}),
    {ok, _} = barrel_docdb:put_doc(B, #{<<"id">> => <<"m">>, <<"text">> => <<"other">>}),
    {ok, _} = replicate(B, A),
    {ok, Doc} = barrel_docdb:get_doc(A, <<"m">>),
    ?assertMatch({_, _}, binary:match(maps:get(<<"_rev">>, Doc), <<"@auto:">>)),
    {ok, []} = barrel_docdb:get_conflicts(A, <<"m">>),
    Doc.

choose_keep(Versions) ->
    [#{token := T}] = [V || #{body := #{<<"keep">> := true}} = V <- Versions],
    {choose, T}.

merge_keep(Versions) ->
    [#{body := B}] = [V || #{body := #{<<"keep">> := true}} = V <- Versions],
    {merge, B}.

merge_new(Versions) ->
    {merge, lists:foldl(fun(#{body := B}, Acc) -> maps:merge(Acc, B) end,
                        #{<<"merged">> => true}, Versions)}.

%%====================================================================
%% Helpers
%%====================================================================

dbs(Config) -> {?config(a, Config), ?config(b, Config)}.

doc(Id, Text, Vec) ->
    #{<<"id">> => Id, <<"text">> => Text, <<"_embedding">> => Vec}.

replicate(Src, Tgt) ->
    barrel_rep:replicate(Src, Tgt, #{attachments => false}).

vector(Db, Id) ->
    {ok, Doc} = barrel_docdb:get_doc(Db, Id, #{include_embedding => true}),
    case maps:get(<<"_embedding">>, Doc, undefined) of
        undefined -> none;
        #{<<"vector">> := V, <<"source">> := <<"client">>} -> {client, V};
        #{<<"vector">> := V, <<"source">> := <<"computed">>} -> {computed, V}
    end.
