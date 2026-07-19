%%%-------------------------------------------------------------------
%%% @doc Incremental-index tests for barrel_ngram (M2).
%%%
%%% Exercises the live lifecycle: updates and deletes reflected in
%%% results, query fan across multiple segments, watermark recovery, and
%%% the incremental oracle (a live put/update/delete workload must yield
%%% byte-identical results to a brute-force scan over the final database
%%% state). `refresh/1' is the deterministic catch-up point.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_incremental_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([live_subscription/1, live_update/1, live_delete/1,
         multi_segment_fan/1, recovery/1, incremental_oracle/1]).

all() ->
    [live_subscription, live_update, live_delete, multi_segment_fan,
     recovery, incremental_oracle].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_inc_">>, atom_to_binary(TC, utf8)]),
    Corpus = Db,
    Dir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => Dir}),
    [{db, Db}, {corpus, Corpus}, {dir, Dir} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_ngram:close(?config(corpus, Config)),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

live_subscription(Config) ->
    %% No refresh: changes must become searchable purely from the
    %% background feed subscription (delivery + apply + ack keep flowing).
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Ids = [iolist_to_binary([<<"s">>, integer_to_binary(N)])
           || N <- lists:seq(1, 25)],
    lists:foreach(fun(Id) -> _ = put_doc(Db, Id, <<"subscribed payload">>) end, Ids),
    Expected = lists:sort(Ids),
    ok = wait_until(fun() -> search(C, <<"subscribed">>) =:= Expected end, 100),
    ?assertEqual(Expected, search(C, <<"subscribed">>)).

live_update(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Rev1 = put_doc(Db, <<"d1">>, <<"alpha connect_timeout beta">>),
    refresh(C),
    ?assertEqual([<<"d1">>], search(C, <<"connect_timeout">>)),
    %% update: drop the old identifier, add new text
    _Rev2 = put_doc(Db, <<"d1">>, <<"alpha gamma delta">>, Rev1),
    refresh(C),
    %% old grams still sit in the first segment; confirm drops the stale hit
    ?assertEqual([], search(C, <<"connect_timeout">>)),
    ?assertEqual([<<"d1">>], search(C, <<"gamma delta">>)).

live_delete(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Rev1 = put_doc(Db, <<"d1">>, <<"error connect_timeout here">>),
    _ = put_doc(Db, <<"d2">>, <<"unrelated content">>),
    refresh(C),
    ?assertEqual([<<"d1">>], search(C, <<"connect_timeout">>)),
    ok = del_doc(Db, <<"d1">>, Rev1),
    refresh(C),
    %% gone from results though its grams remain in the frozen segment
    ?assertEqual([], search(C, <<"connect_timeout">>)),
    ?assertEqual([<<"d2">>], search(C, <<"content">>)).

multi_segment_fan(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    _ = put_doc(Db, <<"a">>, <<"apple pie recipe">>), refresh(C),
    _ = put_doc(Db, <<"b">>, <<"banana apple split">>), refresh(C),
    _ = put_doc(Db, <<"c">>, <<"cherry apple tart">>), refresh(C),
    {ok, Segs} = barrel_ngram_shard:get_manifest(C),
    ?assert(length(Segs) >= 3),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], search(C, <<"apple">>)),
    ?assertEqual([<<"b">>], search(C, <<"banana">>)),
    ?assertEqual([], search(C, <<"durian">>)).

recovery(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Dir = ?config(dir, Config),
    _ = put_doc(Db, <<"a">>, <<"recover alpha payload">>),
    refresh(C),
    ?assertEqual([<<"a">>], search(C, <<"recover">>)),
    %% take the shard down, mutate while it is gone
    ok = barrel_ngram:close(C),
    _ = put_doc(Db, <<"b">>, <<"recover beta payload">>),
    %% bring it back: manifest reloads, resubscribe replays the tail
    ok = barrel_ngram:open(C, #{db => Db, data_dir => Dir}),
    refresh(C),
    ?assertEqual([<<"a">>, <<"b">>], search(C, <<"recover">>)),
    ?assertEqual([<<"b">>], search(C, <<"beta">>)).

incremental_oracle(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    _ = rand:seed(exsss, {7, 11, 13}),
    Tracker = run_workload(Db, C, 80, #{}, 0),
    refresh(C),
    Literals = [<<"connect">>, <<"timeout">>, <<"retry">>, <<"backoff">>,
                <<"error">>, <<"pool">>, <<"config">>, <<"jitter">>,
                <<"upstream">>, <<"widget">>, <<"retry backoff">>,
                <<"connect timeout">>, <<"nonexistent">>, <<"co">>, <<"e ">>,
                <<"z">>, <<" ">>],
    lists:foreach(
        fun(Lit) ->
            Expected = brute_force(Tracker, Lit),
            Actual = search(C, Lit),
            ?assertEqual({Lit, Expected}, {Lit, Actual})
        end, Literals).

%%====================================================================
%% Workload (deterministic)
%%====================================================================

vocab() ->
    [<<"connect">>, <<"timeout">>, <<"retry">>, <<"backoff">>, <<"error">>,
     <<"pool">>, <<"config">>, <<"jitter">>, <<"upstream">>, <<"widget">>].

%% Tracker: #{Id => {Text, Rev}} for live docs. Counter mints fresh ids so
%% deletes never resurrect (which would need the tombstone rev).
run_workload(_Db, _C, 0, Tracker, _Counter) ->
    Tracker;
run_workload(Db, C, N, Tracker, Counter) ->
    Ids = maps:keys(Tracker),
    {Tracker1, Counter1} =
        case rand:uniform(6) of
            R when R =< 3 ->
                %% new document
                Id = iolist_to_binary([<<"doc">>, integer_to_binary(Counter)]),
                Text = random_text(),
                Rev = put_doc(Db, Id, Text),
                {Tracker#{Id => {Text, Rev}}, Counter + 1};
            4 when Ids =/= [] ->
                %% update an existing document
                Id = lists:nth(rand:uniform(length(Ids)), Ids),
                {_Old, OldRev} = maps:get(Id, Tracker),
                Text = random_text(),
                Rev = put_doc(Db, Id, Text, OldRev),
                {Tracker#{Id => {Text, Rev}}, Counter};
            5 when Ids =/= [] ->
                %% delete an existing document
                Id = lists:nth(rand:uniform(length(Ids)), Ids),
                {_Old, OldRev} = maps:get(Id, Tracker),
                ok = del_doc(Db, Id, OldRev),
                {maps:remove(Id, Tracker), Counter};
            _ ->
                %% freeze point (forces multiple segments)
                refresh(C),
                {Tracker, Counter}
        end,
    run_workload(Db, C, N - 1, Tracker1, Counter1).

random_text() ->
    V = vocab(),
    K = 2 + rand:uniform(4),
    Words = [lists:nth(rand:uniform(length(V)), V) || _ <- lists:seq(1, K)],
    iolist_to_binary(lists:join(<<" ">>, Words)).

%%====================================================================
%% Helpers
%%====================================================================

put_doc(Db, Id, Text) ->
    put_doc(Db, Id, Text, undefined).

put_doc(Db, Id, Text, Rev) ->
    Doc0 = #{<<"id">> => Id, <<"body">> => Text},
    Doc = case Rev of
        undefined -> Doc0;
        _ -> Doc0#{<<"_rev">> => Rev}
    end,
    {ok, R} = barrel_docdb:put_doc(Db, Doc),
    maps:get(<<"rev">>, R).

del_doc(Db, Id, Rev) ->
    {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev}),
    ok.

refresh(C) ->
    {ok, _} = barrel_ngram:refresh(C),
    ok.

%% Poll a predicate up to Attempts times, 50 ms apart.
wait_until(_Pred, 0) ->
    {error, timeout};
wait_until(Pred, Attempts) ->
    case Pred() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Pred, Attempts - 1)
    end.

search(C, Lit) ->
    {ok, Hits} = barrel_ngram:search(C, Lit),
    lists:sort([maps:get(id, H) || H <- Hits]).

brute_force(Tracker, Lit) ->
    lists:sort(
      [Id || {Id, {Text, _Rev}} <- maps:to_list(Tracker),
             begin
                 DocText = barrel_ngram_corpus:doc_text(#{<<"body">> => Text},
                                                        #{fields => all}),
                 binary:match(DocText, Lit) =/= nomatch
             end]).
