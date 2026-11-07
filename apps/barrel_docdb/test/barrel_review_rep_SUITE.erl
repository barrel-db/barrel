%%% @doc Replication defects from the 2.0.0 review: filtered paging,
%%% missing bodies, rejected clock sync and invalid query filters.
-module(barrel_review_rep_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([filtered_query_pages_copy_every_match/1,
         filtered_page_resumes_after_last_returned/1,
         channel_query_pages_past_empty_batches/1,
         channel_replication_passes_deletes/1,
         task_channel_query_pages_past_empty_batches/1,
         empty_advanced_page_continues/1,
         task_empty_advanced_page_continues/1,
         missing_body_rejected/1,
         clock_skew_version_rejected/1,
         clock_skew_state_rejected/1,
         invalid_query_changes_rejected/1,
         invalid_query_replication_rejected/1,
         invalid_query_task_rejected/1]).

-define(CHANNELS, #{<<"posts">> => [<<"type/post">>]}).

all() ->
    [filtered_query_pages_copy_every_match,
     filtered_page_resumes_after_last_returned,
     channel_query_pages_past_empty_batches,
     channel_replication_passes_deletes,
     task_channel_query_pages_past_empty_batches,
     empty_advanced_page_continues,
     task_empty_advanced_page_continues,
     missing_body_rejected,
     clock_skew_version_rejected,
     clock_skew_state_rejected,
     invalid_query_changes_rejected,
     invalid_query_replication_rejected,
     invalid_query_task_rejected].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Src = <<(atom_to_binary(TC))/binary, "_src">>,
    Tgt = <<(atom_to_binary(TC))/binary, "_tgt">>,
    [begin
         _ = barrel_docdb:delete_db(D),
         {ok, _} = barrel_docdb:create_db(D, #{channels => ?CHANNELS})
     end || D <- [Src, Tgt]],
    [{src, Src}, {tgt, Tgt} | Config].

end_per_testcase(_TC, Config) ->
    _ = meck:unload(),
    ok = hlc:set_maxoffset(0, whereis(barrel_hlc_clock)),
    _ = barrel_docdb:delete_db(?config(src, Config)),
    _ = barrel_docdb:delete_db(?config(tgt, Config)),
    ok.

%%====================================================================
%% Filtered paging
%%====================================================================

filtered_query_pages_copy_every_match(Config) ->
    {Src, Tgt} = dbs(Config),
    %% every match, then one match in seven
    [put(Src, I, #{<<"type">> => <<"wanted">>}) || I <- lists:seq(1, 150)],
    [put(Src, I, #{<<"type">> => kind(I)}) || I <- lists:seq(151, 500)],
    Wanted = 150 + length([I || I <- lists:seq(151, 500), I rem 7 =:= 0]),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{batch_size => 10,
                                               attachments => false,
                                               filter => wanted()}),
    ?assertEqual(Wanted, maps:get(docs_written, R)),
    ?assertEqual(Wanted, length(changes(Tgt))).

filtered_page_resumes_after_last_returned(Config) ->
    {Src, _} = dbs(Config),
    [put(Src, I, #{<<"type">> => <<"wanted">>}) || I <- lists:seq(1, 30)],
    {ok, Page, Last} = barrel_docdb:get_changes(
                         Src, first, #{limit => 10, query => query()}),
    ?assertEqual(10, length(Page)),
    ?assertEqual(maps:get(hlc, lists:last(Page)), Last),
    {ok, Next, _} = barrel_docdb:get_changes(
                      Src, Last, #{limit => 100, query => query()}),
    ?assertEqual(20, length(Next)).

channel_query_pages_past_empty_batches(Config) ->
    {Src, Tgt} = dbs(Config),
    put_posts(Src),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{batch_size => 5,
                                               attachments => false,
                                               filter => published()}),
    ?assertEqual(10, maps:get(docs_written, R)),
    ?assertEqual(lists:sort(published_ids()), lists:sort(ids(changes(Tgt)))).

%% Under replication a delete passes the channel filter's query.
channel_replication_passes_deletes(Config) ->
    {Src, Tgt} = dbs(Config),
    put_posts(Src),
    Opts = #{batch_size => 100, attachments => false, filter => published()},
    {ok, _} = barrel_rep:replicate(Src, Tgt, Opts),
    [Id | _] = published_ids(),
    {ok, _} = barrel_docdb:get_doc(Tgt, Id),
    {ok, Doc} = barrel_docdb:get_doc(Src, Id),
    {ok, _} = barrel_docdb:delete_doc(Src, Id, #{rev => maps:get(<<"_rev">>, Doc)}),
    {ok, _} = barrel_rep:replicate(Src, Tgt, Opts),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Tgt, Id)).

task_channel_query_pages_past_empty_batches(Config) ->
    {Src, Tgt} = dbs(Config),
    put_posts(Src),
    {ok, TaskId} = barrel_rep_tasks:start_task(
                     #{source => Src, target => Tgt, mode => one_shot,
                       batch_size => 5, attachments => false,
                       filter => published()}),
    ok = wait_task(TaskId, completed, 200),
    _ = barrel_rep_tasks:delete_task(TaskId),
    ?assertEqual(lists:sort(published_ids()), lists:sort(ids(changes(Tgt)))).

%% A source page with nothing to copy but a newer sequence goes on.
empty_advanced_page_continues(Config) ->
    {Src, Tgt} = dbs(Config),
    Skip = empty_first_page(Src),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{attachments => false}),
    ?assertEqual(10 - Skip, maps:get(docs_written, R)),
    ?assertEqual(10 - Skip, length(changes(Tgt))).

task_empty_advanced_page_continues(Config) ->
    {Src, Tgt} = dbs(Config),
    Skip = empty_first_page(Src),
    {ok, TaskId} = barrel_rep_tasks:start_task(
                     #{source => Src, target => Tgt, mode => one_shot,
                       attachments => false}),
    ok = wait_task(TaskId, completed, 200),
    _ = barrel_rep_tasks:delete_task(TaskId),
    ?assertEqual(10 - Skip, length(changes(Tgt))).

%%====================================================================
%% Missing bodies
%%====================================================================

missing_body_rejected(Config) ->
    {Src, Tgt} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(Src, #{<<"id">> => <<"d">>,
                                         <<"valuable">> => 42}),
    {ok, #{state := State, bodies := Bodies}} =
        barrel_docdb:get_doc_state(Src, <<"d">>),
    Tokens = lists:sort(maps:keys(Bodies)),
    ?assertEqual({error, {missing_bodies, Tokens}},
                 barrel_docdb:put_doc_state(Tgt, <<"d">>, State, #{})),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Tgt, <<"d">>)),
    ?assertMatch({ok, <<"d">>, _},
                 barrel_docdb:put_doc_state(Tgt, <<"d">>, State, Bodies)),
    {ok, Doc} = barrel_docdb:get_doc(Tgt, <<"d">>),
    ?assertEqual(42, maps:get(<<"valuable">>, Doc)),
    %% versions the target holds need no body
    ?assertMatch({unchanged, <<"d">>, _},
                 barrel_docdb:put_doc_state(Tgt, <<"d">>, State, #{})).

%%====================================================================
%% Clock skew
%%====================================================================

clock_skew_version_rejected(Config) ->
    {Db, _} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>, <<"v">> => 0}),
    {ok, Before} = barrel_docdb:get_doc(Db, <<"d">>),
    {H, Token} = future_token(),
    ok = hlc:set_maxoffset(1000, whereis(barrel_hlc_clock)),
    ?assertEqual({error, clock_skew},
                 barrel_docdb:put_version(Db, #{<<"id">> => <<"d">>,
                                                <<"v">> => 1},
                                          Token,
                                          barrel_vv:encode(#{<<"peer">> => H}),
                                          false)),
    ?assertEqual({ok, Before}, barrel_docdb:get_doc(Db, <<"d">>)),
    writer_serves(Db, Before).

clock_skew_state_rejected(Config) ->
    {_, Tgt} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(Tgt, #{<<"id">> => <<"d">>, <<"v">> => 0}),
    {ok, Before} = barrel_docdb:get_doc(Tgt, <<"d">>),
    %% a peer state holding a version an hour ahead
    {H, Token} = future_token(),
    State = barrel_live:encode_state(
              barrel_live:author(barrel_live:new(), {H, <<"peer">>}, false, [])),
    Bodies = #{Token => #{<<"v">> => 1}},
    ok = hlc:set_maxoffset(1000, whereis(barrel_hlc_clock)),
    ?assertEqual({error, clock_skew},
                 barrel_docdb:put_doc_state(Tgt, <<"d">>, State, Bodies)),
    ?assertEqual({ok, Before}, barrel_docdb:get_doc(Tgt, <<"d">>)),
    writer_serves(Tgt, Before).

%%====================================================================
%% Invalid query filters
%%====================================================================

invalid_query_changes_rejected(Config) ->
    {Src, _} = dbs(Config),
    put_posts(Src),
    [?assertMatch({error, {invalid_query, _}},
                  try barrel_docdb:get_changes(Src, first, Opts)
                  catch C:R -> {C, R}
                  end)
     || Opts <- [#{query => bad_query()},
                 #{query => not_a_map},
                 #{query => bad_query(), limit => 5, replication => true},
                 #{query => bad_query(), channel => <<"posts">>},
                 #{query => bad_query(), paths => [<<"type/post">>]}]].

invalid_query_replication_rejected(Config) ->
    {Src, Tgt} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(Src, #{<<"id">> => <<"private">>,
                                         <<"secret">> => true}),
    [?assertMatch({error, {invalid_query, _}},
                  barrel_rep:replicate(Src, Tgt, #{attachments => false,
                                                   filter => Filter}))
     || Filter <- [#{query => bad_query()}, #{query => [not_a_map]},
                   #{query => bad_query(), channel => <<"posts">>}]],
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Tgt, <<"private">>)).

invalid_query_task_rejected(Config) ->
    {Src, Tgt} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(Src, #{<<"id">> => <<"private">>,
                                         <<"secret">> => true}),
    ?assertMatch({error, {invalid_query, _}},
                 barrel_rep_tasks:start_task(
                   #{source => Src, target => Tgt, mode => one_shot,
                     attachments => false,
                     filter => #{query => bad_query()}})),
    timer:sleep(200),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Tgt, <<"private">>)).

%%====================================================================
%% Helpers
%%====================================================================

dbs(Config) -> {?config(src, Config), ?config(tgt, Config)}.

put(Db, I, Body) ->
    {ok, _} = barrel_docdb:put_doc(Db, Body#{<<"id">> => id(I)}).

id(I) -> iolist_to_binary(io_lib:format("d~4..0B", [I])).

kind(I) when I rem 7 =:= 0 -> <<"wanted">>;
kind(_) -> <<"other">>.

query() -> #{where => [{path, [<<"type">>], <<"wanted">>}]}.

wanted() -> #{query => query()}.

bad_query() -> #{where => [{unknown_condition, [<<"secret">>], true}]}.

%% 20 drafts, then 10 published posts, in the posts channel
put_posts(Db) ->
    [put(Db, I, #{<<"type">> => <<"post">>, <<"status">> => <<"draft">>})
     || I <- lists:seq(1, 20)],
    [put(Db, I, #{<<"type">> => <<"post">>, <<"status">> => <<"published">>})
     || I <- lists:seq(21, 30)],
    ok.

published_ids() -> [id(I) || I <- lists:seq(21, 30)].

published() ->
    #{channel => <<"posts">>,
      query => #{where => [{path, [<<"status">>], <<"published">>}]}}.

changes(Db) ->
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    Changes.

ids(Changes) -> [maps:get(id, C) || C <- Changes].

%% The source answers its first page empty, but moved past Skip rows,
%% as a remote filtering source does.
empty_first_page(Src) ->
    [put(Src, I, #{}) || I <- lists:seq(1, 10)],
    Skip = 4,
    {ok, Head, _} = barrel_docdb:get_changes(Src, first, #{limit => Skip}),
    SkipHlc = maps:get(hlc, lists:last(Head)),
    ok = meck:new(barrel_rep_transport_local, [passthrough]),
    ok = meck:expect(barrel_rep_transport_local, get_changes,
                     fun(Db, first, _Opts) when Db =:= Src ->
                             {ok, [], SkipHlc};
                        (Db, Since, Opts) ->
                             meck:passthrough([Db, Since, Opts])
                     end),
    Skip.

wait_task(_TaskId, Status, 0) ->
    {timeout, Status};
wait_task(TaskId, Status, N) ->
    case barrel_rep_tasks:get_task(TaskId) of
        {ok, #{status := Status}} ->
            ok;
        _ ->
            timer:sleep(50),
            wait_task(TaskId, Status, N - 1)
    end.

%% A version token an hour ahead of the clock, refused once the clock
%% allows one second of drift.
future_token() ->
    H = barrel_hlc:from_wall_time(erlang:system_time(millisecond) + 3600000),
    {H, barrel_version:to_token({H, <<"peer">>})}.

%% The writer still answers: a local write on the same document and on
%% another one both return.
writer_serves(Db, #{<<"_rev">> := Rev}) ->
    ok = hlc:set_maxoffset(1000, whereis(barrel_hlc_clock)),
    Parent = self(),
    Caller = spawn(fun() ->
        Parent ! {local, barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                                    <<"_rev">> => Rev,
                                                    <<"v">> => 2})},
        Parent ! {other, barrel_docdb:put_doc(Db, #{<<"id">> => <<"e">>})}
    end),
    Result = [receive {Tag, R} -> R after 2000 -> still_running end
              || Tag <- [local, other]],
    exit(Caller, kill),
    ?assertMatch([{ok, _}, {ok, _}], Result).
