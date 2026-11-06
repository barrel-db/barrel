%%% @doc Reliable delivery: a document that fails is retried by id and
%%% never skipped by the saved position, whatever fails and when.
-module(barrel_rep_delivery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([poison_doc_pending_then_retried/1,
         poison_in_first_batch/1,
         target_checkpoint_write_fails/1,
         source_checkpoint_write_fails/1,
         overflow_stops_without_skipping/1,
         unreadable_checkpoint_aborts/1,
         clock_skew_aborts_batch/1,
         retry_drops_ids_gone_from_source/1,
         both_task_keeps_two_positions/1]).

-define(T, barrel_rep_fault_transport).

all() ->
    [poison_doc_pending_then_retried, poison_in_first_batch,
     target_checkpoint_write_fails, source_checkpoint_write_fails,
     overflow_stops_without_skipping, unreadable_checkpoint_aborts,
     clock_skew_aborts_batch, retry_drops_ids_gone_from_source,
     both_task_keeps_two_positions].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Src = <<(atom_to_binary(TC))/binary, "_src">>,
    Tgt = <<(atom_to_binary(TC))/binary, "_tgt">>,
    [begin _ = barrel_docdb:delete_db(D), {ok, _} = barrel_docdb:create_db(D) end
     || D <- [Src, Tgt]],
    [{src, Src}, {tgt, Tgt} | Config].

end_per_testcase(_TC, Config) ->
    ok = ?T:clear(),
    _ = barrel_docdb:delete_db(?config(src, Config)),
    _ = barrel_docdb:delete_db(?config(tgt, Config)),
    ok.

%%====================================================================

poison_doc_pending_then_retried(Config) ->
    {Src, Tgt} = dbs(Config),
    put_docs(Src, 30),
    ?T:set(#{{put_version, <<"d7">>} => {error, boom}}),
    {ok, R1} = rep(Src, Tgt, #{batch_size => 5}),
    ?assertEqual([<<"d7">>], maps:get(pending, R1)),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Tgt, <<"d7">>)),
    {ok, _} = barrel_docdb:get_doc(Tgt, <<"d30">>),
    %% the next run retries it by id, though the position is past it
    ok = ?T:clear(),
    {ok, R2} = rep(Src, Tgt, #{batch_size => 5}),
    ?assertEqual([], maps:get(pending, R2)),
    {ok, _} = barrel_docdb:get_doc(Tgt, <<"d7">>).

poison_in_first_batch(Config) ->
    {Src, Tgt} = dbs(Config),
    put_docs(Src, 10),
    ?T:set(#{{put_version, <<"d1">>} => {error, boom}}),
    {ok, R1} = rep(Src, Tgt, #{batch_size => 3}),
    ?assertEqual([<<"d1">>], maps:get(pending, R1)),
    {ok, _} = barrel_docdb:get_doc(Tgt, <<"d10">>),
    ok = ?T:clear(),
    {ok, _} = rep(Src, Tgt, #{batch_size => 3}),
    all_there(Tgt, 10).

%% One side's checkpoint is written, the other's fails: the next run
%% starts from the lower position, so nothing is skipped.
target_checkpoint_write_fails(Config) ->
    one_side_fails(Config, tgt).

source_checkpoint_write_fails(Config) ->
    one_side_fails(Config, src).

one_side_fails(Config, Side) ->
    {Src, Tgt} = dbs(Config),
    put_docs(Src, 10),
    {ok, _} = rep(Src, Tgt, #{}),
    put_docs(Src, 11, 20),
    ?T:set(#{{put_local_doc, ?config(Side, Config)} => {error, down},
             {put_version, <<"d15">>} => {error, boom}}),
    ?assertMatch({error, {checkpoint_failed, _, down}}, rep(Src, Tgt, #{})),
    ok = ?T:clear(),
    {ok, _} = rep(Src, Tgt, #{}),
    all_there(Tgt, 20).

overflow_stops_without_skipping(Config) ->
    {Src, Tgt} = dbs(Config),
    put_docs(Src, 10),
    ?T:set(maps:from_list([{{put_version, id(I)}, {error, boom}}
                           || I <- [2, 4, 6]])),
    ?assertEqual({error, too_many_failed_docs},
                 rep(Src, Tgt, #{batch_size => 2, max_pending => 2})),
    ok = ?T:clear(),
    {ok, _} = rep(Src, Tgt, #{batch_size => 2, max_pending => 2}),
    all_there(Tgt, 10).

unreadable_checkpoint_aborts(Config) ->
    {Src, Tgt} = dbs(Config),
    put_docs(Src, 3),
    ?T:set(#{{get_local_doc, Tgt} => {error, down}}),
    ?assertMatch({error, {checkpoint_unreadable, target, down}},
                 rep(Src, Tgt, #{})),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Tgt, <<"d1">>)).

%% A target that can't move its clock aborts the batch: the position
%% doesn't advance and nothing becomes pending.
clock_skew_aborts_batch(Config) ->
    {Src, Tgt} = dbs(Config),
    put_docs(Src, 5),
    ?T:set(#{{put_version, <<"d3">>} => {error, clock_skew}}),
    ?assertEqual({error, clock_skew}, rep(Src, Tgt, #{})),
    ok = ?T:clear(),
    {ok, R} = rep(Src, Tgt, #{}),
    ?assertEqual([], maps:get(pending, R)),
    all_there(Tgt, 5).

retry_drops_ids_gone_from_source(Config) ->
    {Src, Tgt} = dbs(Config),
    put_docs(Src, 5),
    ?T:set(#{{put_version, <<"d2">>} => {error, boom}}),
    {ok, #{pending := [<<"d2">>]}} = rep(Src, Tgt, #{}),
    ?T:set(#{{get_doc, <<"d2">>} => {error, not_found}}),
    {ok, R} = rep(Src, Tgt, #{}),
    ?assertEqual([], maps:get(pending, R)).

%% A `both' task saves one position per direction.
both_task_keeps_two_positions(Config) ->
    {Src, Tgt} = dbs(Config),
    put_docs(Src, 5),
    {ok, _} = barrel_docdb:put_doc(Tgt, #{<<"id">> => <<"t1">>}),
    {ok, TaskId} = barrel_rep_tasks:start_task(#{source => Src, target => Tgt,
                                                 direction => both,
                                                 mode => one_shot}),
    try
        ok = wait(fun() ->
                      case barrel_rep_tasks:get_task(TaskId) of
                          {ok, #{status := completed}} -> true;
                          _ -> false
                      end
                  end, 100),
        {ok, Doc} = barrel_docdb:get_local_doc(<<"_replication_tasks">>,
                                               <<"rep_task:", TaskId/binary>>),
        ?assertNotEqual(<<"first">>, maps:get(<<"last_seq">>, Doc)),
        ?assertNotEqual(<<"first">>, maps:get(<<"pull_last_seq">>, Doc)),
        all_there(Tgt, 5),
        {ok, _} = barrel_docdb:get_doc(Src, <<"t1">>)
    after
        barrel_rep_tasks:delete_task(TaskId)
    end.

%%====================================================================

dbs(Config) -> {?config(src, Config), ?config(tgt, Config)}.

id(I) -> <<"d", (integer_to_binary(I))/binary>>.

put_docs(Db, N) -> put_docs(Db, 1, N).
put_docs(Db, From, To) ->
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => id(I), <<"i">> => I})
     || I <- lists:seq(From, To)],
    ok.

all_there(Db, N) ->
    [?assertMatch({ok, _}, barrel_docdb:get_doc(Db, id(I)))
     || I <- lists:seq(1, N)],
    ok.

rep(Src, Tgt, Opts) ->
    barrel_rep:replicate(Src, Tgt, Opts#{source_transport => ?T,
                                        target_transport => ?T,
                                        attachments => false,
                                        checkpoint_size => 1}).

wait(_Fun, 0) -> timeout;
wait(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(50), wait(Fun, N - 1)
    end.
