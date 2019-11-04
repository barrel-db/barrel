-module(barrel_buffer_SUITE).

%% SUTE API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).




%%  exported tests
-export([
         basic/1,
         blocking_writer/1,
         blocking_writer_2/1,
         blocking_reader/1,
         dequeue_all/1,
         close_on_dequeue/1
        ]).

all() ->
  [
   basic,
   blocking_writer,
   blocking_writer,
   blocking_reader,
   dequeue_all,
   close_on_dequeue
  ].

init_per_suite(Config) -> Config.
end_per_suite(Config)-> Config.

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, _Config) -> ok.


basic(_Config) ->
  {ok, B} = barrel_buffer:start_link([]),

  ok = barrel_buffer:enqueue(B, a),
  #{ num := 1 } = barrel_buffer:infos(B),
  {ok, [a]} = barrel_buffer:dequeue(B, 1),
  #{ num := 0 } = barrel_buffer:infos(B),

  {ok, []} = barrel_buffer:dequeue(B, 0),

  ok = barrel_buffer:enqueue(B, a),
  ok = barrel_buffer:enqueue(B, b),
  ok = barrel_buffer:enqueue(B, c),
  ok = barrel_buffer:enqueue(B, d),

  ok = barrel_buffer:enqueue(B, e),
  ok = barrel_buffer:enqueue(B, f),
  #{ num := 6 } = barrel_buffer:infos(B),

  {ok, [a, b, c, d]} = barrel_buffer:dequeue(B, 4),

  #{ num := 2 } = barrel_buffer:infos(B),

  {ok, [e, f]} = barrel_buffer:dequeue(B, 3),

  #{ num := 0 } = barrel_buffer:infos(B),
  barrel_buffer:close(B).


blocking_writer(_Config) ->
  {ok, B} = barrel_buffer:start_link([]),
  Self = self(),
  WriterPid = spawn_link(fun() ->
                             Res = barrel_buffer:dequeue(B, 1),
                             Self ! {self(), Res}
                         end),
  ok = barrel_buffer:enqueue(B, a),
  receive
    {WriterPid, {ok, [a]}} -> ok
  end,
  barrel_buffer:close(B).



blocking_writer_2(_Config) ->
  {ok, B} = barrel_buffer:start_link([]),
  Self = self(),
  WriterPid = spawn_link(fun() ->
                             Res = barrel_buffer:dequeue(B, 2),
                             Self ! {self(), Res}
                         end),
  ok = barrel_buffer:enqueue(B, a),
  ok = barrel_buffer:enqueue(B, b),
  receive
    {WriterPid, {ok, [a, b]}} -> ok
  end,
  barrel_buffer:close(B).


blocking_reader(_Config) ->
  {ok, B} = barrel_buffer:start_link([{max_items, 2}]),
  Self = self(),
  ReaderPid = spawn_link(fun() ->
                             Self ! {self(), reader_ready},
                             ok = barrel_buffer:enqueue(B, a),
                             Self ! {self(), reader_ready},
                             ok = barrel_buffer:enqueue(B, b),
                             Self ! {self(), reader_ready}
                         end),

  [ok, ok] = wait_reader(ReaderPid, []),
  {ok, [a, b]} = barrel_buffer:dequeue(B, 3),
  [ok] = wait_reader(ReaderPid, []),
  barrel_buffer:close(B),
  {ok, B1} = barrel_buffer:start_link([]),
  ReaderPid1 = spawn_link(fun() ->
                             Self ! {self(), reader_ready},
                             ok = barrel_buffer:enqueue(B1, a),
                             Self ! {self(), reader_ready},
                             ok = barrel_buffer:enqueue(B1, b),
                             Self ! {self(), reader_ready}
                         end),
  [ok, ok, ok] = wait_reader(ReaderPid1, []),
  {ok, [a, b]} = barrel_buffer:dequeue(B1, 3),
  barrel_buffer:close(B1).

wait_reader(ReaderPid, Acc) ->
  receive
    {ReaderPid, reader_ready} ->
      wait_reader(ReaderPid, [ok | Acc])
  after 100 ->
          Acc
  end.

dequeue_all(_Config) ->
  {ok, B} = barrel_buffer:start_link([]),
  ok = barrel_buffer:enqueue(B, a),
  ok = barrel_buffer:enqueue(B, b),
  ok = barrel_buffer:enqueue(B, c),
  ok = barrel_buffer:enqueue(B, d),
  ok = barrel_buffer:enqueue(B, e),
  ok = barrel_buffer:enqueue(B, f),
  {ok, [a, b, c, d, e, f]} = barrel_buffer:dequeue(B),
  barrel_buffer:close(B).


close_on_dequeue(_Config) ->
  {ok, B} = barrel_buffer:start_link([]),

  ok = barrel_buffer:enqueue(B, a),
  ok = barrel_buffer:enqueue(B, b),
  barrel_buffer:close(B),

  {ok, [a]} = barrel_buffer:dequeue(B, 1),
  {ok, [b]} = barrel_buffer:dequeue(B, 1),
  ok.
