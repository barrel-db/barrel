-module(barrel_testing).

-export([test/0]).

new_doc(Bin0) ->
  Bin = barrel_lib:make_uid(Bin0),

  #{ <<"a">> => 1,
     <<"_attachments">> => #{ <<"media">> => #{ <<"data">> => Bin } }
   }.

put_doc(Barrel, Bin) ->
  Doc0 = new_doc(Bin),
  Doc = Doc0#{ <<"id">> => barrel_id:binary_id(62) },
  timer:tc(fun() ->
               barrel:save_doc(Barrel, Doc)
           end).


read_doc(Barrel, DocId) ->
  {T, Res} = timer:tc(fun() ->
               barrel:fetch_doc(Barrel, DocId, #{})
           end),

  OK = case Res of
         {ok, _} -> true;
         _ -> false
       end,

  io:format("read time=~p ok=~p~n", [T, OK]).

worker(Barrel, Bin) ->
  {T, Res} = put_doc(Barrel, Bin),
  io:format("time=~p, res=~p~n", [T, Res]),
  case Res of
    {ok, DocId, _} ->
      spawn(fun() -> read_doc(Barrel, DocId) end);
    _ ->
      ok
  end,

  timer:sleep(rand:uniform(150)),
  worker(Barrel, Bin).

test() ->
  _ = (catch barrel:delete_barrel(<<"test">>)),
  ok = barrel:create_barrel(<<"test">>),
  {ok, Barrel} = barrel:open_barrel(<<"test">>),
  Bin =  iolist_to_binary([<<"X">> || _I <- lists:seq(1, 2048 * 1000)]),
  erlang:send_after(1000 * 60 * 5, self(), done),
  Pids = [spawn(fun() -> worker(Barrel, Bin) end) || _I <- lists:seq(1, 10)],
  receive
    done ->
      [erlang:exit(Pid, kill) || Pid <- Pids]
  end.




