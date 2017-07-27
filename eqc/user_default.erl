-module(user_default).
-compile(export_all).
-define(DB, <<"testdb3">>).

setup() ->
    sync:go(),

    ok.

init_db() ->
    barrel_httpc_eqc:init_db().

run(X) ->
     barrel_httpc_eqc:run(X).

eqc() ->
    eqc(30).

eqc(N) ->
    lager:set_loglevel(lager_console_backend, notice),
    [] = eqc:module({numtests,N}, barrel_rpc_events_eqc),
    [] = eqc:module({numtests,N}, create_delete_database_eqc),
    [] = eqc:module({numtests,N}, barrel_rpc_eqc),

 
    ok.
lt() ->
    eqc:check(barrel_rpc_events_eqc:prop_barrel_rpc_events_eqc(), 
              [[{model,barrel_rpc_events_eqc},
               {init,
                {state,
                 [<<"test01">>,<<"test02">>],
                 0,
                 {dict,0,16,16,8,80,48,
                  {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
                  {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}},
                 true,false,
                 {set,0,16,16,8,80,48,
                  {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
                  {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}}}},
               {set,
                {var,1},
                {call,barrel,post,
                 [<<"test01">>,
                  #{<<"content">> => <<0,0,0,0,0,0,0,0>>,
                    <<"id">> => <<217,161,199,187,195,190,2,217,154,198,154>>},
                  #{}]}},
               {set,
                {var,2},
                {call,barrel_rpc_events_eqc,put,
                 [<<"test01">>,
                  <<217,161,199,187,195,190,2,217,154,198,154>>,
                  #{<<"content">> => <<0,0,0,0>>,
                    <<"newcontent">> => <<0,0,0,0,0,0,0,0,0,0,0,0,0,0>>},
                  #{}]}},
               {set,
                {var,3},
                {call,barrel_rpc_events_eqc,get,
                 [<<"test01">>,
                  <<217,161,199,187,195,190,2,217,154,198,154>>,
                  {dict,1,16,16,8,80,48,
                   {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
                   {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],
                     [[<<217,161,199,187,195,190,2,217,154,198,154>>|{var,1}]]}}},
                  history]}}]]
             ).

create_delete() ->
    DB     = uuid:get_v4_urandom(),
    barrel:delete_database(DB),
    N      = barrel:database_names(),
    {ok,_} = barrel:create_database(#{<<"database_id">> => DB}),
    ok     = barrel:delete_database(DB),
    N      = barrel:database_names(),
    ok.


postget(Id) ->

    {ok, _,_} = barrel:post(?DB,
                            #{<<99, 111, 110, 116, 101, 110, 116>>
                                  => <<0, 0, 0, 0, 0, 0, 0, 0>>,
                              <<"id">>
                                  => Id
                             },
                            #{}),
    barrel:get(?DB,
               Id,
               #{}).

names() ->
    barrel:database_names().


cleanup() ->
    [barrel:create_database(#{<<"database_id">> => uuid:get_v4()}) 
     || _ <-  lists:seq(1,100)],
    Dbs = barrel:database_names(),
    [begin
         ok = barrel:delete_database(D)
     end|| D<- Dbs],
    [] = barrel:database_names(),
    ok.


no_delete() ->
    Id = uuid:get_v4(),
    {ok, #{<<"database_id">> := Id}} = barrel:create_database(#{<<"database_id">> => Id}),
    ok = barrel:delete_database(Id),
    {error, not_found} = barrel:database_infos(Id),
    ok = barrel:delete_database(Id),
    ok = barrel:delete_database(Id),
    {error,not_found} = barrel:database_infos(Id).
