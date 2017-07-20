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
    lager:set_loglevel(lager_console_backend, notice),
    [] = eqc:module({numtests,100}, create_delete_database_eqc),
    [] = eqc:module({numtests,15}, barrel_rpc_events_eqc),
    [] = eqc:module({numtests,250}, barrel_rpc_eqc),
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
