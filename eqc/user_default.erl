-module(user_default).
-compile(export_all).
-define(DB, <<"testdb3">>).

setup() ->
    application:ensure_all_started(lager),
    net_adm:ping('barrel2@localhost'),
    sync:go(),

    ok.

init_db() ->
    barrel_httpc_eqc:init_db().

run(X) ->
    barrel_httpc_eqc:run(X).

eqc() ->
    eqc(30).

eqc(N) ->
    %application:ensure_all_started(lager),
    %lager:set_loglevel(lager_console_backend, notice),
    [] = eqc:module({numtests,N}, barrel_rpc_events_eqc),
    ok.



lt() ->
    lager:error("~n********************************************************************************~n~n~n~n",[]),
    eqc:check(barrel_rpc_events_eqc:prop_barrel_rpc_events_eqc(), eqc:counterexample()).


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