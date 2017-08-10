%%% @author Zachary Kessin <>
%%% @copyright (C) 2017, Zachary Kessin
%%% @doc
%%%
%%% @end
%%% Created : 19 Jul 2017 by Zachary Kessin <>

-module(common_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).


init_db()->
    lager:set_loglevel(lager_console_backend, error),

    {ok, _} = application:ensure_all_started(barrel),
    fun delete_db/0.

cleanup() ->
    Dbs = barrel:database_names(),
    [ok = barrel:delete_database(D)|| D<- Dbs],
    application:stop(barrel),
    ok.



delete_db() ->
    cleanup(),
    application:stop(barrel),

    ok.



ascii_string() ->
    ?LET(S,
         non_empty(list(oneof([choose($0,$9),
                               choose($A,$Z),
                               choose($a,$z)]))),
         list_to_binary(S)
        ).
