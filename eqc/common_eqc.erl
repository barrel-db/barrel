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
    {ok, _} = application:ensure_all_started(barrel),
    fun delete_db/0.

cleanup() ->
    Dbs = barrel:database_names(),
    [ok = barrel:delete_database(D)|| D<- Dbs],

   % [] = barrel:database_names(),
    ok.



delete_db() ->
    cleanup(),
    application:stop(barrel),

    ok.



ascii_string() ->
    ?LET({S,Ss},
         {choose($a,$z),
          non_empty(list(oneof([choose($a,$z),
                                choose($0,$9)
                               ]
                               )))},
         list_to_binary([S|Ss] )
        ).
