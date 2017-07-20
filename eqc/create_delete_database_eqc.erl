%%% @author Zachary Kessin <>
%%% @copyright (C) 2017, Zachary Kessin
%%% @doc
%%%
%%% @end
%%% Created : 19 Jul 2017 by Zachary Kessin <>

-module(create_delete_database_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).


prop_create_delete_db() ->
    ?FORALL(DBS,
						non_empty(list(utf8(10))),
            begin
                L = length( barrel:database_names()),
                [begin
										 Id = I,
                     {ok, #{<<"database_id">> := Id}} = 
												 barrel:create_database(#{<<"database_id">> => Id}),
                     barrel:post(Id, #{<<"id">> => <<"1234">>,<<"content">> => <<"A">>}, #{}),
                     ok = barrel:delete_database(Id)
                 end|| I <- DBS],
                L1 =  length( barrel:database_names()),
                L == L1
            end).


