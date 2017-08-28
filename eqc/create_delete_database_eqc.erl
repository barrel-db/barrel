%%% @author Zachary Kessin <>
%%% @copyright (C) 2017, Zachary Kessin
%%% @doc
%%%
%%% @end
%%% Created : 19 Jul 2017 by Zachary Kessin <>

-module(create_delete_database_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).



prop_create_delete_database() ->
    ?SETUP(fun common_eqc:init_db/0,
    ?FORALL(DBS,
            non_empty(list(common_eqc:ascii_string())),
            begin
                L = length( barrel:database_names()),
                [begin

                     {ok, #{<<"database_id">> := Id}} =
                         barrel:create_database(#{<<"database_id">> => Id}),
                     barrel:post(Id, #{<<"id">> => <<"1234">>,<<"content">> => <<"A">>}, #{}),
                     ok = barrel:delete_database(Id),
                     {error, db_not_found} = barrel:post(Id, #{<<"id">> => <<"1234">>,<<"content">> => <<"A">>}, #{})
                 end|| Id <- DBS],
                L1 =  length( barrel:database_names()),
                L == L1
            end)).
