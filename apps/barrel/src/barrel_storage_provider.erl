%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Nov 2018 11:26
%%%-------------------------------------------------------------------
-module(barrel_storage_provider).
-author("benoitc").


-callback init_store(Name :: term(), Options :: any()) -> ok.