%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Nov 2018 05:11
%%%-------------------------------------------------------------------
-author("benoitc").

-define(DB_OPEN_RETRIES, 30).
-define(DB_OPEN_RETRY_DELAY, 2000).

-define(rdb_store(Name), {n, l, {barrel_rocksdb_store, Name}}).
-define(rdb_cache(Name), {n, l, {barrel_rocksdb_cache, Name}}).