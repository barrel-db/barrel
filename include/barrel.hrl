
-define(DATA_DIR, "data").

-define(IMAX1, 16#ffffFFFFffffFFFF).

-define(jobs_broker, barrel_jobs_broker).
-define(jobs_pool, barrel_jobs_pool).

-define(db_stream_broker, barrel_db_stream_broker).

%% jobs limit
-define(JOBS_QUEUE_LIMIT, 128).
-define(JOBS_QUEUE_TIMEOUT, 15000).
-define(JOBS_IDLE_MIN_LIMIT, 32).
-define(JOBS_IDLE_MAX_LIMIT, 1000).

-define(barrel(Name), {n, l, {barrel, Name}}).
-define(store(Name), {n, l, {barrel_storage, Name}}).
-define(via_store(Name), {via, gproc, {n, l, {barrel_storage, Name}}}).


-define(store_provider(Name), {n, l, {barrel_store_provider, Name}}).


-define(index(Name), {n, l, {barrel_index, Name}}).

-define(STREAMS, barrel_db_streams).

-record(write_op, {
  type = merge :: merge | merge_with_conflict | purge,
  doc :: map(),
  from :: {pid(), reference()}
}).


-define(LOCAL_DOC_PREFIX, "_local").


-define(MIN_ITEM_COUNT, 100).
-define(MAX_ITEM_COUNT, 1000).


-define(MAX_WAIT, 16#ffffffff).

-define(WORKER_WAIT,
  application:get_env(barrel, worker_shutdown_timeout, 30000)).


-define(db_reader_pool(Name), {p, l, {barrel_db_reader_pool, Name}}).
-define(db_reader_sup(Name), {n, l, {barrel_db_reader_sup, Name}}).
-define(db_reader(Name, Id), {n, l, {barrel_db_reader, {Name, Id}}}).


-define(BARREL_CALL(From, Req), {'$BARREL_CALL', From, Req}).