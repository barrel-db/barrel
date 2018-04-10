
-define(DATA_DIR, "data").

-define(IMAX1, 16#ffffFFFFffffFFFF).

-define(jobs_broker, barrel_jobs_broker).
-define(jobs_pool, barrel_jobs_pool).

%% jobs limit
-define(JOBS_QUEUE_LIMIT, 128).
-define(JOBS_QUEUE_TIMEOUT, 5000).
-define(JOBS_IDLE_MIN_LIMIT, 32).
-define(JOBS_IDLE_MAX_LIMIT, 128).

-define(barrel(Name), {n, l, {barrel, Name}}).
-define(store(Name), {n, l, {barrel_storage, Name}}).
-define(via_store(Name), {via, gproc, {n, l, {barrel_storage, Name}}}).



-record(write_op, {
  type = merge :: merge | merge_with_conflict | purge,
  doc :: map(),
  from :: {pid(), reference()}
}).



-define(MIN_ITEM_COUNT, 100).
-define(MAX_ITEM_COUNT, 1000).


-define(MAX_WAIT, 16#ffffffff).

-define(WORKER_WAIT,
  application:get_env(barrel, worker_shutdown_timeout, 30000)).


