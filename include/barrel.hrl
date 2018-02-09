
-define(DATA_DIR, "data").

-define(IMAX1, 16#ffffFFFFffffFFFF).

-define(jobs_broker, barrel_jobs_broker).
-define(jobs_pool, barrel_jobs_pool).

%% jobs limit
-define(JOBS_QUEUE_LIMIT, 128).
-define(JOBS_QUEUE_TIMEOUT, 5000).
-define(JOBS_IDLE_MIN_LIMIT, 32).
-define(JOBS_IDLE_MAX_LIMIT, 128).

-define(barrel(Id), {via, gproc, n, l, {barrel_db, Id}}).

-record(write_op, {
  type = merge :: merge | merge_with_conflict,
  doc :: map(),
  from :: {pid(), reference()}
}).



-define(MIN_ITEM_COUNT, 100).
-define(MAX_ITEM_COUNT, 1000).


-define(MAX_WAIT, 16#ffffffff).
-define(SUPERVISOR_WAIT,
  application:get_env(rabbit, supervisor_shutdown_timeout, infinity)).
-define(WORKER_WAIT,
  application:get_env(barrel, worker_shutdown_timeout, 30000)).
