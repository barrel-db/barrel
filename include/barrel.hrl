
-define(DATA_DIR, "data").

-define(default_fold_options,
  #{
    start_key => nil,
    end_key => nil,
    gt => nil,
    gte => nil,
    lt => nil,
    lte => nil,
    max => 0,
    move => next
  }
).

-record(db, {
  name = <<>>,
  id = <<>>,
  path = <<>>,
  db_opts = [],
  conf = #{},
  pid = nil,
  ref = nil,
  store = nil,
  last_rid = 0,
  updated_seq = 0,
  docs_count = 0,
  system_docs_count = 0,
  deleted_count = 0,
  deleted = false,
  write_buffer_size = 64 * 1024 * 1024,
  write_interval = 200
}).


%% db metadata
-define(DB_INFO, 1).

-define(DEFAULT_CHANGES_SIZE, 200).

-record(doc, {
  id :: binary(),
  revs :: [binary()],
  body :: map(),
  deleted = false :: boolean()
}).


-define(MAX_WAIT, 16#ffffffff).
-define(SUPERVISOR_WAIT,
  application:get_env(rabbit, supervisor_shutdown_timeout, infinity)).
-define(WORKER_WAIT,
  application:get_env(barrel, worker_shutdown_timeout, 30000)).
