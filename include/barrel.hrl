
-type barrel_name() :: binary().
-type barrel() :: term().
-type barrel_config() :: map().


-define(APP, barrel).

-define(VIEWS, barrel_views).

-define(DATA_DIR, "data").
-define(DEFAULT_ADAPTER, "barrel_rocksdb").


-define(STORE, (barrel_config:storage())).

%% -----------------
%% -- attachements

-define(att(Path), {n, l, {barrel_attachment, Path}}).
-define(att_proc(Path), {via, gproc, ?att(Path)}).

%% -----------------
%% -- loggin


-define(LOG_DEBUG(Fmt, Args), ?DISPATCH_LOG(debug, Fmt, Args)).
-define(LOG_INFO(Fmt, Args), ?DISPATCH_LOG(info, Fmt, Args)).
-define(LOG_NOTICE(Fmt, Args), ?DISPATCH_LOG(notice, Fmt, Args)).
-define(LOG_WARNING(Fmt, Args), ?DISPATCH_LOG(warning, Fmt, Args)).
-define(LOG_ERROR(Fmt, Args), ?DISPATCH_LOG(error, Fmt, Args)).

-define(DISPATCH_LOG(Level, Fmt, Args),
        case barrel_config:get('$barrel_logger') of
          logger ->
            logger:log(Level, Fmt, Args,
                       #{mfa => {?MODULE,
                                 ?FUNCTION_NAME,
                                 ?FUNCTION_ARITY},
                         file => ?FILE,
                         line => ?LINE});
          lager ->
            lager:log(Level, [{pid, self()},
                              {mfa, {?MODULE,
                                     ?FUNCTION_NAME,
                                     ?FUNCTION_ARITY}},
                              {file, ?FILE},
                              {line, ?LINE}], Fmt, Args);
          _ ->
            ok
        end).

-define(barrel(Name), {n, l, {barrel, Name}}).
-define(IMAX1, 16#ffffFFFFffffFFFF).
