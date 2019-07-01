
-type barrel_name() :: binary().
-type barrel() :: term().
-type barrel_config() :: map().





-define(APP, barrel).

-define(VIEWS, barrel_views).

-define(DATA_DIR, "data").
-define(DEFAULT_ADAPTER, "barrel_rocksdb").
-define(TS_FILE, "BARREL_TS").


-define(STORE, (barrel_config:storage())).
-define(EPOCH_STORE, (barrel_config:epoch_store())).



%% -----------------
%% -- attachements

-define(att(Path), {n, l, {barrel_attachment, Path}}).
-define(att_proc(Path), {via, gproc, ?att(Path)}).


%% -----------------
%% -- replication

-define(endpoint_listener(Name), {n, l, {barrel_endpoint_listener, Name}}).
-define(endpoint_listener_proc(Name), {via, gproc, ?endpoint_listener(Name)}).


-record(erlang_endpoint, {node :: node(),
                          barrel :: barrel_name() }).

-record(barrel_endpoint, {barrel :: barrel_name() }).


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


-define(MFA_SPAN_NAME,
        iolist_to_binary(
          io_lib:format("~s:~s/~p",
                        [?MODULE_STRING, ?FUNCTION_NAME, ?FUNCTION_ARITY]
                       )
         )
       ).

-define(start_span,
        _ = ocp:with_child_span(?MFA_SPAN_NAME)
       ).

-define(start_span(Attrs),
        _ = ocp:with_child_span(?MFA_SPAN_NAME, Attrs)
       ).

-define(end_span,
        ocp:finish_span()
       ).


-define(TRY_UPDATE(F), try F()
                       catch C:E:T ->
                               ?LOG_ERROR("update error=~p~n", [E]),
                               ?LOG_DEBUG("update class=~p error=~p traceback=~p~n", [C,E,T]),
                               exit(update_error)
                       end).
