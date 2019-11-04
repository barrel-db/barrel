
-include_lib("barrel_stdlib/include/barrel_stdlib.hrl").


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


-define(barrel(Name), {n, l, {barrel, Name}}).


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
