-ifdef('LAGER').

-include_lib("lager/include/lager.hrl").

-define(LOG_INFO(Format, Args), ?lager_info(Format, Args)).
-define(LOG_ERROR(Format, Args), ?lagger_error(Format, Args)).
-define(LOG_WARNING(Format, Args), ?lager_warning(Format, Args)).
-define(LOG_DEBUG(Format, Args), ?lager_debug(Format, Args)).
-else.
-ifdef('OTP_RELEASE').
-include_lib("kernel/include/logger.hrl").
-else.

-define(__fmt(__Fmt, __Args), lists:flatten(io_lib:format(__Fmt, __Args))).

-define(LOG_INFO(Format, Args), error_logger:info_msg(Format, Args)).
-define(LOG_ERROR(Format, Args), error_logger:error_msg(Format, Args)).
-define(LOG_WARNING(Format, Args), error_logger:warning_msg(Format, Args)).
-define(LOG_DEBUG(Format, Args),
  ((fun() ->
      __CurrentLevel = application:get_env(kernel, logger_level, error),
      if
        __CurrentLevel =:= debug; __CurrentLevel =:= all ->
          error_logger:error_msg([{level, debug}, _fmt(Format, Args)]);
        true -> ok
      end
    end)())).
-endif.
-endif.


-define(ANY_LOG(Level, Format, Args),
  case Level of
    info -> ?LOG_INFO(Format, Args);
    error -> ?LOG_ERROR(Format, Args);
    warning -> ?LOG_WARNING(Format, Args);
    debug -> ?LOG_DEBUG(Format, Args)
  end).