%%%-------------------------------------------------------------------
%%% @doc barrel_docdb application module
%%%
%%% Starts and stops the barrel_docdb application.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% Application callbacks
%%====================================================================

%% @doc Start the barrel_docdb application
-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    %% Install instrument logger filter for trace context enrichment
    ok = instrument_logger:install(),
    %% Configure observability exporters
    ok = configure_tracing(),
    ok = configure_metrics(),
    ok = configure_logging(),
    logger:info("Starting barrel_docdb application"),
    barrel_docdb_sup:start_link().

%% @doc Stop the barrel_docdb application
-spec stop(term()) -> ok.
stop(_State) ->
    logger:info("Stopping barrel_docdb application"),
    %% Shutdown all exporters
    _ = instrument_exporter:shutdown(),
    _ = instrument_metrics_exporter:shutdown(),
    _ = instrument_log_exporter:shutdown(),
    %% Uninstall instrument logger filter
    instrument_logger:uninstall(),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Configure tracing based on application environment.
%%
%% Configuration options:
%% - {tracing, enabled} - Enable/disable tracing (default: true)
%% - {tracing, exporter} - console | otlp | {module, Config} (default: none)
%% - {tracing, otlp_endpoint} - OTLP endpoint URL (for otlp exporter)
%%
%% Example sys.config:
%% ```
%% {barrel_docdb, [
%%     {tracing, [
%%         {enabled, true},
%%         {exporter, console}  %% or otlp or none
%%     ]}
%% ]}
%% '''
-spec configure_tracing() -> ok.
configure_tracing() ->
    TracingConfig = application:get_env(barrel_docdb, tracing, []),
    Enabled = proplists:get_value(enabled, TracingConfig, true),
    case Enabled of
        true ->
            configure_exporter(TracingConfig);
        false ->
            %% Disable tracing globally for zero overhead
            instrument_config:set_tracing_enabled(false),
            logger:info("Tracing disabled globally"),
            ok
    end.

%% @doc Configure the span exporter based on configuration.
configure_exporter(TracingConfig) ->
    Exporter = proplists:get_value(exporter, TracingConfig, none),
    case Exporter of
        none ->
            ok;
        console ->
            Format = proplists:get_value(console_format, TracingConfig, text),
            _ = instrument_exporter:register(instrument_exporter_console:new(#{
                format => Format
            })),
            logger:info("Tracing enabled with console exporter"),
            ok;
        otlp ->
            Endpoint = proplists:get_value(otlp_endpoint, TracingConfig, "http://localhost:4318"),
            _ = instrument_exporter:register(instrument_exporter_otlp:new(#{
                endpoint => Endpoint
            })),
            logger:info("Tracing enabled with OTLP exporter, endpoint: ~s", [Endpoint]),
            ok;
        {Module, Config} when is_atom(Module), is_map(Config) ->
            _ = instrument_exporter:register(#{module => Module, config => Config}),
            logger:info("Tracing enabled with custom exporter: ~p", [Module]),
            ok;
        _ ->
            logger:warning("Unknown tracing exporter configuration: ~p", [Exporter]),
            ok
    end.

%% @doc Configure metrics exporter based on application environment.
%%
%% Configuration options:
%% - {metrics, exporter} - none | console | otlp (default: none)
%% - {metrics, otlp_endpoint} - OTLP endpoint URL
%% - {metrics, otlp_headers} - Additional HTTP headers
%% - {metrics, otlp_compression} - none | gzip
%% - {metrics, export_interval} - Export interval in ms (default: 60000)
-spec configure_metrics() -> ok.
configure_metrics() ->
    MetricsConfig = application:get_env(barrel_docdb, metrics, []),
    Exporter = proplists:get_value(exporter, MetricsConfig, none),
    case Exporter of
        none ->
            ok;
        console ->
            _ = instrument_metrics_exporter:register(
                instrument_metrics_exporter_console:new(#{})),
            logger:info("Metrics exporter enabled: console"),
            ok;
        otlp ->
            Endpoint = proplists:get_value(otlp_endpoint, MetricsConfig, "http://localhost:4318"),
            Headers = proplists:get_value(otlp_headers, MetricsConfig, #{}),
            Compression = proplists:get_value(otlp_compression, MetricsConfig, none),
            _ = instrument_metrics_exporter:register(
                instrument_metrics_exporter_otlp:new(#{
                    endpoint => Endpoint,
                    headers => Headers,
                    compression => Compression
                })),
            logger:info("Metrics exporter enabled: OTLP (~s)", [Endpoint]),
            ok;
        {Module, Config} when is_atom(Module), is_map(Config) ->
            _ = instrument_metrics_exporter:register(#{module => Module, config => Config}),
            logger:info("Metrics exporter enabled: ~p", [Module]),
            ok;
        _ ->
            logger:warning("Unknown metrics exporter configuration: ~p", [Exporter]),
            ok
    end.

%% @doc Configure logging exporter based on application environment.
%%
%% Configuration options:
%% - {logging, exporter} - none | console | file | otlp (default: none)
%% - {logging, file_path} - Log file path (for file exporter)
%% - {logging, file_format} - text | json (default: text)
%% - {logging, file_max_size} - Max file size in bytes (default: 10MB, 0 = unlimited)
%% - {logging, file_max_files} - Number of rotated files (default: 5)
%% - {logging, file_compress} - Compress rotated files (default: false)
%% - {logging, otlp_endpoint} - OTLP endpoint URL
%% - {logging, otlp_headers} - Additional HTTP headers
%% - {logging, otlp_compression} - none | gzip
-spec configure_logging() -> ok.
configure_logging() ->
    LoggingConfig = application:get_env(barrel_docdb, logging, []),
    Exporter = proplists:get_value(exporter, LoggingConfig, none),
    case Exporter of
        none ->
            ok;
        console ->
            _ = instrument_log_exporter:register(
                instrument_log_exporter_console:new(#{})),
            logger:info("Log exporter enabled: console"),
            ok;
        file ->
            Path = proplists:get_value(file_path, LoggingConfig, "/var/log/barrel.log"),
            Format = proplists:get_value(file_format, LoggingConfig, text),
            MaxSize = proplists:get_value(file_max_size, LoggingConfig, 10485760),
            MaxFiles = proplists:get_value(file_max_files, LoggingConfig, 5),
            Compress = proplists:get_value(file_compress, LoggingConfig, false),
            _ = instrument_log_exporter:register(
                instrument_log_exporter_file:new(#{
                    path => Path,
                    format => Format,
                    max_size => MaxSize,
                    max_files => MaxFiles,
                    compress => Compress
                })),
            logger:info("Log exporter enabled: file (~s)", [Path]),
            ok;
        otlp ->
            Endpoint = proplists:get_value(otlp_endpoint, LoggingConfig, "http://localhost:4318"),
            Headers = proplists:get_value(otlp_headers, LoggingConfig, #{}),
            Compression = proplists:get_value(otlp_compression, LoggingConfig, none),
            _ = instrument_log_exporter:register(
                instrument_log_exporter_otlp:new(#{
                    endpoint => Endpoint,
                    headers => Headers,
                    compression => Compression
                })),
            logger:info("Log exporter enabled: OTLP (~s)", [Endpoint]),
            ok;
        {Module, Config} when is_atom(Module), is_map(Config) ->
            _ = instrument_log_exporter:register(#{module => Module, config => Config}),
            logger:info("Log exporter enabled: ~p", [Module]),
            ok;
        _ ->
            logger:warning("Unknown logging exporter configuration: ~p", [Exporter]),
            ok
    end.
