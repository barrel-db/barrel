%%%-------------------------------------------------------------------
%%% @doc Garbage-collects abandoned S3 multipart uploads. A hard crash
%%% between starting and completing a multipart upload leaves orphaned
%%% parts that S3 keeps billing until aborted or expired --
%%% `barrel_att_s3_store:abort_stream/1' only covers a graceful
%%% in-process failure, not a crash. `open/2' registers each store's
%%% `{Endpoint, Bucket, Prefix}' here; a periodic pass lists in-progress
%%% uploads under each registered prefix and aborts the ones older than
%%% `multipart_gc_max_age'. See docs/limitations.md.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_multipart_gc).
-behaviour(gen_server).

-export([start_link/0, register_target/4, deregister_target/3,
         sweep_now/0, sweep_now/1, abort_stale/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(DEFAULT_INTERVAL, 3600000). %% 1h, ms
-define(DEFAULT_MAX_AGE, 86400).    %% 24h, seconds -- matches the bucket
                                     %% lifecycle rule docs/limitations.md
                                     %% already recommends as a backstop

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Called by open/2 right after its S3 client exists. Idempotent --
%% re-registering the same {Endpoint, Bucket, Prefix} just replaces the
%% stored client.
-spec register_target(binary(), binary(), binary(), term()) -> ok.
register_target(Endpoint, Bucket, Prefix, Client) ->
    gen_server:cast(?MODULE, {register, Endpoint, Bucket, Prefix, Client}).

%% @doc Called by destroy/2 -- that prefix will never need sweeping again,
%% unlike close/1 (a bucket can still be shared by other open stores).
-spec deregister_target(binary(), binary(), binary()) -> ok.
deregister_target(Endpoint, Bucket, Prefix) ->
    gen_server:cast(?MODULE, {deregister, Endpoint, Bucket, Prefix}).

%% @doc Test/ops hook: run one pass now, using the configured max age.
-spec sweep_now() -> {ok, non_neg_integer()}.
sweep_now() ->
    gen_server:call(?MODULE, sweep, infinity).

%% @doc Same, with an explicit age override -- 0 means "everything is
%% stale," for deterministic tests.
-spec sweep_now(non_neg_integer()) -> {ok, non_neg_integer()}.
sweep_now(MaxAgeSec) when is_integer(MaxAgeSec), MaxAgeSec >= 0 ->
    gen_server:call(?MODULE, {sweep, MaxAgeSec}, infinity).

%% @doc Sweep one target directly, no gen_server involved -- so a caller
%% like destroy/2 still works even if the sweeper process is down.
-spec abort_stale(term(), binary(), binary(), non_neg_integer()) ->
    non_neg_integer().
abort_stale(Client, Bucket, Prefix, MaxAgeSec) ->
    sweep_bucket(Client, Bucket, [Prefix], MaxAgeSec).

%%====================================================================
%% gen_server -- State :: #{{Endpoint :: binary(), Bucket :: binary(),
%%                           Prefix :: binary()} => livery_s3:client() (term())}
%%====================================================================

init([]) ->
    arm(),
    {ok, #{}}.

handle_call(sweep, _From, State) ->
    {reply, {ok, do_sweep(State, configured_max_age())}, State};
handle_call({sweep, MaxAgeSec}, _From, State) ->
    {reply, {ok, do_sweep(State, MaxAgeSec)}, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({register, Endpoint, Bucket, Prefix, Client}, State) ->
    {noreply, State#{{Endpoint, Bucket, Prefix} => Client}};
handle_cast({deregister, Endpoint, Bucket, Prefix}, State) ->
    {noreply, maps:remove({Endpoint, Bucket, Prefix}, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep, State) ->
    _ = do_sweep(State, configured_max_age()),
    arm(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%====================================================================
%% Internal
%%====================================================================

%% @private A bad (non-integer, non-positive) configured interval must
%% never raise here: this runs inside init/1, so a crash fails the whole
%% barrel_att_s3_sup on its first start, not just this one child -- worse
%% than just leaving GC disabled. Treated the same as the documented `0'.
arm() ->
    case application:get_env(barrel_att_s3, multipart_gc_interval, ?DEFAULT_INTERVAL) of
        Interval when is_integer(Interval), Interval > 0 ->
            erlang:send_after(Interval, self(), sweep), ok;
        _ ->
            ok
    end.

configured_max_age() ->
    application:get_env(barrel_att_s3, multipart_gc_max_age, ?DEFAULT_MAX_AGE).

%% @private Grouped by {Endpoint, Bucket} first so N registered prefixes
%% sharing one bucket cost one full-bucket listing per sweep, not N.
do_sweep(Targets, MaxAgeSec) ->
    maps:fold(
        fun({_Endpoint, Bucket}, {Client, Prefixes}, Total) ->
            Total + sweep_bucket(Client, Bucket, Prefixes, MaxAgeSec)
        end, 0, group_by_bucket(Targets)).

group_by_bucket(Targets) ->
    maps:fold(
        fun({Endpoint, Bucket, Prefix}, Client, Acc) ->
            Key = {Endpoint, Bucket},
            {_, Prefixes} = maps:get(Key, Acc, {Client, []}),
            Acc#{Key => {Client, [Prefix | Prefixes]}}
        end, #{}, Targets).

%% @private One bucket's failure must never crash this gen_server -- the
%% registry lives only in its State, and a crash-and-restart under
%% one_for_one would silently wipe every registration accumulated since
%% boot (already-open stores only re-register at their next open/2).
%%
%% Filters by prefix CLIENT-side rather than passing `prefix' to
%% `list_multipart_uploads' -- confirmed against real MinIO that its
%% ListMultipartUploads ignores/mishandles a non-empty `prefix' query
%% param (returns zero results even for a prefix that's a true match),
%% unlike plain object listing (`list_objects_all', used elsewhere in
%% this module, which honors `prefix' correctly). Fetches every
%% in-progress upload in the bucket once per sweep, shared across every
%% prefix registered for that bucket, instead of a server-filtered subset
%% per prefix.
sweep_bucket(Client, Bucket, Prefixes, MaxAgeSec) ->
    try
        ListPrefixes = [<<P/binary, "/">> || P <- Prefixes],
        case list_multipart_uploads_all(Client, Bucket, #{}) of
            {ok, Uploads} ->
                NowSec = erlang:system_time(second),
                lists:foldl(
                    fun(Upload, Count) ->
                        #{key := Key} = Upload,
                        case lists:any(fun(LP) -> under_prefix(Key, LP) end, ListPrefixes)
                             andalso is_stale(Upload, NowSec, MaxAgeSec) of
                            true -> Count + abort_one(Client, Bucket, Upload);
                            false -> Count
                        end
                    end, 0, Uploads);
            {error, Reason} ->
                logger:warning("multipart GC list failed for ~s: ~p",
                               [Bucket, Reason]),
                0
        end
    catch
        Class:CrashReason:Stack ->
            logger:warning("multipart GC sweep crashed for ~s: ~p:~p~n~p",
                           [Bucket, Class, CrashReason, Stack]),
            0
    end.

%% A malformed <Upload> entry missing its <Key> parses to `key => undefined'
%% (see parse_multipart_uploads/1 in the livery_s3 dependency) -- skip it
%% rather than let byte_size/1 crash the whole bucket's sweep over one bad
%% entry.
under_prefix(undefined, _Prefix) ->
    false;
under_prefix(Key, Prefix) ->
    Size = byte_size(Prefix),
    byte_size(Key) >= Size andalso binary:part(Key, 0, Size) =:= Prefix.

is_stale(_Upload, _NowSec, 0) ->
    true; %% explicit override -- caller wants everything, skip parsing
is_stale(#{initiated := Initiated}, NowSec, MaxAgeSec) ->
    case parse_initiated(Initiated) of
        {ok, InitiatedSec} -> (NowSec - InitiatedSec) >= MaxAgeSec;
        error -> false %% can't prove it's abandoned -- never guess-abort
    end.

%% Same rfc3339 parsing already used for S3's own timestamps elsewhere
%% (livery_s3_credentials.erl, barrel_server_http.erl).
parse_initiated(Initiated) ->
    try calendar:rfc3339_to_system_time(binary_to_list(Initiated), [{unit, second}]) of
        Sec -> {ok, Sec}
    catch
        _:_ -> error
    end.

abort_one(Client, Bucket, #{key := Key, upload_id := UploadId}) ->
    case livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId) of
        ok -> 1;
        {error, Reason} ->
            logger:warning("multipart GC abort failed for ~s ~s: ~p",
                           [Bucket, Key, Reason]),
            0
    end.

%% Mirrors livery_s3:list_all_loop/5's shape -- no list-everything helper
%% exists for list_multipart_uploads/3.
list_multipart_uploads_all(Client, Bucket, Opts) ->
    list_multipart_uploads_loop(Client, Bucket, Opts, []).

list_multipart_uploads_loop(Client, Bucket, Opts, Acc) ->
    case livery_s3:list_multipart_uploads(Client, Bucket, Opts) of
        {ok, #{uploads := U, is_truncated := true,
               next_key_marker := KM, next_upload_id_marker := UM}}
          when KM =/= undefined ->
            list_multipart_uploads_loop(Client, Bucket,
                Opts#{key_marker => KM, upload_id_marker => UM}, Acc ++ U);
        {ok, #{uploads := U, is_truncated := true}} ->
            logger:warning(
                "multipart GC list for ~s truncated with no next_key_marker "
                "-- stopping early, some uploads may be missed this pass",
                [Bucket]),
            {ok, Acc ++ U};
        {ok, #{uploads := U}} ->
            {ok, Acc ++ U};
        {error, _} = Err ->
            Err
    end.
