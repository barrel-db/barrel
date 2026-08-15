%%%-------------------------------------------------------------------
%%% @doc Dynamic supervisor for ngram shard processes.
%%%
%%% Plain `one_for_one' with NO static children -- every shard is added
%%% and removed dynamically, each keyed by an EXPLICIT, deterministic
%%% child id, `{shard, Ref}' (computable directly from a shard ref
%%% alone, no lookup needed). `one_for_one' preserves the isolation a
%%% `simple_one_for_one' strategy gave (one shard's crash/restart never
%%% touches a sibling shard); what changed is purely HOW an individual
%%% shard is addressed -- explicit ids instead of anonymous pids.
%%%
%%% This matters for rollback/close: a captured pid that has already
%%% died cannot be used to authoritatively cancel or wait out an
%%% in-flight restart of ITS replacement, because `terminate_child/2'
%%% only synchronizes with a restart when it targets THAT restart's own
%%% pid -- and a dead pid never is. Addressed by a stable id instead,
%%% `terminate_child/2'/`delete_child/2' are synchronous requests into
%%% this supervisor's own single process, serialized against its
%%% internal restart-on-EXIT handling by construction: no registry
%%% lookup, no retry loop needed to know a ref is durably vacated.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_shard_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([start_shard/2, stop_shard/1]).
-export([start_shard_tracked/2, stop_shards_confirmed/1]).
-export([init/1]).

-define(SERVER, ?MODULE).
-define(STOP_RETRIES, 5).
-define(STOP_RETRY_DELAY_MS, 50).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a shard by ref. Returns the existing shard if one is
%% already running for that ref. Public contract UNCHANGED from before
%% the `simple_one_for_one' -> id-based conversion.
-spec start_shard(barrel_ngram_shards:ref(), map()) -> {ok, pid()} | {error, term()}.
start_shard(Ref, Config) ->
    case supervisor:start_child(?SERVER, shard_child_spec(Ref, Config)) of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, Reason} -> {error, unwrap_start_error(Reason)}
    end.

%% @doc Stop the shard for a ref, leaving no trace in the supervisor
%% afterward (matching `simple_one_for_one''s old "a stopped child
%% vanishes completely" behavior). Public contract UNCHANGED (today's
%% real contract already includes `{error, not_found}'; this widens the
%% possible error reasons -- `running'/`restarting' from `delete_child/2'
%% -- it does not introduce error-returning where there was none).
-spec stop_shard(barrel_ngram_shards:ref()) -> ok | {error, term()}.
stop_shard(Ref) ->
    Id = {shard, Ref},
    case supervisor:terminate_child(?SERVER, Id) of
        ok -> delete_stopped_child(Id);
        {error, not_found} -> ok;
        {error, _} = Err -> Err
    end.

delete_stopped_child(Id) ->
    case supervisor:delete_child(?SERVER, Id) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Err -> Err   %% `running' / `restarting' -- NOT
                                   %% actually vacated yet; must not be
                                   %% silently treated as ok
    end.

%% @doc New, narrowly-scoped, exported specifically for
%% `barrel_ngram_corpus_lifecycle': distinguishes `started' from
%% `existing', which `start_shard/2' above deliberately collapses for
%% its other callers.
-spec start_shard_tracked(barrel_ngram_shards:ref(), map()) ->
    {ok, pid(), started | existing} | {error, term()}.
start_shard_tracked(Ref, Config) ->
    case supervisor:start_child(?SERVER, shard_child_spec(Ref, Config)) of
        {ok, Pid} -> {ok, Pid, started};
        {error, {already_started, Pid}} -> {ok, Pid, existing};
        {error, Reason} -> {error, unwrap_start_error(Reason)}
    end.

%% @private For a PLAIN (non-simple_one_for_one) supervisor, a genuine
%% start FAILURE (e.g. the child's own init/1 returning {stop, Reason})
%% comes back as {error, {Reason, ChildSpec}} -- the child spec appended
%% for diagnostics -- unlike simple_one_for_one, which returned the bare
%% Reason directly. Confirmed empirically (OTP wraps the internal
%% `child' record, recognizable by its first element). Unwrapped here so
%% every caller keeps seeing the same clean Reason as before this
%% supervisor's simple_one_for_one -> id-based conversion.
unwrap_start_error(Reason) when is_tuple(Reason), tuple_size(Reason) =:= 2 ->
    case element(2, Reason) of
        ChildSpec when is_tuple(ChildSpec), tuple_size(ChildSpec) > 0,
                       element(1, ChildSpec) =:= child ->
            element(1, Reason);
        _ ->
            Reason
    end;
unwrap_start_error(Reason) ->
    Reason.

%% @doc Stop every ref in `Refs', reporting `ok' only once EVERY one is
%% CONFIRMED vacated -- not merely attempted. Retries (with a short
%% delay, since `running'/`restarting' reflects a supervisor-side
%% restart backoff that needs real time to resolve) rather than trusting
%% a single attempt, converging even against a stale captured pid whose
%% replacement was mid-restart when this was first called.
-spec stop_shards_confirmed([barrel_ngram_shards:ref()]) -> ok | {error, term()}.
stop_shards_confirmed(Refs) ->
    stop_shards_confirmed(Refs, ?STOP_RETRIES).

stop_shards_confirmed([], _Retries) ->
    ok;
stop_shards_confirmed(Refs, 0) ->
    {error, {shard_stop_failed, Refs}};
stop_shards_confirmed(Refs, Retries) ->
    case [Ref || Ref <- Refs, stop_shard(Ref) =/= ok] of
        [] -> ok;
        Remaining ->
            timer:sleep(?STOP_RETRY_DELAY_MS),
            stop_shards_confirmed(Remaining, Retries - 1)
    end.

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },
    {ok, {SupFlags, []}}.

%%====================================================================
%% Internal
%%====================================================================

%% transient: a deliberate stop (terminate_child, via stop_shard/1) is
%% not restarted, but an abnormal crash is restarted with the same args,
%% so the shard reloads its manifest and resubscribes rather than
%% leaving the corpus with no live shard.
shard_child_spec(Ref, Config) ->
    #{
        id => {shard, Ref},
        start => {barrel_ngram_shard, start_link, [Ref, Config]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [barrel_ngram_shard]
    }.
