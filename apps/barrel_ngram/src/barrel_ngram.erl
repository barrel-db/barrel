%%%-------------------------------------------------------------------
%%% @doc barrel_ngram: exact substring search over barrel_docdb.
%%%
%%% A byte-level trigram index giving exact lexical recall (identifiers,
%%% error strings, config keys) that semantic search misses. A corpus is
%%% bound to a database and a gram selector; indexing is driven by the
%%% database's changes feed, and every query result is confirmed against
%%% the real document text.
%%%
%%% == M1 usage ==
%%% ```
%%% ok = barrel_ngram:open(<<"code">>, #{db => <<"mydb">>}),
%%% {ok, _} = barrel_ngram:index(<<"code">>),
%%% {ok, Hits} = barrel_ngram:search(<<"code">>, <<"connect_timeout">>, #{}).
%%% '''
%%%
%%% Requires the barrel_ngram application to be started.
%%%
%%% A second, positional (phase-2) index narrows candidates to a specific
%%% byte position and, with a `source' configured (see
%%% {@link barrel_ngram_source}), verifies by reading just that window
%%% instead of the whole document. See {@link barrel_ngram_planner}'s
%%% moduledoc for how narrowing and case-insensitive search interact.
%%%
%%% `open/2' and `close/1' are serialized per corpus (never interleaved,
%%% even across concurrent callers) by a one-shot
%%% {@link barrel_ngram_corpus_lifecycle} coordinator -- see its
%%% moduledoc for the full lifecycle design.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram).

-export([open/2, close/1, is_open/1, index/1, refresh/1, compact/1,
         search/2, search/3, regex/2, regex/3]).
-export([safe_shard_call/2]).

-type corpus() :: binary() | atom().
-export_type([corpus/0]).

-define(MAX_SHARDS, 4096).
-define(MAX_SAMPLE_RATE, (1 bsl 32)).
-define(LIFECYCLE_RETRIES, 5).
-define(LIFECYCLE_CONTENTION_TIMEOUT, 5000).

%% @doc Create or re-attach a corpus bound to a database.
%%
%% There is no separate create step: this creates the corpus if it does not
%% exist and re-attaches (resuming from its on-disk state) if it does. It
%% starts a feed subscription that keeps the index in sync. `phase2_selector_opts',
%% `fields', `shards', `postings', and `db' (including which underlying
%% database INSTANCE, not just name -- a delete+recreate under the same
%% name is detected) are fixed for the life of a corpus: reopening with a
%% different value fails with
%% `{error, {config_mismatch, Field, Persisted, Requested}}' rather than
%% silently reindexing or rebinding under the new value.
%%
%% Every corpus indexes both a dense (phase-1, exhaustive) and a sparse
%% (phase-2, content-defined, positional) index; there is no longer a
%% corpus-wide selector choice. `selector' is rejected outright with
%% `{error, {unsupported_option, selector}}'.
%%
%% Options:
%% <ul>
%%   <li>`db' (required) - the barrel_docdb database name to index.</li>
%%   <li>`phase2_selector_opts' - phase-2 sampling tuning map (default
%%       `#{}'): `radius' (0-256) and `sample_rate' (1 to 2^32).</li>
%%   <li>`fields' - `all' or a list of field names to index (default
%%       `all').</li>
%%   <li>`shards' - number of shards to spread the corpus across by
%%       rendezvous hashing (default 1, max 4096).</li>
%%   <li>`postings' - posting-list codec, `varint' (default) or `roaring'
%%       (a native bitmap AND for large dense corpora).</li>
%%   <li>`data_dir' - segment storage directory (default from app env);
%%       segments live under `data_dir/<corpus>/'.</li>
%%   <li>`freeze_threshold' - buffer size before an automatic freeze
%%       (default 1000).</li>
%%   <li>`compact_threshold' - live segment count before an automatic
%%       compaction (default 16; `infinity' disables it).</li>
%%   <li>`source' - `{Module, InitArg}', a {@link barrel_ngram_source} for
%%       verifying candidates without a full `barrel_docdb' fetch
%%       (optional; falls back to `barrel_docdb:get_docs/2' when
%%       absent).</li>
%% </ul>
-spec open(corpus(), map()) -> ok | {error, term()}.
open(Corpus, Opts) ->
    case validate_open_opts(Corpus, Opts) of
        ok -> lifecycle_call(normalize_corpus(Corpus), {open, Opts}, ?LIFECYCLE_RETRIES);
        {error, _} = Err -> Err
    end.

%% @doc Close a corpus, stopping every shard. Idempotent: closing a
%% corpus that was never durably opened (or is already closed) is `ok'.
-spec close(corpus()) -> ok | {error, term()}.
close(Corpus) ->
    case validate_corpus(Corpus) of
        ok -> lifecycle_call(normalize_corpus(Corpus), close, ?LIFECYCLE_RETRIES);
        {error, _} = Err -> Err
    end.

%% @doc Whether a corpus is currently open: a cheap, NON-AUTHORITATIVE
%% pre-filter, not the safety guarantee (that is `safe_shard_call/2',
%% used by every actual query/fan call site -- a shard dying in the
%% instant after this check still returns `{error, corpus_not_open}'
%% there, never a crash). Checks the declared meta AND that every one
%% of its shard refs currently resolves via `whereis_name/1', not just
%% the meta alone -- meta is a `persistent_term' entry that survives an
%% in-process supervision cascade (e.g. `barrel_ngram_registry'
%% crashing, which force-restarts `barrel_ngram_shard_sup' empty via
%% `rest_for_one'), so meta alone would keep reporting `true' for every
%% previously-open corpus in the VM even though none of them have a
%% live shard anymore. Only ever reports `true' once a corpus is fully
%% activated and query-trusted -- never for a corpus still mid-open
%% (interrupted or in-flight) or one whose bookkeeping-only activation
%% write failed.
-spec is_open(corpus()) -> boolean().
is_open(Corpus) ->
    NCorpus = normalize_corpus(Corpus),
    case barrel_ngram_shards:get_meta(NCorpus) of
        {ok, #{shards := N}} ->
            lists:all(
                fun(Ref) -> barrel_ngram_registry:whereis_name({shard, Ref}) =/= undefined end,
                barrel_ngram_shards:refs(NCorpus, N));
        undefined ->
            false
    end.

%% @doc Catch the corpus up to the current head of its database's changes
%% feed and freeze the buffer. The index is kept live in the background by
%% a feed subscription; this is the synchronous catch-up point for tests
%% and ops. Alias of {@link refresh/1}.
-spec index(corpus()) -> {ok, map()} | {error, term()}.
index(Corpus) ->
    refresh(Corpus).

%% @doc Synchronously drain the changes feed up to now and freeze every
%% shard's buffer into a segment.
-spec refresh(corpus()) -> {ok, map()} | {error, term()}.
refresh(Corpus) ->
    fan(normalize_corpus(Corpus), refresh).

%% @doc Compact every shard's live segments, physically evicting superseded
%% and deleted entries. Returns `{error, busy}' if a background compaction
%% is already running on a shard.
-spec compact(corpus()) -> {ok, map()} | {error, term()}.
compact(Corpus) ->
    fan(normalize_corpus(Corpus), compact).

%% @equiv search(Corpus, Literal, #{})
-spec search(corpus(), binary()) -> {ok, [barrel_ngram_query:hit()]} | {error, term()}.
search(Corpus, Literal) ->
    search(Corpus, Literal, #{}).

%% @doc Substring search. Returns hits with the matching document id and
%% the match spans within its corpus text.
-spec search(corpus(), binary(), map()) ->
    {ok, [barrel_ngram_query:hit()]} | {error, term()}.
search(Corpus, Literal, Opts) ->
    barrel_ngram_query:search(normalize_corpus(Corpus), Literal, Opts).

%% @equiv regex(Corpus, Regex, #{})
-spec regex(corpus(), binary()) ->
    {ok, [barrel_ngram_query:hit()]} | {error, term()}.
regex(Corpus, Regex) ->
    regex(Corpus, Regex, #{}).

%% @doc Regex search (PCRE syntax). Returns hits with the matching id and
%% the match spans within its corpus text. `{error, {bad_regex, _}}' if the
%% pattern does not compile.
-spec regex(corpus(), binary(), map()) ->
    {ok, [barrel_ngram_query:hit()]} | {error, term()}.
regex(Corpus, Regex, Opts) ->
    barrel_ngram_query:regex_search(normalize_corpus(Corpus), Regex, Opts).

%%====================================================================
%% Lifecycle coordination
%%====================================================================

%% @private ReplyRef is generated HERE, before starting the coordinator,
%% and passed to it -- NOT derived from the coordinator's pid or monitor
%% ref -- because a monitor installed AFTER start_child/2 returns can
%% race a FAST handle_continue/2 that already finished: monitoring an
%% already-dead pid yields a synthetic `noproc' DOWN, not the real
%% {shutdown, Result} reason, losing a genuinely successful result. The
%% coordinator sends the result directly, by this reference, and the
%% monitor is used ONLY to detect a crash that never replies.
lifecycle_call(Corpus, Op, Retries) ->
    ReplyRef = make_ref(),
    Caller = self(),
    try supervisor:start_child(barrel_ngram_corpus_lifecycle_sup,
                               [Caller, ReplyRef, Corpus, Op]) of
        {ok, Pid} ->
            Mon = monitor(process, Pid),
            receive
                {ReplyRef, Result} ->
                    demonitor(Mon, [flush]),
                    Result;
                {'DOWN', Mon, process, Pid, Reason} ->
                    %% died WITHOUT ever sending a reply -- a genuine
                    %% crash, not a race with a successful completion
                    %% (which always sends before stopping, and message
                    %% order from one sender is FIFO)
                    {error, {internal_error, Reason}}
            end;
        {error, {already_started, Pid}} when Retries > 0 ->
            %% genuine contention: wait for the current holder to finish,
            %% then retry, instead of failing immediately
            Ref = monitor(process, Pid),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after ?LIFECYCLE_CONTENTION_TIMEOUT -> demonitor(Ref, [flush])
            end,
            lifecycle_call(Corpus, Op, Retries - 1);
        {error, {already_started, _Pid}} ->
            {error, busy};
        {error, Reason} ->
            {error, {internal_error, Reason}}
    catch
        exit:Reason ->
            %% supervisor:start_child/2 is itself an RPC into the
            %% lifecycle supervisor's own process; if that process
            %% doesn't exist right now (e.g. a rest_for_one cascade is
            %% mid-flight, tearing it down and restarting it after a
            %% registry crash), the call EXITS in the caller rather than
            %% returning an error tuple -- caught here narrowly rather
            %% than crashing whoever called open/2 or close/1. Not
            %% retried automatically like already_started contention
            %% above: this is a narrow, transient infrastructure hiccup,
            %% not genuine corpus-lock contention.
            {error, {lifecycle_unavailable, Reason}}
    end.

%%====================================================================
%% Safe shard calls
%%====================================================================

%% @doc `gen_server:call/3' to a shard, converted from a possible
%% crash/`noproc' into `{error, corpus_not_open}' (the shard is gone or
%% shutting down) or `{error, {shard_call_failed, Reason}}' (a genuine
%% internal error, distinguishable and never silently mislabeled). This
%% is the SAFETY GUARANTEE for a query/fan/lifecycle call racing a shard
%% that dies in the narrow window after any cheap liveness pre-check --
%% not the pre-check itself. `Target' is either a bare `pid()' (when the
%% caller already holds a captured, verified pid -- e.g. the lifecycle
%% coordinator's own existing-shard config diff) or a shard `ref()' (via
%% the registry, the common query-path case).
-spec safe_shard_call(pid() | barrel_ngram_shards:ref(), term()) ->
    term() | {error, corpus_not_open} | {error, {shard_call_failed, term()}}.
safe_shard_call(Target, Request) ->
    try
        gen_server:call(call_target(Target), Request, infinity)
    catch
        exit:Reason ->
            case is_shutdown_reason(Reason) of
                true -> {error, corpus_not_open};
                false -> {error, {shard_call_failed, Reason}}
            end
    end.

call_target(Pid) when is_pid(Pid) -> Pid;
call_target(Ref) -> {via, barrel_ngram_registry, {shard, Ref}}.

%% @private Matches the reasons a gen_server:call to a gone/shutting-down
%% shard is known to raise. Confirmed empirically during implementation
%% (killing a live gen_server mid-call and inspecting the caught reason)
%% rather than assumed. Deliberately narrow: an arbitrary crash reason
%% (badarg, function_clause, {badmatch, _}, ...) must NOT match here, so
%% a real bug surfaces as {shard_call_failed, Reason}, not a misleading
%% "corpus not open".
is_shutdown_reason(noproc) -> true;
is_shutdown_reason(normal) -> true;
is_shutdown_reason(killed) -> true;
is_shutdown_reason(shutdown) -> true;
is_shutdown_reason({shutdown, _}) -> true;
is_shutdown_reason({noproc, _}) -> true;
is_shutdown_reason(_) -> false.

%%====================================================================
%% Option validation
%%====================================================================

%% @private Pure, runs BEFORE the corpus lock is even acquired -- nothing
%% touched on failure.
validate_open_opts(_Corpus, Opts) when is_map_key(selector, Opts) ->
    {error, {unsupported_option, selector}};
validate_open_opts(Corpus, Opts) ->
    case validate_corpus(Corpus) of
        {error, _} = Err -> Err;
        ok -> validate_open_opts_fields(Opts)
    end.

validate_open_opts_fields(Opts) ->
    Checks = [
        fun() -> validate_db(Opts) end,
        fun() -> validate_data_dir(Opts) end,
        fun() -> validate_shards(Opts) end,
        fun() -> validate_postings(Opts) end,
        fun() -> validate_phase2_selector_opts(Opts) end,
        fun() -> validate_fields(Opts) end,
        fun() -> validate_threshold(freeze_threshold, Opts, fun is_pos_integer/1) end,
        fun() -> validate_threshold(compact_threshold, Opts, fun is_pos_integer_or_infinity/1) end,
        fun() -> validate_source(Opts) end
    ],
    run_checks(Checks).

run_checks([]) -> ok;
run_checks([Check | Rest]) ->
    case Check() of
        ok -> run_checks(Rest);
        {error, _} = Err -> Err
    end.

%% @private Validates the BINARY FORM regardless of whether an atom or
%% binary was given: reject empty, ".", "..", or containing "/", "\", or
%% a NUL byte. Closes a real path-traversal risk -- the corpus name is
%% used directly as a filesystem path component.
validate_corpus(Corpus) when is_binary(Corpus); is_atom(Corpus) ->
    case is_safe_path_component(normalize_corpus(Corpus)) of
        true -> ok;
        false -> {error, {invalid_option, corpus, Corpus}}
    end;
validate_corpus(Corpus) ->
    {error, {invalid_option, corpus, Corpus}}.

%% @private The `corpus()' identity used EVERYWHERE past this point --
%% the lifecycle lock's `via' name, `barrel_ngram_shards' refs/meta
%% keys, and every shard registry entry -- so `foo' (atom) and
%% `<<"foo">>' (binary) are never treated as two different corpora that
%% happen to collide on the same on-disk directory (which `corpus_name/1'
%% in `barrel_ngram_corpus_config'/`barrel_ngram_corpus_lifecycle'
%% already normalizes to the same path either way). Applied at every
%% public entry point that accepts a `corpus()', not just `open/2': a
%% mismatch here would let the atom and binary forms acquire DIFFERENT
%% corpus locks and get DIFFERENT shard refs for the identical
%% directory, defeating the whole point of the per-corpus lock -- two
%% independent shard processes writing the same manifest/segments
%% concurrently. Passes through anything that isn't an atom or binary
%% unchanged (validate_corpus/1 already rejects those for `open/2' and
%% `close/1'; the read-only call sites -- `is_open/1', `search/2,3',
%% `regex/2,3' -- have never validated their input's type either, so
%% this stays exactly as lenient as they always were for that case).
normalize_corpus(Corpus) when is_binary(Corpus) -> Corpus;
normalize_corpus(Corpus) when is_atom(Corpus) -> atom_to_binary(Corpus, utf8);
normalize_corpus(Corpus) -> Corpus.

is_safe_path_component(<<>>) -> false;
is_safe_path_component(<<".">>) -> false;
is_safe_path_component(<<"..">>) -> false;
is_safe_path_component(Bin) ->
    not (binary:match(Bin, <<"/">>) =/= nomatch orelse
         binary:match(Bin, <<"\\">>) =/= nomatch orelse
         binary:match(Bin, <<0>>) =/= nomatch).

validate_db(Opts) ->
    case maps:find(db, Opts) of
        {ok, Db} when is_binary(Db) -> ok;
        {ok, Db} -> {error, {invalid_option, db, Db}};
        error -> {error, {missing_option, db}}
    end.

validate_data_dir(Opts) ->
    case maps:find(data_dir, Opts) of
        error -> ok;
        {ok, D} when is_binary(D); is_list(D) -> ok;
        {ok, D} -> {error, {invalid_option, data_dir, D}}
    end.

validate_shards(Opts) ->
    case maps:get(shards, Opts, 1) of
        N when is_integer(N), N >= 1, N =< ?MAX_SHARDS -> ok;
        N -> {error, {invalid_option, shards, N}}
    end.

validate_postings(Opts) ->
    case maps:get(postings, Opts, varint) of
        varint -> ok;
        roaring -> ok;
        P -> {error, {invalid_option, postings, P}}
    end.

validate_phase2_selector_opts(Opts) ->
    case maps:get(phase2_selector_opts, Opts, #{}) of
        M when is_map(M) ->
            case validate_radius(M) of
                ok -> validate_sample_rate(M);
                {error, _} = Err -> Err
            end;
        Other ->
            {error, {invalid_option, phase2_selector_opts, Other}}
    end.

validate_radius(M) ->
    case maps:find(radius, M) of
        error -> ok;
        {ok, R} when is_integer(R), R >= 0, R =< 256 -> ok;
        {ok, R} -> {error, {invalid_option, radius, R}}
    end.

validate_sample_rate(M) ->
    case maps:find(sample_rate, M) of
        error -> ok;
        {ok, S} when is_integer(S), S >= 1, S =< ?MAX_SAMPLE_RATE -> ok;
        {ok, S} -> {error, {invalid_option, sample_rate, S}}
    end.

validate_fields(Opts) ->
    case maps:get(fields, Opts, all) of
        all -> ok;
        L when is_list(L) ->
            case lists:all(fun is_binary/1, L) of
                true -> ok;
                false -> {error, {invalid_option, fields, L}}
            end;
        Other -> {error, {invalid_option, fields, Other}}
    end.

validate_threshold(Key, Opts, Pred) ->
    case maps:find(Key, Opts) of
        error -> ok;
        {ok, V} ->
            case Pred(V) of
                true -> ok;
                false -> {error, {invalid_option, Key, V}}
            end
    end.

is_pos_integer(V) -> is_integer(V) andalso V > 0.
is_pos_integer_or_infinity(infinity) -> true;
is_pos_integer_or_infinity(V) -> is_pos_integer(V).

validate_source(Opts) ->
    case maps:find(source, Opts) of
        error -> ok;
        {ok, {Mod, _InitArg}} when is_atom(Mod) -> ok;
        {ok, Other} -> {error, {invalid_option, source, Other}}
    end.

%%====================================================================
%% Internal
%%====================================================================

corpus_refs(Corpus) ->
    N = case barrel_ngram_shards:get_meta(Corpus) of
        {ok, #{shards := Sh}} -> Sh;
        undefined -> 1
    end,
    barrel_ngram_shards:refs(Corpus, N).

%% @private Fan a shard request across every shard. A single-shard
%% corpus passes the shard's result straight through (so per-shard
%% fields like segments/doc_count survive); a multi-shard corpus returns
%% an aggregate, or the first error.
fan(Corpus, Request) ->
    case corpus_refs(Corpus) of
        [Ref] ->
            safe_shard_call(Ref, Request);
        Refs ->
            Results = [safe_shard_call(Ref, Request) || Ref <- Refs],
            case [E || {error, _} = E <- Results] of
                [Err | _] ->
                    Err;
                [] ->
                    {ok, #{
                        shards => length(Refs),
                        segments => sum(segments, Results),
                        doc_count => sum(doc_count, Results)
                    }}
            end
    end.

sum(Key, Results) ->
    lists:sum([maps:get(Key, M, 0) || {ok, M} <- Results]).
