%%%-------------------------------------------------------------------
%%% @doc Contexts error catalog: one stable code per cause, shared by
%%% Erlang (`{error, {Code, Details}}'), REST and MCP
%%% (`{"error", "message", "hint", "details"}'), plus the text that
%%% explains a failed source.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_error).

-export([normalize/1, to_map/1, codes/0, source_error/2]).

-define(MAX_ROWS, 1000).
-define(MAX_CONTEXTS, 8).

-type code() :: invalid_argument | invalid_query | limit_required
              | limit_too_large | too_many_contexts | duplicate_context
              | unsupported_federated_query | merge_not_allowed
              | merge_not_supported | scores_not_comparable
              | unknown_context | ambiguous_context | unknown_working_set
              | not_attached | already_attached | already_exists
              | invalid_card | no_location | over_budget | offline
              | invalid_snapshot | source_unavailable | forbidden
              | internal.
-type error() :: {code(), map()}.
-export_type([code/0, error/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Every code, with the HTTP status REST answers for it.
-spec codes() -> [{code(), pos_integer()}].
codes() ->
    [{invalid_argument, 400}, {invalid_query, 400}, {limit_required, 400},
     {limit_too_large, 400}, {too_many_contexts, 400},
     {duplicate_context, 400}, {unsupported_federated_query, 400},
     {merge_not_allowed, 400}, {merge_not_supported, 400},
     {scores_not_comparable, 400}, {unknown_context, 404},
     {ambiguous_context, 409}, {unknown_working_set, 404},
     {not_attached, 404}, {already_attached, 409}, {already_exists, 409},
     {invalid_card, 400}, {no_location, 400}, {over_budget, 413},
     {offline, 409}, {invalid_snapshot, 400}, {source_unavailable, 502},
     {forbidden, 403},
     {internal, 500}].

%% @doc Map any error term of the contexts modules to `{Code, Details}'.
-spec normalize(term()) -> error().
normalize({Code, Details} = E) when is_atom(Code), is_map(Details) ->
    case lists:keymember(Code, 1, codes()) of
        true -> E;
        false -> {internal, #{detail => text(E)}}
    end;
normalize({bad_request, duplicate_context}) ->
    {duplicate_context, #{}};
normalize({bad_request, Field}) ->
    {invalid_argument, argument(Field)};
normalize({unsupported_federated_query, Reason}) ->
    unsupported(Reason);
normalize({invalid_query, BqlError}) ->
    {invalid_query, #{bql_error => bql_error(BqlError)}};
normalize({unknown_context, Id}) ->
    {unknown_context, #{context => Id, suggestions => []}};
normalize({unknown_working_set, Id}) ->
    {unknown_working_set, #{working_set => Id}};
normalize({already_attached, Ctx}) ->
    {already_attached, #{context => Ctx}};
normalize({not_attached, Ctx}) ->
    {not_attached, #{context => Ctx}};
normalize({over_budget, Kind, #{} = Info}) ->
    {over_budget, Info#{budget => Kind}};
normalize({over_budget, Kind, Limit}) ->
    {over_budget, #{budget => Kind, limit => Limit}};
normalize({no_location, Mode}) ->
    {no_location, #{mode => Mode}};
normalize({invalid_card, {Tag, Detail}}) when is_atom(Tag) ->
    {invalid_card, #{reason => Tag, detail => text(Detail)}};
normalize({invalid_card, Reason}) ->
    {invalid_card, #{reason => text(Reason)}};
normalize({invalid_member, _}) ->
    {invalid_argument, #{field => mode,
                         expected => <<"local or remote">>}};
normalize({manifest_unreadable, Reason}) ->
    {invalid_snapshot, #{reason => manifest_unreadable,
                         detail => text(Reason)}};
normalize({Tag, _Ref}) when Tag =:= part_checksum_mismatch;
                            Tag =:= checksum_mismatch ->
    {invalid_snapshot, #{reason => Tag}};
normalize({part_unreadable, _Ref, Reason}) ->
    {invalid_snapshot, #{reason => part_unreadable, detail => text(Reason)}};
normalize({name_taken, Name, _Other}) ->
    {already_exists, #{local_db => Name}};
normalize({source_unavailable, Reason}) ->
    {source_unavailable, #{reason => text(Reason)}};
normalize({unsupported, attachments}) ->
    {invalid_argument, #{field => <<"include.attachments">>,
                         expected => <<"false (attachments are not copied)">>}};
normalize(already_exists) ->
    {already_exists, #{}};
normalize(forbidden) ->
    {forbidden, #{}};
normalize(Other) ->
    {internal, #{detail => text(Other)}}.

%% @doc The JSON body of an error: code, one sentence, next step, details.
-spec to_map(term()) -> map().
to_map(Error) ->
    {Code, Details} = normalize(Error),
    #{error => Code,
      message => message(Code, Details),
      hint => hint(Code, Details),
      details => jsonable(Details)}.

%% @doc Add `message' and `hint' to the error block of a source that did
%% not answer (status and reason as the executor reports them).
-spec source_error(atom(), map()) -> map().
source_error(Status, #{reason := Reason} = Error) ->
    {Msg, Hint} = source_text(Status, known_reason(Reason), Error),
    Error#{message => Msg, hint => Hint};
source_error(Status, Error) ->
    source_error(Status, Error#{reason => Status}).

%%====================================================================
%% Normalization helpers
%%====================================================================

unsupported(limit_required) ->
    {limit_required, #{max_limit => ?MAX_ROWS}};
unsupported(limit_too_large) ->
    {limit_too_large, #{max_limit => ?MAX_ROWS}};
unsupported(too_many_contexts) ->
    {too_many_contexts, #{max_contexts => max_contexts()}};
unsupported({merge_not_supported, Merge}) ->
    {merge_not_supported, #{merge => Merge, allowed => allowed_merges()}};
unsupported({merge_not_allowed, Merge}) ->
    {merge_not_allowed, #{merge => Merge, allowed => allowed_merges()}};
unsupported({Reason, Ctx}) when Reason =:= fingerprint_mismatch;
                                Reason =:= not_score_comparable ->
    {scores_not_comparable, #{reason => Reason, context => Ctx}};
unsupported(Reason) ->
    {unsupported_federated_query,
     #{reason => Reason, accepted_shapes => accepted_shapes()}}.

argument(contexts) ->
    #{field => contexts,
      expected => <<"a non-empty array of context names or ids">>};
argument(scope) ->
    #{field => working_set,
      expected => <<"either contexts or working_set, not both">>};
argument(merge) ->
    #{field => merge, allowed => allowed_merges()};
argument(Field) when Field =:= deadline_ms; Field =:= per_context_timeout_ms;
                     Field =:= max_parallel ->
    #{field => Field, expected => <<"a positive integer">>};
argument(offline) ->
    #{field => offline, expected => <<"true or false">>};
argument(Field) ->
    #{field => Field}.

bql_error(B) when is_binary(B) -> B;
bql_error(E) ->
    try iolist_to_binary(barrel_bql:format_error(E))
    catch _:_ -> text(E)
    end.

allowed_merges() ->
    [<<"ordered">>, <<"grouped">>, <<"interleave">>, <<"score">>].

accepted_shapes() ->
    [<<"ordered rows: SELECT ... ORDER BY <selected field> LIMIT n">>,
     <<"unordered rows: SELECT ... LIMIT n">>,
     <<"retrieval: SELECT ... FROM bm25_top_k|vector_top_k|hybrid_top_k"
       "('text', k => n) AS t">>].

max_contexts() ->
    application:get_env(barrel, ctx_max_contexts, ?MAX_CONTEXTS).

%%====================================================================
%% Text
%%====================================================================

message(invalid_argument, #{field := F, accepted := _}) ->
    fmt("The argument ~s is not known.", [F]);
message(invalid_argument, #{field := F, allowed := _}) ->
    fmt("The argument ~s has a value that is not accepted.", [F]);
message(invalid_argument, #{field := F}) ->
    fmt("The argument ~s is missing or has the wrong type.", [F]);
message(invalid_argument, _) ->
    <<"The request body is not a JSON object.">>;
message(invalid_query, #{bql_error := E}) ->
    fmt("The BQL statement does not parse: ~s.", [E]);
message(limit_required, _) ->
    <<"A row query over contexts needs a LIMIT.">>;
message(limit_too_large, #{max_limit := Max}) ->
    fmt("The LIMIT or k is larger than ~b.", [Max]);
message(too_many_contexts, #{max_contexts := Max} = D) ->
    case maps:find(requested, D) of
        {ok, N} -> fmt("~b contexts requested, at most ~b per query.",
                       [N, Max]);
        error -> fmt("At most ~b contexts per query.", [Max])
    end;
message(duplicate_context, #{context := C}) ->
    fmt("The context ~s is listed more than once.", [C]);
message(duplicate_context, _) ->
    <<"A context is listed more than once.">>;
message(unsupported_federated_query, #{reason := R}) ->
    fmt("This statement cannot run over several contexts (~s).",
        [shape_reason(R)]);
message(merge_not_allowed, #{merge := M}) ->
    fmt("The merge ~s does not apply to this statement.", [M]);
message(merge_not_supported, #{merge := M}) ->
    fmt("The merge ~s is not supported: its scores do not compare "
        "across contexts.", [M]);
message(scores_not_comparable, #{context := C, reason := R}) ->
    fmt("Vector scores cannot be merged: ~s (~s).", [C, R]);
message(unknown_context, #{context := C}) ->
    fmt("No context has the id or name ~s.", [C]);
message(ambiguous_context, #{name := N}) ->
    fmt("Several contexts are named ~s.", [N]);
message(unknown_working_set, #{working_set := W}) ->
    fmt("No working set has the id ~s.", [W]);
message(not_attached, #{context := C}) ->
    fmt("The context ~s is not in this working set.", [C]);
message(already_attached, #{context := C}) ->
    fmt("The context ~s is already in this working set.", [C]);
message(already_exists, #{local_db := Db}) ->
    fmt("A local database named ~s already exists.", [Db]);
message(already_exists, _) ->
    <<"A context with this id already exists.">>;
message(invalid_card, #{reason := R}) ->
    fmt("The context card is invalid (~s).", [R]);
message(no_location, #{mode := M}) ->
    fmt("The context has no ~s location.", [M]);
message(over_budget, #{budget := B} = D) ->
    over_budget_message(B, D);
message(offline, _) ->
    <<"The node is offline and this operation needs a remote source.">>;
message(invalid_snapshot, #{reason := R}) ->
    fmt("The snapshot directory cannot be imported (~s).", [R]);
message(source_unavailable, #{reason := R}) ->
    fmt("The source did not deliver the documents (~s).", [R]);
message(forbidden, _) ->
    <<"This operation needs a global (admin) principal.">>;
message(internal, _) ->
    <<"The server failed to handle the request.">>;
message(Code, _) ->
    atom_to_binary(Code).

over_budget_message(contexts, #{limit := Max}) ->
    fmt("The working set already holds ~b contexts, its limit.", [Max]);
over_budget_message(B, #{needed := N, available := A}) ->
    fmt("~b bytes needed, ~b available in the ~s budget.", [N, A, B]);
over_budget_message(B, #{needed := N, limit := L}) ->
    fmt("~b bytes needed, the ~s limit is ~b.", [N, B, L]);
over_budget_message(B, #{limit := L}) ->
    fmt("The ~s limit (~b) is exceeded.", [B, L]);
over_budget_message(B, _) ->
    fmt("The ~s budget is exceeded.", [B]).

hint(invalid_argument, #{accepted := []}) ->
    <<"This tool takes no arguments.">>;
hint(invalid_argument, #{accepted := Accepted}) ->
    fmt("Accepted arguments: ~s.", [join(Accepted)]);
hint(invalid_argument, #{field := F} = D) ->
    case maps:find(allowed, D) of
        {ok, Allowed} -> fmt("Use one of: ~s.", [join(Allowed)]);
        error ->
            case maps:find(expected, D) of
                {ok, E} -> fmt("Pass ~s as ~s.", [F, E]);
                error -> fmt("Check the value of ~s.", [F])
            end
    end;
hint(invalid_argument, _) ->
    <<"Send a JSON object.">>;
hint(invalid_query, _) ->
    <<"Strings use single quotes ('text'); double quotes name fields. "
      "Example statements: context_capabilities (GET "
      "/contexts/_capabilities).">>;
hint(limit_required, #{max_limit := Max}) ->
    fmt("Add LIMIT n (n <= ~b), and ORDER BY a selected field to merge "
        "rows in order.", [Max]);
hint(limit_too_large, #{max_limit := Max}) ->
    fmt("Use LIMIT or k <= ~b; narrow the WHERE clause to see other rows.",
        [Max]);
hint(too_many_contexts, _) ->
    <<"Split the contexts over several queries.">>;
hint(duplicate_context, _) ->
    <<"List each context once (a name and its id are the same context).">>;
hint(unsupported_federated_query, #{reason := order_key_not_projected}) ->
    <<"Add the ORDER BY field to the SELECT list.">>;
hint(unsupported_federated_query, #{reason := order_by_required}) ->
    <<"Add ORDER BY a selected field, or use merge grouped.">>;
hint(unsupported_federated_query, #{reason := offset}) ->
    <<"Remove OFFSET: there is no pagination; narrow the WHERE clause.">>;
hint(unsupported_federated_query, _) ->
    <<"Rewrite the statement in one of details.accepted_shapes.">>;
hint(merge_not_allowed, #{merge := score}) ->
    <<"Leave merge unset: retrieval is grouped per context, rows are "
      "merged by ORDER BY.">>;
hint(merge_not_allowed, _) ->
    <<"Leave merge unset to get the default for this statement.">>;
hint(merge_not_supported, _) ->
    <<"Leave merge unset (grouped per context) or use interleave.">>;
hint(scores_not_comparable, _) ->
    <<"Leave merge unset: results are then grouped per context.">>;
hint(unknown_context, #{suggestions := [_ | _] = S}) ->
    fmt("Did you mean: ~s? context_list (GET /contexts) shows every "
        "context.",
        [join([N || #{name := N} <- S])]);
hint(unknown_context, _) ->
    <<"Use a name or id from context_list (GET /contexts) or "
      "context_discover.">>;
hint(ambiguous_context, _) ->
    <<"Pass one of the ids in details.candidates.">>;
hint(unknown_working_set, _) ->
    <<"List working sets with context_working_sets (GET /worksets), or "
      "omit working_set to create one.">>;
hint(not_attached, _) ->
    <<"Read the working set (context_working_sets, GET /worksets/:ws) "
      "to see its members.">>;
hint(already_attached, _) ->
    <<"Detach the context first, or use another working set.">>;
hint(already_exists, #{local_db := _}) ->
    <<"Pass another name for the import.">>;
hint(already_exists, _) ->
    <<"Omit id to get a new one, or update the existing card.">>;
hint(invalid_card, _) ->
    <<"A card needs a name and locations ([{kind: local, db} or "
      "{kind: remote, endpoint, db}]) and no secrets.">>;
hint(no_location, _) ->
    <<"Omit mode to use the card's first location.">>;
hint(over_budget, #{budget := contexts}) ->
    <<"Detach a context or use another working set.">>;
hint(over_budget, #{budget := transfer_bytes}) ->
    <<"Select fewer documents (lower LIMIT or k), set include.embeddings "
      "false, or raise max_bytes.">>;
hint(over_budget, _) ->
    <<"Detach a member or delete a working set to free space.">>;
hint(offline, _) ->
    <<"Switch offline mode off (context_offline or PUT /contexts/_offline "
      "{\"offline\": false}) and retry.">>;
hint(invalid_snapshot, _) ->
    <<"Pass the directory an export wrote (it holds manifest.json).">>;
hint(source_unavailable, _) ->
    <<"Check that the source is reachable and retry.">>;
hint(forbidden, _) ->
    <<"Use a global token; capability tokens can only list and query "
      "contexts.">>;
hint(internal, _) ->
    <<"Retry; if it persists, check the server log.">>;
hint(_Code, _) ->
    <<>>.

shape_reason(order_key_not_projected) ->
    <<"the ORDER BY field is not selected">>;
shape_reason(order_by_required) -> <<"ordered merge needs ORDER BY">>;
shape_reason(offset) -> <<"OFFSET is not supported">>;
shape_reason(subscribe) -> <<"SUBSCRIBE is not supported">>;
shape_reason(unnest) -> <<"UNNEST is not supported">>;
shape_reason(continuation) -> <<"continuations are not supported">>;
shape_reason(R) -> text(R).

%% A remote server reports its reason as text: read it as the local atom.
known_reason(R) when is_binary(R) ->
    try binary_to_existing_atom(R)
    catch error:badarg -> R
    end;
known_reason(R) ->
    R.

%% Per-source text: status first, then the reasons worth a hint.
source_text(timeout, deadline_before_start, _E) ->
    {<<"the request deadline passed before this context started">>,
     <<"Raise deadline_ms or query fewer contexts.">>};
source_text(timeout, _Reason, #{after_ms := Ms}) ->
    {fmt("no answer within ~b ms", [Ms]),
     <<"Raise per_context_timeout_ms (and deadline_ms), or retry.">>};
source_text(timeout, _Reason, _E) ->
    {<<"no answer before the timeout">>,
     <<"Raise per_context_timeout_ms (and deadline_ms), or retry.">>};
source_text(unreachable, Reason, _E) ->
    {fmt("the server did not accept the connection (~s)", [text(Reason)]),
     <<"Check that the remote node is up; to answer without it, attach "
       "and materialize or import a local copy.">>};
source_text(unauthorized, _Reason, _E) ->
    {<<"the source refused the credentials">>,
     <<"Configure a token for this endpoint (ctx_credentials) or check "
       "the capability grant.">>};
source_text(skipped_offline, _Reason, _E) ->
    {<<"skipped: offline and no local copy">>,
     <<"Materialize a slice or import a snapshot while online to answer "
       "offline.">>};
source_text(skipped_budget, _Reason, _E) ->
    {<<"skipped: the node's limit of parallel remote queries was reached">>,
     <<"Retry, or lower max_parallel.">>};
source_text(error, db_not_found, _E) ->
    {<<"the card names a database that does not exist on this node">>,
     <<"Fix the card's location (db) or create the database.">>};
source_text(error, local_copy_missing, _E) ->
    {<<"the local copy of this member is missing">>,
     <<"Detach the member and materialize or import it again.">>};
source_text(error, no_queryable_location, _E) ->
    {<<"the card has no local or remote location">>,
     <<"Add a location to the card.">>};
source_text(error, embedder_not_configured, _E) ->
    {<<"the source has no embedder, so vector_top_k cannot run">>,
     <<"Use bm25_top_k, or configure an embedder on the source.">>};
source_text(error, response_too_large, _E) ->
    {<<"the answer exceeded the response size cap">>,
     <<"Lower LIMIT or k, or select fewer fields.">>};
source_text(error, missing_meta, _E) ->
    {<<"the remote server did not report query metadata">>,
     <<"Upgrade the remote barrel_server.">>};
source_text(error, Reason, E) ->
    {fmt("the source answered with an error (~s)",
         [text(maps:get(detail, E, Reason))]),
     <<"Check the statement against this source (fields, functions).">>};
source_text(Status, Reason, _E) ->
    {fmt("~s (~s)", [Status, text(Reason)]), <<>>}.

%%====================================================================
%% Helpers
%%====================================================================

%% Details must encode as JSON: terms become text.
jsonable(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) -> Acc#{K => jsonable(V)} end, #{}, M);
jsonable(L) when is_list(L) ->
    case io_lib:printable_unicode_list(L) andalso L =/= [] of
        true -> unicode:characters_to_binary(L);
        false -> [jsonable(X) || X <- L]
    end;
jsonable(V) when is_binary(V); is_atom(V); is_number(V) -> V;
jsonable(V) -> text(V).

join(Items) ->
    lists:join(<<", ">>, [text(I) || I <- Items]).

text(B) when is_binary(B) -> B;
text(A) when is_atom(A) -> atom_to_binary(A);
text(N) when is_integer(N) -> integer_to_binary(N);
text(Other) -> iolist_to_binary(io_lib:format("~0p", [Other])).

fmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, [text_arg(A) || A <- Args])).

text_arg(N) when is_integer(N) -> N;
text_arg(L) when is_list(L) -> iolist_to_binary(L);
text_arg(Other) -> text(Other).
