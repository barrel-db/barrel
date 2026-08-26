%%%-------------------------------------------------------------------
%%% @doc Sessions: an agent's working context inside a space, folded
%%% down into barrel primitives. A session is a regular
%%% document (`session:Sid') in the space database carrying data,
%%% summary, and pinned context; its messages are separate documents
%%% (`session:Sid:msg:PaddedTs-Rand') whose ids sort chronologically,
%%% so history reads are ordered prefix folds.
%%%
%%% TTL is sliding: every mutation rewrites the session doc with
%%% `expires_at = now + ttl'. Expiry is the space database's doc-TTL
%%% machinery: reads go blind to an idle session immediately (lazy
%%% expiry) and the space's TTL sweeper tombstones it; the janitor
%%% (barrel_spaces_janitor) then collects the orphaned message docs.
%%% add_message writes the message and slides the session in two
%%% writes (not atomic; a crash between them loses only the slide).
%%%
%%% A session created with `ttl => infinity' (stored as 0) never
%%% expires: a durable record the TTL machinery skips. Existing
%%% corpora move in through create's `id' option, import_session/2
%%% and import_message/3, so no consumer has to write `session:'
%%% documents by hand.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_session).

-export([create/1, create/2,
         get/2,
         touch/2,
         delete/2,
         list/1, list/2,
         import_session/2,
         add_message/3,
         import_message/3,
         get_messages/2, get_messages/3,
         set_data/4, get_data/3,
         set_summary/3,
         pin_context/3, unpin_context/3, list_pinned/2]).

-define(DEFAULT_TTL, 3600).

%%====================================================================
%% Lifecycle
%%====================================================================

%% @doc Create a session with default options.
-spec create(barrel_spaces:space()) -> {ok, binary()} | {error, term()}.
create(Space) ->
    create(Space, #{}).

%% @doc Create a session. Opts: `agent' (binary), `ttl' (seconds;
%% `infinity' or 0 = never expires, stored as 0; default the space's
%% session_ttl or 3600), `data', `metadata', `id' (caller-supplied
%% session id, no `:' allowed; creating an existing id fails with a
%% conflict).
-spec create(barrel_spaces:space(), map()) ->
    {ok, binary()} | {error, term()}.
create(#{id := SpaceId, db := Db}, Opts) when is_map(Opts) ->
    Sid = maps:get(id, Opts, barrel_spaces:new_id(<<"ses_">>)),
    case valid_sid(Sid) of
        true ->
            Ttl = normalize_ttl(maps:get(ttl, Opts,
                                         default_ttl(SpaceId))),
            Now = barrel_spaces:now_ms(),
            Doc = #{
                <<"id">> => <<"session:", Sid/binary>>,
                <<"type">> => <<"session">>,
                <<"session">> => Sid,
                <<"agent">> => maps:get(agent, Opts, <<>>),
                <<"data">> => maps:get(data, Opts, #{}),
                <<"metadata">> => maps:get(metadata, Opts, #{}),
                <<"summary">> => <<>>,
                <<"pinned">> => [],
                <<"ttl">> => Ttl,
                <<"created_at">> => Now,
                <<"updated_at">> => Now
            },
            case barrel:put_doc(Db, Doc, expiry_opts(Now, Ttl)) of
                {ok, _} -> {ok, Sid};
                {error, _} = Err -> Err
            end;
        false ->
            {error, invalid_session_id}
    end.

%% @doc Import a session under its own id (the migration path: no
%% generated ids, timestamps preserved, this module keeps owning the
%% document schema). Map keys: `id' (required, no `:'), `agent',
%% `data', `metadata', `summary', `pinned', `ttl' (seconds; 0 or
%% `infinity' = never, the DEFAULT for imports), `created_at' and
%% `updated_at' (unix ms, default now). A ttl > 0 arms expiry from
%% `updated_at' (sliding semantics; touch/2 restarts the clock).
%% Fails with a conflict if the session already exists.
-spec import_session(barrel_spaces:space(), map()) ->
    {ok, binary()} | {error, term()}.
import_session(#{db := Db}, #{id := Sid} = SessionMap) ->
    case valid_sid(Sid) of
        true ->
            Now = barrel_spaces:now_ms(),
            Ttl = normalize_ttl(maps:get(ttl, SessionMap, 0)),
            UpdatedAt = maps:get(updated_at, SessionMap, Now),
            Doc = #{
                <<"id">> => <<"session:", Sid/binary>>,
                <<"type">> => <<"session">>,
                <<"session">> => Sid,
                <<"agent">> => maps:get(agent, SessionMap, <<>>),
                <<"data">> => maps:get(data, SessionMap, #{}),
                <<"metadata">> => maps:get(metadata, SessionMap, #{}),
                <<"summary">> => maps:get(summary, SessionMap, <<>>),
                <<"pinned">> => maps:get(pinned, SessionMap, []),
                <<"ttl">> => Ttl,
                <<"created_at">> => maps:get(created_at, SessionMap,
                                             Now),
                <<"updated_at">> => UpdatedAt
            },
            case barrel:put_doc(Db, Doc, expiry_opts(UpdatedAt, Ttl)) of
                {ok, _} -> {ok, Sid};
                {error, _} = Err -> Err
            end;
        false ->
            {error, invalid_session_id}
    end;
import_session(_Space, _SessionMap) ->
    {error, id_required}.

%% @doc The session document (expired sessions read as not found).
-spec get(barrel_spaces:space(), binary()) ->
    {ok, map()} | {error, not_found}.
get(#{db := Db}, Sid) when is_binary(Sid) ->
    barrel:get_doc(Db, <<"session:", Sid/binary>>).

%% @doc Slide the session's TTL without changing it. Returns
%% `{ok, 0}' for a session without expiry.
-spec touch(barrel_spaces:space(), binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
touch(Space, Sid) ->
    update(Space, Sid, fun(Doc) -> Doc end).

%% @doc Delete a session and all of its messages.
-spec delete(barrel_spaces:space(), binary()) -> ok.
delete(#{db := Db} = Space, Sid) when is_binary(Sid) ->
    {ok, Messages} = get_messages(Space, Sid, #{}),
    lists:foreach(
        fun(#{<<"id">> := MsgDocId}) ->
            _ = barrel:delete_doc(Db, MsgDocId)
        end, Messages),
    _ = barrel:delete_doc(Db, <<"session:", Sid/binary>>),
    ok.

%% @doc Live sessions of a space.
-spec list(barrel_spaces:space()) -> {ok, [map()]}.
list(Space) ->
    list(Space, #{}).

%% @doc Live sessions, filtered and optionally bounded. Opts:
%% <ul>
%% <li>`agent' - exact match on the agent field</li>
%% <li>`match' - `#{FieldPath => Value}': equality conditions on
%%     session fields (`<<"data.user_id">>' or
%%     `[<<"data">>, <<"user_id">>]'), resolved through the space
%%     database's path indexes; the supported query path for spaces
%%     holding thousands of sessions</li>
%% <li>`limit' - max sessions returned</li>
%% </ul>
-spec list(barrel_spaces:space(), map()) ->
    {ok, [map()]} | {error, term()}.
list(#{db := #{docdb := DbBin}}, Opts) ->
    Agent = maps:get(agent, Opts, undefined),
    Match = maps:get(match, Opts, #{}),
    Limit = maps:get(limit, Opts, infinity),
    case Agent =:= undefined andalso map_size(Match) =:= 0 of
        true ->
            {ok, Sessions} = barrel_docdb:fold_docs(
                DbBin,
                fun(#{<<"type">> := <<"session">>} = Doc, Acc) ->
                        {ok, [Doc | Acc]};
                   (_Doc, Acc) ->
                        {ok, Acc}
                end, [], #{id_prefix => <<"session:">>}),
            {ok, take(lists:reverse(Sessions), Limit)};
        false ->
            find_sessions(DbBin, Agent, Match, Limit)
    end.

%%====================================================================
%% Messages
%%====================================================================

%% @doc Append a message: `#{role := binary(), content := term(),
%% metadata => map()}'. Slides the session's TTL.
-spec add_message(barrel_spaces:space(), binary(), map()) ->
    {ok, binary()} | {error, term()}.
add_message(#{db := Db} = Space, Sid, #{role := Role,
                                        content := Content} = Msg) ->
    case get(Space, Sid) of
        {ok, _} ->
            Now = barrel_spaces:now_ms(),
            MsgId = msg_id(Now),
            Doc = #{
                <<"id">> => <<"session:", Sid/binary, ":msg:",
                              MsgId/binary>>,
                <<"type">> => <<"message">>,
                <<"session">> => Sid,
                <<"role">> => Role,
                <<"content">> => Content,
                <<"metadata">> => maps:get(metadata, Msg, #{}),
                <<"ts">> => Now
            },
            case barrel:put_doc(Db, Doc) of
                {ok, _} ->
                    _ = touch(Space, Sid),
                    {ok, MsgId};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Import a message with its own timestamp (the chronological id
%% derives from it). Map keys: `role' and `content' (required), `ts'
%% (unix ms, default now), `seq' (disambiguates messages inside one
%% millisecond, default a fresh monotonic value), `id' (caller-supplied
%% message id, keys the document instead of the derived id; an id
%% already used in this session fails with a conflict), `metadata'.
%% Does not slide the session's TTL.
-spec import_message(barrel_spaces:space(), binary(), map()) ->
    {ok, binary()} | {error, term()}.
import_message(#{db := Db} = Space, Sid, #{role := Role,
                                           content := Content} = Msg) ->
    case get(Space, Sid) of
        {ok, _} ->
            Ts = maps:get(ts, Msg, barrel_spaces:now_ms()),
            MsgId = import_msg_id(Msg, Ts),
            Doc = #{
                <<"id">> => <<"session:", Sid/binary, ":msg:",
                              MsgId/binary>>,
                <<"type">> => <<"message">>,
                <<"session">> => Sid,
                <<"role">> => Role,
                <<"content">> => Content,
                <<"metadata">> => maps:get(metadata, Msg, #{}),
                <<"ts">> => Ts
            },
            case barrel:put_doc(Db, Doc) of
                {ok, _} -> {ok, MsgId};
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%% The imported message's id: the caller's, else derived from ts+seq.
import_msg_id(#{id := Id}, _Ts) when is_binary(Id), Id =/= <<>> ->
    Id;
import_msg_id(#{seq := Seq}, Ts) ->
    msg_id(Ts, Seq);
import_msg_id(_Msg, Ts) ->
    msg_id(Ts).

%% @doc The session's messages in chronological order.
-spec get_messages(barrel_spaces:space(), binary()) -> {ok, [map()]}.
get_messages(Space, Sid) ->
    get_messages(Space, Sid, #{}).

%% @doc Messages with options: `limit' (from the chosen end), `order'
%% (asc | desc, default asc), `since'/`before' (unix ms bounds on the
%% message timestamp).
-spec get_messages(barrel_spaces:space(), binary(), map()) ->
    {ok, [map()]}.
get_messages(#{db := #{docdb := DbBin}}, Sid, Opts) ->
    Since = maps:get(since, Opts, 0),
    Before = maps:get(before, Opts, infinity),
    {ok, Messages} = barrel_docdb:fold_docs(
        DbBin,
        fun(#{<<"ts">> := Ts} = Doc, Acc) ->
                case Ts >= Since andalso (Before =:= infinity
                                          orelse Ts < Before) of
                    true -> {ok, [Doc | Acc]};
                    false -> {ok, Acc}
                end;
           (_Doc, Acc) ->
                {ok, Acc}
        end, [], #{id_prefix => <<"session:", Sid/binary, ":msg:">>}),
    %% caller-supplied message ids need not sort chronologically, so
    %% order by ts (id as tiebreak keeps generated ids' exact order)
    Asc = lists:sort(
        fun(#{<<"ts">> := TA, <<"id">> := IA},
            #{<<"ts">> := TB, <<"id">> := IB}) ->
            {TA, IA} =< {TB, IB}
        end, Messages),
    Limit = maps:get(limit, Opts, infinity),
    case maps:get(order, Opts, asc) of
        asc -> {ok, take(Asc, Limit)};
        desc -> {ok, take(lists:reverse(Asc), Limit)}
    end.

%%====================================================================
%% Data, summary, pinned context
%%====================================================================

%% @doc Set one key in the session's data map.
-spec set_data(barrel_spaces:space(), binary(), binary(), term()) ->
    {ok, non_neg_integer()} | {error, term()}.
set_data(Space, Sid, Key, Value) when is_binary(Key) ->
    update(Space, Sid, fun(Doc) ->
        Data = maps:get(<<"data">>, Doc, #{}),
        Doc#{<<"data">> => Data#{Key => Value}}
    end).

%% @doc One key from the session's data map.
-spec get_data(barrel_spaces:space(), binary(), binary()) ->
    {ok, term()} | {error, not_found}.
get_data(Space, Sid, Key) ->
    case get(Space, Sid) of
        {ok, Doc} ->
            case maps:find(Key, maps:get(<<"data">>, Doc, #{})) of
                {ok, Value} -> {ok, Value};
                error -> {error, not_found}
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Set the session's summary checkpoint.
-spec set_summary(barrel_spaces:space(), binary(), binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
set_summary(Space, Sid, Summary) when is_binary(Summary) ->
    update(Space, Sid, fun(Doc) -> Doc#{<<"summary">> => Summary} end).

%% @doc Pin context that must survive truncation: `#{content := term(),
%% priority => 0..10 (0 highest, default 5), id => binary()
%% (caller-supplied pin id, default generated; an id already pinned in
%% this session fails with conflict), pinned_at => unix ms (default
%% now), metadata => map()}'. Returns the pin's id.
-spec pin_context(barrel_spaces:space(), binary(), map()) ->
    {ok, binary()} | {error, term()}.
pin_context(Space, Sid, #{content := Content} = Pin) ->
    PinId = maps:get(id, Pin, barrel_spaces:new_id(<<"pin_">>)),
    case valid_pin_id(PinId) of
        true ->
            Item = #{
                <<"id">> => PinId,
                <<"content">> => Content,
                <<"priority">> => maps:get(priority, Pin, 5),
                <<"metadata">> => maps:get(metadata, Pin, #{}),
                <<"pinned_at">> => maps:get(pinned_at, Pin,
                                            barrel_spaces:now_ms())
            },
            case update(Space, Sid,
                        fun(Doc) -> add_pin(Doc, Item) end) of
                {ok, _} -> {ok, PinId};
                {error, _} = Err -> Err
            end;
        false ->
            {error, invalid_pin_id}
    end.

%% Reject a duplicate id loudly instead of quietly corrupting the
%% pinned list (unpin would then remove an arbitrary one of the two).
add_pin(Doc, #{<<"id">> := PinId} = Item) ->
    Pinned = maps:get(<<"pinned">>, Doc, []),
    case [P || #{<<"id">> := I} = P <- Pinned, I =:= PinId] of
        [_ | _] ->
            {error, conflict};
        [] ->
            Sorted = lists:sort(
                fun(A, B) ->
                    maps:get(<<"priority">>, A)
                        =< maps:get(<<"priority">>, B)
                end, [Item | Pinned]),
            Doc#{<<"pinned">> => Sorted}
    end.

valid_pin_id(Id) when is_binary(Id), byte_size(Id) > 0 -> true;
valid_pin_id(_) -> false.

%% @doc Remove a pinned item by id.
-spec unpin_context(barrel_spaces:space(), binary(), binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
unpin_context(Space, Sid, PinId) when is_binary(PinId) ->
    update(Space, Sid, fun(Doc) ->
        Pinned = [P || P <- maps:get(<<"pinned">>, Doc, []),
                       maps:get(<<"id">>, P) =/= PinId],
        Doc#{<<"pinned">> => Pinned}
    end).

%% @doc The pinned items, highest priority first.
-spec list_pinned(barrel_spaces:space(), binary()) ->
    {ok, [map()]} | {error, not_found}.
list_pinned(Space, Sid) ->
    case get(Space, Sid) of
        {ok, Doc} -> {ok, maps:get(<<"pinned">>, Doc, [])};
        {error, _} = Err -> Err
    end.

%%====================================================================
%% Internal
%%====================================================================

%% Every mutation slides the TTL: rewrite the doc with a fresh
%% expires_at derived from the session's own ttl (0 = durable: the
%% expiry stays cleared and callers get {ok, 0}).
update(#{db := Db} = Space, Sid, Fun) ->
    case get(Space, Sid) of
        {ok, Doc} ->
            Now = barrel_spaces:now_ms(),
            Ttl = normalize_ttl(maps:get(<<"ttl">>, Doc,
                                         ?DEFAULT_TTL)),
            ExpiresAt = case Ttl of
                0 -> 0;
                _ -> Now + Ttl * 1000
            end,
            case Fun(Doc) of
                {error, _} = Veto ->
                    Veto;
                Updated0 ->
                    Updated = Updated0#{<<"updated_at">> => Now},
                    case barrel:put_doc(Db, Updated,
                                        #{expires_at => ExpiresAt}) of
                        {ok, _} -> {ok, ExpiresAt};
                        {error, _} = Err -> Err
                    end
            end;
        {error, _} = Err ->
            Err
    end.

default_ttl(SpaceId) ->
    case barrel_spaces:space_info(SpaceId) of
        {ok, #{<<"session_ttl">> := Ttl}} -> Ttl;
        _ -> ?DEFAULT_TTL
    end.

%% zero-padded ms timestamp + zero-padded monotonic sequence: strict
%% chronological id order even for messages inside one millisecond
msg_id(NowMs) ->
    msg_id(NowMs, erlang:unique_integer([positive, monotonic])).

msg_id(NowMs, Seq0) ->
    Ts = integer_to_binary(NowMs),
    TsPad = binary:copy(<<"0">>, 20 - byte_size(Ts)),
    Seq = integer_to_binary(Seq0),
    SeqPad = binary:copy(<<"0">>, 20 - byte_size(Seq)),
    <<TsPad/binary, Ts/binary, "-", SeqPad/binary, Seq/binary>>.

take(List, infinity) -> List;
take(List, N) -> lists:sublist(List, N).

%% ttl: `infinity' and 0 both mean "never expires", stored as 0.
normalize_ttl(infinity) -> 0;
normalize_ttl(Ttl) when is_integer(Ttl), Ttl >= 0 -> Ttl.

%% expires_at write option: 0 clears any expiry on the doc.
expiry_opts(_Base, 0) -> #{expires_at => 0};
expiry_opts(Base, Ttl) -> #{expires_at => Base + Ttl * 1000}.

%% Caller-supplied ids share the `session:' keyspace with the
%% `\:msg:' suffix, so a colon inside one is ambiguous.
valid_sid(Sid) when is_binary(Sid), byte_size(Sid) > 0 ->
    binary:match(Sid, <<":">>) =:= nomatch;
valid_sid(_) ->
    false.

split_path(Path) when is_list(Path) -> Path;
split_path(Path) when is_binary(Path) ->
    binary:split(Path, <<".">>, [global]).

%% Filtered listing through the query engine: equality conditions
%% intersect the database's path posting lists instead of folding
%% every session. Just-expired sessions the sweeper has not
%% tombstoned yet are dropped for parity with the fold path (their
%% expiry is updated_at + ttl by construction).
find_sessions(DbBin, Agent, Match, Limit) ->
    Conds0 = [{path, [<<"type">>], <<"session">>}],
    Conds1 = case Agent of
        undefined -> Conds0;
        _ -> [{path, [<<"agent">>], Agent} | Conds0]
    end,
    Conds = maps:fold(
        fun(Path, Value, Acc) ->
            [{path, split_path(Path), Value} | Acc]
        end, Conds1, Match),
    Spec = case Limit of
        infinity -> #{where => Conds};
        _ -> #{where => Conds, limit => Limit}
    end,
    case find_all(DbBin, Spec, undefined, [], Limit) of
        {ok, Rows} ->
            %% find returns #{<<"id">>, <<"doc">>} rows
            Docs = [maps:get(<<"doc">>, R, R) || R <- Rows],
            Now = barrel_spaces:now_ms(),
            {ok, take([D || D <- Docs, live_session(D, Now)], Limit)};
        {error, _} = Err ->
            Err
    end.

%% Drain find's continuations (the spec limit sizes chunks rather
%% than bounding the total); stop once Limit docs are in hand.
find_all(DbBin, Spec, Cont, Acc, Limit) ->
    Opts = case Cont of
        undefined -> #{};
        _ -> #{continuation => Cont}
    end,
    case barrel_docdb:find(DbBin, Spec, Opts) of
        {ok, Docs, #{has_more := true, continuation := Next}} ->
            Acc1 = [Docs | Acc],
            Got = lists:sum([length(L) || L <- Acc1]),
            case Limit =/= infinity andalso Got >= Limit of
                true -> {ok, lists:append(lists:reverse(Acc1))};
                false -> find_all(DbBin, Spec, Next, Acc1, Limit)
            end;
        {ok, Docs, _Meta} ->
            {ok, lists:append(lists:reverse([Docs | Acc]))};
        {error, _} = Err ->
            Err
    end.

live_session(#{<<"ttl">> := 0}, _Now) ->
    true;
live_session(#{<<"ttl">> := Ttl, <<"updated_at">> := U}, Now)
  when is_integer(Ttl), is_integer(U) ->
    U + Ttl * 1000 > Now;
live_session(_Doc, _Now) ->
    true.
