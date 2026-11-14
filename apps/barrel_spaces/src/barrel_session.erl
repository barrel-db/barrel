%%%-------------------------------------------------------------------
%%% @doc Sessions: an agent's working context inside a space, folded
%%% down into barrel primitives. Every fact is a document of its own,
%%% written once or replaced as a whole, so two nodes writing the same
%%% session never lose each other's writes when the space replicates:
%%%
%%% <ul>
%%% <li>`session:Sid': the header (agent, metadata, ttl, timestamps)</li>
%%% <li>`session:Sid:data:Key': one data value</li>
%%% <li>`session:Sid:pin:PinId': one pinned item</li>
%%% <li>`session:Sid:summary': the summary</li>
%%% <li>`session:Sid:count:Replica': the counters of one replica,
%%%     written by that replica only</li>
%%% <li>`session:Sid:msg:PaddedTs-Rand': one message; ids sort
%%%     chronologically, so history reads are ordered prefix folds</li>
%%% </ul>
%%%
%%% Removing a data key or a pin deletes its document, so a node still
%%% holding the old copy cannot bring it back. The same key written on
%%% two nodes keeps the later write.
%%%
%%% TTL is sliding: every mutation rewrites the header with
%%% `expires_at = now + ttl'. Expiry is the space database's doc-TTL
%%% machinery: reads go blind to an idle session immediately (lazy
%%% expiry) and the space's TTL sweeper tombstones the header; the
%%% janitor (barrel_spaces_janitor) then collects the orphaned
%%% documents. A mutation writes its document and slides the header in
%%% two writes (not atomic; a crash between them loses only the slide).
%%%
%%% A session created with `ttl => infinity' (stored as 0) never
%%% expires: a durable record the TTL machinery skips. Existing
%%% corpora move in through create's `id' option, import_session/2
%%% and import_message/3, so no consumer has to write `session:'
%%% documents by hand. A session written in one document before 2.0
%%% is split into this layout on its first mutation, or by migrate/1.
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
         set_data/4, get_data/3, delete_data/3,
         set_summary/3,
         pin_context/3, unpin_context/3, list_pinned/2,
         incr_counter/4, get_counters/2,
         migrate/1]).

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
            Header = header(Sid, maps:get(agent, Opts, <<>>),
                            maps:get(metadata, Opts, #{}), Ttl, Now, Now),
            write_new(Db, Sid, Header, expiry_opts(Now, Ttl),
                      #{data => maps:get(data, Opts, #{})});
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
            Header = header(Sid, maps:get(agent, SessionMap, <<>>),
                            maps:get(metadata, SessionMap, #{}), Ttl,
                            maps:get(created_at, SessionMap, Now),
                            UpdatedAt),
            write_new(Db, Sid, Header, expiry_opts(UpdatedAt, Ttl),
                      #{data => maps:get(data, SessionMap, #{}),
                        summary => maps:get(summary, SessionMap, <<>>),
                        pinned => maps:get(pinned, SessionMap, [])});
        false ->
            {error, invalid_session_id}
    end;
import_session(_Space, _SessionMap) ->
    {error, id_required}.

%% @doc The session: its header with `data', `summary', `pinned' and
%% `counters' read from their documents (expired sessions read as not
%% found).
-spec get(barrel_spaces:space(), binary()) ->
    {ok, map()} | {error, not_found}.
get(#{db := Db}, Sid) when is_binary(Sid) ->
    case barrel:get_doc(Db, header_id(Sid)) of
        {ok, Header} -> {ok, assemble(Header, parts(Db, Sid))};
        {error, _} = Err -> Err
    end.

%% @doc Slide the session's TTL without changing it. Returns
%% `{ok, 0}' for a session without expiry.
-spec touch(barrel_spaces:space(), binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
touch(Space, Sid) ->
    mutate(Space, Sid, fun() -> ok end).

%% @doc Delete a session and all of its documents.
-spec delete(barrel_spaces:space(), binary()) -> ok.
delete(#{db := Db}, Sid) when is_binary(Sid) ->
    Ids = barrel_spaces_docs:fold_prefix(
            Db, <<"session:", Sid/binary, ":">>,
            fun(#{<<"id">> := Id}, Acc) -> [Id | Acc] end, []),
    lists:foreach(fun(Id) -> _ = barrel_spaces_docs:remove(Db, Id) end, Ids),
    _ = barrel_spaces_docs:remove(Db, header_id(Sid)),
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
list(#{db := Db}, Opts) ->
    Agent = maps:get(agent, Opts, undefined),
    Match = maps:get(match, Opts, #{}),
    Limit = maps:get(limit, Opts, infinity),
    case Agent =:= undefined andalso map_size(Match) =:= 0 of
        true ->
            {ok, take(fold_sessions(Db), Limit)};
        false ->
            find_sessions(Db, Agent, Match, Limit)
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
    case barrel:get_doc(Db, header_id(Sid)) of
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
import_message(#{db := Db}, Sid, #{role := Role,
                                   content := Content} = Msg) ->
    case barrel:get_doc(Db, header_id(Sid)) of
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
set_data(#{db := Db} = Space, Sid, Key, Value) when is_binary(Key) ->
    mutate(Space, Sid, fun() -> put_data(Db, Sid, Key, Value) end).

%% @doc One key from the session's data map.
-spec get_data(barrel_spaces:space(), binary(), binary()) ->
    {ok, term()} | {error, not_found}.
get_data(#{db := Db}, Sid, Key) ->
    case barrel:get_doc(Db, header_id(Sid)) of
        {ok, Header} ->
            case barrel:get_doc(Db, data_id(Sid, Key)) of
                {ok, #{<<"value">> := Value}} ->
                    {ok, Value};
                {error, not_found} ->
                    case maps:find(Key, maps:get(<<"data">>, Header, #{})) of
                        {ok, Value} -> {ok, Value};
                        error -> {error, not_found}
                    end
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Remove one key from the session's data map. The removal
%% replicates as a delete: a node holding the old value cannot bring
%% it back.
-spec delete_data(barrel_spaces:space(), binary(), binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
delete_data(#{db := Db} = Space, Sid, Key) when is_binary(Key) ->
    mutate(Space, Sid, fun() ->
                           barrel_spaces_docs:remove(Db, data_id(Sid, Key))
                       end).

%% @doc Set the session's summary checkpoint.
-spec set_summary(barrel_spaces:space(), binary(), binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
set_summary(#{db := Db} = Space, Sid, Summary) when is_binary(Summary) ->
    mutate(Space, Sid, fun() -> put_summary(Db, Sid, Summary) end).

%% @doc Pin context that must survive truncation: `#{content := term(),
%% priority => 0..10 (0 highest, default 5), id => binary()
%% (caller-supplied pin id, default generated; an id already pinned in
%% this session fails with conflict), pinned_at => unix ms (default
%% now), metadata => map()}'. Returns the pin's id.
-spec pin_context(barrel_spaces:space(), binary(), map()) ->
    {ok, binary()} | {error, term()}.
pin_context(#{db := Db} = Space, Sid, #{content := Content} = Pin) ->
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
            case mutate(Space, Sid, fun() -> put_pin(Db, Sid, Item) end) of
                {ok, _} -> {ok, PinId};
                {error, _} = Err -> Err
            end;
        false ->
            {error, invalid_pin_id}
    end.

valid_pin_id(Id) when is_binary(Id), byte_size(Id) > 0 -> true;
valid_pin_id(_) -> false.

%% @doc Remove a pinned item by id. The removal replicates as a
%% delete: a node holding the old copy cannot bring it back.
-spec unpin_context(barrel_spaces:space(), binary(), binary()) ->
    {ok, non_neg_integer()} | {error, term()}.
unpin_context(#{db := Db} = Space, Sid, PinId) when is_binary(PinId) ->
    mutate(Space, Sid, fun() ->
                           barrel_spaces_docs:remove(Db, pin_id(Sid, PinId))
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
%% Counters
%%====================================================================

%% @doc Add `By' to the session counter `Name' and return its total.
%% Each replica of the space keeps its own count in its own document;
%% the total is their sum, so two nodes counting at once never lose an
%% increment.
-spec incr_counter(barrel_spaces:space(), binary(), binary(), integer()) ->
    {ok, integer()} | {error, term()}.
incr_counter(#{db := Db} = Space, Sid, Name, By)
  when is_binary(Name), is_integer(By) ->
    case mutate(Space, Sid, fun() -> bump(Db, Sid, Name, By, 32) end) of
        {ok, _} ->
            {ok, maps:get(Name, counters(Db, Sid), 0)};
        {error, _} = Err ->
            Err
    end.

%% @doc The session's counters, summed over every replica.
-spec get_counters(barrel_spaces:space(), binary()) ->
    {ok, #{binary() => integer()}} | {error, not_found}.
get_counters(#{db := Db}, Sid) ->
    case barrel:get_doc(Db, header_id(Sid)) of
        {ok, _} -> {ok, counters(Db, Sid)};
        {error, _} = Err -> Err
    end.

%%====================================================================
%% Migration
%%====================================================================

%% @doc Split every session of the space written in one document
%% before 2.0 into its header and child documents. Returns how many
%% were split. Safe to run again.
-spec migrate(barrel_spaces:space()) -> {ok, non_neg_integer()}.
migrate(#{db := Db}) ->
    Legacy = barrel_spaces_docs:fold_prefix(
               Db, <<"session:">>,
               fun(#{<<"type">> := <<"session">>} = H, Acc) ->
                       case is_legacy(H) of
                           true -> [H | Acc];
                           false -> Acc
                       end;
                  (_Doc, Acc) -> Acc
               end, []),
    lists:foreach(fun(H) -> {ok, _} = split(Db, H) end, Legacy),
    {ok, length(Legacy)}.

%%====================================================================
%% Internal
%%====================================================================

%% Every mutation writes its document, then slides the TTL: the header
%% is rewritten with a fresh expires_at derived from the session's own
%% ttl (0 = durable: the expiry stays cleared and callers get
%% {ok, 0}). A header from before 2.0 is split first.
mutate(#{db := Db}, Sid, Write) ->
    case barrel:get_doc(Db, header_id(Sid)) of
        {ok, Header0} ->
            {ok, Header} = split(Db, Header0),
            case Write() of
                ok -> slide(Db, Header);
                {ok, _} -> slide(Db, Header);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

slide(Db, Header) ->
    Now = barrel_spaces:now_ms(),
    Ttl = normalize_ttl(maps:get(<<"ttl">>, Header, ?DEFAULT_TTL)),
    ExpiresAt = case Ttl of
        0 -> 0;
        _ -> Now + Ttl * 1000
    end,
    case barrel_spaces_docs:replace(Db, Header#{<<"updated_at">> => Now},
                                    #{expires_at => ExpiresAt}) of
        {ok, _} -> {ok, ExpiresAt};
        {error, _} = Err -> Err
    end.

header(Sid, Agent, Metadata, Ttl, CreatedAt, UpdatedAt) ->
    #{<<"id">> => header_id(Sid),
      <<"type">> => <<"session">>,
      <<"session">> => Sid,
      <<"agent">> => Agent,
      <<"metadata">> => Metadata,
      <<"ttl">> => Ttl,
      <<"created_at">> => CreatedAt,
      <<"updated_at">> => UpdatedAt}.

%% A new session: the header first (an existing id conflicts), then
%% one document per data key and pin, and the summary.
write_new(Db, Sid, Header, ExpiryOpts, Parts) ->
    case barrel_spaces_docs:create(Db, Header, ExpiryOpts) of
        {ok, _} ->
            ok = write_parts(Db, Sid, Parts, create),
            {ok, Sid};
        {error, _} = Err ->
            Err
    end.

%% Parts of a session: `create' keeps a document already there (a split
%% never overwrites a newer write), `replace' writes over it.
write_parts(Db, Sid, Parts, How) ->
    maps:foreach(
      fun(Key, Value) ->
              ok = keep(put_data(Db, Sid, Key, Value, How))
      end, maps:get(data, Parts, #{})),
    lists:foreach(fun(Item) -> ok = keep(put_pin(Db, Sid, Item)) end,
                  maps:get(pinned, Parts, [])),
    case maps:get(summary, Parts, <<>>) of
        <<>> -> ok;
        Summary -> ok = keep(put_summary(Db, Sid, Summary, How))
    end.

keep({ok, _}) -> ok;
keep({error, conflict}) -> ok.

put_data(Db, Sid, Key, Value) ->
    put_data(Db, Sid, Key, Value, replace).

put_data(Db, Sid, Key, Value, How) ->
    write(How, Db, #{<<"id">> => data_id(Sid, Key),
                     <<"type">> => <<"session_data">>,
                     <<"session">> => Sid,
                     <<"key">> => Key,
                     <<"value">> => Value}).

put_summary(Db, Sid, Summary) ->
    put_summary(Db, Sid, Summary, replace).

put_summary(Db, Sid, Summary, How) ->
    write(How, Db, #{<<"id">> => summary_id(Sid),
                     <<"type">> => <<"session_summary">>,
                     <<"session">> => Sid,
                     <<"summary">> => Summary}).

%% A pin id already pinned conflicts.
put_pin(Db, Sid, #{<<"id">> := PinId} = Item) ->
    write(create, Db, #{<<"id">> => pin_id(Sid, PinId),
                        <<"type">> => <<"session_pin">>,
                        <<"session">> => Sid,
                        <<"pin">> => Item}).

write(create, Db, Doc) -> barrel_spaces_docs:create(Db, Doc, #{});
write(replace, Db, Doc) -> barrel_spaces_docs:replace(Db, Doc, #{}).

%% This replica's count document is written by this replica only, so
%% a read-modify-write on it races only with local writers: retry on
%% their conflict.
bump(Db, Sid, Name, By, N) ->
    Replica = replica(Db),
    Id = count_id(Sid, Replica),
    Doc0 = case barrel:get_doc(Db, Id) of
        {ok, D} -> D;
        {error, not_found} -> #{<<"id">> => Id,
                                <<"type">> => <<"session_count">>,
                                <<"session">> => Sid,
                                <<"replica">> => Replica,
                                <<"counters">> => #{}}
    end,
    Counters = maps:get(<<"counters">>, Doc0, #{}),
    Doc = Doc0#{<<"counters">> =>
                    Counters#{Name => maps:get(Name, Counters, 0) + By}},
    case barrel:put_doc(Db, Doc) of
        {ok, _} -> ok;
        {error, conflict} when N > 0 ->
            barrel_spaces_docs:backoff(N),
            bump(Db, Sid, Name, By, N - 1);
        {error, _} = Err -> Err
    end.

replica(#{docdb := DbBin}) ->
    {ok, Id} = barrel_docdb:db_instance_id(DbBin),
    Id.

counters(Db, Sid) ->
    sum_counters(barrel_spaces_docs:fold_prefix(
                   Db, child_prefix(Sid, <<"count">>),
                   fun(D, Acc) -> [D | Acc] end, [])).

sum_counters(CountDocs) ->
    lists:foldl(
      fun(#{<<"counters">> := C}, Acc) ->
              maps:fold(fun(K, V, A) -> A#{K => maps:get(K, A, 0) + V} end,
                        Acc, C);
         (_Doc, Acc) ->
              Acc
      end, #{}, CountDocs).

%% The documents of one session, read by kind (messages are not read).
parts(Db, Sid) ->
    Fold = fun(Kind) ->
                   barrel_spaces_docs:fold_prefix(
                     Db, child_prefix(Sid, Kind),
                     fun(D, Acc) -> [D | Acc] end, [])
           end,
    Summary = case barrel:get_doc(Db, summary_id(Sid)) of
        {ok, S} -> [S];
        {error, _} -> []
    end,
    Fold(<<"data">>) ++ Fold(<<"pin">>) ++ Fold(<<"count">>) ++ Summary.

%% The session as callers see it: the header with data, pinned,
%% summary and counters folded in. A header from before 2.0 holds its
%% own data, pinned and summary; its documents win over those.
assemble(Header, Parts) ->
    LegacyData = maps:get(<<"data">>, Header, #{}),
    Data = maps:merge(LegacyData,
                      maps:from_list([{K, V} || #{<<"type">> := <<"session_data">>,
                                                  <<"key">> := K,
                                                  <<"value">> := V} <- Parts])),
    Pins = [P || #{<<"type">> := <<"session_pin">>, <<"pin">> := P} <- Parts],
    PinIds = [I || #{<<"id">> := I} <- Pins],
    Legacy = [P || #{<<"id">> := I} = P <- maps:get(<<"pinned">>, Header, []),
                   not lists:member(I, PinIds)],
    Summary = case [S || #{<<"type">> := <<"session_summary">>,
                           <<"summary">> := S} <- Parts] of
        [S | _] -> S;
        [] -> maps:get(<<"summary">>, Header, <<>>)
    end,
    Counters = sum_counters([C || #{<<"type">> := <<"session_count">>} = C
                                      <- Parts]),
    (maps:without([<<"_rev">>], Header))#{
        <<"data">> => Data,
        <<"pinned">> => sort_pins(Pins ++ Legacy),
        <<"summary">> => Summary,
        <<"counters">> => Counters}.

%% Priority first (0 highest), then the newest pin, then the id: the
%% same order on every node.
sort_pins(Pins) ->
    Key = fun(P) -> {maps:get(<<"priority">>, P, 5),
                     -maps:get(<<"pinned_at">>, P, 0),
                     maps:get(<<"id">>, P, <<>>)}
          end,
    lists:sort(fun(A, B) -> Key(A) =< Key(B) end, Pins).

is_legacy(Header) ->
    maps:is_key(<<"data">>, Header) orelse
        maps:is_key(<<"pinned">>, Header) orelse
        maps:is_key(<<"summary">>, Header).

%% A header from before 2.0 holds data, pinned and summary itself: move
%% them to their own documents (never over a newer one) and return the
%% header without them. The caller rewrites the header.
split(Db, #{<<"session">> := Sid} = Header) ->
    case is_legacy(Header) of
        true ->
            ok = write_parts(Db, Sid,
                             #{data => maps:get(<<"data">>, Header, #{}),
                               pinned => maps:get(<<"pinned">>, Header, []),
                               summary => maps:get(<<"summary">>, Header,
                                                   <<>>)},
                             create),
            Bare = maps:without([<<"data">>, <<"pinned">>, <<"summary">>],
                                Header),
            case barrel_spaces_docs:replace(Db, Bare, header_expiry(Bare)) of
                {ok, _} -> {ok, Bare};
                {error, _} = Err -> Err
            end;
        false ->
            {ok, Header}
    end.

%% The header's current expiry, kept by a rewrite that is not a slide.
header_expiry(#{<<"ttl">> := 0}) ->
    #{expires_at => 0};
header_expiry(#{<<"ttl">> := Ttl, <<"updated_at">> := U})
  when is_integer(Ttl), is_integer(U) ->
    #{expires_at => U + Ttl * 1000};
header_expiry(_Header) ->
    #{}.

header_id(Sid) -> <<"session:", Sid/binary>>.
child_prefix(Sid, Kind) -> <<"session:", Sid/binary, ":", Kind/binary, ":">>.
data_id(Sid, Key) -> <<(child_prefix(Sid, <<"data">>))/binary, Key/binary>>.
pin_id(Sid, PinId) -> <<(child_prefix(Sid, <<"pin">>))/binary, PinId/binary>>.
count_id(Sid, Replica) ->
    <<(child_prefix(Sid, <<"count">>))/binary, Replica/binary>>.
summary_id(Sid) -> <<"session:", Sid/binary, ":summary">>.

%% Every session of the space in one fold: headers and their parts
%% (messages skipped), grouped by session id.
fold_sessions(Db) ->
    {Headers, Parts} = barrel_spaces_docs:fold_prefix(
        Db, <<"session:">>,
        fun(#{<<"type">> := <<"session">>, <<"session">> := Sid} = H,
            {Hs, Ps}) ->
                {[{Sid, H} | Hs], Ps};
           (#{<<"type">> := <<"message">>}, Acc) ->
                Acc;
           (#{<<"session">> := Sid} = P, {Hs, Ps}) ->
                {Hs, Ps#{Sid => [P | maps:get(Sid, Ps, [])]}};
           (_Doc, Acc) ->
                Acc
        end, {[], #{}}),
    [assemble(H, maps:get(Sid, Parts, [])) || {Sid, H} <- lists:reverse(Headers)].

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
%% every session. A `data.Key' condition matches the data documents of
%% that key (and the data of headers written before 2.0). Just-expired
%% sessions the sweeper has not tombstoned yet are dropped for parity
%% with the fold path (their expiry is updated_at + ttl by
%% construction).
find_sessions(#{docdb := DbBin} = Db, Agent, Match, Limit) ->
    {DataConds, HeaderConds} =
        lists:partition(fun({[<<"data">>, _ | _], _}) -> true;
                           (_) -> false
                        end,
                        [{split_path(P), V} || {P, V} <- maps:to_list(Match)]),
    Conds0 = [{path, [<<"type">>], <<"session">>}
              | [{path, P, V} || {P, V} <- HeaderConds]],
    Conds = case Agent of
        undefined -> Conds0;
        _ -> [{path, [<<"agent">>], Agent} | Conds0]
    end,
    HeaderLimit = case DataConds of
        [] -> Limit;
        _ -> infinity
    end,
    maybe
        {ok, Headers} ?= find_docs(DbBin, Conds, HeaderLimit),
        {ok, Allowed} ?= data_sessions(DbBin, DataConds),
        Now = barrel_spaces:now_ms(),
        Live = [H || #{<<"session">> := Sid} = H <- Headers,
                     live_session(H, Now),
                     Allowed =:= all orelse sets:is_element(Sid, Allowed)],
        {ok, [assemble(H, parts(Db, Sid))
              || #{<<"session">> := Sid} = H <- take(Live, Limit)]}
    end.

%% The sessions matching every `data.Key' condition: a data document
%% of that key with that value, or a pre-2.0 header holding it.
data_sessions(_DbBin, []) ->
    {ok, all};
data_sessions(DbBin, DataConds) ->
    Sets = [data_matches(DbBin, Key, Rest, V)
            || {[<<"data">>, Key | Rest], V} <- DataConds],
    case [E || {error, _} = E <- Sets] of
        [Err | _] -> Err;
        [] -> {ok, sets:intersection([S || {ok, S} <- Sets])}
    end.

data_matches(DbBin, Key, Rest, V) ->
    Child = [{path, [<<"type">>], <<"session_data">>},
             {path, [<<"key">>], Key},
             {path, [<<"value">> | Rest], V}],
    Legacy = [{path, [<<"type">>], <<"session">>},
              {path, [<<"data">>, Key | Rest], V}],
    maybe
        {ok, A} ?= find_docs(DbBin, Child, infinity),
        {ok, B} ?= find_docs(DbBin, Legacy, infinity),
        {ok, sets:from_list([Sid || #{<<"session">> := Sid} <- A ++ B])}
    end.

find_docs(DbBin, Conds, Limit) ->
    Spec = case Limit of
        infinity -> #{where => Conds};
        _ -> #{where => Conds, limit => Limit}
    end,
    case find_all(DbBin, Spec, undefined, [], Limit) of
        {ok, Rows} ->
            %% find returns #{<<"id">>, <<"doc">>} rows
            {ok, [maps:get(<<"doc">>, R, R) || R <- Rows]};
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
