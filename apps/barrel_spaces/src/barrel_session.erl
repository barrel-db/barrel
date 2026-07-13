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
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_session).

-export([create/1, create/2,
         get/2,
         touch/2,
         delete/2,
         list/1, list/2,
         add_message/3,
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

%% @doc Create a session. Opts: `agent' (binary), `ttl' (seconds,
%% default the space's session_ttl or 3600), `data', `metadata'.
-spec create(barrel_spaces:space(), map()) ->
    {ok, binary()} | {error, term()}.
create(#{id := SpaceId, db := Db}, Opts) when is_map(Opts) ->
    Sid = barrel_spaces:new_id(<<"ses_">>),
    Ttl = maps:get(ttl, Opts, default_ttl(SpaceId)),
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
    case barrel:put_doc(Db, Doc, #{expires_at => Now + Ttl * 1000}) of
        {ok, _} -> {ok, Sid};
        {error, _} = Err -> Err
    end.

%% @doc The session document (expired sessions read as not found).
-spec get(barrel_spaces:space(), binary()) ->
    {ok, map()} | {error, not_found}.
get(#{db := Db}, Sid) when is_binary(Sid) ->
    barrel:get_doc(Db, <<"session:", Sid/binary>>).

%% @doc Slide the session's TTL without changing it.
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

%% @doc Live sessions, optionally filtered by `agent'.
-spec list(barrel_spaces:space(), map()) -> {ok, [map()]}.
list(#{db := #{docdb := DbBin}}, Opts) ->
    Agent = maps:get(agent, Opts, undefined),
    {ok, Sessions} = barrel_docdb:fold_docs(
        DbBin,
        fun(#{<<"type">> := <<"session">>} = Doc, Acc) ->
                case Agent =:= undefined
                     orelse maps:get(<<"agent">>, Doc, <<>>) =:= Agent of
                    true -> {ok, [Doc | Acc]};
                    false -> {ok, Acc}
                end;
           (_Doc, Acc) ->
                {ok, Acc}
        end, [], #{id_prefix => <<"session:">>}),
    {ok, lists:reverse(Sessions)}.

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
    Asc = lists:reverse(Messages),
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
%% priority => 0..10 (0 highest, default 5)}'. Returns the pin's id.
-spec pin_context(barrel_spaces:space(), binary(), map()) ->
    {ok, binary()} | {error, term()}.
pin_context(Space, Sid, #{content := Content} = Pin) ->
    PinId = barrel_spaces:new_id(<<"pin_">>),
    Item = #{
        <<"id">> => PinId,
        <<"content">> => Content,
        <<"priority">> => maps:get(priority, Pin, 5),
        <<"pinned_at">> => barrel_spaces:now_ms()
    },
    case update(Space, Sid, fun(Doc) ->
             Pinned = maps:get(<<"pinned">>, Doc, []),
             Sorted = lists:sort(
                 fun(A, B) ->
                     maps:get(<<"priority">>, A)
                         =< maps:get(<<"priority">>, B)
                 end, [Item | Pinned]),
             Doc#{<<"pinned">> => Sorted}
         end) of
        {ok, _} -> {ok, PinId};
        {error, _} = Err -> Err
    end.

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
%% expires_at derived from the session's own ttl.
update(#{db := Db} = Space, Sid, Fun) ->
    case get(Space, Sid) of
        {ok, Doc} ->
            Now = barrel_spaces:now_ms(),
            Ttl = maps:get(<<"ttl">>, Doc, ?DEFAULT_TTL),
            ExpiresAt = Now + Ttl * 1000,
            Updated = (Fun(Doc))#{<<"updated_at">> => Now},
            case barrel:put_doc(Db, Updated,
                                #{expires_at => ExpiresAt}) of
                {ok, _} -> {ok, ExpiresAt};
                {error, _} = Err -> Err
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
    Ts = integer_to_binary(NowMs),
    TsPad = binary:copy(<<"0">>, 20 - byte_size(Ts)),
    Seq = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    SeqPad = binary:copy(<<"0">>, 20 - byte_size(Seq)),
    <<TsPad/binary, Ts/binary, "-", SeqPad/binary, Seq/binary>>.

take(List, infinity) -> List;
take(List, N) -> lists:sublist(List, N).
