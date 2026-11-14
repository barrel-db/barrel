%%%-------------------------------------------------------------------
%%% @doc Handoffs: transfer work between agents as a shared space plus
%%% a capability. Creating a handoff issues a grant for the space and
%%% returns the token once; POSSESSION OF THE TOKEN IS THE RIGHT TO
%%% ACCEPT (a naive design lets any authenticated caller accept
%%% anything; this closes that hole). Accepting flips the
%%% handoff doc pending -> accepted with a rev CAS (double accepts
%%% lose), opens the shared space, and creates a session for the
%%% acceptor IN it: context is read in place, no snapshot copying.
%%%
%%% Handoff docs live in the `_barrel_spaces' registry, so pending
%%% handoffs are discoverable with folds and the registry's changes
%%% feed. Completing revokes the grant by default; chaining mints a
%%% new handoff plus grant carrying parent/root lineage.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_handoff).
-compile({no_auto_import, [get/1]}).

-export([create/2,
         list/1,
         get/1,
         accept/2,
         complete/2,
         chain/2,
         migrate/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a handoff for a space. Opts: `task_name' (required),
%% `from_agent', `to_agent' (advisory routing labels), `from_session',
%% `context' (binary or map), `completed'/`pending'/`blockers'/`files'
%% (lists), `priority', `rights' (grant rights, default [read, write]),
%% `expires_at' (grant expiry, unix ms). Returns the handoff id and
%% the capability token, shown once.
-spec create(binary() | barrel_spaces:space(), map()) ->
    {ok, #{handoff_id := binary(), token := binary()}} |
    {error, term()}.
create(#{id := SpaceId}, Opts) ->
    create(SpaceId, Opts);
create(SpaceId, #{task_name := TaskName} = Opts)
  when is_binary(SpaceId), is_binary(TaskName) ->
    Registry = barrel_spaces:registry_db(),
    Rights = maps:get(rights, Opts, [read, write]),
    GrantOpts0 = #{rights => Rights,
                   subject => maps:get(to_agent, Opts, <<>>),
                   issued_by => maps:get(from_agent, Opts, <<>>),
                   label => <<"handoff">>},
    GrantOpts = case maps:find(expires_at, Opts) of
        {ok, E} -> GrantOpts0#{expires_at => E};
        error -> GrantOpts0
    end,
    case barrel_caps:grant(SpaceId, GrantOpts) of
        {ok, Token, Grant} ->
            Hid = barrel_spaces:new_id(<<"ho_">>),
            Root = maps:get(chain_root, Opts, Hid),
            Depth = maps:get(chain_depth, Opts, 0),
            Doc = #{
                <<"id">> => <<"handoff:", Hid/binary>>,
                <<"type">> => <<"handoff">>,
                <<"handoff">> => Hid,
                <<"space">> => SpaceId,
                <<"token_id">> => maps:get(<<"token_id">>, Grant),
                <<"task_name">> => TaskName,
                <<"from_agent">> => maps:get(from_agent, Opts, <<>>),
                <<"to_agent">> => maps:get(to_agent, Opts, <<>>),
                <<"from_session">> => maps:get(from_session, Opts, <<>>),
                <<"context">> => maps:get(context, Opts, <<>>),
                <<"completed">> => maps:get(completed, Opts, []),
                <<"pending">> => maps:get(pending, Opts, []),
                <<"blockers">> => maps:get(blockers, Opts, []),
                <<"files">> => maps:get(files, Opts, []),
                <<"priority">> => maps:get(priority, Opts, <<"normal">>),
                <<"parent_handoff">> =>
                    maps:get(parent_handoff, Opts, <<>>),
                <<"chain_root">> => Root,
                <<"chain_depth">> => Depth,
                <<"created_at">> => barrel_spaces:now_ms()
            },
            {ok, _} = barrel_docdb:put_doc(Registry, Doc),
            TokenId = maps:get(<<"token_id">>, Grant),
            {ok, _} = barrel_docdb:put_doc(Registry, #{
                <<"id">> => <<"handoff_token:", TokenId/binary>>,
                <<"type">> => <<"handoff_token">>,
                <<"handoff">> => Hid
            }),
            {ok, #{handoff_id => Hid, token => Token}};
        {error, _} = Err ->
            Err
    end;
create(_SpaceId, _Opts) ->
    {error, task_name_required}.

%% @doc Handoffs filtered by `to_agent', `from_agent', `status',
%% `space' (all optional).
-spec list(map()) -> {ok, [map()]}.
list(Filter) when is_map(Filter) ->
    Registry = barrel_spaces:registry_db(),
    {Bases, Steps} = barrel_spaces_docs:fold_prefix(
        Registry, <<"handoff:">>,
        fun(#{<<"type">> := <<"handoff">>} = Doc, {B, S}) ->
                {[Doc | B], S};
           (#{<<"handoff">> := Hid} = Doc, {B, S}) ->
                {B, S#{Hid => [Doc | maps:get(Hid, S, [])]}};
           (_Doc, Acc) ->
                Acc
        end, {[], #{}}),
    Views = [view(B, maps:get(Hid, Steps, []))
             || #{<<"handoff">> := Hid} = B <- lists:reverse(Bases)],
    {ok, [V || V <- Views, matches(V, Filter)]}.

%% @doc One handoff by id, with its status and steps.
-spec get(binary()) -> {ok, map()} | {error, not_found}.
get(Hid) when is_binary(Hid) ->
    Registry = barrel_spaces:registry_db(),
    case barrel_docdb:get_doc(Registry, <<"handoff:", Hid/binary>>) of
        {ok, Base} ->
            {ok, view(Base, steps(Registry, Hid))};
        {error, _} = Err ->
            Err
    end.

%% @doc Accept a handoff by presenting its token. Opts: `agent' (the
%% acceptor's name), `open_opts' (runtime open options for the space,
%% e.g. its encryption spec), `session' (map passed to
%% barrel_session:create, or `false' to accept WITHOUT opening the
%% space or creating a session: the token discipline alone, for
%% consumers with their own session model). Returns the shared space
%% handle and a fresh session in it (both absent with
%% `session => false'); the from-agent's context is read in place.
-spec accept(binary(), map()) ->
    {ok, #{handoff := map(), space => barrel_spaces:space(),
           session => binary()}} |
    {error, term()}.
accept(Token, Opts) when is_binary(Token), is_map(Opts) ->
    case resolve(Token) of
        {ok, #{<<"status">> := <<"pending">>, <<"space">> := SpaceId,
               <<"handoff">> := Hid}} ->
            case barrel_caps:verify(Token, SpaceId, read) of
                {ok, _Grant} ->
                    Agent = maps:get(agent, Opts, <<>>),
                    case write_accept(Hid, Agent) of
                        ok ->
                            case maps:get(session, Opts, #{}) of
                                false ->
                                    {ok, Accepted} = get(Hid),
                                    {ok, #{handoff => Accepted}};
                                _ ->
                                    open_accepted(Hid, SpaceId, Agent,
                                                  Opts)
                            end;
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {ok, #{<<"status">> := <<"accepted">>}} ->
            {error, already_accepted};
        {ok, #{<<"status">> := <<"completed">>}} ->
            {error, already_completed};
        {error, _} = Err ->
            Err
    end.

%% @doc Complete a handoff. Opts: `result' (term), `notes' (binary),
%% `revoke' (revoke the grant, default true).
-spec complete(binary(), map()) -> {ok, map()} | {error, term()}.
complete(Token, Opts) when is_binary(Token), is_map(Opts) ->
    case resolve(Token) of
        {ok, #{<<"status">> := Status, <<"space">> := SpaceId} = Doc}
          when Status =:= <<"pending">>; Status =:= <<"accepted">> ->
            case barrel_caps:verify(Token, SpaceId, read) of
                {ok, _} ->
                    case write_complete(Doc, Opts) of
                        ok ->
                            case maps:get(revoke, Opts, true) of
                                true -> ok = barrel_caps:revoke(Token);
                                false -> ok
                            end,
                            get(maps:get(<<"handoff">>, Doc));
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {ok, #{<<"status">> := <<"completed">>}} ->
            {error, already_completed};
        {error, _} = Err ->
            Err
    end.

%% @doc Chain: hand the task on. Creates a new handoff (and grant) in
%% the same space carrying the lineage; the presented token's handoff
%% is marked completed (not revoked by default so the chainer can
%% keep working; pass `revoke => true' to cut it).
-spec chain(binary(), map()) ->
    {ok, #{handoff_id := binary(), token := binary()}} |
    {error, term()}.
chain(Token, #{task_name := _} = CreateOpts) when is_binary(Token) ->
    case resolve(Token) of
        {ok, #{<<"space">> := SpaceId, <<"handoff">> := Hid,
               <<"chain_root">> := Root0,
               <<"chain_depth">> := Depth} = _Doc} ->
            case barrel_caps:verify(Token, SpaceId, read) of
                {ok, _} ->
                    Root = case Root0 of <<>> -> Hid; _ -> Root0 end,
                    case complete(Token, #{revoke =>
                                               maps:get(revoke,
                                                        CreateOpts,
                                                        false)}) of
                        {ok, _} ->
                            create(SpaceId, CreateOpts#{
                                parent_handoff => Hid,
                                chain_root => Root,
                                chain_depth => Depth + 1});
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Give every handoff accepted or completed in place before 2.0
%% its step documents. Returns how many were written. Safe to run
%% again.
-spec migrate() -> {ok, non_neg_integer()}.
migrate() ->
    Registry = barrel_spaces:registry_db(),
    Bases = barrel_spaces_docs:fold_prefix(
              Registry, <<"handoff:">>,
              fun(#{<<"type">> := <<"handoff">>} = D, Acc) -> [D | Acc];
                 (_Doc, Acc) -> Acc
              end, []),
    {ok, lists:sum([legacy_steps(Registry, B) || B <- Bases])}.

%%====================================================================
%% Internal
%%====================================================================

%% The handoff of a token: token id -> `handoff_token:' index doc
%% (written at create); handoffs created before the index existed
%% fall back to a registry scan.
resolve(Token) ->
    case barrel_caps:token_id(Token) of
        {ok, TokenId} ->
            Registry = barrel_spaces:registry_db(),
            case barrel_docdb:get_doc(Registry,
                                      <<"handoff_token:",
                                        TokenId/binary>>) of
                {ok, #{<<"handoff">> := Hid}} ->
                    get(Hid);
                {error, not_found} ->
                    resolve_scan(TokenId)
            end;
        error ->
            {error, invalid_token}
    end.

resolve_scan(TokenId) ->
    {ok, Handoffs} = list(#{}),
    case [H || #{<<"token_id">> := T} = H <- Handoffs,
               T =:= TokenId] of
        [Doc] -> {ok, Doc};
        [] -> {error, not_found}
    end.

%% Accept documents are per agent: a second accept by the same agent
%% conflicts, and an agent that is not the owner once its document is
%% in (a local race) loses like the old rev check made it lose.
write_accept(Hid, Agent) ->
    Registry = barrel_spaces:registry_db(),
    Doc = #{<<"id">> => <<"handoff:", Hid/binary, ":accept:", Agent/binary>>,
            <<"type">> => <<"handoff_accept">>,
            <<"handoff">> => Hid,
            <<"agent">> => Agent,
            <<"accepted_at">> => barrel_spaces:now_ms()},
    case barrel_spaces_docs:create(Registry, Doc, #{}) of
        {ok, _} ->
            case get(Hid) of
                {ok, #{<<"status">> := <<"accepted">>,
                       <<"accepted_by">> := Agent}} -> ok;
                {ok, #{<<"status">> := <<"completed">>}} ->
                    {error, already_completed};
                {ok, _} -> {error, already_accepted}
            end;
        {error, conflict} ->
            {error, already_accepted};
        {error, _} = Err ->
            Err
    end.

write_complete(#{<<"handoff">> := Hid}, Opts) ->
    Registry = barrel_spaces:registry_db(),
    {ok, Base} = barrel_docdb:get_doc(Registry, <<"handoff:", Hid/binary>>),
    _ = legacy_steps(Registry, Base),
    Doc = #{<<"id">> => <<"handoff:", Hid/binary, ":complete">>,
            <<"type">> => <<"handoff_complete">>,
            <<"handoff">> => Hid,
            <<"result">> => maps:get(result, Opts, <<>>),
            <<"notes">> => maps:get(notes, Opts, <<>>),
            <<"completed_at">> => barrel_spaces:now_ms()},
    case barrel_spaces_docs:create(Registry, Doc, #{}) of
        {ok, _} -> ok;
        {error, conflict} -> {error, already_completed};
        {error, _} = Err -> Err
    end.

%% The steps of one handoff.
steps(Registry, Hid) ->
    barrel_spaces_docs:fold_prefix(Registry, <<"handoff:", Hid/binary, ":">>,
                                   fun(D, Acc) -> [D | Acc] end, []).

%% The handoff as callers see it: the create document, its status (the
%% furthest step present) and the owner and completion of its steps.
%% A handoff edited in place before 2.0 carries its steps in its own
%% fields; they count as steps too.
view(Base0, Steps) ->
    Base = maps:without([<<"_rev">>], Base0),
    Accepts = [#{<<"agent">> => A, <<"accepted_at">> => At}
               || #{<<"type">> := <<"handoff_accept">>, <<"agent">> := A,
                    <<"accepted_at">> := At} <- Steps] ++ legacy_accept(Base),
    Completes = [maps:with([<<"result">>, <<"notes">>, <<"completed_at">>], C)
                 || #{<<"type">> := <<"handoff_complete">>} = C <- Steps],
    Status = status(maps:get(<<"status">>, Base, <<"pending">>),
                    Accepts, Completes),
    V0 = maps:merge(Base, owner(Accepts)),
    V1 = case Completes of
        [C | _] -> maps:merge(V0, C);
        [] -> V0
    end,
    V1#{<<"status">> => Status}.

status(_Legacy, _Accepts, [_ | _]) -> <<"completed">>;
status(<<"completed">>, _Accepts, []) -> <<"completed">>;
status(_Legacy, [_ | _], []) -> <<"accepted">>;
status(Legacy, [], []) -> Legacy.

legacy_accept(#{<<"accepted_by">> := A, <<"accepted_at">> := At}) ->
    [#{<<"agent">> => A, <<"accepted_at">> => At}];
legacy_accept(_Base) ->
    [].

%% Earliest accepted_at, then agent name: every node names the same
%% owner once it holds the same accept documents.
owner([]) ->
    #{};
owner(Accepts) ->
    [#{<<"agent">> := A, <<"accepted_at">> := At} | _] =
        lists:sort(fun(#{<<"agent">> := A1, <<"accepted_at">> := T1},
                       #{<<"agent">> := A2, <<"accepted_at">> := T2}) ->
                       {T1, A1} =< {T2, A2}
                   end, Accepts),
    #{<<"accepted_by">> => A, <<"accepted_at">> => At}.

%% A handoff edited in place before 2.0 gains its step documents.
legacy_steps(Registry, #{<<"handoff">> := Hid} = Base) ->
    Accept = case Base of
        #{<<"accepted_by">> := A, <<"accepted_at">> := At} ->
            [#{<<"id">> => <<"handoff:", Hid/binary, ":accept:", A/binary>>,
               <<"type">> => <<"handoff_accept">>, <<"handoff">> => Hid,
               <<"agent">> => A, <<"accepted_at">> => At}];
        _ ->
            []
    end,
    Complete = case Base of
        #{<<"status">> := <<"completed">>} ->
            [#{<<"id">> => <<"handoff:", Hid/binary, ":complete">>,
               <<"type">> => <<"handoff_complete">>, <<"handoff">> => Hid,
               <<"result">> => maps:get(<<"result">>, Base, <<>>),
               <<"notes">> => maps:get(<<"notes">>, Base, <<>>),
               <<"completed_at">> => maps:get(<<"completed_at">>, Base, 0)}];
        _ ->
            []
    end,
    lists:foldl(fun(Doc, N) ->
                    case barrel_spaces_docs:create(Registry, Doc, #{}) of
                        {ok, _} -> N + 1;
                        {error, conflict} -> N
                    end
                end, 0, Accept ++ Complete).

open_accepted(Hid, SpaceId, Agent, Opts) ->
    OpenOpts = maps:get(open_opts, Opts, #{}),
    case barrel_spaces:open_space(SpaceId, OpenOpts) of
        {ok, Space} ->
            SessionOpts = maps:get(session, Opts, #{}),
            Data = maps:get(data, SessionOpts, #{}),
            case barrel_session:create(Space, SessionOpts#{
                     agent => Agent,
                     data => Data#{<<"handoff_id">> => Hid}}) of
                {ok, Sid} ->
                    {ok, Doc} = get(Hid),
                    {ok, #{handoff => Doc, space => Space,
                           session => Sid}};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

matches(Doc, Filter) ->
    Checks = [{to_agent, <<"to_agent">>}, {from_agent, <<"from_agent">>},
              {status, <<"status">>}, {space, <<"space">>}],
    lists:all(
        fun({OptKey, DocKey}) ->
            case maps:find(OptKey, Filter) of
                error -> true;
                {ok, Want} -> maps:get(DocKey, Doc, <<>>) =:= Want
            end
        end, Checks).
