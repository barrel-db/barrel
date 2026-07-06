%%%-------------------------------------------------------------------
%%% @doc Handoffs: transfer work between agents as a shared space plus
%%% a capability. Creating a handoff issues a grant for the space and
%%% returns the token once; POSSESSION OF THE TOKEN IS THE RIGHT TO
%%% ACCEPT (barrel_memory's handoffs let any authenticated caller
%%% accept anything; this closes that hole). Accepting flips the
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
         chain/2]).

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
                <<"status">> => <<"pending">>,
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
    {ok, Docs} = barrel_docdb:fold_docs(
        Registry,
        fun(#{<<"type">> := <<"handoff">>} = Doc, Acc) ->
                case matches(Doc, Filter) of
                    true -> {ok, [Doc | Acc]};
                    false -> {ok, Acc}
                end;
           (_Doc, Acc) ->
                {ok, Acc}
        end, [], #{id_prefix => <<"handoff:">>}),
    {ok, lists:reverse(Docs)}.

%% @doc One handoff by id.
-spec get(binary()) -> {ok, map()} | {error, not_found}.
get(Hid) when is_binary(Hid) ->
    barrel_docdb:get_doc(barrel_spaces:registry_db(),
                         <<"handoff:", Hid/binary>>).

%% @doc Accept a handoff by presenting its token. Opts: `agent' (the
%% acceptor's name), `open_opts' (runtime open options for the space,
%% e.g. its encryption spec), `session' (map passed to
%% barrel_session:create). Returns the shared space handle and a fresh
%% session in it; the from-agent's context is read in place.
-spec accept(binary(), map()) ->
    {ok, #{handoff := map(), space := barrel_spaces:space(),
           session := binary()}} |
    {error, term()}.
accept(Token, Opts) when is_binary(Token), is_map(Opts) ->
    case resolve(Token) of
        {ok, #{<<"status">> := <<"pending">>, <<"space">> := SpaceId,
               <<"handoff">> := Hid} = Doc} ->
            case barrel_caps:verify(Token, SpaceId, read) of
                {ok, _Grant} ->
                    Agent = maps:get(agent, Opts, <<>>),
                    Flipped = Doc#{
                        <<"status">> => <<"accepted">>,
                        <<"accepted_by">> => Agent,
                        <<"accepted_at">> => barrel_spaces:now_ms()
                    },
                    case barrel_docdb:put_doc(
                             barrel_spaces:registry_db(), Flipped) of
                        {ok, _} ->
                            open_accepted(Hid, SpaceId, Agent, Opts);
                        {error, conflict} ->
                            {error, already_accepted};
                        {error, {conflict, _}} ->
                            {error, already_accepted};
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
                    Done = Doc#{
                        <<"status">> => <<"completed">>,
                        <<"result">> => maps:get(result, Opts, <<>>),
                        <<"notes">> => maps:get(notes, Opts, <<>>),
                        <<"completed_at">> => barrel_spaces:now_ms()
                    },
                    {ok, _} = barrel_docdb:put_doc(
                        barrel_spaces:registry_db(), Done),
                    case maps:get(revoke, Opts, true) of
                        true -> ok = barrel_caps:revoke(Token);
                        false -> ok
                    end,
                    {ok, public(Done)};
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

%%====================================================================
%% Internal
%%====================================================================

%% The handoff of a token: token id -> registry scan (the registry is
%% small metadata; an index is a later optimization).
resolve(Token) ->
    case barrel_caps:token_id(Token) of
        {ok, TokenId} ->
            {ok, Handoffs} = list(#{}),
            case [H || #{<<"token_id">> := T} = H <- Handoffs,
                       T =:= TokenId] of
                [Doc] -> {ok, Doc};
                [] -> {error, not_found}
            end;
        error ->
            {error, invalid_token}
    end.

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
                    {ok, #{handoff => public(Doc), space => Space,
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

public(Doc) ->
    maps:without([<<"_rev">>], Doc).
