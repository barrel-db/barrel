%%%-------------------------------------------------------------------
%%% @doc Capability tokens for spaces: opaque random tokens verified
%%% against server-side grant documents in the `_barrel_spaces'
%%% registry, so revocation is immediate and no key material lives on
%%% disk (only a SHA-256 of the full token is stored).
%%%
%%% Token shape: `bsp_' + TokenId (16 base32 chars, names the grant
%%% doc `grant:TokenId') + `_' + Secret (40 base32 chars). Rights form
%%% a ladder: `read < write < admin' (admin covers grants, drops,
%%% branch/merge, and handoff issuance). A token is shown ONCE at
%%% grant time; treat it like a password.
%%%
%%% The granted scope is any binary id: grant/2 does not require a
%%% space database to exist, so purely logical scopes (a tenant id, a
%%% handoff id) get revocable, hash-at-rest, rights-laddered tokens
%%% without a single space being created. This is intended and
%%% supported.
%%%
%%% The registry is a regular barrel database (name from the
%%% `registry_db' app env of barrel_spaces). Replicating it is
%%% intended: grant docs are authorization state, so a token minted on
%%% one node verifies on any node the registry reaches. A grant is
%%% written once and never edited; revoking writes a separate
%%% `revoke:TokenId' document, so no concurrent write can undo a
%%% revocation. Verification itself is always a local read.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_caps).

-export([grant/2,
         verify/3,
         revoke/1, revoke/2,
         revoke_all/1,
         list/1,
         auth_context/1,
         token_id/1,
         migrate/0]).

-type right() :: read | write | admin.
-export_type([right/0]).

-define(TOKEN_ID_BYTES, 10).   %% 16 base32 chars
-define(SECRET_BYTES, 25).     %% 40 base32 chars

%%====================================================================
%% API
%%====================================================================

%% @doc Issue a capability for a space. Opts: `rights' (default
%% [read]), `subject', `label', `issued_by' (metadata binaries),
%% `expires_at' (unix ms, 0 = never). Returns the token (shown once)
%% and the stored grant.
-spec grant(binary(), map()) ->
    {ok, Token :: binary(), Grant :: map()} | {error, term()}.
grant(SpaceId, Opts) when is_binary(SpaceId), is_map(Opts) ->
    Rights = maps:get(rights, Opts, [read]),
    case validate_rights(Rights) of
        ok ->
            Registry = barrel_spaces:registry_db(),
            TokenId = barrel_spaces:new_id(<<>>),
            Secret = barrel_spaces:base32(
                crypto:strong_rand_bytes(?SECRET_BYTES)),
            Token = <<"bsp_", TokenId/binary, "_", Secret/binary>>,
            Grant = #{
                <<"id">> => <<"grant:", TokenId/binary>>,
                <<"type">> => <<"grant">>,
                <<"token_id">> => TokenId,
                %% hex: the registry replicates as JSON
                <<"token_hash">> => binary:encode_hex(crypto:hash(sha256, Token),
                                                      lowercase),
                <<"space">> => SpaceId,
                <<"rights">> => [atom_to_binary(R, utf8) || R <- Rights],
                <<"subject">> => maps:get(subject, Opts, <<>>),
                <<"issued_by">> => maps:get(issued_by, Opts, <<>>),
                <<"label">> => maps:get(label, Opts, <<>>),
                <<"created_at">> => barrel_spaces:now_ms(),
                <<"expires_at">> => maps:get(expires_at, Opts, 0),
                <<"revoked_at">> => 0
            },
            {ok, _} = barrel_docdb:put_doc(Registry, Grant),
            {ok, Token, public_grant(Grant)};
        {error, _} = Err ->
            Err
    end.

%% @doc Verify a token grants `Right' on `SpaceId'. Fail closed:
%% unknown, tampered, revoked, expired, wrong-space, and insufficient
%% tokens all return errors that never echo token material.
-spec verify(binary(), binary(), right()) ->
    {ok, map()} | {error, invalid_token | not_found | revoked | expired
                          | wrong_space | forbidden}.
verify(Token, SpaceId, Right)
  when is_binary(Token), is_binary(SpaceId),
       (Right =:= read orelse Right =:= write orelse Right =:= admin) ->
    case check(Token) of
        {ok, Grant} ->
            case maps:get(<<"space">>, Grant) of
                SpaceId ->
                    Rights = grant_rights(Grant),
                    case covers(Rights, Right) of
                        true -> {ok, public_grant(Grant)};
                        false -> {error, forbidden}
                    end;
                _Other ->
                    {error, wrong_space}
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Revoke a grant by full token or token id. Idempotent.
-spec revoke(binary()) -> ok | {error, term()}.
revoke(TokenOrId) ->
    revoke(TokenOrId, #{}).

%% @doc Revoke a grant, recording who did it. Opts: `by' (binary).
%% Writes the `revoke:TokenId' document once, also on a node the grant
%% has not reached yet; every node revoking the same token writes the
%% same id, so the token stays refused whichever copy wins.
-spec revoke(binary(), map()) -> ok | {error, term()}.
revoke(TokenOrId, Opts) when is_binary(TokenOrId), is_map(Opts) ->
    case token_id(TokenOrId) of
        {ok, TokenId} ->
            Registry = barrel_spaces:registry_db(),
            case barrel_docdb:get_doc(Registry,
                                      <<"grant:", TokenId/binary>>) of
                {ok, Grant} ->
                    write_revoke(Registry, Grant,
                                 maps:get(by, Opts, <<>>));
                {error, not_found} ->
                    %% the grant may not have replicated here yet: the
                    %% revocation must still reach it
                    write_revoke(Registry, #{<<"token_id">> => TokenId,
                                             <<"space">> => <<>>},
                                 maps:get(by, Opts, <<>>))
            end;
        error ->
            {error, invalid_token}
    end.

%% @doc Revoke every live grant of a space (drop_space calls this).
-spec revoke_all(binary()) -> ok.
revoke_all(SpaceId) when is_binary(SpaceId) ->
    Registry = barrel_spaces:registry_db(),
    {ok, Grants} = fold_grants(Registry, SpaceId),
    lists:foreach(fun(Grant) ->
                      ok = write_revoke(Registry, Grant, <<>>)
                  end, Grants),
    ok.

%% @doc The grants of a space (hashes stripped).
-spec list(binary()) -> {ok, [map()]}.
list(SpaceId) when is_binary(SpaceId) ->
    Registry = barrel_spaces:registry_db(),
    {ok, Grants} = fold_grants(Registry, SpaceId),
    Revokes = revokes(Registry),
    {ok, [public_grant(with_revoke(G, Revokes)) || G <- Grants]}.

%% @doc Resolve a token into an authentication context: the seam the
%% HTTP middleware and the MCP auth provider consume. Checks
%% everything verify/3 checks except a specific space/right.
-spec auth_context(binary()) ->
    {ok, #{space := binary(), subject := binary(),
           rights := [right()], scopes := [binary()]}} |
    {error, term()}.
auth_context(Token) when is_binary(Token) ->
    case check(Token) of
        {ok, Grant} ->
            Rights = grant_rights(Grant),
            {ok, #{space => maps:get(<<"space">>, Grant),
                   subject => maps:get(<<"subject">>, Grant, <<>>),
                   rights => Rights,
                   scopes => [atom_to_binary(R, utf8) || R <- Rights]}};
        {error, _} = Err ->
            Err
    end.

%% @doc Give every grant revoked in place before 2.0 its `revoke:'
%% document. Returns how many were written. Safe to run again.
-spec migrate() -> {ok, non_neg_integer()}.
migrate() ->
    Registry = barrel_spaces:registry_db(),
    Revokes = revokes(Registry),
    Legacy = barrel_spaces_docs:fold_prefix(
               Registry, <<"grant:">>,
               fun(#{<<"token_id">> := T, <<"revoked_at">> := R} = G, Acc)
                     when is_integer(R), R > 0 ->
                       case maps:is_key(T, Revokes) of
                           true -> Acc;
                           false -> [G | Acc]
                       end;
                  (_Doc, Acc) -> Acc
               end, []),
    lists:foreach(fun(G) -> ok = write_revoke(Registry, G, <<>>) end, Legacy),
    {ok, length(Legacy)}.

%%====================================================================
%% Internal
%%====================================================================

%% Shared token checks: parse, hash compare (constant time), revocation
%% and expiry.
check(Token) ->
    case token_id(Token) of
        {ok, TokenId} when byte_size(Token) =:=
                           4 + 16 + 1 + (?SECRET_BYTES * 8 div 5) ->
            Registry = barrel_spaces:registry_db(),
            case barrel_docdb:get_doc(Registry,
                                      <<"grant:", TokenId/binary>>) of
                {ok, Grant} ->
                    Stored = stored_hash(maps:get(<<"token_hash">>, Grant)),
                    case crypto:hash_equals(crypto:hash(sha256, Token),
                                            Stored) of
                        true -> check_liveness(revoked(Registry, Grant));
                        false -> {error, invalid_token}
                    end;
                {error, not_found} ->
                    {error, not_found}
            end;
        _ ->
            {error, invalid_token}
    end.

%% Hex since 1.3.0; grants minted before hold the raw 32 bytes.
stored_hash(<<Hex:64/binary>>) -> binary:decode_hex(Hex);
stored_hash(<<Raw:32/binary>>) -> Raw.

%% The grant with its revocation: the `revoke:' document, else the
%% `revoked_at' a grant edited in place before 2.0 carries.
revoked(Registry, #{<<"token_id">> := TokenId} = Grant) ->
    case barrel_docdb:get_doc(Registry, <<"revoke:", TokenId/binary>>) of
        {ok, Revoke} -> with_revoke(Grant, #{TokenId => Revoke});
        {error, not_found} -> Grant
    end.

revokes(Registry) ->
    barrel_spaces_docs:fold_prefix(
      Registry, <<"revoke:">>,
      fun(#{<<"token_id">> := T} = R, Acc) -> Acc#{T => R};
         (_Doc, Acc) -> Acc
      end, #{}).

with_revoke(#{<<"token_id">> := TokenId} = Grant, Revokes) ->
    case maps:find(TokenId, Revokes) of
        {ok, #{<<"revoked_at">> := At} = R} ->
            Grant#{<<"revoked_at">> => At,
                   <<"revoked_by">> => maps:get(<<"by">>, R, <<>>)};
        error ->
            Grant
    end.

%% Written once. A grant revoked in place before 2.0 keeps its time.
write_revoke(Registry, #{<<"token_id">> := TokenId,
                         <<"space">> := SpaceId} = Grant, By) ->
    At = case maps:get(<<"revoked_at">>, Grant, 0) of
        R when is_integer(R), R > 0 -> R;
        _ -> barrel_spaces:now_ms()
    end,
    case barrel_docdb:get_doc(Registry, <<"revoke:", TokenId/binary>>) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            Doc = #{<<"id">> => <<"revoke:", TokenId/binary>>,
                    <<"type">> => <<"revoke">>,
                    <<"token_id">> => TokenId,
                    <<"space">> => SpaceId,
                    <<"revoked_at">> => At,
                    <<"by">> => By},
            case barrel_spaces_docs:create(Registry, Doc, #{}) of
                {ok, _} -> ok;
                {error, conflict} -> ok;
                {error, _} = Err -> Err
            end
    end.

check_liveness(#{<<"revoked_at">> := R}) when R > 0 ->
    {error, revoked};
check_liveness(#{<<"expires_at">> := E} = Grant) ->
    case E > 0 andalso E =< barrel_spaces:now_ms() of
        true -> {error, expired};
        false -> {ok, Grant}
    end.

%% @doc The grant-naming id inside a token (or a bare token id).
-spec token_id(binary()) -> {ok, binary()} | error.
token_id(<<"bsp_", TokenId:16/binary, "_", _Secret/binary>>) ->
    {ok, TokenId};
token_id(TokenId) when is_binary(TokenId), byte_size(TokenId) =:= 16 ->
    {ok, TokenId};
token_id(_) ->
    error.

grant_rights(Grant) ->
    [binary_to_right(R) || R <- maps:get(<<"rights">>, Grant, [])].

binary_to_right(<<"read">>) -> read;
binary_to_right(<<"write">>) -> write;
binary_to_right(<<"admin">>) -> admin.

validate_rights([]) ->
    {error, empty_rights};
validate_rights(Rights) when is_list(Rights) ->
    case lists:all(fun(R) -> lists:member(R, [read, write, admin]) end,
                   Rights) of
        true -> ok;
        false -> {error, invalid_rights}
    end;
validate_rights(_) ->
    {error, invalid_rights}.

%% read < write < admin
rank(read) -> 1;
rank(write) -> 2;
rank(admin) -> 3.

covers(Granted, Required) ->
    lists:any(fun(G) -> rank(G) >= rank(Required) end, Granted).

fold_grants(Registry, SpaceId) ->
    barrel_docdb:fold_docs(
        Registry,
        fun(#{<<"type">> := <<"grant">>, <<"space">> := S} = Doc, Acc)
              when S =:= SpaceId ->
                {ok, [Doc | Acc]};
           (_Doc, Acc) ->
                {ok, Acc}
        end, [], #{id_prefix => <<"grant:">>}).

public_grant(Grant) ->
    maps:without([<<"token_hash">>, <<"_rev">>], Grant).
