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
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_caps).

-export([grant/2,
         verify/3,
         revoke/1,
         revoke_all/1,
         list/1,
         auth_context/1]).

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
                <<"token_hash">> => crypto:hash(sha256, Token),
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
revoke(TokenOrId) when is_binary(TokenOrId) ->
    case token_id(TokenOrId) of
        {ok, TokenId} ->
            Registry = barrel_spaces:registry_db(),
            case barrel_docdb:get_doc(Registry,
                                      <<"grant:", TokenId/binary>>) of
                {ok, #{<<"revoked_at">> := R}} when R > 0 ->
                    ok;
                {ok, Grant} ->
                    {ok, _} = barrel_docdb:put_doc(
                        Registry,
                        Grant#{<<"revoked_at">> =>
                                   barrel_spaces:now_ms()}),
                    ok;
                {error, not_found} ->
                    ok
            end;
        error ->
            {error, invalid_token}
    end.

%% @doc Revoke every live grant of a space (drop_space calls this).
-spec revoke_all(binary()) -> ok.
revoke_all(SpaceId) when is_binary(SpaceId) ->
    Registry = barrel_spaces:registry_db(),
    Now = barrel_spaces:now_ms(),
    {ok, Grants} = fold_grants(Registry, SpaceId),
    lists:foreach(
        fun(#{<<"revoked_at">> := R} = Grant) when R =:= 0 ->
                {ok, _} = barrel_docdb:put_doc(
                    Registry, Grant#{<<"revoked_at">> => Now});
           (_) ->
                ok
        end, Grants),
    ok.

%% @doc The grants of a space (hashes stripped).
-spec list(binary()) -> {ok, [map()]}.
list(SpaceId) when is_binary(SpaceId) ->
    Registry = barrel_spaces:registry_db(),
    {ok, Grants} = fold_grants(Registry, SpaceId),
    {ok, [public_grant(G) || G <- Grants]}.

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
                    Stored = maps:get(<<"token_hash">>, Grant),
                    case crypto:hash_equals(crypto:hash(sha256, Token),
                                            Stored) of
                        true -> check_liveness(Grant);
                        false -> {error, invalid_token}
                    end;
                {error, not_found} ->
                    {error, not_found}
            end;
        _ ->
            {error, invalid_token}
    end.

check_liveness(#{<<"revoked_at">> := R}) when R > 0 ->
    {error, revoked};
check_liveness(#{<<"expires_at">> := E} = Grant) ->
    case E > 0 andalso E =< barrel_spaces:now_ms() of
        true -> {error, expired};
        false -> {ok, Grant}
    end.

token_id(<<"bsp_", TokenId:16/binary, "_", _Secret/binary>>) ->
    {ok, TokenId};
token_id(TokenId) when byte_size(TokenId) =:= 16 ->
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
