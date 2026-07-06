%%%-------------------------------------------------------------------
%%% @doc Server-side database access: a thin, stateless shim over the
%%% facade's lifecycle manager ({@link barrel_dbs}), which owns the
%%% open handles, touches them on use, closes idle ones, and bounds
%%% the open set. This module adds only the HTTP concerns: name
%%% validation and the server-wide open options from the app env.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_dbs).

-export([ensure/1, close/1, branch/3, destroy/1, list/0]).

-define(MAX_NAME_LEN, 128).

%%====================================================================
%% API
%%====================================================================

%% @doc Return the (possibly newly opened) handle for `Name'.
-spec ensure(binary()) -> {ok, barrel:db()} | {error, term()}.
ensure(Name) when is_binary(Name) ->
    case valid_name(Name) of
        true -> barrel_dbs:ensure(Name, open_opts());
        false -> {error, invalid_name}
    end.

%% @doc Close and forget the database `Name'. Idempotent.
-spec close(binary()) -> ok.
close(Name) when is_binary(Name) ->
    barrel_dbs:close(Name).

%% @doc Fork `Parent' into `BranchName'. Opts: at => now | HlcT.
-spec branch(binary(), binary(), map()) ->
    {ok, barrel:db()} | {error, term()}.
branch(Parent, BranchName, Opts) when is_binary(Parent),
                                      is_binary(BranchName),
                                      is_map(Opts) ->
    case valid_name(BranchName) of
        true ->
            barrel_dbs:branch(Parent, BranchName,
                              Opts#{open_opts => open_opts()});
        false ->
            {error, invalid_name}
    end.

%% @doc Destroy the database `Name': close it and delete its files.
-spec destroy(binary()) -> ok | {error, term()}.
destroy(Name) when is_binary(Name) ->
    case valid_name(Name) of
        true -> barrel_dbs:destroy(Name);
        false -> {error, invalid_name}
    end.

%% @doc Names of the currently open databases.
-spec list() -> [binary()].
list() ->
    barrel_dbs:list().

%%====================================================================
%% Internal
%%====================================================================

%% Default open options from the barrel_server app env (e.g. a
%% server-wide embedding policy, encryption spec, or vectordb config),
%% tagged so stopping the server closes exactly the databases it
%% opened (barrel_server_app calls barrel_dbs:close_owned/1).
open_opts() ->
    Opts = application:get_env(barrel_server, open_opts, #{}),
    Opts#{owner => barrel_server}.

%% @private Accept only short, filesystem-safe names.
-spec valid_name(binary()) -> boolean().
valid_name(Name) ->
    Size = byte_size(Name),
    Size >= 1 andalso Size =< ?MAX_NAME_LEN
        andalso match =:= re:run(Name, "^[A-Za-z0-9_-]+$",
                                 [{capture, none}]).
