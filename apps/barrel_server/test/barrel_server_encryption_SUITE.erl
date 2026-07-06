%%%-------------------------------------------------------------------
%%% @doc Server-side encryption: the `open_opts' app env carries the
%%% encryption spec into every `barrel:open' the server performs. Keys
%%% come from the node's environment (here `BARREL_ENCRYPTION_KEY'
%%% through the default provider); no key material rides the HTTP API.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_encryption_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_open_opts_encrypt/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_open_opts_encrypt].

init_per_suite(Config) ->
    os:putenv("BARREL_ENCRYPTION_KEY", "server suite passphrase"),
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, open_opts, #{encryption => default}),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, open_opts),
    os:unsetenv("BARREL_ENCRYPTION_KEY"),
    ok.

req(Method, Url, none) ->
    {ok, S, _H, Body} = hackney:request(Method, list_to_binary(Url),
                                        [], <<>>, [with_body]),
    {S, decode(Body)};
req(Method, Url, JsonTerm) ->
    {ok, S, _H, Body} = hackney:request(
        Method, list_to_binary(Url),
        [{<<"content-type">>, <<"application/json">>}],
        json:encode(JsonTerm), [with_body]),
    {S, decode(Body)}.

decode(<<>>) -> #{};
decode(Bin) ->
    try json:decode(Bin) catch _:_ -> Bin end.

%%====================================================================
%% Cases
%%====================================================================

t_open_opts_encrypt(Config) ->
    B = ?config(base, Config),
    {201, _} = req(put, B ++ "/db/enc_srv", #{}),
    {201, _} = req(put, B ++ "/db/enc_srv/doc/a", #{<<"v">> => 1}),
    {200, #{<<"v">> := 1}} = req(get, B ++ "/db/enc_srv/doc/a", none),
    %% the db the server opened is encrypted: key-check marker on disk
    Marker = filename:join([?config(priv_dir, Config), "enc_srv",
                            "CRYPTO"]),
    ?assert(filelib:is_regular(Marker)),
    ok.
