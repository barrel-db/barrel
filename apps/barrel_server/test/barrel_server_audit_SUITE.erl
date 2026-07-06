%%%-------------------------------------------------------------------
%%% @doc Audit over REST: provenance from write headers, the retained
%%% history endpoint with cursors, and per-document versions with
%%% archived bodies.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_audit_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_write_with_provenance_headers/1,
    t_history_endpoint/1,
    t_history_cursor_roundtrip/1,
    t_versions_and_body/1,
    t_invalid_header_400/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_write_with_provenance_headers, t_history_endpoint,
     t_history_cursor_roundtrip, t_versions_and_body,
     t_invalid_header_400].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_list(TC) ++ "_db",
    B = ?config(base, Config),
    {201, _} = req(put, B ++ "/db/" ++ Db, #{}),
    [{db, Db} | Config].

end_per_testcase(_TC, Config) ->
    B = ?config(base, Config),
    _ = req(delete, B ++ "/db/" ++ ?config(db, Config) ++ "?purge=true",
            none),
    ok.

req(Method, Url, Body) ->
    req(Method, Url, Body, []).

req(Method, Url, none, Headers) ->
    {ok, S, _H, RespBody} = hackney:request(Method, list_to_binary(Url),
                                            Headers, <<>>, [with_body]),
    {S, decode(RespBody)};
req(Method, Url, JsonTerm, Headers) ->
    {ok, S, _H, RespBody} = hackney:request(
        Method, list_to_binary(Url),
        [{<<"content-type">>, <<"application/json">>} | Headers],
        json:encode(JsonTerm), [with_body]),
    {S, decode(RespBody)}.

decode(<<>>) -> #{};
decode(Bin) ->
    try json:decode(Bin) catch _:_ -> Bin end.

%%====================================================================
%% Cases
%%====================================================================

t_write_with_provenance_headers(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Hdrs = [{<<"x-barrel-actor">>, <<"agent-9">>},
            {<<"x-barrel-session">>, <<"ses-4">>},
            {<<"x-barrel-source">>, <<"rest">>}],
    {201, _} = req(put, B ++ "/db/" ++ Db ++ "/doc/a",
                   #{<<"v">> => 1}, Hdrs),
    {200, #{<<"history">> := [Entry]}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history", none),
    ?assertEqual(<<"a">>, maps:get(<<"id">>, Entry)),
    ?assertEqual(<<"local">>, maps:get(<<"cause">>, Entry)),
    ?assertEqual(#{<<"actor">> => <<"agent-9">>,
                   <<"session">> => <<"ses-4">>,
                   <<"source">> => <<"rest">>},
                 maps:get(<<"provenance">>, Entry)),
    %% deletes carry headers too
    {200, _} = req(delete, B ++ "/db/" ++ Db ++ "/doc/a", none,
                   [{<<"x-barrel-actor">>, <<"reaper">>}]),
    {200, #{<<"history">> := [_, DelEntry]}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history", none),
    ?assertEqual(true, maps:get(<<"deleted">>, DelEntry)),
    ?assertEqual(#{<<"actor">> => <<"reaper">>},
                 maps:get(<<"provenance">>, DelEntry)),
    ok.

t_history_endpoint(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    {201, _} = req(put, B ++ "/db/" ++ Db ++ "/doc/a", #{<<"v">> => 1}),
    {201, _} = req(put, B ++ "/db/" ++ Db ++ "/doc/b", #{<<"v">> => 1}),
    {200, #{<<"history">> := All}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history", none),
    ?assertEqual(2, length(All)),
    %% id filter
    {200, #{<<"history">> := OnlyA}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history?id=a", none),
    ?assertEqual([<<"a">>], [maps:get(<<"id">>, E) || E <- OnlyA]),
    %% limit bounds the scan
    {200, #{<<"history">> := One}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history?limit=1", none),
    ?assertEqual(1, length(One)),
    ok.

t_history_cursor_roundtrip(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    {201, _} = req(put, B ++ "/db/" ++ Db ++ "/doc/a", #{}),
    {200, #{<<"history">> := [E1]}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history", none),
    Cursor = maps:get(<<"hlc">>, E1),
    {201, _} = req(put, B ++ "/db/" ++ Db ++ "/doc/b", #{}),
    %% since is inclusive of the retained scan start; entries from the
    %% cursor on include both writes, until= caps at the first
    {200, #{<<"history">> := Since}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history?since="
                 ++ binary_to_list(Cursor), none),
    ?assert(length(Since) >= 1),
    {200, #{<<"history">> := Until}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history?until="
                 ++ binary_to_list(Cursor), none),
    ?assertEqual([<<"a">>], [maps:get(<<"id">>, E) || E <- Until]),
    %% garbage cursors are 400
    {400, #{<<"error">> := <<"bad_since">>}} =
        req(get, B ++ "/db/" ++ Db ++ "/_history?since=garbage", none),
    ok.

t_versions_and_body(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    {201, _} = req(put, B ++ "/db/" ++ Db ++ "/doc/a", #{<<"v">> => 1}),
    {200, #{<<"_rev">> := R1}} =
        req(get, B ++ "/db/" ++ Db ++ "/doc/a", none),
    {201, _} = req(put, B ++ "/db/" ++ Db ++ "/doc/a",
                   #{<<"v">> => 2, <<"_rev">> => R1}),
    {200, #{<<"id">> := <<"a">>, <<"versions">> := Versions}} =
        req(get, B ++ "/db/" ++ Db ++ "/doc/a/_versions", none),
    [Current, Superseded] = Versions,
    ?assertEqual(<<"current">>, maps:get(<<"status">>, Current)),
    ?assertEqual(<<"superseded">>, maps:get(<<"status">>, Superseded)),
    ?assertEqual(R1, maps:get(<<"rev">>, Superseded)),
    %% archived body by rev
    {200, #{<<"v">> := 1}} =
        req(get, B ++ "/db/" ++ Db ++ "/doc/a/_versions/"
                 ++ binary_to_list(R1), none),
    {404, _} = req(get, B ++ "/db/" ++ Db
                        ++ "/doc/a/_versions/1-deadbeef", none),
    ok.

t_invalid_header_400(Config) ->
    B = ?config(base, Config),
    Db = ?config(db, Config),
    Big = binary:copy(<<"x">>, 300),
    {400, #{<<"error">> := <<"invalid_provenance">>}} =
        req(put, B ++ "/db/" ++ Db ++ "/doc/a", #{},
            [{<<"x-barrel-actor">>, Big}]),
    %% nothing written
    {404, _} = req(get, B ++ "/db/" ++ Db ++ "/doc/a", none),
    ok.
