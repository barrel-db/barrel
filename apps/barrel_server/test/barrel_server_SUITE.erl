%%%-------------------------------------------------------------------
%%% @doc End-to-end tests for the barrel REST server.
%%%
%%% Boots barrel_server on an ephemeral port (discovered via
%%% livery:which_listeners/1) and drives the HTTP API with hackney: database
%%% lifecycle, documents, bulk endpoints, attachments, vector add + search, the
%%% changes feed (JSON and SSE), and error paths.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_db_lifecycle/1,
    t_doc_crud/1,
    t_bulk/1,
    t_attachment/1,
    t_vector_search/1,
    t_changes_json/1,
    t_changes_sse/1,
    t_not_found/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DB, "srvdb").

all() ->
    [t_db_lifecycle, t_doc_crud, t_bulk, t_attachment,
     t_vector_search, t_changes_json, t_changes_sse, t_not_found].

init_per_suite(Config) ->
    %% Load first, then override env (application:load resets to .app defaults).
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Port = discover_port(),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    %% Open the database used across cases.
    {201, _} = req(put, Base ++ "/db/" ++ ?DB, <<>>),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    ok.

discover_port() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    Port.

%%====================================================================
%% Cases
%%====================================================================

t_db_lifecycle(Config) ->
    B = base(Config),
    {200, Info} = req(get, B ++ "/db/" ++ ?DB, <<>>),
    ?assert(is_map(Info)),
    %% A fresh db opens on demand and can be closed.
    {201, _} = req(put, B ++ "/db/tmpdb", <<>>),
    {200, #{<<"ok">> := true}} = req(delete, B ++ "/db/tmpdb", <<>>),
    ok.

t_doc_crud(Config) ->
    B = base(Config),
    {201, _} = req_json(put, url("/doc/a", B), #{<<"title">> => <<"hello">>}),
    {200, Doc} = req(get, url("/doc/a", B), <<>>),
    ?assertEqual(<<"hello">>, maps:get(<<"title">>, Doc)),
    {200, _} = req(delete, url("/doc/a", B), <<>>),
    {404, _} = req(get, url("/doc/a", B), <<>>),
    ok.

t_bulk(Config) ->
    B = base(Config),
    {201, PutResp} = req_json(post, url("/_bulk_docs", B),
                              #{<<"docs">> => [#{<<"id">> => <<"x1">>},
                                               #{<<"id">> => <<"x2">>}]}),
    ?assertEqual(2, length(maps:get(<<"results">>, PutResp))),
    {200, GetResp} = req_json(post, url("/_bulk_get", B),
                              #{<<"ids">> => [<<"x1">>, <<"x2">>, <<"missing">>]}),
    Results = maps:get(<<"results">>, GetResp),
    ?assertEqual(3, length(Results)),
    %% Last one is the missing id, reported as an error element.
    ?assertMatch(#{<<"error">> := <<"not_found">>}, lists:last(Results)),
    ok.

t_attachment(Config) ->
    B = base(Config),
    {201, _} = req_json(put, url("/doc/att-doc", B), #{}),
    {201, _} = req(put, url("/doc/att-doc/att/f.bin", B), <<"raw-bytes">>),
    {200, Body} = req_raw(get, url("/doc/att-doc/att/f.bin", B)),
    ?assertEqual(<<"raw-bytes">>, Body),
    {200, _} = req(delete, url("/doc/att-doc/att/f.bin", B), <<>>),
    ok.

t_vector_search(Config) ->
    B = base(Config),
    %% The server opens databases with the default 768-dim vector store.
    Vec = [0.1 || _ <- lists:seq(1, 768)],
    {201, _} = req_json(post, url("/vector", B),
                        #{<<"id">> => <<"vec-a">>, <<"text">> => <<"hello">>,
                          <<"vector">> => Vec}),
    {200, SR} = req_json(post, url("/search/vector", B),
                         #{<<"vector">> => Vec, <<"k">> => 5}),
    Hits = maps:get(<<"hits">>, SR),
    ?assert(lists:any(fun(H) -> maps:get(<<"key">>, H, undefined) =:= <<"vec-a">> end, Hits)),
    ok.

t_changes_json(Config) ->
    B = base(Config),
    {200, Ch} = req(get, url("/changes", B), <<>>),
    ?assert(is_list(maps:get(<<"changes">>, Ch))),
    ?assert(is_binary(maps:get(<<"last">>, Ch))),
    ok.

t_changes_sse(Config) ->
    B = base(Config),
    {200, Body} = req_raw_h(get, url("/changes", B),
                            [{<<"accept">>, <<"text/event-stream">>}]),
    ?assertNotEqual(nomatch, binary:match(Body, <<"data: ">>)),
    ?assertNotEqual(nomatch, binary:match(Body, <<"event: last">>)),
    ok.

t_not_found(Config) ->
    B = base(Config),
    {404, _} = req(get, url("/doc/does-not-exist", B), <<>>),
    ok.

%%====================================================================
%% Helpers (hackney)
%%====================================================================

base(Config) -> ?config(base, Config).

url(Path, Base) -> Base ++ "/db/" ++ ?DB ++ Path.

req(Method, Url, Body) ->
    decode(hackney:request(Method, list_to_binary(Url), [], Body, [with_body])).

req_json(Method, Url, Map) ->
    Headers = [{<<"content-type">>, <<"application/json">>}],
    decode(hackney:request(Method, list_to_binary(Url), Headers,
                           json:encode(Map), [with_body])).

req_raw(Method, Url) ->
    req_raw_h(Method, Url, []).

req_raw_h(Method, Url, Headers) ->
    {ok, S, _H, Body} = hackney:request(Method, list_to_binary(Url), Headers,
                                        <<>>, [with_body]),
    {S, Body}.

decode({ok, S, _H, Body}) ->
    Decoded = case Body of
        <<>> -> #{};
        _ -> try json:decode(Body) catch _:_ -> Body end
    end,
    {S, Decoded};
decode(Other) ->
    error({http, Other}).
