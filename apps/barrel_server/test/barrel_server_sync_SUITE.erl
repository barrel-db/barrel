%%%-------------------------------------------------------------------
%%% @doc Wire-level tests for the replication endpoints under
%%% /db/:db/_sync/*: every request/response shape, the error mapping,
%%% and the passive HLC header coupling, driven with raw hackney.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_sync_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_info/1,
    t_hlc_exchange/1,
    t_put_get_version/1,
    t_changes_and_filters/1,
    t_diff/1,
    t_local_docs/1,
    t_hlc_header/1,
    t_encoded_doc_id/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DB, "syncdb").

all() ->
    [t_info, t_hlc_exchange, t_put_get_version, t_changes_and_filters,
     t_diff, t_local_docs, t_hlc_header, t_encoded_doc_id].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Port = discover_port(),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
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
%% Helpers
%%====================================================================

base(Config) -> ?config(base, Config).

url(Path, Config) -> base(Config) ++ "/db/" ++ ?DB ++ "/_sync" ++ Path.

req(Method, Url, Body) ->
    decode(hackney:request(Method, list_to_binary(Url), [], Body,
                           [with_body])).

req_json(Method, Url, Map) ->
    Headers = [{<<"content-type">>, <<"application/json">>}],
    decode(hackney:request(Method, list_to_binary(Url), Headers,
                           json:encode(Map), [with_body])).

decode({ok, S, _H, Body}) ->
    Decoded = case Body of
        <<>> -> #{};
        _ -> try json:decode(Body) catch _:_ -> Body end
    end,
    {S, Decoded}.

remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), base64:encode(barrel_vv:encode(VV))}.

put_version(Config, DocId, Doc, Author) ->
    {Token, VVB64} = remote_version(Author),
    {200, Resp} = req_json(put, url("/doc/" ++ DocId, Config),
                           #{doc => Doc, version => Token, vv => VVB64,
                             deleted => false}),
    {Token, Resp}.

%%====================================================================
%% Cases
%%====================================================================

t_info(Config) ->
    {200, Info} = req(get, url("/info", Config), <<>>),
    ?assertEqual(list_to_binary(?DB), maps:get(<<"db">>, Info)),
    %% floors absent until retention ever swept
    ?assertNot(maps:is_key(<<"history_floor">>, Info)),
    ?assertNot(maps:is_key(<<"att_floor">>, Info)),
    ok.

t_hlc_exchange(Config) ->
    Mine = base64:encode(barrel_hlc:encode(barrel_hlc:new_hlc())),
    {200, Resp} = req_json(post, url("/hlc", Config), #{hlc => Mine}),
    Theirs = barrel_hlc:decode(base64:decode(maps:get(<<"hlc">>, Resp))),
    %% the server's post-sync clock covers the shipped timestamp
    ?assertNot(barrel_hlc:less(Theirs,
                               barrel_hlc:decode(base64:decode(Mine)))),
    {400, #{<<"error">> := <<"bad_hlc">>}} =
        req_json(post, url("/hlc", Config), #{hlc => <<"nonsense">>}),
    ok.

t_put_get_version(Config) ->
    Doc = #{<<"id">> => <<"pv1">>, <<"kind">> => <<"wire">>},
    {Token, Resp} = put_version(Config, "pv1", Doc, <<"peer_a">>),
    ?assertEqual(Token, maps:get(<<"winner">>, Resp)),
    ?assertEqual(<<"pv1">>, maps:get(<<"id">>, Resp)),
    {200, Got} = req(get, url("/doc/pv1", Config), <<>>),
    ?assertEqual(<<"wire">>,
                 maps:get(<<"kind">>, maps:get(<<"doc">>, Got))),
    ?assertEqual(Token, maps:get(<<"version">>, Got)),
    ?assertEqual(false, maps:get(<<"deleted">>, Got)),
    ?assert(is_binary(base64:decode(maps:get(<<"vv">>, Got)))),
    {404, #{<<"error">> := <<"not_found">>}} =
        req(get, url("/doc/nope", Config), <<>>),
    %% malformed body
    {400, #{<<"error">> := <<"bad_json">>}} =
        req_json(put, url("/doc/pv2", Config), #{doc => <<"nope">>}),
    ok.

t_changes_and_filters(Config) ->
    {_T1, _} = put_version(Config, "ch1",
                           #{<<"id">> => <<"ch1">>,
                             <<"type">> => <<"post">>}, <<"peer_c">>),
    {_T2, _} = put_version(Config, "ch2",
                           #{<<"id">> => <<"ch2">>,
                             <<"type">> => <<"user">>}, <<"peer_c">>),
    {200, Resp} = req_json(post, url("/changes", Config),
                           #{since => <<"first">>, limit => 100}),
    Changes = maps:get(<<"changes">>, Resp),
    Ids = [maps:get(<<"id">>, C) || C <- Changes],
    ?assert(lists:member(<<"ch1">>, Ids)),
    LastSeq = maps:get(<<"last_seq">>, Resp),
    ?assert(is_binary(base64:decode(LastSeq))),
    lists:foreach(
        fun(C) ->
            ?assert(is_binary(base64:decode(maps:get(<<"hlc">>, C)))),
            ?assert(maps:is_key(<<"rev">>, C))
        end,
        Changes),
    %% incremental from the cursor
    {200, Resp2} = req_json(post, url("/changes", Config),
                            #{since => LastSeq}),
    ?assertEqual([], maps:get(<<"changes">>, Resp2)),
    %% path filter rides the wire
    {200, Resp3} = req_json(post, url("/changes", Config),
                            #{since => <<"first">>,
                              filter => #{paths => [<<"type/post">>]}}),
    ?assertEqual([<<"ch1">>],
                 [maps:get(<<"id">>, C)
                  || C <- maps:get(<<"changes">>, Resp3),
                     lists:member(maps:get(<<"id">>, C),
                                  [<<"ch1">>, <<"ch2">>])]),
    %% error mapping
    {400, #{<<"error">> := <<"bad_filter">>}} =
        req_json(post, url("/changes", Config),
                 #{since => <<"first">>,
                   filter => #{query => #{where => [[<<"zap">>]]}}}),
    {400, #{<<"error">> := <<"bad_since">>}} =
        req_json(post, url("/changes", Config), #{since => <<"@@@">>}),
    ok.

t_diff(Config) ->
    {Token, _} = put_version(Config, "df1",
                             #{<<"id">> => <<"df1">>}, <<"peer_d">>),
    {UnknownTok, _VV} = remote_version(<<"peer_other">>),
    {200, Resp} = req_json(post, url("/diff", Config),
                           #{versions => #{<<"df1">> => Token,
                                           <<"df2">> => UnknownTok}}),
    Diff = maps:get(<<"diff">>, Resp),
    ?assertEqual(<<"have">>, maps:get(<<"df1">>, Diff)),
    ?assertEqual(<<"missing">>, maps:get(<<"df2">>, Diff)),
    ok.

t_local_docs(Config) ->
    CheckpointDoc = #{<<"history">> => [
        #{<<"source_last_seq">> => <<"Zmlyc3Q=">>,
          <<"session_id">> => <<"abc">>,
          <<"end_time_microsec">> => 12345}
    ]},
    {200, #{<<"ok">> := true}} =
        req_json(put, url("/local/replication-checkpoint-x", Config),
                 CheckpointDoc),
    {200, Got} = req(get, url("/local/replication-checkpoint-x", Config),
                     <<>>),
    ?assertEqual(CheckpointDoc, Got),
    {200, _} = req(delete, url("/local/replication-checkpoint-x", Config),
                   <<>>),
    {404, _} = req(get, url("/local/replication-checkpoint-x", Config),
                   <<>>),
    ok.

t_hlc_header(Config) ->
    {ok, 200, Headers, _} = hackney:request(
        get, list_to_binary(url("/info", Config)),
        [{<<"x-barrel-hlc">>,
          base64:encode(barrel_hlc:encode(barrel_hlc:new_hlc()))}],
        <<>>, [with_body]),
    HlcHeader = proplists:get_value(<<"x-barrel-hlc">>, Headers),
    ?assertNotEqual(undefined, HlcHeader),
    _ = barrel_hlc:decode(base64:decode(HlcHeader)),
    ok.

t_encoded_doc_id(Config) ->
    DocId = <<"user 1/report">>,
    Encoded = uri_string:quote(binary_to_list(DocId)),
    Doc = #{<<"id">> => DocId, <<"k">> => 1},
    {Token, VVB64} = remote_version(<<"peer_e">>),
    {200, _} = req_json(put, url("/doc/" ++ Encoded, Config),
                        #{doc => Doc, version => Token, vv => VVB64,
                          deleted => false}),
    {200, Got} = req(get, url("/doc/" ++ Encoded, Config), <<>>),
    ?assertEqual(DocId,
                 maps:get(<<"id">>, maps:get(<<"doc">>, Got))),
    ok.
