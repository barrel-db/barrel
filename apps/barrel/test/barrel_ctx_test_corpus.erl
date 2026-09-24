%%%-------------------------------------------------------------------
%%% @doc Test corpus and deterministic vectors for the context suites.
%%% Reads the OTP module corpus named by BARREL_CTX_CORPUS (JSON lines:
%%% id, app, path, body, moduledoc), else the committed sample.
%%% Vectors are feature-hashed token counts: no embedding service.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_test_corpus).

-export([docs/1, docs/2, vec/1, dim/0, mock_embed/0, unmock_embed/0,
         text/1, corpus_file/0]).

-define(DIM, 64).
-define(MAX_BODY, 8192).

dim() -> ?DIM.

corpus_file() ->
    %% Defaults to the committed tools/sasl/eunit sample next to this module.
    os:getenv("BARREL_CTX_CORPUS",
              filename:join(filename:dirname(?FILE), "barrel_ctx_corpus.jsonl")).

%% @doc Docs of the given apps (all apps for `all'), bodies truncated.
docs(Apps) ->
    docs(Apps, infinity).

docs(Apps, Max) ->
    case file:open(corpus_file(), [read, raw, binary, read_ahead]) of
        {ok, Fd} ->
            try take(read_lines(Fd, Apps, []), Max)
            after file:close(Fd)
            end;
        {error, _} ->
            take(synthetic(Apps), Max)
    end.

take(Docs, infinity) -> Docs;
take(Docs, Max) -> lists:sublist(Docs, Max).

read_lines(Fd, Apps, Acc) ->
    case file:read_line(Fd) of
        {ok, Line} ->
            #{<<"app">> := App} = Row = json:decode(Line),
            case Apps =:= all orelse lists:member(App, Apps) of
                true -> read_lines(Fd, Apps, [doc(Row) | Acc]);
                false -> read_lines(Fd, Apps, Acc)
            end;
        eof ->
            lists:reverse(Acc)
    end.

doc(#{<<"id">> := Id, <<"app">> := App, <<"path">> := Path,
      <<"body">> := Body, <<"moduledoc">> := Md}) ->
    #{<<"id">> => Id, <<"app">> => App, <<"path">> => Path,
      <<"moduledoc">> => Md,
      <<"body">> => binary:part(Body, 0, min(byte_size(Body), ?MAX_BODY))}.

synthetic(Apps) ->
    Names = case Apps of all -> [<<"alpha">>, <<"beta">>]; _ -> Apps end,
    [#{<<"id">> => <<App/binary, "_mod", (integer_to_binary(I))/binary>>,
       <<"app">> => App,
       <<"path">> => <<App/binary, "/src/m", (integer_to_binary(I))/binary>>,
       <<"moduledoc">> => <<"module ", (integer_to_binary(I))/binary,
                            " of ", App/binary, " handles ",
                            (lists:nth(1 + I rem 4, [<<"sockets">>,
                                                     <<"files">>,
                                                     <<"timers">>,
                                                     <<"tables">>]))/binary>>,
       <<"body">> => <<"-module(m", (integer_to_binary(I))/binary, ").">>}
     || App <- Names, I <- lists:seq(1, 20)].

%% @doc The text vectors and BM25 are built from.
text(#{<<"moduledoc">> := Md, <<"path">> := P}) ->
    <<Md/binary, " ", P/binary>>.

%% @doc Feature hashing of lowercase word tokens, L2-normalized.
vec(Text) ->
    Tokens = [T || T <- re:split(string:lowercase(Text), "[^a-z0-9_]+",
                                 [{return, binary}]), T =/= <<>>],
    Counts = lists:foldl(fun(T, Acc) ->
                             I = erlang:phash2(T, ?DIM) + 1,
                             setelement(I, Acc, element(I, Acc) + 1.0)
                         end, erlang:make_tuple(?DIM, 0.0), Tokens),
    L = tuple_to_list(Counts),
    Norm = math:sqrt(lists:sum([X * X || X <- L])),
    case Norm == 0.0 of
        true -> [1.0 | lists:duplicate(?DIM - 1, 0.0)];
        false -> [X / Norm || X <- L]
    end.

%% @doc Stub every embedder call with vec/1.
mock_embed() ->
    unmock_embed(),
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed, fun(T, _S) -> {ok, vec(T)} end),
    meck:expect(barrel_embed, embed_batch,
                fun(Ts, _S) -> {ok, [vec(T) || T <- Ts]} end),
    ok.

unmock_embed() ->
    try meck:unload(barrel_embed) catch _:_ -> ok end,
    ok.
