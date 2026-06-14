%% @doc Basic barrel_embed usage examples
%%
%% Run with: rebar3 shell
%% Then copy/paste the examples below.
-module(basic_usage).
-export([ollama_example/0, openai_example/0, local_example/0, similarity/2]).

%% @doc Ollama embedding example
%% Requires: ollama serve && ollama pull nomic-embed-text
ollama_example() ->
    {ok, State} = barrel_embed:init(#{
        embedder => {ollama, #{
            url => <<"http://localhost:11434">>,
            model => <<"nomic-embed-text">>
        }}
    }),

    {ok, Vec1} = barrel_embed:embed(<<"The quick brown fox">>, State),
    {ok, Vec2} = barrel_embed:embed(<<"A fast auburn canine">>, State),

    Sim = similarity(Vec1, Vec2),
    io:format("Similarity: ~.4f~n", [Sim]),
    {ok, Sim}.

%% @doc OpenAI embedding example
%% Requires: OPENAI_API_KEY environment variable
openai_example() ->
    {ok, State} = barrel_embed:init(#{
        embedder => {openai, #{
            model => <<"text-embedding-3-small">>
        }}
    }),

    {ok, Vec} = barrel_embed:embed(<<"Hello world">>, State),
    io:format("Dimension: ~p~n", [length(Vec)]),
    {ok, Vec}.

%% @doc Local Python embedding example
%% Requires: pip install sentence-transformers
local_example() ->
    {ok, State} = barrel_embed:init(#{
        embedder => {local, #{
            model => "BAAI/bge-small-en-v1.5"
        }}
    }),

    {ok, Vectors} = barrel_embed:embed_batch([
        <<"First document">>,
        <<"Second document">>,
        <<"Third document">>
    ], State),

    io:format("Generated ~p vectors~n", [length(Vectors)]),
    {ok, Vectors}.

%% @doc Calculate cosine similarity between two vectors
similarity(Vec1, Vec2) ->
    Dot = lists:sum(lists:zipwith(fun(A, B) -> A * B end, Vec1, Vec2)),
    Norm1 = math:sqrt(lists:sum([X * X || X <- Vec1])),
    Norm2 = math:sqrt(lists:sum([X * X || X <- Vec2])),
    Dot / (Norm1 * Norm2).
