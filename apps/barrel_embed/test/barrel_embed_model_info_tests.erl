%% Provider model identity: pure, from stub configs (no network).
-module(barrel_embed_model_info_tests).

-include_lib("eunit/include/eunit.hrl").

defaults_test_() ->
    [?_assertEqual(#{model => Default}, Mod:model_info(#{}))
     || {Mod, Default} <- [
            {barrel_embed_ollama, <<"nomic-embed-text:latest">>},
            {barrel_embed_openai, <<"text-embedding-3-small">>},
            {barrel_embed_cohere, <<"embed-english-v3.0">>},
            {barrel_embed_voyage, <<"voyage-3">>},
            {barrel_embed_jina, <<"jina-embeddings-v3">>},
            {barrel_embed_mistral, <<"mistral-embed">>},
            {barrel_embed_bedrock, <<"amazon.titan-embed-text-v2:0">>},
            {barrel_embed_vertex, <<"text-embedding-004">>},
            {barrel_embed_local, <<"BAAI/bge-base-en-v1.5">>},
            {barrel_embed_fastembed, <<"BAAI/bge-small-en-v1.5">>},
            {barrel_embed_splade, <<"prithivida/Splade_PP_en_v1">>},
            {barrel_embed_colbert, <<"colbert-ir/colbertv2.0">>},
            {barrel_embed_clip, <<"openai/clip-vit-base-patch32">>},
            {barrel_embed_azure, undefined}]].

configured_model_and_revision_test_() ->
    Cfg = #{model => "org/model-x", revision => <<"abc123">>},
    [?_assertEqual(#{model => <<"org/model-x">>, revision => <<"abc123">>},
                   Mod:model_info(Cfg))
     || Mod <- [barrel_embed_openai, barrel_embed_local,
                barrel_embed_fastembed, barrel_embed_bedrock,
                barrel_embed_azure]].

ollama_tag_test_() ->
    [?_assertEqual(#{model => <<"nomic-embed-text:latest">>},
                   barrel_embed_ollama:model_info(
                     #{model => <<"nomic-embed-text">>})),
     ?_assertEqual(#{model => <<"mxbai-embed-large:v1">>},
                   barrel_embed_ollama:model_info(
                     #{model => <<"mxbai-embed-large:v1">>})),
     ?_assertEqual(#{model => <<"host:5000/ns/m:latest">>},
                   barrel_embed_ollama:model_info(
                     #{model => <<"host:5000/ns/m">>}))].

azure_deployment_test() ->
    ?assertEqual(#{model => <<"my-ada">>},
                 barrel_embed_azure:model_info(#{deployment => <<"my-ada">>})).

fallback_without_callback_test() ->
    %% a module without model_info/1 falls back to the config keys
    ?assertEqual(#{model => <<"m">>, revision => <<"r">>},
                 barrel_embed_provider:model_info(
                   lists, #{model => m, revision => "r"})),
    ?assertEqual(#{model => undefined},
                 barrel_embed_provider:model_info(lists, #{})).

info_reports_model_test() ->
    State = #{providers => [{barrel_embed_ollama,
                             #{model => <<"nomic-embed-text">>,
                               revision => <<"0a109f42">>}}],
              dimension => 768, batch_size => 32},
    ?assertMatch(#{configured := true, dimension := 768,
                   providers := [#{name := ollama,
                                   module := barrel_embed_ollama,
                                   model := <<"nomic-embed-text:latest">>,
                                   revision := <<"0a109f42">>}]},
                 barrel_embed:info(State)).
