# CLIP Provider

Cross-modal embedding generation using CLIP models.

## What is CLIP?

CLIP (Contrastive Language-Image Pre-training) encodes **both images and text into the same vector space**. This enables:

- Searching images with text queries
- Searching text with image queries
- Finding similar images
- Zero-shot image classification

## Requirements

```bash
# Using virtualenv with uv (recommended)
./scripts/setup_venv.sh
uv pip install transformers torch pillow --python .venv/bin/python

# Or install manually
pip install transformers torch pillow
```

## Configuration

```erlang
%% Using virtualenv (recommended)
{ok, State} = barrel_embed:init(#{
    embedder => {clip, #{
        venv => "/absolute/path/to/.venv",
        model => "openai/clip-vit-base-patch32",   % default
        timeout => 120000                           % default, ms
    }}
}).

%% Using system Python
{ok, State} = barrel_embed:init(#{
    embedder => {clip, #{
        python => "python3",                        % default
        model => "openai/clip-vit-base-patch32",   % default
        timeout => 120000                           % default, ms
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `venv` | string | `undefined` | Path to virtualenv (recommended) |
| `python` | string | `"python3"` | Python executable (if no venv) |
| `model` | string | `"openai/clip-vit-base-patch32"` | Model name |
| `timeout` | integer | `120000` | Timeout in milliseconds |

## Supported Models

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `openai/clip-vit-base-patch32` | 512 | Default, fast |
| `openai/clip-vit-base-patch16` | 512 | Higher quality |
| `openai/clip-vit-large-patch14` | 768 | Best quality |
| `laion/CLIP-ViT-B-32-laion2B-s34B-b79K` | 512 | LAION trained |

## API

### Text Embedding

```erlang
%% Text embeddings (same space as images)
{ok, TextVec} = barrel_embed:embed(<<"a photo of a cat">>, State).
{ok, TextVecs} = barrel_embed:embed_batch([<<"cat">>, <<"dog">>], State).
```

### Image Embedding

Images must be base64-encoded:

```erlang
%% Read and encode image
{ok, ImageData} = file:read_file("photo.jpg").
ImageBase64 = base64:encode(ImageData).

%% Get embedding
{ok, ImageVec} = barrel_embed_clip:embed_image(ImageBase64, Config).

%% Batch
{ok, ImageVecs} = barrel_embed_clip:embed_image_batch([Img1, Img2], Config).
```

## Example: Image Search with Text

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{embedder => {clip, #{}}}).
{_, Config} = hd(maps:get(providers, State)).

%% Index images (do once)
Images = [<<"img1.jpg">>, <<"img2.jpg">>, <<"img3.jpg">>],
ImageVecs = lists:map(fun(Path) ->
    {ok, Data} = file:read_file(Path),
    {ok, Vec} = barrel_embed_clip:embed_image(base64:encode(Data), Config),
    {Path, Vec}
end, Images).

%% Search with text query
{ok, QueryVec} = barrel_embed:embed(<<"a sunset over the ocean">>, State).

%% Find most similar images
Scores = [{Path, cosine_similarity(QueryVec, ImgVec)}
          || {Path, ImgVec} <- ImageVecs],
Ranked = lists:reverse(lists:keysort(2, Scores)).
```

## Example: Similar Image Search

```erlang
%% Find images similar to a reference image
{ok, RefData} = file:read_file("reference.jpg").
{ok, RefVec} = barrel_embed_clip:embed_image(base64:encode(RefData), Config).

%% Compare with other images
Scores = [{Path, cosine_similarity(RefVec, ImgVec)}
          || {Path, ImgVec} <- ImageVecs],
Similar = lists:reverse(lists:keysort(2, Scores)).
```

## Example: Zero-Shot Classification

```erlang
%% Classify an image into categories
Categories = [<<"a photo of a cat">>,
              <<"a photo of a dog">>,
              <<"a photo of a bird">>],
{ok, CategoryVecs} = barrel_embed:embed_batch(Categories, State).

{ok, ImageData} = file:read_file("mystery_animal.jpg"),
{ok, ImageVec} = barrel_embed_clip:embed_image(base64:encode(ImageData), Config).

%% Find best matching category
Scores = lists:zipwith(fun(Cat, CatVec) ->
    {Cat, cosine_similarity(ImageVec, CatVec)}
end, Categories, CategoryVecs),
[{BestCategory, _} | _] = lists:reverse(lists:keysort(2, Scores)).
```

## Use Cases

- **Image search**: Find images by description
- **Reverse image search**: Find similar images
- **Content moderation**: Classify images automatically
- **Multi-modal retrieval**: Combined text and image search
