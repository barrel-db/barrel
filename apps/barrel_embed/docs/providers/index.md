# Providers Overview

barrel_embed supports multiple embedding providers, each with different characteristics and use cases.

## Cloud Providers

Production-ready cloud embedding APIs with high availability and no infrastructure to manage.

| Provider | Requirements | Dimensions | Best For |
|----------|-------------|------------|----------|
| [OpenAI](openai.md) | API key | 256-3072 | Production, general purpose |
| [Cohere](cohere.md) | API key | 384-1024 | Production, input type optimization |
| [Voyage AI](voyage.md) | API key | 512-1536 | RAG, retrieval, domain-specific |
| [Jina AI](jina.md) | API key | 768-1024 | Long context (8K), multilingual |
| [Mistral](mistral.md) | API key | 1024 | EU data residency |
| [Azure OpenAI](azure.md) | Azure subscription | 1536-3072 | Enterprise, compliance |
| [AWS Bedrock](bedrock.md) | AWS credentials | 1024-1536 | AWS ecosystem, enterprise |
| [Google Vertex AI](vertex.md) | GCP project | 768 | GCP ecosystem, enterprise |

## Local Providers

Run embedding models locally without external API calls.

| Provider | Requirements | Dimensions | Best For |
|----------|-------------|------------|----------|
| [Ollama](ollama.md) | Ollama server | Model-dependent | Local development |
| [Local](local.md) | Python + sentence-transformers | Model-dependent | Offline use |
| [FastEmbed](fastembed.md) | Python + fastembed | Model-dependent | Lightweight local |

## Specialized Providers

Advanced embedding types for specific use cases.

| Provider | Type | Requirements | Best For |
|----------|------|-------------|----------|
| [SPLADE](splade.md) | Sparse | Python + transformers + torch | Hybrid search |
| [ColBERT](colbert.md) | Multi-vector | Python + transformers + torch | Fine-grained matching |
| [CLIP](clip.md) | Cross-modal | Python + transformers + torch + pillow | Image-text search |

## Choosing a Provider

### For Production

**Cloud providers** are recommended:

- **OpenAI** - Best general-purpose quality
- **Cohere** - Input type optimization (search_query vs search_document)
- **Voyage AI** - Top retrieval performance, domain-specific models
- **Jina AI** - Longest context length (8K tokens)

### For Enterprise

**Hyperscaler providers** with compliance features:

- **Azure OpenAI** - SOC 2, HIPAA, regional deployment
- **AWS Bedrock** - VPC integration, IAM, CloudWatch
- **Google Vertex AI** - VPC-SC, CMEK, BigQuery integration

### For EU Data Residency

- **Mistral** - EU-based company
- **Azure OpenAI** - Deploy in EU regions
- **Google Vertex AI** - Deploy in EU regions

### For Local Development

**Ollama** is recommended:

- No Python dependencies
- Easy model management
- Good performance

### For Offline/Air-gapped

**Local** or **FastEmbed**:

- No external API calls
- Full data privacy
- FastEmbed is lighter (~100MB vs ~2GB)

### For Specialized Use Cases

- **Hybrid search**: Use [SPLADE](splade.md) for sparse + dense combination
- **Passage retrieval**: Use [ColBERT](colbert.md) for token-level matching
- **Image search**: Use [CLIP](clip.md) for cross-modal embeddings

## Provider Chain

Configure fallback providers for high availability:

```erlang
#{embedder => [
    {voyage, #{api_key => <<"pa-...">>}},      % Primary: best retrieval
    {openai, #{api_key => <<"sk-...">>}},      % Fallback: OpenAI
    {ollama, #{url => <<"http://localhost:11434">>}}  % Offline fallback
]}
```

Providers are tried in order. If one fails, the next is attempted automatically.

## Quick Comparison

| Feature | OpenAI | Cohere | Voyage | Jina | Mistral | Azure | Bedrock | Vertex |
|---------|--------|--------|--------|------|---------|-------|---------|--------|
| Batch support | Yes | Yes | Yes | Yes | Yes | Yes | No | Yes |
| Free tier | No | Limited | No | Yes | No | No | No | No |
| EU residency | No | No | No | No | Yes | Yes | Yes | Yes |
| Max context | 8K | 512 | 32K | 8K | 8K | 8K | 8K | 2K |
| Domain models | No | No | Yes | No | No | No | Yes | No |
