# Changelog

## 0.2.0 (2026-07-08)

Coordinated umbrella release. Added tests for the sidecar response decoder.
See the umbrella [CHANGELOG](../../CHANGELOG.md).

## 0.1.1 (2026-04-02)

- Fix edoc documentation syntax
- Update ex_doc configuration

## 0.1.0 (2026-04-02)

Initial release. Extracted from barrel_vectordb.

### Features

- Cross-encoder reranking server with async Python backend
- Request multiplexing for concurrent rerank operations
- Managed Python virtual environment with auto-dependency installation
- Support for multiple cross-encoder models:
  - cross-encoder/ms-marco-MiniLM-L-6-v2 (default)
  - cross-encoder/ms-marco-MiniLM-L-12-v2
  - BAAI/bge-reranker-base
  - BAAI/bge-reranker-large
