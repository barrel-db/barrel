# Changelog

## 1.0.2 (2026-08-23)

- The venv shell commands (venv creation, pip install, uvloop check) run
  their port in a short-lived owner process. The port used to be opened in
  the caller's process, leaving `{Port, _}` and, for a caller trapping exits,
  `{'EXIT', Port, normal}` messages in its mailbox after the command ended.
  Same fix as barrel_embed 2.3.2.

## 1.0.1 (2026-07-18)

- Fail fast on a Python startup exit instead of hanging the full timeout: the
  init handshake now handles the port's `exit_status`.

## 1.0.0 (2026-07-10)

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
