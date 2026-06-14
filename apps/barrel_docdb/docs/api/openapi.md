# OpenAPI Specification

The Barrel DocDB HTTP API is described by an OpenAPI 3.1 document
generated at boot from the route table in
`src/web/barrel_http_server.erl`. The doc is **not** hand-maintained;
the running service is always the source of truth.

## Endpoints

- `GET /openapi.json` — the OpenAPI 3.1 document (JSON).
- `GET /docs` — a [Redoc](https://github.com/Redocly/redoc) browser UI
  that loads the spec from `/openapi.json`.

Both endpoints are public (no API key required) so tooling can pull
the spec without provisioning credentials.

## Download

```bash
curl http://localhost:8080/openapi.json > barrel_docdb.openapi.json
```

## Generate client SDKs

Use [OpenAPI Generator](https://openapi-generator.tech/) against the
live endpoint or a downloaded snapshot:

```bash
# Python
openapi-generator generate -i http://localhost:8080/openapi.json -g python -o ./python-client

# TypeScript / fetch
openapi-generator generate -i http://localhost:8080/openapi.json -g typescript-fetch -o ./ts-client

# Go
openapi-generator generate -i http://localhost:8080/openapi.json -g go -o ./go-client
```

## Import into tools

- **Postman**: Import → Link → paste `http://localhost:8080/openapi.json`.
- **Insomnia**: Import/Export → Import Data → From URL.
- **Bruno**: Import Collection → OpenAPI.
- **curl**: use with tools like
  [openapi2curl](https://github.com/openapi-generator/openapi-generator).

## Updating the document

Edit the route metadata in `src/web/barrel_http_server.erl:routes/0`
(per-route `Meta` map: `tags`, `summary`, `operation_id`,
`parameters`, `request_body`, `responses`). The next service start
serves the updated document; no separate doc file to keep in sync.
