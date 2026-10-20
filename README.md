# HarnessX

HarnessX is an open, server-side agent harness. It provides a durable agent
loop and stable service contracts for models, context construction, tool
execution, filesystem-like volumes, and ordered event/audit streams.

HarnessX is intentionally a harness rather than a complete AI product. Product
features such as document ingestion, knowledge bases, memory products, search
providers, identity systems, billing, and user interfaces integrate through
versioned service contracts instead of being embedded in the orchestration
kernel.

## Architecture

HarnessX is microservice-first:

- `api-gateway` exposes the public HTTP and SSE API.
- `harness-core` owns durable runs and the bounded agent state machine.
- `model-core` owns model adapters, routing, fallback, and normalized usage.
- `context-core` owns prompt assembly, context budgeting, and compaction.
- `tool-core` owns server-side tool discovery and execution.
- `volume-core` owns durable filesystem-like volumes.
- `event-core` owns ordered run events, audit append, and replay.

Internal service APIs use protobuf and gRPC. Services may share infrastructure
in small deployments, but never access another service's owned data directly.

See [Architecture Decision 0001](docs/adr/0001-microservice-first-open-kernel.md)
and the contracts under [`proto/harnessx/v1`](proto/harnessx/v1).

## Current foundation

The repository now contains a runnable first production foundation rather than
an in-process demonstration:

- a bounded, durable agent loop with immutable policy snapshots, persisted
  model/tool steps, cancellation, idempotent run creation, and usage accounting;
- OpenAI-compatible model adapters with explicit provider/model routing,
  capability discovery, streaming, normalized tool calls, response limits, and
  retry-only fallback;
- configuration-registered HTTP tools with operator-owned endpoints and
  credentials, per-run allow-lists, bounded concurrency, cancellation, and
  response limits;
- PostgreSQL-backed ordered events and a transactional harness outbox for audit
  delivery across service failures;
- a PostgreSQL-indexed filesystem volume service with an atomic local/PVC
  backend, digest preconditions, range reads, pagination, and path confinement;
- an authenticated HTTP/SSE gateway plus the canonical protobuf/gRPC APIs.

This is the beginning of the production implementation, not a claim that every
planned backend is complete. Remote volume mount grants, context reduction
plugins, asynchronous run leasing/recovery, provider-specific adapters, and
full telemetry are deliberately still explicit follow-on work.

## Requirements

- Go 1.22 or newer
- `protoc` 3.21 or newer
- `protoc-gen-go` and `protoc-gen-go-grpc`
- Docker with Docker Compose for the local stack

## Run the local stack

The checked-in Compose topology runs PostgreSQL and all seven services. Edit
[`deployments/docker-compose/model.json`](deployments/docker-compose/model.json)
to point at an OpenAI-compatible endpoint and choose its actual model ID, then:

```bash
docker compose -f deployments/docker-compose/compose.yaml up --build
```

The example points at port `11434` on the Docker host. The local gateway is
intentionally configured in insecure development mode; callers must supply a
tenant header:

```bash
curl --fail-with-body http://localhost:8080/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'X-HarnessX-Tenant-ID: local' \
  -H 'Idempotency-Key: first-run' \
  -d '{"input":"Say hello.","models":["local-openai-compatible/configured-model"]}'
```

For any non-local deployment, leave `GATEWAY_ALLOW_INSECURE` unset and provide
`GATEWAY_API_KEYS_JSON` as a JSON object mapping bearer tokens to tenant IDs.
The public contract is documented in
[`api/openapi/openapi.yaml`](api/openapi/openapi.yaml).

## Development

```bash
make proto-generate
make test
make build
```

Database changes are forward-only numbered SQL files. Apply them with:

```bash
DATABASE_URL='postgres://...' go run ./cmd/migrate -dir db/migrations
```

The migrator serializes concurrent runs and refuses to continue if an already
applied migration's checksum changed.

## License

HarnessX is available under the [MIT License](LICENSE).
