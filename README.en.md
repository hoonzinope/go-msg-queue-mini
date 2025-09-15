# Go Message Queue Mini

[한국어](./README.md)

A compact yet robust Go-based message queue. It supports Producer/Consumer runtime and HTTP/gRPC APIs, featuring SQLite-backed persistence, Ack/Nack, DLQ, lease and renew, peek, and queue status.

## Key Features

- **Pluggable persistence**: `memory` (in-memory SQLite) or `file` (file-backed SQLite)
- **Concurrency**: Multiple producers/consumers via goroutines
- **Processing guarantees**: Ack/Nack, inflight, DLQ, retry with backoff + jitter
- **Lease/Renew**: Message lease with `/renew` to extend
- **Peek**: Inspect next message without claiming via `/peek`
- **Status**: `/status` returns totals for queue/acked/inflight/dlq
- **Modes**: `debug` (local producers/consumers + monitor) or HTTP/gRPC server

## Quick Start

- Prereqs: Go 1.21+; optionally `protoc` if editing gRPC proto
- Env: set API key, e.g. `export API_KEY=dev-key`
- Run (debug mode): `go run cmd/main.go` (default `memory` persistence)
- Health: `curl -s localhost:8080/health`
- Create queue: `curl -X POST localhost:8080/api/v1/default/create -H 'X-API-Key: $API_KEY'`
- Enqueue: `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message": {"text":"hello"}}'`
- Dequeue: `curl -X POST localhost:8080/api/v1/default/dequeue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"group":"g1","consumer_id":"c-1"}'`

## Project Structure

```
.
├── cmd/
│   └── main.go                  # Entrypoint: choose debug/HTTP/gRPC
├── client/
│   └── client.go                # Built-in client wrapper
├── config.yml                   # Runtime and persistence config
├── go.mod / go.sum
├── README.md / README.en.md
├── proto/
│   └── queue.proto              # gRPC protocol definition
├── internal/
│   ├── queue.go                 # Queue interface and status types
│   ├── config.go                # YAML config loader
│   ├── config_test.go           # Config loader test
│   ├── api/http/
│   │   ├── server.go            # Gin HTTP server bootstrap
│   │   ├── handler.go           # API handlers (create/delete/enqueue/dequeue/ack/nack/peek/renew/status/health)
│   │   └── dto.go               # Request/response DTOs
│   ├── api/grpc/                # gRPC server + generated pb files
│   │   ├── server.go
│   │   ├── interceptor.go       # gRPC interceptors (logging/error/recovery/auth)
│   │   ├── queue.pb.go
│   │   └── queue_grpc.pb.go
│   ├── core/
│   │   ├── filedb_queue.go      # Queue adapter (Queue implementation)
│   │   └── filedb_manager.go    # SQLite engine (Ack/Nack, DLQ, leases, cleanup)
│   ├── metrics/                 # Prometheus metrics
│   └── runner/
│       ├── producer.go          # Debug-mode producer
│       ├── consumer.go          # Debug-mode consumer
│       └── stat.go              # Periodic status monitor
└── util/
    ├── randUtil.go              # Random messages/numbers, jitter
    ├── uuid.go                  # Global ID generator
    └── logger.go                # Simple logger
```

## Build/Run/Dev

- Run: `go run cmd/main.go` (or `go run ./cmd`)
- Build: `go build -o bin/go-msg-queue-mini ./...`
- Test: `go test ./... -v` (coverage: `-cover`)
- Format/Lint: `go fmt ./...`, `go vet ./...`, `go mod tidy`
- Regenerate gRPC (after editing `proto/queue.proto`): `protoc --go_out=. --go-grpc_out=. proto/queue.proto`

## Configuration (`config.yml`)

```yaml
persistence:
  # memory: in-memory SQLite (no disk writes)
  # file:   file-backed SQLite (recommended for local dev/tests)
  # Example 1) default (memory):
  type: memory
  options:
    dirs-path: ./data/persistence

max-retry: 3           # Maximum retry attempts
retry-interval: 30s    # Base for backoff
lease-duration: 3s     # Message lease duration
debug: false           # If true, runs built-in producers/consumers/monitor

http:
  enabled: true        # Enable HTTP API
  port: 8080           # Port (default 8080)
  rate:                # IP-based rate limiting (429 on exceed)
    limit: 1
    burst: 5
  auth:                # Protect writer endpoints
    api_key: ${API_KEY}

grpc:
  enabled: true        # Enable gRPC server
  port: 50051
  auth:                # API key for gRPC interceptor auth
    api_key: ${API_KEY}
```

- For `file` mode, create the directory: `mkdir -p ./data/persistence`
- SQLite DB file path: `./data/persistence/filedb_queue.db`
- `.env` is loaded (via `github.com/joho/godotenv`), and `config.yml` supports env expansion (`$VAR`/`${VAR}`).

## Modes

- `debug: true`
  - Internal producers/consumers and a status monitor run locally.
  - Observe production/consumption/status logs in the console.

- `debug: false` + `http.enabled: true`
  - Runs the HTTP API server on `:8080` (or configured port).
- `grpc.enabled: true`
  - Runs the gRPC server on `:50051` (or configured port).

## HTTP API

- Health: `GET /health`
  - Example: `curl -s localhost:8080/health`
- Metrics: `GET /metrics` (Prometheus)

- Create Queue: `POST /api/v1/:queue_name/create` (requires `X-API-Key`)
  - Response: `201 Created`, `{ "status": "created" }`

- Delete Queue: `DELETE /api/v1/:queue_name/delete` (requires `X-API-Key`)
  - Response: `200 OK`, `{ "status": "deleted" }`

- Enqueue: `POST /api/v1/:queue_name/enqueue` (header: `X-API-Key: <key>` required)
  - Request: `{ "message": <any JSON> }`
  - Example: `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message": {"text":"hello"}}'`

- Dequeue: `POST /api/v1/:queue_name/dequeue` (requires `X-API-Key`)
  - Request: `{ "group": "g1", "consumer_id": "c-1" }`
  - Success: `200 OK`, `{ status, message: { id, receipt, payload } }`
  - Empty queue: `204 No Content`
  - Contended: `409 Conflict`, `{ "status": "message is being processed" }`

- Ack: `POST /api/v1/:queue_name/ack` (requires `X-API-Key`)
  - Request: `{ "group": "g1", "message_id": 1, "receipt": "..." }`

- Nack: `POST /api/v1/:queue_name/nack` (requires `X-API-Key`)
  - Request: `{ "group": "g1", "message_id": 1, "receipt": "..." }`
  - Applies exponential backoff + jitter; moves to DLQ after `max-retry`.

- Peek: `POST /api/v1/:queue_name/peek`
  - Request: `{ "group": "g1" }` (inspect next available message without lease)
  - Empty queue: `204 No Content`

- Renew: `POST /api/v1/:queue_name/renew` (requires `X-API-Key`)
  - Request: `{ "group": "g1", "message_id": 1, "receipt": "...", "extend_sec": 5 }`
  - Extends only if the current lease is valid; returns 409 if expired.

- Status: `GET /api/v1/:queue_name/status`
  - Response: `{ queue_status: { queue_type, total_messages, acked_messages, inflight_messages, dlq_messages } }`

Common: server returns `X-Request-ID` (auto-generated if missing). Bursts may receive 429 Too Many Requests due to rate limiting.

-## gRPC API
- Enable with `grpc.enabled: true`, default port `50051`.
- Health: `queue.v1.QueueService/HealthCheck` → `{ status: "ok" }`.
- Core RPCs: `CreateQueue`, `DeleteQueue`, `Enqueue`, `Dequeue`, `Ack`, `Nack`, `Peek`, `Renew`, `Status` (proto: `proto/queue.proto`).
- Message type: gRPC `message` is a string; HTTP supports arbitrary JSON payloads.
- Example (grpcurl):
  - List services: `grpcurl -plaintext localhost:50051 list`
  - Enqueue: `grpcurl -plaintext -d '{"queue_name":"default","message":"hello"}' localhost:50051 queue.v1.QueueService/Enqueue`

## gRPC Interceptors
- Logging: `LoggerInterceptor` logs method and latency per call.
- Error log: `ErrorInterceptor` logs handler errors.
- Recovery: `RecoveryInterceptor` converts panics to `codes.Internal`.
- Auth: `AuthInterceptor(apiKey, protectedMethods)` enforces metadata `x-api-key` for protected methods.
  - Protected: `/CreateQueue`, `/DeleteQueue`, `/Enqueue`, `/Dequeue`, `/Ack`, `/Nack`, `/Renew`
  - Public: `/Peek`, `/Status`, `/HealthCheck`
  - Note: gRPC metadata key is lowercase `x-api-key` (unlike HTTP header casing).
- Chain order: Recovery → Logger → Error → Auth (via `grpc.ChainUnaryInterceptor`).

Examples (grpcurl)
- Public RPC: `grpcurl -plaintext localhost:50051 queue.v1.QueueService/HealthCheck`
- Protected RPC: `grpcurl -plaintext -H 'x-api-key: $API_KEY' -d '{"queue_name":"default","message":"hello"}' localhost:50051 queue.v1.QueueService/Enqueue`

## Tests

- Uses standard `testing`: `go test ./... -v`
- Config loader test: `internal/config_test.go`
- Optionally use `-cover` for coverage

## Changes Summary
- Added gRPC Queue Service: `internal/api/grpc/*`, `proto/queue.proto`, and `grpc.enabled/port` config.
- Added HTTP middlewares: logging, error handling, Request-ID, API key auth, and IP-based rate limiting (with periodic cleanup).
- Config improvements: `.env` loading and env expansion for `config.yml`; new rate/auth/gRPC sections.
- Standardized JSON fields, improved error handling, and graceful shutdown on exit.
- HTTP refactor into server/handler/dto files.
- Added gRPC interceptors: logging/error/recovery/API-key auth via metadata (`x-api-key`), with protected vs public methods. Added `grpc.auth.api_key` to `config.yml`.

## Metrics (Prometheus)
- Counters: `mq_enqueue_total`, `mq_dequeue_total`, `mq_ack_total`, `mq_nack_total` (label: `queue`)
- Gauges: `mq_total_messages`, `mq_in_flight_messages`, `mq_dlq_messages` (label: `queue`)
- Endpoint: `GET /metrics`

## Security/Operations

- Never commit secrets; `config.yml` is for local development.
- Ensure `persistence.options.dirs-path` exists and is writable when using `file` mode.
- `memory` mode keeps data in memory; data disappears when the process restarts.

---
If you want this document to stay in sync with new features, feel free to request updates.
