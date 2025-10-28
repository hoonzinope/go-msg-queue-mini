# Go Message Queue Mini

[한국어](./README.md)

A compact yet robust Go-based message queue. It supports Producer/Consumer runtime and HTTP/gRPC APIs, featuring SQLite-backed persistence, Ack/Nack, DLQ, lease and renew, peek, and queue status.

## Key Features

- **Pluggable persistence**: `memory` (in-memory SQLite) or `file` (file-backed SQLite)
- **Concurrency**: Multiple producers/consumers via goroutines
- **Processing guarantees**: Ack/Nack, inflight, DLQ, retry with backoff + jitter
- **Delayed delivery**: Schedule visibility via the optional `delay` parameter (e.g., "10s", "5m", "1h30m")
- **Lease/Renew**: Message lease with `/renew` to extend
- **Peek/Detail**: `/peek` accepts options to inspect up to 100 messages with paging/preview, and `/messages/:message_id` returns an individual message payload
- **Status**: `/status` returns totals for queue/acked/inflight/dlq
- **Batch enqueue**: Load multiple messages at once via `/enqueue/batch` and gRPC `EnqueueBatch`, supporting `stopOnFailure`/`partialSuccess` modes
- **Deduplication**: Optional `deduplication_id` enforces per-queue idempotency within a 1-hour window
- **Modes**: `debug` (local producers/consumers + monitor) or HTTP/gRPC server
- **Binary-safe payloads**: Queue core and gRPC APIs store payloads as raw `[]byte`; the HTTP layer accepts arbitrary JSON and forwards the serialized bytes

## Quick Start

- Prereqs: Go 1.21+; optionally `protoc` if editing gRPC proto
- Env: set API key, e.g. `export API_KEY=dev-key`
- Run (debug mode): `go run cmd/main.go` (default `memory` persistence)
- Health: `curl -s localhost:8080/health`
- Create queue: `curl -X POST localhost:8080/api/v1/default/create -H 'X-API-Key: $API_KEY'`
- Enqueue: `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message":{"text":"hello"}}'` (defaults to immediate visibility when `delay` is omitted)
- Delayed enqueue: `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message":{"text":"hello-later"},"delay":"30s"}'`
- Batch enqueue (apply 1m delay): `curl -X POST localhost:8080/api/v1/default/enqueue/batch -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"mode":"partialSuccess","messages":[{"text":"hello"},{"text":"world"}],"delay":"1m"}'`
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
│   ├── queue.go                 # Queue interface (`[]byte` payloads) and status types
│   ├── config.go                # YAML config loader
│   ├── config_test.go           # Config loader test
│   ├── api/http/
│   │   ├── server.go            # Gin HTTP server bootstrap
│   │   ├── handler.go           # API handlers (create/delete/enqueue(+batch)/dequeue/ack/nack/peek/renew/status/health)
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
│       └── consumer.go          # Debug-mode consumer
│       
└── util/
    ├── randUtil.go              # Random messages/numbers, jitter
    ├── uuid.go                  # Global ID generator
    ├── partition.go             # List partitioning utility
    └── logger.go                # slog-backed logger initialization
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
  - Observe production/consumption logs in the console.
  - Producer: loads random messages (1-5 bytes) every second.
  - Consumer: 3 goroutines consuming messages in parallel.
  - (Note) http/gRPC servers do not run.

- `debug: false` + `http.enabled: true`
  - Runs the HTTP API server on `:8080` (or configured port).
- `debug: false` + `grpc.enabled: true`
  - Runs the gRPC server on `:50051` (or configured port).

## Logging

- All components use Go's `log/slog` for structured logging (`util/logger.go`).
- `debug: true` switches to the text handler; other modes emit JSON to stdout.
- Queue core and HTTP/gRPC layers log shared fields (e.g., `error`, `queue`, `status`) for easier correlation.

## HTTP API

- Health: `GET /health`
  - Example: `curl -s localhost:8080/health`
- Metrics: `GET /metrics` (Prometheus)

- Create Queue: `POST /api/v1/:queue_name/create` (requires `X-API-Key`)
  - Response: `201 Created`, `{ "status": "created" }`

- Delete Queue: `DELETE /api/v1/:queue_name/delete` (requires `X-API-Key`)
  - Response: `200 OK`, `{ "status": "deleted" }`

- Enqueue: `POST /api/v1/:queue_name/enqueue` (header: `X-API-Key: <key>` required)
  - Request: `{ "message": <any JSON>, "delay": "<Go Duration>", "deduplication_id": "<string>" }` (`delay`/`deduplication_id` optional; e.g., "10s", "1m")
  - Example: `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message": {"text":"hello"}}'`
  - Note: `delay` must be a valid Go duration string; invalid values reject the request and negatives clamp to zero.
  - Note: When `deduplication_id` is present the queue prevents re-enqueuing the same ID for 1 hour; duplicates return an error (or show up as failures in batch mode).
- Enqueue Batch: `POST /api/v1/:queue_name/enqueue/batch` (header: `X-API-Key: <key>` required)
  - Request: `{ "mode": "partialSuccess"|"stopOnFailure", "messages": [{"message": <any JSON>, "delay": "<Go Duration>", "deduplication_id": "<string>"}, ...] }`
  - Response: `202 Accepted`, `{ "status": "enqueued", "success_count": <int>, "failure_count": <int>, "failed_messages": [{ "index": <int>, "message": "<raw>", "error": "<reason>" }, ...] }`
  - Behavior: `stopOnFailure` stops on the first error and returns a 5xx, while `partialSuccess` enqueues what it can and reports failures via `failed_messages`.
  - Note: Each message can supply its own `delay` and `deduplication_id`; when a duplicate ID is detected that entry is reported in `failed_messages`.

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
  - Request: `{ "group": "g1", "options": { "limit": 3, "cursor": 25, "order": "asc", "preview": true } }`
  - Notes: `limit` defaults to 1 (max 100), `cursor` paginates by message ID, `order` accepts `asc`/`desc`, `preview` returns payload snippets up to 50 runes (truncated with `...`)
  - Extra: With `preview=false` the handler attempts to parse payloads as JSON; failures yield an empty string payload plus `error_msg`.
  - Empty queue: `204 No Content`

- Detail: `GET /api/v1/:queue_name/messages/:message_id`
  - Description: Fetch a single message’s payload/timestamps by `queue_name` + `message_id`; no API key required.
  - Response: `200 OK`, `{ "status": "ok", "message": { "id": 42, "payload": <JSON>, "receipt": "", "inserted_at": "<RFC3339>", "error_msg": "<string>" } }`
  - Extra: Non-JSON payloads become an empty string with `error_msg` explaining the failure. Missing message IDs currently surface as a 500 response (`failed to get message detail`).

- Renew: `POST /api/v1/:queue_name/renew` (requires `X-API-Key`)
  - Request: `{ "group": "g1", "message_id": 1, "receipt": "...", "extend_sec": 5 }`
  - Extends only if the current lease is valid; returns 409 if expired.

- Status: `GET /api/v1/:queue_name/status`
  - Response: `{ queue_status: { queue_type, total_messages, acked_messages, inflight_messages, dlq_messages } }`

Common: server returns `X-Request-ID` (auto-generated if missing). Bursts may receive 429 Too Many Requests due to rate limiting.

## gRPC API
- Enable with `grpc.enabled: true`, default port `50051`.
- Health: `queue.v1.QueueService/HealthCheck` → `{ status: "ok" }`.
- Core RPCs: `CreateQueue`, `DeleteQueue`, `Enqueue`, `EnqueueBatch`, `Dequeue`, `Ack`, `Nack`, `Peek`, `Renew`, `Status` (see `proto/queue.proto`).
- Peek requests accept optional `options` (`limit`, `cursor`, `order`, `preview`) to fetch up to 100 messages or return payload previews (50 runes; truncated with `...`).
- Message type: gRPC `message` payloads are **bytes** (JSON tools such as `grpcurl` expect Base64 strings), while the HTTP API accepts arbitrary JSON and stores the serialized bytes.
- `Enqueue`/`EnqueueBatch` accept optional `delay` (Go duration) and `deduplication_id` (string) for scheduling or 1-hour idempotency; omit them for immediate delivery.
- For `EnqueueBatch`, set `mode` to `stopOnFailure` or `partialSuccess`; responses include `failure_count`/`failed_messages`, and `stopOnFailure` propagates errors as 5xx responses.
- Example (grpcurl):
  - List services: `grpcurl -plaintext localhost:50051 list`
  - Delayed enqueue (note: `message` is Base64 when using JSON):  
    `grpcurl -plaintext -d '{"queue_name":"default","message":"aGVsbG8=","delay":"45s"}' localhost:50051 queue.v1.QueueService/Enqueue`
  - Batch enqueue: `grpcurl -plaintext -d '{"queue_name":"default","mode":"partialSuccess","messages":[{"message":"aGVsbG8="},{"message":"d29ybGQ="}]}' localhost:50051 queue.v1.QueueService/EnqueueBatch`

## gRPC Interceptors
- Logging: `LoggerInterceptor` logs method and latency per call.
- Error log: `ErrorInterceptor` logs handler errors.
- Recovery: `RecoveryInterceptor` converts panics to `codes.Internal`.
- Auth: `AuthInterceptor(apiKey, protectedMethods)` enforces metadata `x-api-key` for protected methods.
  - Protected: `/CreateQueue`, `/DeleteQueue`, `/Enqueue`, `/EnqueueBatch`, `/Dequeue`, `/Ack`, `/Nack`, `/Renew`
  - Public: `/Peek`, `/Status`, `/HealthCheck`
  - Note: gRPC metadata key is lowercase `x-api-key` (unlike HTTP header casing).
- Chain order: Recovery → Logger → Error → Auth (via `grpc.ChainUnaryInterceptor`).

Examples (grpcurl)
- Public RPC: `grpcurl -plaintext localhost:50051 queue.v1.QueueService/HealthCheck`
- Protected RPC: `grpcurl -plaintext -H 'x-api-key: $API_KEY' -d '{"queue_name":"default","messages":["hello","world"]}' localhost:50051 queue.v1.QueueService/EnqueueBatch`

## Tests

- Uses standard `testing`: `go test ./... -v`
- Config loader test: `internal/config_test.go`
- Optionally use `-cover` for coverage

## Changes Summary
- Added message deduplication: optional `deduplication_id` on HTTP/gRPC enqueue paths enforces a 1-hour per-queue idempotency window.
- Added scheduled delivery: optional `delay` on HTTP/gRPC enqueue paths backed by the file DB `visible_at` column.
- Added batch enqueue support: HTTP `/enqueue/batch`, gRPC `EnqueueBatch`, and batched writes in the file-backed queue.
- Switched to structured logging via Go `log/slog` (text in debug mode, JSON otherwise) with shared diagnostic fields.
- Introduced the gRPC Queue Service and interceptors (logging/error/recovery/API key) and expanded `proto/queue.proto`.
- Refactored the HTTP server into server/handler/dto packages with logging, Request-ID, rate limiting, and API key middleware.
- Enhanced configuration: `.env` loading, environment expansion in `config.yml`, and extended HTTP/GRPC/rate limit options.
- Standardized JSON fields, strengthened error handling, ensured graceful shutdown, and enriched Prometheus metrics.

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
