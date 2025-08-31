# Go Message Queue Mini

[한국어](./README.md)

A compact yet robust Go-based message queue. It supports both Producer/Consumer runtime and an HTTP API, featuring SQLite-backed persistence, Ack/Nack, DLQ, lease and renew, peek, and queue status.

## Key Features

- **Pluggable persistence**: `memory` (in-memory SQLite) or `file` (file-backed SQLite)
- **Concurrency**: Multiple producers/consumers via goroutines
- **Processing guarantees**: Ack/Nack, inflight, DLQ, retry with backoff + jitter
- **Lease/Renew**: Message lease with `/renew` to extend
- **Peek**: Inspect next message without claiming via `/peek`
- **Status**: `/status` returns totals for queue/acked/inflight/dlq
- **Modes**: `debug` (local producers/consumers + monitor) or HTTP API server

## Project Structure

```
.
├── main.go                      # Entrypoint: choose debug/HTTP mode
├── config.yml                   # Runtime and persistence config
├── go.mod / go.sum
├── README.md / README.en.md
├── internal/
│   ├── queue.go                 # Queue interface and status types
│   ├── config.go                # YAML config loader
│   ├── config_test.go           # Config loader test
│   ├── api/http/
│   │   ├── server.go            # Gin HTTP server bootstrap
│   │   ├── handler.go           # API handlers (enqueue/dequeue/ack/nack/peek/renew/status/health)
│   │   └── dto.go               # Request/response DTOs
│   ├── core/
│   │   ├── filedb_queue.go      # Queue adapter (Queue implementation)
│   │   └── filedb_manager.go    # SQLite engine (Ack/Nack, DLQ, leases, cleanup)
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

- Run: `go run main.go`
- Build: `go build -o bin/go-msg-queue-mini ./...`
- Test: `go test ./... -v` (coverage: `-cover`)
- Format/Lint: `go fmt ./...`, `go vet ./...`, `go mod tidy`

## Configuration (`config.yml`)

```yaml
persistence:
  # memory: in-memory SQLite (no disk writes)
  # file:   file-backed SQLite (recommended for local dev/tests)
  type: file
  options:
    dirs-path: ./data/persistence

max-retry: 3           # Maximum retry attempts
retry-interval: 30s    # Base for backoff
lease-duration: 3s     # Message lease duration
debug: false           # If true, runs built-in producers/consumers/monitor

http:
  enabled: true        # Enable HTTP API
  port: 8080           # Port (default 8080)
```

- For `file` mode, create the directory: `mkdir -p ./data/persistence`
- SQLite DB file path: `./data/persistence/filedb_queue.db`

## Modes

- `debug: true`
  - Two internal producers, two consumers, and a status monitor run locally.
  - Observe production/consumption/status logs in the console.

- `debug: false` + `http.enabled: true`
  - Runs the HTTP API server on `:8080` (or configured port).

## HTTP API

- Health: `GET /api/v1/health`
  - Example: `curl -s localhost:8080/api/v1/health`

- Enqueue: `POST /api/v1/enqueue`
  - Request: `{ "message": <any JSON> }`
  - Example: `curl -X POST localhost:8080/api/v1/enqueue -H 'Content-Type: application/json' -d '{"message": {"text":"hello"}}'`

- Dequeue: `POST /api/v1/dequeue`
  - Request: `{ "group": "g1", "consumer_id": "c-1" }`
  - Response: `{ status, message: { id, receipt, payload } }`

- Ack: `POST /api/v1/ack`
  - Request: `{ "group": "g1", "message_id": 1, "receipt": "..." }`

- Nack: `POST /api/v1/nack`
  - Request: `{ "group": "g1", "message_id": 1, "receipt": "..." }`
  - Applies exponential backoff + jitter; moves to DLQ after `max-retry`.

- Peek: `POST /api/v1/peek`
  - Request: `{ "group": "g1" }` (inspect next available message without lease)

- Renew: `POST /api/v1/renew`
  - Request: `{ "group": "g1", "message_id": 1, "receipt": "...", "extend_sec": 5 }`
  - Extends only if the current lease is valid; returns 409 if expired.

- Status: `GET /api/v1/status`
  - Response: `{ queue_status: { queue_type, total_messages, acked_messages, inflight_messages, dlq_messages } }`

## Tests

- Uses standard `testing`: `go test ./... -v`
- Config loader test: `internal/config_test.go`
- Optionally use `-cover` for coverage

## Security/Operations

- Never commit secrets; `config.yml` is for local development.
- Ensure `persistence.options.dirs-path` exists and is writable when using `file` mode.
- `memory` mode keeps data in memory; data disappears when the process restarts.

---

If you want this document to stay in sync with new features, feel free to request updates.
