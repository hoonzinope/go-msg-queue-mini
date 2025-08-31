# Go 메시지 큐 미니

[English](./README.en.md)

간단하지만 견고한 Go 기반 메시지 큐입니다. Producer/Consumer 실행과 HTTP API를 모두 지원하며, SQLite 기반 영속성과 Ack/Nack, DLQ, 리스(lease) 연장 등 기본기를 갖춘 소형 큐 엔진입니다.

## 주요 특징

- **영속성 선택**: `memory`(in-memory SQLite) 또는 `file`(파일 기반 SQLite)
- **동시성 처리**: 다수 Producer/Consumer를 고루틴으로 병렬 실행
- **처리 보장**: Ack/Nack, Inflight, DLQ, 재시도(backoff + jitter)
- **리스/갱신**: 메시지 점유 기간(lease)과 `/renew`를 통한 연장
- **미리보기/피킹**: 할당 없이 확인하는 `/peek`
- **상태 확인**: 합계/ACK/Inflight/DLQ를 반환하는 `/status`
- **운영 모드**: `debug` 모드(내장 프로듀서/컨슈머 + 모니터) 또는 HTTP API 서버

## 프로젝트 구조

```
.
├── main.go                      # 엔트리포인트: debug/HTTP 모드 선택
├── config.yml                   # 런타임 및 영속성 설정
├── go.mod / go.sum
├── README.md / README.en.md
├── internal/
│   ├── queue.go                 # 큐 인터페이스 및 상태 타입
│   ├── config.go                # YAML 설정 로더
│   ├── config_test.go           # 설정 로더 테스트
│   ├── api/http/
│   │   ├── server.go            # Gin HTTP 서버 부트스트랩
│   │   ├── handler.go           # API 핸들러 (enqueue/dequeue/ack/nack/peek/renew/status/health)
│   │   └── dto.go               # 요청/응답 DTO
│   ├── core/
│   │   ├── filedb_queue.go      # 큐 어댑터 (Queue 구현)
│   │   └── filedb_manager.go    # SQLite 엔진 (Ack/Nack, DLQ, 리스, 정리 작업)
│   └── runner/
│       ├── producer.go          # 디버그 모드용 프로듀서
│       ├── consumer.go          # 디버그 모드용 컨슈머
│       └── stat.go              # 주기 상태 모니터
└── util/
    ├── randUtil.go              # 랜덤 메시지/수, 지터
    ├── uuid.go                  # 글로벌 ID 생성
    └── logger.go                # 간단 로거
```

## 빌드/실행/개발

- 실행: `go run main.go`
- 빌드: `go build -o bin/go-msg-queue-mini ./...`
- 테스트: `go test ./... -v` (커버리지: `-cover`)
- 포맷/린트: `go fmt ./...` / `go vet ./...` / `go mod tidy`

## 설정 (`config.yml`)

```yaml
persistence:
  # memory: 메모리 내 SQLite (디스크 쓰기 없음)
  # file:   파일 기반 SQLite (권장: 로컬 개발/테스트)
  type: file
  options:
    dirs-path: ./data/persistence

max-retry: 3           # 최대 재시도 횟수
retry-interval: 30s    # 재시도 간격(백오프 베이스)
lease-duration: 3s     # 메시지 리스 기간
debug: false           # true면 내장 프로듀서/컨슈머/모니터 실행

http:
  enabled: true        # HTTP API 활성화
  port: 8080           # 포트 (기본 8080)
```

- `file` 모드 사용 시 디렉터리 생성: `mkdir -p ./data/persistence`
- 실제 DB 파일 경로: `./data/persistence/filedb_queue.db`

## 모드

- `debug: true`
  - 내부 프로듀서 2개/컨슈머 2개와 상태 모니터가 함께 실행됩니다.
  - 콘솔에서 생산/소비/상태 로그를 관찰할 수 있습니다.

- `debug: false` + `http.enabled: true`
  - HTTP API 서버가 `:8080` (또는 설정 포트)에서 기동됩니다.

## HTTP API

- 헬스체크: `GET /api/v1/health`
  - 예) `curl -s localhost:8080/api/v1/health`

- Enqueue: `POST /api/v1/enqueue`
  - 요청: `{ "message": <任意 JSON> }`
  - 예) `curl -X POST localhost:8080/api/v1/enqueue -H 'Content-Type: application/json' -d '{"message": {"text":"hello"}}'`

- Dequeue: `POST /api/v1/dequeue`
  - 요청: `{ "group": "g1", "consumer_id": "c-1" }`
  - 응답: `{ status, message: { id, receipt, payload } }`

- Ack: `POST /api/v1/ack`
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "..." }`

- Nack: `POST /api/v1/nack`
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "..." }`
  - 내부적으로 백오프 + 지터가 적용되며, `max-retry` 초과 시 DLQ로 이동

- Peek: `POST /api/v1/peek`
  - 요청: `{ "group": "g1" }` (할당/리스 없이 가장 앞 메시지 확인)

- Renew: `POST /api/v1/renew`
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "...", "extend_sec": 5 }`
  - 현재 리스가 유효한 경우만 연장, 만료 시 409 반환

- Status: `GET /api/v1/status`
  - 응답: `{ queue_status: { queue_type, total_messages, acked_messages, inflight_messages, dlq_messages } }`

## 테스트

- 표준 `testing` 프레임워크 사용: `go test ./... -v`
- 설정 로더 테스트: `internal/config_test.go`
- 필요 시 `-cover`로 커버리지 확인

## 보안/운영 팁

- 비밀정보는 커밋하지 마세요. `config.yml`은 로컬 개발용입니다.
- `file` 모드 사용 시 `persistence.options.dirs-path`가 존재하고 쓰기 가능해야 합니다.
- `memory` 모드는 디스크에 기록하지 않으므로 프로세스 재시작 시 데이터가 사라집니다.

---
