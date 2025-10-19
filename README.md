# Go 메시지 큐 미니

[English](./README.en.md)

간단하지만 견고한 Go 기반 메시지 큐입니다. Producer/Consumer 실행과 HTTP/gRPC API를 지원하며, SQLite 기반 영속성과 Ack/Nack, DLQ, 리스(lease) 연장 등 기본기를 갖춘 소형 큐 엔진입니다.

## 주요 특징

- **영속성 선택**: `memory`(in-memory SQLite) 또는 `file`(파일 기반 SQLite)
- **동시성 처리**: 다수 Producer/Consumer를 고루틴으로 병렬 실행
- **처리 보장**: Ack/Nack, Inflight, DLQ, 재시도(backoff + jitter)
- **지연 전송**: `delay` 파라미터로 메시지 가시 시점을 예약 (예: "10s", "5m", "1h30m")
- **리스/갱신**: 메시지 점유 기간(lease)과 `/renew`를 통한 연장
- **미리보기/피킹**: `/peek` 옵션으로 최대 100개 메시지 확인, 페이징/미리보기 지원
- **상태 확인**: 합계/ACK/Inflight/DLQ를 반환하는 `/status`
- **배치 Enqueue**: `/enqueue/batch` 및 gRPC `EnqueueBatch`로 여러 메시지를 일괄 적재하며 `stopOnFailure`/`partialSuccess` 모드를 지원
- **중복 방지**: `deduplication_id`를 지정하면 동일 큐에서 1시간 동안 같은 ID가 재적재되지 않아 FIFO 기반 멱등성을 제공
- **운영 모드**: `debug` 모드(내장 프로듀서/컨슈머 + 모니터) 또는 HTTP/gRPC 서버

## 빠른 시작(Quick Start)

- 사전 준비: Go 1.21+ 설치, 선택사항으로 `protoc`(gRPC 변경 시 필요)
- 환경 변수: API 키 설정 예시 `export API_KEY=dev-key`
- 실행(디버그 모드): `go run cmd/main.go` (기본 `memory` 영속성)
- HTTP 헬스 확인: `curl -s localhost:8080/health`
- 큐 생성: `curl -X POST localhost:8080/api/v1/default/create -H 'X-API-Key: $API_KEY'`
- 메시지 Enqueue: `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message":{"text":"hello"}}'` (`delay`를 생략하면 즉시 가시)
- 지연 메시지 Enqueue: `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message":{"text":"hello-later"},"delay":"30s"}'`
- 메시지 Batch Enqueue: `curl -X POST localhost:8080/api/v1/default/enqueue/batch -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"mode":"partialSuccess","messages":[{"text":"hello"},{"text":"world"}],"delay":"1m"}'`
- 메시지 Dequeue: `curl -X POST localhost:8080/api/v1/default/dequeue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"group":"g1","consumer_id":"c-1"}'`

## 프로젝트 구조

```
.
├── cmd/
│   └── main.go                  # 엔트리포인트: debug/HTTP/gRPC 선택
├── client/
│   └── client.go                # 내장 클라이언트 래퍼
├── config.yml                   # 런타임 및 영속성 설정
├── go.mod / go.sum
├── README.md / README.en.md
├── proto/
│   └── queue.proto             # gRPC 프로토 정의
├── internal/
│   ├── queue.go                 # 큐 인터페이스 및 상태 타입
│   ├── config.go                # YAML 설정 로더
│   ├── config_test.go           # 설정 로더 테스트
│   ├── api/http/
│   │   ├── server.go            # Gin HTTP 서버 부트스트랩
│   │   ├── handler.go           # API 핸들러 (create/delete/enqueue(+batch)/dequeue/ack/nack/peek/renew/status/health)
│   │   └── dto.go               # 요청/응답 DTO
│   ├── api/grpc/                # gRPC 서버/자동 생성 파일(pb)
│   │   ├── server.go
│   │   ├── interceptor.go       # gRPC 인터셉터(로깅/에러/복구/인증)
│   │   ├── queue.pb.go
│   │   └── queue_grpc.pb.go
│   ├── core/
│   │   ├── filedb_queue.go      # 큐 어댑터 (Queue 구현)
│   │   └── filedb_manager.go    # SQLite 엔진 (Ack/Nack, DLQ, 리스, 정리 작업)
│   ├── metrics/                 # 프로메테우스 메트릭
│   └── runner/
│       ├── producer.go          # 디버그 모드용 프로듀서
│       └── consumer.go          # 디버그 모드용 컨슈머
│       
└── util/
    ├── randUtil.go              # 랜덤 메시지/수, 지터
    ├── uuid.go                  # 글로벌 ID 생성
    ├── partition.go             # 리스트 분할 유틸
    └── logger.go                # slog 기반 로거 초기화
```

## 빌드/실행/개발

- 실행: `go run cmd/main.go` (또는 `go run ./cmd`)
- 빌드: `go build -o bin/go-msg-queue-mini ./...`
- 테스트: `go test ./... -v` (커버리지: `-cover`)
- 포맷/린트: `go fmt ./...` / `go vet ./...` / `go mod tidy`
- gRPC 코드 생성(프로토 변경 시): `protoc --go_out=. --go-grpc_out=. proto/queue.proto`

## 설정 (`config.yml`)

```yaml
persistence:
  # memory: 메모리 내 SQLite (디스크 쓰기 없음)
  # file:   파일 기반 SQLite (권장: 로컬 개발/테스트)
  # 예시1) 기본값(memory):
  type: memory
  options:
    dirs-path: ./data/persistence

max-retry: 3           # 최대 재시도 횟수
retry-interval: 30s    # 재시도 간격(백오프 베이스)
lease-duration: 3s     # 메시지 리스 기간
debug: false           # true면 내장 프로듀서/컨슈머/모니터 실행

http:
  enabled: true        # HTTP API 활성화
  port: 8080           # 포트 (기본 8080)
  rate:                # 전역 IP 기반 Rate Limit (429 반환)
    limit: 1
    burst: 5
  auth:
    api_key: ${API_KEY} # writer 엔드포인트 보호용 API 키 (ENV 치환 지원)

grpc:
  enabled: true        # gRPC 서버 활성화
  port: 50051
  auth:                # gRPC 인터셉터 인증(API 키)
    api_key: ${API_KEY}
```

- `file` 모드 사용 시 디렉터리 생성: `mkdir -p ./data/persistence`
- 실제 DB 파일 경로: `./data/persistence/filedb_queue.db`
- `.env`를 로드하며(`github.com/joho/godotenv`), `config.yml`은 환경변수 치환을 지원합니다(`$VAR`/`${VAR}`).

## 모드

- `debug: true`
  - 내부 프로듀서/컨슈머와 상태 모니터가 함께 실행됩니다.
  - 콘솔에서 생산/소비 로그를 관찰할 수 있습니다.
  - 프로듀서: 1초마다 랜덤 메시지(1~5바이트)를 큐에 적재
  - 컨슈머: 3개의 고루틴이 병렬로 메시지를 소비
  - (주의) http/gRPC 서버는 기동되지 않습니다.

- `debug: false` + `http.enabled: true`
  - HTTP API 서버가 `:8080` (또는 설정 포트)에서 기동됩니다.
- `debug: false` + `grpc.enabled: true`
  - gRPC 서버가 `:50051` (또는 설정 포트)에서 기동됩니다.

## 로깅

- 전 구성요소가 Go `log/slog` 기반 구조화 로깅을 사용합니다(`util/logger.go`).
- `debug: true`일 때는 텍스트 핸들러, 그 외에는 JSON 핸들러로 stdout에 출력합니다.
- 큐 엔진과 HTTP/gRPC 레이어가 공통 필드(`error`, `queue`, `status` 등)를 포함해 메시지를 기록합니다.

## HTTP API

- 헬스체크: `GET /health`
  - 예) `curl -s localhost:8080/health`
- 메트릭: `GET /metrics` (Prometheus)

- Create Queue: `POST /api/v1/:queue_name/create` (헤더: `X-API-Key` 필요)
  - 응답: `201 Created`, `{ "status": "created" }`

- Delete Queue: `DELETE /api/v1/:queue_name/delete` (헤더: `X-API-Key` 필요)
  - 응답: `200 OK`, `{ "status": "deleted" }`

- Enqueue: `POST /api/v1/:queue_name/enqueue` (헤더: `X-API-Key: <키>` 필요)
  - 요청: `{ "message": <JSON format>, "delay": "<Go Duration>", "deduplication_id": "<string>" }` (`delay`/`deduplication_id`는 선택, 예: "10s", "1m")
  - 예) `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message": {"text":"hello"}}'`
  - 비고: `delay`는 Go duration 문자열을 사용하며 잘못된 값은 요청을 실패시키고, 음수는 0초로 보정됩니다.
  - 비고: `deduplication_id`를 지정하면 동일 큐에서 1시간 동안 같은 ID가 재삽입되지 않으며, 중복 요청은 단건에서는 오류로, 배치에서는 실패 항목으로 보고됩니다.
- Enqueue Batch: `POST /api/v1/:queue_name/enqueue/batch` (헤더: `X-API-Key: <키>` 필요)
  - 요청: `{ "mode": "partialSuccess"|"stopOnFailure", "messages": [{"message": <JSON format>, "delay": "<Go Duration>", "deduplication_id": "<string>"}, ...] }`
  - 응답: `202 Accepted`, `{ "status": "enqueued", "success_count": <int>, "failure_count": <int>, "failed_messages": [{ "index": <int>, "message": "<raw>", "error": "<reason>" }, ...] }`
  - 동작: `stopOnFailure`는 첫 오류에서 중단하고 5xx로 실패를 반환하며, `partialSuccess`는 성공한 항목만 적재하고 실패 항목을 `failed_messages`에 포함합니다.
  - 비고: 각 메시지에 `delay`와 `deduplication_id`를 독립적으로 지정할 수 있으며, `deduplication_id` 충돌 시 해당 메시지는 실패 항목으로 기록됩니다.

- Dequeue: `POST /api/v1/:queue_name/dequeue` (헤더: `X-API-Key: <키>` 필요)
  - 요청: `{ "group": "g1", "consumer_id": "c-1" }`
  - 성공: `200 OK`, `{ status, message: { id, receipt, payload } }`
  - 빈 큐: `204 No Content`
  - 경합: `409 Conflict`, `{ "status": "message is being processed" }`

- Ack: `POST /api/v1/:queue_name/ack` (헤더: `X-API-Key` 필요)
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "..." }`

- Nack: `POST /api/v1/:queue_name/nack` (헤더: `X-API-Key` 필요)
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "..." }`
  - 내부적으로 백오프 + 지터가 적용되며, `max-retry` 초과 시 DLQ로 이동

- Peek: `POST /api/v1/:queue_name/peek`
  - 요청: `{ "group": "g1", "options": { "limit": 3, "cursor": 25, "order": "asc", "preview": true } }`
  - 설명: `limit`은 기본 1(최대 100), `cursor`로 메시지 ID 기준 페이징, `order`는 `asc`/`desc`, `preview`는 페이로드를 50자 미리보기로 반환
  - 빈 큐: `204 No Content`

- Renew: `POST /api/v1/:queue_name/renew` (헤더: `X-API-Key` 필요)
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "...", "extend_sec": 5 }`
  - 현재 리스가 유효한 경우만 연장, 만료 시 409 반환

- Status: `GET /api/v1/:queue_name/status`
  - 응답: `{ queue_status: { queue_type, total_messages, acked_messages, inflight_messages, dlq_messages } }`

요청/응답 공통: 서버는 `X-Request-ID`를 반환하며, 미제공 시 자동 생성됩니다. 요청 폭주 시 429(Too Many Requests)를 반환합니다.

## gRPC API
- 활성화: `grpc.enabled: true`, 포트 기본 `50051`.
- 헬스체크: `queue.v1.QueueService/HealthCheck` → `{ status: "ok" }`.
- 주요 RPC: `CreateQueue`, `DeleteQueue`, `Enqueue`, `EnqueueBatch`, `Dequeue`, `Ack`, `Nack`, `Peek`, `Renew`, `Status` (proto: `proto/queue.proto`).
- Peek 요청은 선택적 `options`(`limit`, `cursor`, `order`, `preview`)로 최대 100개 메시지를 조회하거나 페이로드 미리보기를 제공합니다.
- 메시지 타입: gRPC `message` 필드는 문자열(string)이며, HTTP는 임의의 JSON을 지원합니다.
- `Enqueue`/`EnqueueBatch` 요청은 선택적 `delay`(Go duration 문자열)와 `deduplication_id`(문자열)로 메시지 가시 시점을 예약하거나 1시간 멱등성을 적용할 수 있으며, 생략 시 즉시 처리됩니다.
- `EnqueueBatch` 호출 시 `mode`를 `stopOnFailure` 또는 `partialSuccess`로 지정하고, 응답은 `failure_count`/`failed_messages`를 통해 부분 실패를 보고합니다. `stopOnFailure`에서 오류가 발생하면 전체 호출이 실패(5xx)로 끝납니다.
- 호출 예시(grpcurl): `grpcurl -plaintext localhost:50051 list` / `grpcurl -plaintext -d '{"queue_name":"default","message":"hello","delay":"45s"}' localhost:50051 queue.v1.QueueService/Enqueue` / `grpcurl -plaintext -d '{"queue_name":"default","messages":["hi","there"],"delay":"1m"}' localhost:50051 queue.v1.QueueService/EnqueueBatch`.

## gRPC 인터셉터
- 로깅: `LoggerInterceptor`가 호출 메서드/소요시간을 기록합니다.
- 에러 로그: `ErrorInterceptor`가 핸들러 에러를 로깅합니다.
- 복구: `RecoveryInterceptor`가 panic을 내부 오류(codes.Internal)로 변환합니다.
- 인증: `AuthInterceptor(apiKey, protectedMethods)`가 보호 메서드에 대해 메타데이터 `x-api-key`를 검증합니다.
  - 보호됨: `/CreateQueue`, `/DeleteQueue`, `/Enqueue`, `/EnqueueBatch`, `/Dequeue`, `/Ack`, `/Nack`, `/Renew`
  - 공개됨: `/Peek`, `/Status`, `/HealthCheck`
  - gRPC 메타데이터 키는 소문자 `x-api-key`입니다. HTTP와 다르게 헤더 키 대소문자가 중요합니다.
- 체인 순서: Recovery → Logger → Error → Auth (서버 초기화 시 `grpc.ChainUnaryInterceptor`로 묶음).

예시(grpcurl)
- 공개 RPC 호출: `grpcurl -plaintext localhost:50051 queue.v1.QueueService/HealthCheck`
- 보호 RPC 호출: `grpcurl -plaintext -H 'x-api-key: $API_KEY' -d '{"queue_name":"default","messages":["hello","world"]}' localhost:50051 queue.v1.QueueService/EnqueueBatch`

## 테스트

- 표준 `testing` 프레임워크 사용: `go test ./... -v`
- 설정 로더 테스트: `internal/config_test.go`
- 필요 시 `-cover`로 커버리지 확인

## 변경 사항(요약)
- 메시지 중복 방지 추가: HTTP/gRPC Enqueue 경로에 `deduplication_id`를 도입해 큐별 1시간 멱등성 윈도우를 제공합니다.
- 메시지 지연 전송 지원: HTTP/gRPC Enqueue에 `delay`(Go duration) 옵션을 추가하고 파일 DB `visible_at`을 사용해 예약 처리합니다.
- 배치 Enqueue API 추가: HTTP `/enqueue/batch`, gRPC `EnqueueBatch`, 파일 DB 큐의 일괄 쓰기 (`stopOnFailure`/`partialSuccess`) 지원.
- `log/slog` 기반 구조화 로깅으로 전환(디버그 텍스트, 서버 모드 JSON) 및 공통 필드 정비.
- gRPC Queue Service와 인터셉터(로깅/에러/복구/API 키)를 추가하고 `proto/queue.proto`를 확장.
- HTTP 서버를 server/handler/dto로 리팩터링하고 로깅·Request-ID·Rate Limit·API 키 미들웨어를 도입.
- 설정을 `.env` 로드 및 `config.yml` 환경변수 치환으로 개선하고 HTTP/GRPC/Rate Limit 옵션을 확장.
- JSON 필드 정비, 에러 처리 개선, graceful shutdown, Prometheus 메트릭 보강으로 안정성을 높였습니다.

## 메트릭 지표(Prometheus)
- 카운터: `mq_enqueue_total`, `mq_dequeue_total`, `mq_ack_total`, `mq_nack_total` (라벨: `queue`)
- 게이지: `mq_total_messages`, `mq_in_flight_messages`, `mq_dlq_messages` (라벨: `queue`)
- 엔드포인트: `GET /metrics`

## 보안/운영 팁

- 비밀정보는 커밋하지 마세요. `config.yml`은 로컬 개발용입니다.
- `file` 모드 사용 시 `persistence.options.dirs-path`가 존재하고 쓰기 가능해야 합니다.
- `memory` 모드는 디스크에 기록하지 않으므로 프로세스 재시작 시 데이터가 사라집니다.

---

---
