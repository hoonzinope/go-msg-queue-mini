# Go 메시지 큐 미니

[English](./README.en.md)

간단하지만 견고한 Go 기반 메시지 큐입니다. Producer/Consumer 실행과 HTTP/gRPC API를 지원하며, SQLite 기반 영속성과 Ack/Nack, DLQ, 리스(lease) 연장 등 기본기를 갖춘 소형 큐 엔진입니다.

## 주요 특징

- **영속성 선택**: `memory`(in-memory SQLite) 또는 `file`(파일 기반 SQLite)
- **동시성 처리**: 다수 Producer/Consumer를 고루틴으로 병렬 실행
- **처리 보장**: Ack/Nack, Inflight, DLQ, 재시도(backoff + jitter)
- **리스/갱신**: 메시지 점유 기간(lease)과 `/renew`를 통한 연장
- **미리보기/피킹**: 할당 없이 확인하는 `/peek`
- **상태 확인**: 합계/ACK/Inflight/DLQ를 반환하는 `/status`
- **운영 모드**: `debug` 모드(내장 프로듀서/컨슈머 + 모니터) 또는 HTTP/gRPC 서버

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
│   │   ├── handler.go           # API 핸들러 (enqueue/dequeue/ack/nack/peek/renew/status/health)
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
│       ├── consumer.go          # 디버그 모드용 컨슈머
│       └── stat.go              # 주기 상태 모니터
└── util/
    ├── randUtil.go              # 랜덤 메시지/수, 지터
    ├── uuid.go                  # 글로벌 ID 생성
    └── logger.go                # 간단 로거
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
  - 콘솔에서 생산/소비/상태 로그를 관찰할 수 있습니다.

- `debug: false` + `http.enabled: true`
  - HTTP API 서버가 `:8080` (또는 설정 포트)에서 기동됩니다.

## HTTP API

- 헬스체크: `GET /health`
  - 예) `curl -s localhost:8080/health`
- 메트릭: `GET /metrics` (Prometheus)

- Enqueue: `POST /api/v1/:queue_name/enqueue` (헤더: `X-API-Key: <키>` 필요)
  - 요청: `{ "message": <任意 JSON> }`
  - 예) `curl -X POST localhost:8080/api/v1/default/enqueue -H 'Content-Type: application/json' -H 'X-API-Key: $API_KEY' -d '{"message": {"text":"hello"}}'`

- Dequeue: `POST /api/v1/:queue_name/dequeue` (헤더: `X-API-Key: <키>` 필요)
  - 요청: `{ "group": "g1", "consumer_id": "c-1" }`
  - 응답: `{ status, message: { id, receipt, payload } }`

- Ack: `POST /api/v1/:queue_name/ack` (헤더: `X-API-Key` 필요)
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "..." }`

- Nack: `POST /api/v1/:queue_name/nack` (헤더: `X-API-Key` 필요)
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "..." }`
  - 내부적으로 백오프 + 지터가 적용되며, `max-retry` 초과 시 DLQ로 이동

- Peek: `POST /api/v1/:queue_name/peek`
  - 요청: `{ "group": "g1" }` (할당/리스 없이 가장 앞 메시지 확인)

- Renew: `POST /api/v1/:queue_name/renew` (헤더: `X-API-Key` 필요)
  - 요청: `{ "group": "g1", "message_id": 1, "receipt": "...", "extend_sec": 5 }`
  - 현재 리스가 유효한 경우만 연장, 만료 시 409 반환

- Status: `GET /api/v1/:queue_name/status`
  - 응답: `{ queue_status: { queue_type, total_messages, acked_messages, inflight_messages, dlq_messages } }`

요청/응답 공통: 서버는 `X-Request-ID`를 반환하며, 미제공 시 자동 생성됩니다. 요청 폭주 시 429(Too Many Requests)를 반환합니다.

## gRPC API
- 활성화: `grpc.enabled: true`, 포트 기본 `50051`.
- 헬스체크: `queue.v1.QueueService/HealthCheck` → `{ status: "ok" }`.
- 주요 RPC: `Enqueue`, `Dequeue`, `Ack`, `Nack`, `Peek`, `Renew`, `Status` (proto: `proto/queue.proto`).
- 호출 예시(grpcurl): `grpcurl -plaintext localhost:50051 list` / `grpcurl -plaintext -d '{"queue_name":"default","message":"hi"}' localhost:50051 queue.v1.QueueService/Enqueue`.

## gRPC 인터셉터
- 로깅: `LoggerInterceptor`가 호출 메서드/소요시간을 기록합니다.
- 에러 로그: `ErrorInterceptor`가 핸들러 에러를 로깅합니다.
- 복구: `RecoveryInterceptor`가 panic을 내부 오류(codes.Internal)로 변환합니다.
- 인증: `AuthInterceptor(apiKey, protectedMethods)`가 보호 메서드에 대해 메타데이터 `x-api-key`를 검증합니다.
  - 보호됨: `/queue.v1.QueueService/Enqueue`, `/Dequeue`, `/Ack`, `/Nack`, `/Renew`
  - 공개됨: `/Peek`, `/Status`, `/HealthCheck`
  - gRPC 메타데이터 키는 소문자 `x-api-key`입니다. HTTP와 다르게 헤더 키 대소문자가 중요합니다.
- 체인 순서: Recovery → Logger → Error → Auth (서버 초기화 시 `grpc.ChainUnaryInterceptor`로 묶음).

예시(grpcurl)
- 공개 RPC 호출: `grpcurl -plaintext localhost:50051 queue.v1.QueueService/HealthCheck`
- 보호 RPC 호출: `grpcurl -plaintext -H 'x-api-key: $API_KEY' -d '{"queue_name":"default","message":"hello"}' localhost:50051 queue.v1.QueueService/Enqueue`

## 테스트

- 표준 `testing` 프레임워크 사용: `go test ./... -v`
- 설정 로더 테스트: `internal/config_test.go`
- 필요 시 `-cover`로 커버리지 확인

## 변경 사항(요약)
- gRPC Queue Service 추가: `internal/api/grpc/*`, `proto/queue.proto`, `grpc.enabled/port` 설정 지원.
- HTTP 미들웨어 도입: 로깅/에러/Request-ID/인증(API 키)/Rate Limit(전역 IP 기준, 1분 단위 정리).
- 설정 개선: `.env` 로드 및 `config.yml` 환경변수 치환 지원, 레이트 리밋/인증/GRPC 섹션 추가.
- JSON 필드 표준화 및 에러 처리 개선, 서버 종료 시 graceful shutdown 보장.
- HTTP 구조 리팩터링: server/handler/dto 파일 분리.
- 문서: 기여자 가이드 `AGENTS.md` 추가.
 - gRPC 인터셉터 추가: 로깅/에러/복구/메타데이터 기반 API 키 인증(`x-api-key`), 보호 메서드 분리. `config.yml`에 `grpc.auth.api_key` 추가.

## 보안/운영 팁

- 비밀정보는 커밋하지 마세요. `config.yml`은 로컬 개발용입니다.
- `file` 모드 사용 시 `persistence.options.dirs-path`가 존재하고 쓰기 가능해야 합니다.
- `memory` 모드는 디스크에 기록하지 않으므로 프로세스 재시작 시 데이터가 사라집니다.

---
