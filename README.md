# Go 메시지 큐 미니

[English](./README.en.md)

Go 언어로 작성된 간단한 메시지 큐 시스템입니다. 생산자-소비자(Producer-Consumer) 패턴을 기반으로 하며, **SQLite**를 사용하여 메시지의 영속성을 보장합니다.

## 주요 특징

-   **SQLite 기반 영속성**: 메시지를 로컬 SQLite 데이터베이스에 저장하여 애플리케이션이 재시작되어도 데이터가 유실되지 않습니다.
-   **동시성 처리**: 여러 생산자와 소비자가 고루틴(Goroutine)을 통해 동시에 실행되어 메시지를 효율적으로 처리합니다.
-   **상태 모니터링**: 주기적으로 큐의 상태(전체, 처리 중, 확인된, DLQ 메시지 수 등)를 모니터링하고 로그를 출력합니다.
-   **안전한 종료**: `context`와 `sync.WaitGroup`을 사용하여 모든 고루틴이 안전하게 종료되도록 보장합니다.
-   **메시지 처리 보장**: `inflight` 큐, `Ack/Nack` 메커니즘, Dead-Letter Queue(DLQ)를 통해 메시지 처리를 보장합니다.

## 프로젝트 구조

```
/Users/hoonzi/go-proj/go-msg-qu-mini/
├───main.go                # 애플리케이션 진입점
├───config.yml             # 큐 설정 파일
├───go.mod                 # Go 모듈 및 의존성 관리
├───internal/              # 내부 로직 패키지
│   ├───queue.go           # Queue 인터페이스 정의
│   ├───producer.go        # 메시지 생산자 로직
│   ├───consumer.go        # 메시지 소비자 로직
│   ├───config.go          # 설정 파일 파싱 로직
│   ├───stat.go            # 큐 상태 모니터링 로직
│   └───queueType/         # 큐 타입 구현
│       └───fileDB/          # SQLite 기반 큐 구현
│           ├───filedb_queue.go    # fileDB 큐 로직
│           └───filedb_manager.go  # SQLite DB 관리 및 쿼리
└───util/                  # 유틸리티 함수
    └───randomStr.go       # 랜덤 문자열 생성
```

## 실행 방법

1.  **저장소 복제:**
    ```bash
    git clone https://github.com/your-username/go-msg-queue-mini.git
    cd go-msg-queue-mini
    ```

2.  **의존성 설치:**
    ```bash
    go mod tidy
    ```

3.  **설정 (선택 사항):**
    `config.yml` 파일을 열어 데이터베이스 파일 경로(`persistence.options.dirs-path`) 등 필요한 설정을 수정합니다.

4.  **애플리케이션 실행:**
    ```bash
    go run main.go
    ```

애플리케이션이 시작되면 콘솔에서 메시지 생산/소비 및 큐 상태 로그를 확인할 수 있습니다. `Ctrl+C`를 눌러 애플리케이션을 안전하게 종료할 수 있습니다.

## 주요 개념

-   **생산자-소비자 패턴**: 하나 이상의 생산자가 데이터를 생성하고 하나 이상의 소비자가 데이터를 처리하는 고전적인 동시성 패턴입니다.
-   **고루틴**: 생산자와 소비자는 고루틴으로 동시에 실행되어 높은 처리량을 달성합니다.
-   **SQLite 영속성**: 메시지는 SQLite 데이터베이스의 `queue` 테이블에 저장됩니다. 소비자가 메시지를 가져가면 `inflight` 테이블로 이동하여 처리 상태를 추적합니다. 처리가 완료되면 `acked` 테이블로, 실패하면 `dlq` 테이블로 이동합니다.