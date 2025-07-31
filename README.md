# Go 메시지 큐 미니

[English](./README.en.md)

이것은 Go로 구현된 간단한 인메모리 메시지 큐입니다. 고루틴과 채널을 사용하는 생산자-소비자 시스템의 기본 개념을 보여줍니다.

## 프로젝트 구조

```
/Users/hoonzi/go-proj/go-msg-qu-mini/
├───go.mod
├───main.go
├───.git/...
├───internal/
│   ├───consumer.go
│   ├───producer.go
│   └───queue.go
└───util/
    └───randomStr.go
```

- **main.go**: 애플리케이션의 진입점입니다. 큐를 초기화하고 생산자 및 소비자 고루틴을 시작합니다.
- **internal/queue.go**: `container/list`를 사용하여 간단한 스레드 안전 큐를 구현합니다.
- **internal/producer.go**: 생산자는 임의의 메시지를 생성하여 큐에 넣습니다.
- **internal/consumer.go**: 소비자는 큐에서 메시지를 검색하여 콘솔에 인쇄합니다.
- **util/randomStr.go**: 메시지에 대한 임의의 문자열을 생성하기 위한 유틸리티입니다.

## 실행 방법

1.  **저장소 복제:**
    ```bash
    git clone https://github.com/your-username/go-msg-queue-mini.git
    cd go-msg-queue-mini
    ```
2.  **애플리케이션 실행:**
    ```bash
    go run main.go
    ```

애플리케이션이 시작되고 콘솔에서 메시지가 생성되고 소비되는 것을 볼 수 있습니다. 애플리케이션을 중지하려면 `Ctrl+C`를 누르십시오.

## 주요 개념

- **고루틴**: 생산자와 소비자는 고루틴으로 동시에 실행됩니다.
- **채널**: 이 예제에서는 간단한 큐를 사용하지만 채널은 Go에서 고루틴 간의 통신을 처리하는 더 관용적인 방법입니다.
- **생산자-소비자 패턴**: 하나 이상의 생산자가 데이터를 생성하고 하나 이상의 소비자가 데이터를 처리하는 고전적인 동시성 패턴입니다.
