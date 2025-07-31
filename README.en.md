# Go Message Queue Mini

[한국어](./README.md)


This is a simple in-memory message queue implemented in Go. It demonstrates the basic concepts of a producer-consumer system using goroutines and channels.

## Project Structure

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

- **main.go**: The entry point of the application. It initializes the queue and starts the producer and consumer goroutines.
- **internal/queue.go**: Implements a simple thread-safe queue using `container/list`.
- **internal/producer.go**: The producer generates random messages and puts them into the queue.
- **internal/consumer.go**: The consumer retrieves messages from the queue and prints them to the console.
- **util/randomStr.go**: A utility for generating random strings for the messages.

## How to Run

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/go-msg-queue-mini.git
    cd go-msg-queue-mini
    ```
2.  **Run the application:**
    ```bash
    go run main.go
    ```

The application will start, and you will see messages being produced and consumed in the console. Press `Ctrl+C` to stop the application.

## Key Concepts

- **Goroutines**: The producer and consumer run concurrently as goroutines.
- **Channels**: While this example uses a simple queue, channels are a more idiomatic way to handle communication between goroutines in Go.
- **Producer-Consumer Pattern**: A classic concurrency pattern where one or more producers generate data and one or more consumers process it.
