# Go Message Queue Mini

[한국어](./README.md)

This is a simple message queue system written in Go. It is based on the Producer-Consumer pattern and uses **SQLite** to ensure message persistence.

## Key Features

-   **SQLite-based Persistence**: Messages are stored in a local SQLite database, so data is not lost even if the application restarts.
-   **Concurrent Processing**: Multiple producers and consumers run concurrently as Goroutines to process messages efficiently.
-   **Status Monitoring**: Periodically monitors and logs the status of the queue (e.g., total, in-flight, acknowledged, DLQ message counts).
-   **Graceful Shutdown**: Ensures that all Goroutines shut down safely using `context` and `sync.WaitGroup`.
-   **Guaranteed Message Processing**: Guarantees message processing through an `inflight` queue, an `Ack/Nack` mechanism, and a Dead-Letter Queue (DLQ).

## Project Structure

```
/Users/hoonzi/go-proj/go-msg-qu-mini/
├───main.go                # Application entry point
├───config.yml             # Queue configuration file
├───go.mod                 # Go modules and dependency management
├───internal/              # Internal logic packages
│   ├───queue.go           # Queue interface definition
│   ├───producer.go        # Message producer logic
│   ├───consumer.go        # Message consumer logic
│   ├───config.go          # Configuration file parsing logic
│   ├───stat.go            # Queue status monitoring logic
│   └───queueType/         # Queue type implementation
│       └───fileDB/          # SQLite-based queue implementation
│           ├───filedb_queue.go    # fileDB queue logic
│           └───filedb_manager.go  # SQLite DB management and queries
└───util/                  # Utility functions
    └───randomStr.go       # Random string generator
```

## How to Run

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/go-msg-queue-mini.git
    cd go-msg-queue-mini
    ```

2.  **Install dependencies:**
    ```bash
    go mod tidy
    ```

3.  **Configuration (Optional):**
    Open the `config.yml` file to modify necessary settings, such as the database file path (`persistence.options.dirs-path`).

4.  **Run the application:**
    ```bash
    go run main.go
    ```

Once the application starts, you can see message production/consumption and queue status logs in the console. Press `Ctrl+C` to shut down the application gracefully.

## Key Concepts

-   **Producer-Consumer Pattern**: A classic concurrency pattern where one or more producers generate data and one or more consumers process it.
-   **Goroutines**: Producers and consumers run concurrently as Goroutines to achieve high throughput.
-   **SQLite Persistence**: Messages are stored in the `queue` table of a SQLite database. When a consumer retrieves a message, it is moved to the `inflight` table to track its processing status. Upon completion, it is moved to the `acked` table, or to the `dlq` table if it fails.