# tq: A Robust Task Queue with Priorities and Retrials

The `tq` library is a powerful and production-ready tool for managing tasks in a concurrent environment. It provides advanced features like priority queues, configurable retry strategies, task cancellation, rate limiting, and observability hooks—all while ensuring thread safety and robustness.

## Features

- **Thread-Safe Task Management:** Safely add and cancel tasks from multiple goroutines
- **Priority Queue:** Execute high-priority tasks before low-priority ones
- **Configurable Retry Logic:** Opt-in retry with exponential, linear, or constant backoff strategies
- **Worker Pool:** Process tasks concurrently with a configurable pool of worker goroutines
- **Rate Limiting:** Control task execution rate to prevent system overload
- **Task Cancellation:** Cancel queued or running tasks by ID
- **Observability Hooks:** Monitor task lifecycle events for metrics and logging
- **Graceful Shutdown:** Ensure all tasks are processed before shutting down
- **Duplicate Detection:** Optionally prevent duplicate task IDs
- **Flexible Queue Strategies:** Choose how to handle full queues (block, error, or drop oldest)
- **Context Support:** Proper context propagation for timeouts and cancellations

## Installation

```sh
go get github.com/adonese/tq
```

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "log"
    "os"
    "github.com/adonese/tq"
)

func main() {
    logger := log.New(os.Stdout, "taskqueue: ", log.LstdFlags)

    // Create a simple task queue
    taskQueue, err := tq.NewTaskQueueSimple(10, 5, logger, 100)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Add a task
    err = taskQueue.AddTask(ctx, tq.Task{
        ID: "task-1",
        Job: func(ctx context.Context) error {
            log.Println("Processing task")
            return nil
        },
        Priority: tq.MediumPriority,
    })
    if err != nil {
        log.Printf("Failed to add task: %v", err)
    }

    // Graceful shutdown
    defer taskQueue.Shutdown(context.Background())
}
```

## Advanced Configuration

### Using TaskQueueConfig

For advanced use cases, use `NewTaskQueue` with a configuration struct:

```go
config := tq.TaskQueueConfig{
    BufferSize:        100,
    WorkerCount:       10,
    MaxRatePerSecond:  50.0,
    AllowDuplicates:   false,  // Prevent duplicate task IDs
    FullQueueStrategy: tq.ReturnError,  // Return error when queue is full
    Logger:            logger,
    TaskTimeout:       5 * time.Minute,
    Hooks:             &hooks,  // Optional observability hooks
}

taskQueue, err := tq.NewTaskQueue(config)
```

### Configuration Options

- **BufferSize**: Maximum number of tasks that can be queued
- **WorkerCount**: Number of concurrent workers processing tasks
- **MaxRatePerSecond**: Maximum task execution rate (rate limiting)
- **AllowDuplicates**: Whether to allow duplicate task IDs (default: true)
- **FullQueueStrategy**: Behavior when queue is full:
  - `BlockUntilSpace` (default): Block until space is available
  - `ReturnError`: Return `ErrQueueFull` immediately
  - `DropOldest`: Drop the oldest task and add the new one
- **Logger**: Logger for task queue events
- **TaskTimeout**: Maximum execution time per task
- **Hooks**: Callbacks for observability (see below)

## Task Configuration

### Basic Task

```go
task := tq.Task{
    ID:       "unique-task-id",
    Job:      myJobFunction,
    Priority: tq.HighPriority,  // LowPriority, MediumPriority, or HighPriority
}
```

### Task with Retry

Retries are **opt-in**. Only tasks with a `Retry` config will be retried:

```go
task := tq.Task{
    ID: "retry-task",
    Job: func(ctx context.Context) error {
        // Your task logic
        return nil
    },
    Priority: tq.HighPriority,
    Retry: &tq.RetryConfig{
        MaxRetries:      3,
        BackoffStrategy: tq.ExponentialBackoff,  // or LinearBackoff, ConstantBackoff
        BaseDelay:       time.Second,
    },
}
```

### Backoff Strategies

- **ExponentialBackoff**: Delay doubles with each retry (1s, 2s, 4s, 8s...)
- **LinearBackoff**: Delay increases linearly (1s, 2s, 3s, 4s...)
- **ConstantBackoff**: Fixed delay between retries (1s, 1s, 1s, 1s...)

## Task Cancellation

```go
// Cancel a task by ID
err := taskQueue.CancelTask("task-id")
if err == tq.ErrTaskNotFound {
    log.Println("Task not found (already completed or never existed)")
}
```

## Observability and Metrics

Use hooks to implement custom metrics and monitoring:

```go
hooks := &tq.TaskQueueHooks{
    OnTaskStarted: func(taskID string) {
        metrics.IncrementCounter("tasks_started")
    },
    OnTaskCompleted: func(taskID string, duration time.Duration) {
        metrics.RecordDuration("task_duration", duration)
        metrics.IncrementCounter("tasks_completed")
    },
    OnTaskFailed: func(taskID string, err error, retries int) {
        metrics.IncrementCounter("tasks_failed")
        log.Printf("Task %s failed after %d retries: %v", taskID, retries, err)
    },
    OnTaskRetry: func(taskID string, attempt int, err error) {
        metrics.IncrementCounter("task_retries")
    },
    OnTaskCancelled: func(taskID string) {
        metrics.IncrementCounter("tasks_cancelled")
    },
}
```

## Working with Results

### Reading Results

```go
// Get the results channel
results := taskQueue.Results()

// Read results as they come in
go func() {
    for result := range results {
        if result.Error != nil {
            log.Printf("Task %s failed: %v", result.TaskID, result.Error)
        } else {
            log.Printf("Task %s completed in %v", result.TaskID, result.EndTime.Sub(result.StartTime))
        }
    }
}()
```

### Draining Results

```go
// Get all available results without blocking
results := taskQueue.DrainResults()
for _, result := range results {
    log.Printf("Task %s: %v", result.TaskID, result.Error)
}
```

## Queue Inspection

```go
// Get current queue length
length := taskQueue.GetQueueLength()

// Check if a task has been processed
if taskQueue.IsProcessed("task-id") {
    log.Println("Task has been completed successfully")
}
```

## Web Server Example

Using with the Gin web framework:

```go
package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "time"
    "github.com/gin-gonic/gin"
    "github.com/adonese/tq"
)

var taskQueue *tq.TaskQueue

func init() {
    logger := log.New(os.Stdout, "taskqueue: ", log.LstdFlags)
    var err error
    taskQueue, err = tq.NewTaskQueueSimple(100, 10, logger, 50.0)
    if err != nil {
        log.Fatal(err)
    }
}

func addTaskHandler(c *gin.Context) {
    taskID := c.Query("id")
    priority := c.DefaultQuery("priority", "medium")

    var taskPriority tq.Priority
    switch priority {
    case "high":
        taskPriority = tq.HighPriority
    case "low":
        taskPriority = tq.LowPriority
    default:
        taskPriority = tq.MediumPriority
    }

    task := tq.Task{
        ID:       taskID,
        Priority: taskPriority,
        Job: func(ctx context.Context) error {
            log.Printf("Processing task %s", taskID)
            time.Sleep(100 * time.Millisecond)
            return nil
        },
        Retry: &tq.RetryConfig{
            MaxRetries:      3,
            BackoffStrategy: tq.ExponentialBackoff,
        },
    }

    err := taskQueue.AddTask(c.Request.Context(), task)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, gin.H{"status": "task added", "id": taskID})
}

func cancelTaskHandler(c *gin.Context) {
    taskID := c.Query("id")
    err := taskQueue.CancelTask(taskID)
    if err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
        return
    }
    c.JSON(http.StatusOK, gin.H{"status": "task cancelled", "id": taskID})
}

func queueStatusHandler(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{
        "queue_length": taskQueue.GetQueueLength(),
    })
}

func main() {
    r := gin.Default()
    r.GET("/add-task", addTaskHandler)
    r.DELETE("/cancel-task", cancelTaskHandler)
    r.GET("/queue-status", queueStatusHandler)

    // Start result processor
    go func() {
        for result := range taskQueue.Results() {
            if result.Error != nil {
                log.Printf("Task %s failed: %v", result.TaskID, result.Error)
            }
        }
    }()

    r.Run(":8080")
    defer taskQueue.Shutdown(context.Background())
}
```

## Error Handling

The library defines several error types:

```go
var (
    ErrQueueShuttingDown = errors.New("task queue is shutting down")
    ErrQueueFull         = errors.New("task queue is full")
    ErrInvalidConfig     = errors.New("invalid task queue configuration")
    ErrTaskNotFound      = errors.New("task not found")
    ErrDuplicateTask     = errors.New("duplicate task ID")
)
```

Use `errors.Is()` to check for specific errors:

```go
err := taskQueue.AddTask(ctx, task)
if errors.Is(err, tq.ErrQueueFull) {
    log.Println("Queue is full, try again later")
}
```

## Testing

Run the full test suite:

```sh
go test -v
```

Run tests with race detector:

```sh
go test -race -v
```

Run stress tests:

```sh
go test -v -run TestTaskQueueStressTest
```

Run fuzzing tests:

```sh
go test -fuzz=FuzzTaskQueue -fuzztime=30s
go test -fuzz=FuzzTaskQueueConcurrent -fuzztime=30s
```

## Migration from v1.x

If you're using the old API, minimal changes are needed:

**Old:**
```go
tq := tq.NewTaskQueue(10, 5, logger, 100)
task := Task{ID: "1", Job: job, MaxRetries: 3}
```

**New (backward compatible):**
```go
tq, err := tq.NewTaskQueueSimple(10, 5, logger, 100)
if err != nil {
    log.Fatal(err)
}
task := tq.Task{
    ID: "1",
    Job: job,
    Retry: &tq.RetryConfig{
        MaxRetries: 3,
        BackoffStrategy: tq.ExponentialBackoff,
    },
}
```

**Note:** Tasks without `Retry` config will execute once without retries.

## Best Practices

1. **Always check errors** when creating a task queue
2. **Use contexts** for proper timeout and cancellation handling
3. **Set appropriate buffer sizes** based on your workload
4. **Use priority levels** judiciously (most tasks should be MediumPriority)
5. **Implement observability hooks** for production monitoring
6. **Gracefully shutdown** your queue to avoid losing tasks
7. **Configure retry strategies** based on your use case (network calls = exponential, rate limits = linear)
8. **Use duplicate detection** when task idempotency is critical

## Performance Considerations

- Buffer size should be large enough to handle bursts but not so large it consumes excessive memory
- Worker count should match your workload concurrency needs
- Rate limiting prevents overwhelming downstream services
- Task timeout prevents hung tasks from blocking workers

## Contributing

Contributions are welcome! Please:

1. Add tests for new features
2. Run tests with race detector
3. Update documentation
4. Follow Go best practices

## License

[Specify your license here]

## Changelog

### v2.0.0 (Latest)

- **Breaking Changes:**
  - `NewTaskQueue` now returns error and takes config struct
  - `Task.MaxRetries` removed in favor of optional `Task.Retry` config
  - `TaskResult` now includes `StartTime`, `EndTime`, and `Retries` fields

- **New Features:**
  - Task cancellation API
  - Configurable retry strategies (exponential, linear, constant backoff)
  - Observability hooks for metrics
  - Priority queue with index management
  - Duplicate task detection
  - Graceful degradation strategies (return error, drop oldest)
  - Input validation
  - Queue inspection methods
  - Result draining utility

- **Bug Fixes:**
  - Fixed WaitGroup race conditions
  - Fixed context cancellation handling
  - Fixed rate limiter efficiency
  - Fixed worker termination on shutdown
  - Fixed results channel cleanup

- **Performance:**
  - Improved priority queue with O(log n) operations
  - Rate limiting before task execution
  - Non-blocking result sends
