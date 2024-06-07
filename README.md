# tq: A simple task queue with retrials


The `tq` library is a powerful and simple-to-use tool for managing tasks in a concurrent environment. It allows you to add tasks to a queue and process them with a pool of workers, ensuring thread safety and robustness. This library is particularly useful for web applications where multiple HTTP handlers may need to add tasks to a shared queue.

## Features

- **Thread-Safe Task Addition:** Safely add tasks to the queue from multiple goroutines.
- **Worker Pool:** Process tasks concurrently with a pool of worker goroutines.
- **Retry Mechanism:** Automatically retry tasks a specified number of times if they fail.
- **Graceful Shutdown:** Ensure all tasks are processed before shutting down the queue.

## Installation

To install the library, use `go get`:

```sh
go get github.com/adonese/tq
```

## Usage

### Creating a Task Queue

First, initialize the task queue with a specified buffer size and number of workers:

```go
package main

import (
    "log"
    "os"
    "github.com/adonese/tq"
)

var taskQueue *taskqueue.TaskQueue

func init() {
    logger := log.New(os.Stdout, "taskqueue: ", log.LstdFlags)
    taskQueue = taskqueue.NewTaskQueue(10, 5, logger)
}
```

### Adding Tasks

You can add tasks to the queue from anywhere in your application. Each task consists of an ID, a job function, and the maximum number of retries:

```go
func addTask() {
    task := taskqueue.Task{
        ID: "task-1",
        Job: func(ctx context.Context) error {
            // Your task logic here
            log.Println("Processing task")
            return nil
        },
        MaxRetries: 3,
    }
    taskQueue.AddTask(task)
}
```

### Using with a Web Server

Here is an example of using `TaskQueue` with the `Gin` web framework:

```go
package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "github.com/gin-gonic/gin"
    "github.com/adonese/tq"
)

var taskQueue *taskqueue.TaskQueue

func init() {
    logger := log.New(os.Stdout, "taskqueue: ", log.LstdFlags)
    taskQueue = taskqueue.NewTaskQueue(10, 5, logger)
}

func addSimpleTaskHandler(c *gin.Context) {
    taskID := c.Query("id")
    task := taskqueue.Task{
        ID: taskID,
        Job: func(ctx context.Context) error {
            log.Printf("Processing task %s", taskID)
            return nil
        },
        MaxRetries: 3,
    }
    taskQueue.AddTask(task)
    c.JSON(http.StatusOK, gin.H{"status": "task added"})
}

func main() {
    r := gin.Default()
    r.GET("/add-task", addSimpleTaskHandler)
    r.Run(":8080")
    defer taskQueue.Shutdown()
}
```

### Advanced Usage: Adding HTTP Request Tasks

You can also add tasks that perform HTTP requests or other complex operations:

```go
func addHttpRequestTaskHandler(c *gin.Context) {
    taskID := c.Query("id")
    task := taskqueue.Task{
        ID: taskID,
        Job: func(ctx context.Context) error {
            cmd := exec.CommandContext(ctx, "curl", "-s", "https://example.com")
            output, err := cmd.Output()
            if err != nil {
                log.Printf("Task %s failed: %v", taskID, err)
                return err
            }
            log.Printf("Task %s succeeded: %s", taskID, output)
            return nil
        },
        MaxRetries: 3,
    }
    taskQueue.AddTask(task)
    c.JSON(http.StatusOK, gin.H{"status": "HTTP request task added"})
}
```

### Graceful Shutdown

Ensure the task queue is shut down gracefully when your application exits, allowing all tasks to be processed:

```go
func main() {
    r := gin.Default()
    r.GET("/add-task", addSimpleTaskHandler)
    r.GET("/add-http-task", addHttpRequestTaskHandler)
    r.Run(":8080")

    defer taskQueue.Shutdown()
}
```

## Unit Tests

To ensure the robustness of the `tq`, we have a comprehensive set of unit tests:

- **TestAddTask:** Verifies that a single task can be added and processed.
- **TestConcurrentAddTasks:** Simulates adding multiple tasks concurrently.
- **TestAddHttpRequestTask:** Verifies tasks that perform HTTP requests.
- **TestHighConcurrencyAddTasks:** Simulates 1000 concurrent HTTP handlers adding tasks.

Run the tests using the following command:

```sh
go test -v
```

Example of a high concurrency test:

```go
func TestHighConcurrencyAddTasks(t *testing.T) {
    logger := log.New(os.Stdout, "test: ", log.LstdFlags)
    tq := tq.NewTaskQueue(1000, 50, logger)

    var processedTasks int
    var mu sync.Mutex

    job := func(ctx context.Context) error {
        mu.Lock()
        defer mu.Unlock()
        processedTasks++
        return nil
    }

    var wg sync.WaitGroup
    numHandlers := 1000
    wg.Add(numHandlers)

    for i := 0; i < numHandlers; i++ {
        go func(handlerID int) {
            defer wg.Done()
            taskID := fmt.Sprintf("task-%d", handlerID)
            task := tq.Task{
                ID: taskID,
                Job: job,
                MaxRetries: 3,
            }
            tq.AddTask(task)
        }(i)
    }

    wg.Wait()
    tq.Shutdown()

    mu.Lock()
    defer mu.Unlock()
    if processedTasks != numHandlers {
        t.Errorf("expected %d processed tasks, got %d", numHandlers, processedTasks)
    }
}
```

Note: The sync.WaitGroup used in the test is necessary to ensure that all goroutines have finished adding tasks to the queue before we proceed to shut it down. This ensures that we correctly simulate the concurrent behavior of adding tasks to the TaskQueue and allows us to accurately test the handling of high concurrency scenarios. However, in a real-world application, you do not need to manage sync.WaitGroup for adding tasks to the TaskQueue as the library itself ensures thread safety and proper synchronization.

## Conclusion

`tq` library is a simple yet powerful tool for managing tasks in a concurrent environment. Its thread-safe design and easy-to-use API make it an excellent choice for web applications and other scenarios where tasks need to be processed efficiently and reliably.

Feel free to contribute to the project or report any issues on our GitHub repository.

