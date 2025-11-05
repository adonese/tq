package tq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/exp/rand"
)

func TestTaskQueueWithContext(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 5, logger, 100)
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var processedTasks int32
	job := func(ctx context.Context) error {
		atomic.AddInt32(&processedTasks, 1)
		return nil
	}

	for i := 0; i < 5; i++ {
		err := tq.AddTask(ctx, Task{
			ID:  fmt.Sprintf("task-%d", i),
			Job: job,
			Retry: &RetryConfig{
				MaxRetries:      3,
				BackoffStrategy: ExponentialBackoff,
			},
		})
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	results := tq.Results()

	for i := 0; i < 5; i++ {
		select {
		case result := <-results:
			if result.Error != nil {
				t.Errorf("Task %s failed: %v", result.TaskID, result.Error)
			}
		case <-ctx.Done():
			t.Errorf("Context deadline exceeded")
		}
	}

	err = tq.Shutdown(ctx)
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}

	if atomic.LoadInt32(&processedTasks) != 5 {
		t.Errorf("Expected 5 processed tasks, got %d", processedTasks)
	}
}

func TestTaskPriority(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 1, logger, 100)
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	var executionOrder []string
	var mu sync.Mutex
	job := func(id string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			executionOrder = append(executionOrder, id)
			mu.Unlock()
			return nil
		}
	}

	tasks := []Task{
		{ID: "low", Job: job("low"), Priority: LowPriority},
		{ID: "high", Job: job("high"), Priority: HighPriority},
		{ID: "medium", Job: job("medium"), Priority: MediumPriority},
	}

	for _, task := range tasks {
		err := tq.AddTask(ctx, task)
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	results := tq.Results()

	for range tasks {
		<-results
	}

	err = tq.Shutdown(ctx)
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}

	expected := []string{"high", "medium", "low"}
	if !reflect.DeepEqual(executionOrder, expected) {
		t.Errorf("Expected execution order %v, got %v", expected, executionOrder)
	}
}

func TestRateLimiting(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 1, logger, 10) // 10 tasks per second, single worker
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	taskCount := 20
	start := time.Now()

	for i := 0; i < taskCount; i++ {
		err := tq.AddTask(ctx, Task{
			ID:  fmt.Sprintf("task-%d", i),
			Job: func(ctx context.Context) error { return nil },
		})
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	err = tq.Shutdown(ctx)
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}

	duration := time.Since(start)
	expectedDuration := time.Duration(taskCount/10) * time.Second

	if duration < expectedDuration {
		t.Errorf("Tasks completed too quickly. Expected at least %v, got %v", expectedDuration, duration)
	}
}

func TestGracefulShutdown(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 5, logger, 100)
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	var processedTasks int32
	job := func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&processedTasks, 1)
		return nil
	}

	for i := 0; i < 10; i++ {
		err := tq.AddTask(ctx, Task{ID: fmt.Sprintf("task-%d", i), Job: job})
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = tq.Shutdown(shutdownCtx)
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}

	if atomic.LoadInt32(&processedTasks) != 10 {
		t.Errorf("Expected 10 processed tasks, got %d", processedTasks)
	}
}

func TestErrorHandling(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 5, logger, 100)
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	errorJob := func(ctx context.Context) error {
		return errors.New("task error")
	}

	err = tq.AddTask(ctx, Task{
		ID:  "error-task",
		Job: errorJob,
		Retry: &RetryConfig{
			MaxRetries:      3,
			BackoffStrategy: ExponentialBackoff,
		},
	})
	if err != nil {
		t.Errorf("Failed to add task: %v", err)
	}

	results := tq.Results()

	result := <-results
	if result.Error == nil {
		t.Errorf("Expected error, got nil")
	}

	err = tq.Shutdown(ctx)
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}
}

func TestTaskQueueStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	logger := log.New(os.Stdout, "stress-test: ", log.LstdFlags)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tq, err := NewTaskQueueSimple(10000, 6000, logger, 5000) // 5000 tasks/second, 6000 workers
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	var (
		totalTasks        int32 = 100000
		completedTasks    int32
		failedTasks       int32
		highPriorityTasks int32
		lowPriorityTasks  int32
		addingTasks       int32 = 1
	)

	var wg sync.WaitGroup

	// Task generator
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer atomic.StoreInt32(&addingTasks, 0)
		for i := int32(0); i < totalTasks; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			priority := LowPriority
			if rand.Float32() < 0.1 { // 10% high priority tasks
				priority = HighPriority
				atomic.AddInt32(&highPriorityTasks, 1)
			} else {
				atomic.AddInt32(&lowPriorityTasks, 1)
			}

			task := Task{
				ID:       fmt.Sprintf("task-%d", i),
				Priority: priority,
				Job: func(ctx context.Context) error {
					if rand.Float32() < 0.01 { // 1% chance of failure
						return fmt.Errorf("random task failure")
					}
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
					return nil
				},
				Retry: &RetryConfig{
					MaxRetries:      3,
					BackoffStrategy: ExponentialBackoff,
				},
			}

			err := tq.AddTask(ctx, task)
			if err != nil {
				logger.Printf("Failed to add task: %v", err)
				return
			}

			if i%1000 == 0 {
				logger.Printf("Added %d tasks", i)
			}
		}
	}()

	// Result processor
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range tq.Results() {
			if result.Error != nil {
				atomic.AddInt32(&failedTasks, 1)
			} else {
				atomic.AddInt32(&completedTasks, 1)
			}

			if (atomic.LoadInt32(&completedTasks)+atomic.LoadInt32(&failedTasks))%1000 == 0 {
				logger.Printf("Processed %d tasks", atomic.LoadInt32(&completedTasks)+atomic.LoadInt32(&failedTasks))
			}
		}
	}()

	// Simulate periodic high load
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if atomic.LoadInt32(&addingTasks) == 0 {
					return
				}
				burstSize := int32(5000)
				logger.Printf("Adding burst of %d high priority tasks", burstSize)
				for i := int32(0); i < burstSize; i++ {
					task := Task{
						ID:       fmt.Sprintf("burst-task-%d", i),
						Priority: HighPriority,
						Job:      func(ctx context.Context) error { return nil },
					}
					err := tq.AddTask(ctx, task)
					if err != nil {
						logger.Printf("Failed to add burst task: %v", err)
						return
					}
				}
				atomic.AddInt32(&totalTasks, burstSize)
				atomic.AddInt32(&highPriorityTasks, burstSize)
			}
		}
	}()

	// Monitor goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				completed := atomic.LoadInt32(&completedTasks)
				failed := atomic.LoadInt32(&failedTasks)
				total := atomic.LoadInt32(&totalTasks)
				logger.Printf("Progress: %d/%d (%.2f%%) completed, %d failed",
					completed, total, float64(completed)/float64(total)*100, failed)
			}
		}
	}()

	// Wait for all tasks to be processed or timeout
	for {
		select {
		case <-ctx.Done():
			t.Logf("Test timed out")
			goto Shutdown
		default:
			if atomic.LoadInt32(&completedTasks)+atomic.LoadInt32(&failedTasks) >= atomic.LoadInt32(&totalTasks) &&
				atomic.LoadInt32(&addingTasks) == 0 {
				goto Shutdown
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

Shutdown:
	logger.Println("Initiating shutdown")
	err = tq.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}

	logger.Println("Waiting for all goroutines to finish")
	wg.Wait()

	// Final statistics
	completed := atomic.LoadInt32(&completedTasks)
	failed := atomic.LoadInt32(&failedTasks)
	total := atomic.LoadInt32(&totalTasks)
	highPriority := atomic.LoadInt32(&highPriorityTasks)
	lowPriority := atomic.LoadInt32(&lowPriorityTasks)

	logger.Printf("Test completed. Total tasks: %d, Completed: %d, Failed: %d", total, completed, failed)
	logger.Printf("High priority tasks: %d, Low priority tasks: %d", highPriority, lowPriority)

	// Assertions
	if completed+failed != total {
		t.Errorf("Task count mismatch. Total: %d, Completed + Failed: %d", total, completed+failed)
	}

	failureRate := float64(failed) / float64(total)
	if failureRate > 0.02 { // Allow for slightly higher failure rate due to retries
		t.Errorf("Failure rate too high: %.2f%%", failureRate*100)
	}

	if float64(completed)/float64(lowPriority) <= float64(completed)/float64(highPriority) {
		t.Errorf("Priority processing not working as expected")
	}
}

// Test task cancellation
func TestTaskCancellation(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 2, logger, 100)
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	var startedTasks int32
	var completedTasks int32

	job := func(ctx context.Context) error {
		atomic.AddInt32(&startedTasks, 1)
		time.Sleep(500 * time.Millisecond)
		atomic.AddInt32(&completedTasks, 1)
		return nil
	}

	// Add tasks
	for i := 0; i < 5; i++ {
		err := tq.AddTask(ctx, Task{
			ID:  fmt.Sprintf("task-%d", i),
			Job: job,
		})
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	// Cancel one task
	time.Sleep(100 * time.Millisecond)
	err = tq.CancelTask("task-4")
	if err != nil {
		t.Logf("Task cancellation: %v", err)
	}

	err = tq.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}

	t.Logf("Started: %d, Completed: %d", atomic.LoadInt32(&startedTasks), atomic.LoadInt32(&completedTasks))
}

// Test duplicate detection
func TestDuplicateDetection(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueue(TaskQueueConfig{
		BufferSize:        10,
		WorkerCount:       2,
		MaxRatePerSecond:  100,
		AllowDuplicates:   false, // Disable duplicates
		FullQueueStrategy: BlockUntilSpace,
		Logger:            logger,
		TaskTimeout:       5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	job := func(ctx context.Context) error {
		return nil
	}

	// Add first task
	err = tq.AddTask(ctx, Task{ID: "duplicate-task", Job: job})
	if err != nil {
		t.Errorf("Failed to add first task: %v", err)
	}

	// Try to add duplicate - should fail
	err = tq.AddTask(ctx, Task{ID: "duplicate-task", Job: job})
	if !errors.Is(err, ErrDuplicateTask) {
		t.Errorf("Expected ErrDuplicateTask, got: %v", err)
	}

	err = tq.Shutdown(ctx)
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}
}

// Test full queue strategies
func TestFullQueueStrategies(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)

	t.Run("ReturnError", func(t *testing.T) {
		tq, err := NewTaskQueue(TaskQueueConfig{
			BufferSize:        2,
			WorkerCount:       1,
			MaxRatePerSecond:  1,
			AllowDuplicates:   true,
			FullQueueStrategy: ReturnError,
			Logger:            logger,
			TaskTimeout:       5 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Failed to create task queue: %v", err)
		}

		ctx := context.Background()
		slowJob := func(ctx context.Context) error {
			time.Sleep(2 * time.Second)
			return nil
		}

		// Fill the queue
		for i := 0; i < 3; i++ {
			err := tq.AddTask(ctx, Task{
				ID:  fmt.Sprintf("task-%d", i),
				Job: slowJob,
			})
			if i < 2 && err != nil {
				t.Errorf("Failed to add task %d: %v", i, err)
			}
		}

		// This should fail with queue full
		err = tq.AddTask(ctx, Task{ID: "overflow", Job: slowJob})
		if !errors.Is(err, ErrQueueFull) {
			t.Errorf("Expected ErrQueueFull, got: %v", err)
		}

		tq.Shutdown(context.Background())
	})
}

// Test observability hooks
func TestObservabilityHooks(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)

	var startedCount, completedCount, failedCount, retryCount int32

	hooks := &TaskQueueHooks{
		OnTaskStarted: func(taskID string) {
			atomic.AddInt32(&startedCount, 1)
		},
		OnTaskCompleted: func(taskID string, duration time.Duration) {
			atomic.AddInt32(&completedCount, 1)
		},
		OnTaskFailed: func(taskID string, err error, retries int) {
			atomic.AddInt32(&failedCount, 1)
		},
		OnTaskRetry: func(taskID string, attempt int, err error) {
			atomic.AddInt32(&retryCount, 1)
		},
	}

	tq, err := NewTaskQueue(TaskQueueConfig{
		BufferSize:        10,
		WorkerCount:       2,
		MaxRatePerSecond:  100,
		AllowDuplicates:   true,
		FullQueueStrategy: BlockUntilSpace,
		Hooks:             hooks,
		Logger:            logger,
		TaskTimeout:       5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	// Add successful task
	err = tq.AddTask(ctx, Task{
		ID:  "success",
		Job: func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Errorf("Failed to add task: %v", err)
	}

	// Add failing task with retries
	err = tq.AddTask(ctx, Task{
		ID: "fail",
		Job: func(ctx context.Context) error {
			return errors.New("intentional failure")
		},
		Retry: &RetryConfig{
			MaxRetries:      3,
			BackoffStrategy: ConstantBackoff,
			BaseDelay:       10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Errorf("Failed to add task: %v", err)
	}

	err = tq.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}

	if atomic.LoadInt32(&startedCount) != 2 {
		t.Errorf("Expected 2 started tasks, got %d", startedCount)
	}
	if atomic.LoadInt32(&completedCount) != 1 {
		t.Errorf("Expected 1 completed task, got %d", completedCount)
	}
	if atomic.LoadInt32(&failedCount) != 1 {
		t.Errorf("Expected 1 failed task, got %d", failedCount)
	}
	if atomic.LoadInt32(&retryCount) < 2 {
		t.Errorf("Expected at least 2 retries, got %d", retryCount)
	}
}

// Test different backoff strategies
func TestBackoffStrategies(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)

	testCases := []struct {
		name     string
		strategy BackoffStrategy
	}{
		{"Exponential", ExponentialBackoff},
		{"Linear", LinearBackoff},
		{"Constant", ConstantBackoff},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tq, err := NewTaskQueueSimple(10, 2, logger, 100)
			if err != nil {
				t.Fatalf("Failed to create task queue: %v", err)
			}

			ctx := context.Background()
			attempts := 0

			err = tq.AddTask(ctx, Task{
				ID: "retry-task",
				Job: func(ctx context.Context) error {
					attempts++
					if attempts < 3 {
						return errors.New("retry me")
					}
					return nil
				},
				Retry: &RetryConfig{
					MaxRetries:      5,
					BackoffStrategy: tc.strategy,
					BaseDelay:       10 * time.Millisecond,
				},
			})
			if err != nil {
				t.Errorf("Failed to add task: %v", err)
			}

			err = tq.Shutdown(context.Background())
			if err != nil {
				t.Errorf("Failed to shutdown: %v", err)
			}

			if attempts < 3 {
				t.Errorf("Task didn't retry enough times: %d", attempts)
			}
		})
	}
}

// Test context cancellation during execution
func TestContextCancellation(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 2, logger, 100)
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	job := func(ctx context.Context) error {
		time.Sleep(1 * time.Second)
		return nil
	}

	err = tq.AddTask(ctx, Task{ID: "slow-task", Job: job})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context deadline exceeded, got: %v", err)
	}

	tq.Shutdown(context.Background())
}

// Test input validation
func TestInputValidation(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)

	testCases := []struct {
		name   string
		config TaskQueueConfig
	}{
		{
			name: "Zero buffer size",
			config: TaskQueueConfig{
				BufferSize:       0,
				WorkerCount:      2,
				MaxRatePerSecond: 100,
			},
		},
		{
			name: "Zero worker count",
			config: TaskQueueConfig{
				BufferSize:       10,
				WorkerCount:      0,
				MaxRatePerSecond: 100,
			},
		},
		{
			name: "Zero rate",
			config: TaskQueueConfig{
				BufferSize:       10,
				WorkerCount:      2,
				MaxRatePerSecond: 0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.config.Logger = logger
			_, err := NewTaskQueue(tc.config)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("Expected ErrInvalidConfig, got: %v", err)
			}
		})
	}
}

// Test GetQueueLength and IsProcessed
func TestQueueInspection(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 1, logger, 10)
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	// Add a slow task
	tq.AddTask(ctx, Task{
		ID: "slow",
		Job: func(ctx context.Context) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		},
	})

	// Add more tasks
	for i := 0; i < 3; i++ {
		tq.AddTask(ctx, Task{
			ID:  fmt.Sprintf("task-%d", i),
			Job: func(ctx context.Context) error { return nil },
		})
	}

	// Check queue length
	queueLen := tq.GetQueueLength()
	t.Logf("Queue length: %d", queueLen)

	// Wait for tasks to complete
	time.Sleep(1 * time.Second)

	// Check if task is processed
	if !tq.IsProcessed("slow") {
		t.Error("Task 'slow' should be processed")
	}

	tq.Shutdown(context.Background())
}

// Test DrainResults
func TestDrainResults(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(10, 2, logger, 100)
	if err != nil {
		t.Fatalf("Failed to create task queue: %v", err)
	}

	ctx := context.Background()

	// Add tasks
	for i := 0; i < 5; i++ {
		err := tq.AddTask(ctx, Task{
			ID:  fmt.Sprintf("task-%d", i),
			Job: func(ctx context.Context) error { return nil },
		})
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	// Wait a bit for tasks to complete
	time.Sleep(200 * time.Millisecond)

	// Drain results
	results := tq.DrainResults()
	t.Logf("Drained %d results", len(results))

	if len(results) == 0 {
		t.Error("Expected some results to be drained")
	}

	tq.Shutdown(context.Background())
}
