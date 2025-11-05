package tq

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"
)

func FuzzTaskQueue(f *testing.F) {
	// Seed the fuzzer with initial data
	f.Add("task1", 3, false, uint8(0))
	f.Add("task2", 2, true, uint8(1))
	f.Add("fuzz-task", 5, false, uint8(2))

	f.Fuzz(func(t *testing.T, taskID string, maxRetries int, shouldFail bool, priorityVal uint8) {
		// Skip invalid inputs
		if taskID == "" || maxRetries < 0 || maxRetries > 10 {
			return
		}

		logger := log.New(nil, "fuzz: ", log.LstdFlags)
		tq, err := NewTaskQueueSimple(10, 2, logger, 100)
		if err != nil {
			t.Fatalf("Failed to create task queue: %v", err)
		}

		// Map uint8 to priority
		var priority Priority
		switch priorityVal % 3 {
		case 0:
			priority = LowPriority
		case 1:
			priority = MediumPriority
		case 2:
			priority = HighPriority
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		job := func(ctx context.Context) error {
			if shouldFail {
				return errors.New("intentional failure")
			}
			return nil
		}

		var retry *RetryConfig
		if maxRetries > 0 {
			retry = &RetryConfig{
				MaxRetries:      maxRetries,
				BackoffStrategy: ExponentialBackoff,
				BaseDelay:       10 * time.Millisecond,
			}
		}

		err = tq.AddTask(ctx, Task{
			ID:       taskID,
			Job:      job,
			Priority: priority,
			Retry:    retry,
		})
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Logf("AddTask error: %v", err)
		}

		// Attempt to cancel the task (may or may not succeed depending on timing)
		_ = tq.CancelTask(taskID)

		// Drain some results
		_ = tq.DrainResults()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()

		err = tq.Shutdown(shutdownCtx)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Logf("Shutdown error: %v", err)
		}
	})
}

// FuzzTaskQueueConcurrent tests concurrent operations
func FuzzTaskQueueConcurrent(f *testing.F) {
	f.Add(5, 3, true)
	f.Add(10, 2, false)

	f.Fuzz(func(t *testing.T, numTasks int, workerCount int, allowDuplicates bool) {
		if numTasks < 1 || numTasks > 100 || workerCount < 1 || workerCount > 10 {
			return
		}

		logger := log.New(nil, "fuzz-concurrent: ", log.LstdFlags)
		tq, err := NewTaskQueue(TaskQueueConfig{
			BufferSize:        numTasks,
			WorkerCount:       workerCount,
			MaxRatePerSecond:  100,
			AllowDuplicates:   allowDuplicates,
			FullQueueStrategy: BlockUntilSpace,
			Logger:            logger,
			TaskTimeout:       5 * time.Second,
		})
		if err != nil {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		tasks := make([]Task, numTasks)
		for i := 0; i < numTasks; i++ {
			tasks[i] = Task{
				ID:       "task",
				Job:      func(ctx context.Context) error { return nil },
				Priority: LowPriority,
			}
		}

		err = tq.AddTasksConcurrently(ctx, tasks)
		if err != nil && !errors.Is(err, ErrDuplicateTask) && !errors.Is(err, context.DeadlineExceeded) {
			t.Logf("AddTasksConcurrently error: %v", err)
		}

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		tq.Shutdown(shutdownCtx)
	})
}
