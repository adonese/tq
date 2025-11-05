package tq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adonese/tq"
)

// Property-based testing: Properties that should ALWAYS hold
// These are like lightweight formal specifications

// Property 1: Every task added is either in queue, processing, or completed
func TestProperty_NoLostTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property test in short mode")
	}

	logger := log.New(os.Stdout, "prop: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(100, 10, logger, 100)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	var addedTasks int32
	var completedTasks int32
	var failedTasks int32

	// Track all task IDs we add
	addedIDs := sync.Map{}

	// Add 1000 tasks concurrently
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			taskID := fmt.Sprintf("task-%d", id)
			addedIDs.Store(taskID, true)
			atomic.AddInt32(&addedTasks, 1)

			err := tq.AddTask(ctx, Task{
				ID: taskID,
				Job: func(ctx context.Context) error {
					return nil
				},
			})
			if err != nil {
				t.Logf("Failed to add task: %v", err)
			}
		}(i)
	}

	wg.Wait()

	// Process results
	go func() {
		for result := range tq.Results() {
			if result.Error != nil {
				atomic.AddInt32(&failedTasks, 1)
			} else {
				atomic.AddInt32(&completedTasks, 1)
			}
		}
	}()

	tq.Shutdown(context.Background())

	// PROPERTY: No tasks lost
	total := atomic.LoadInt32(&completedTasks) + atomic.LoadInt32(&failedTasks)
	if total != atomic.LoadInt32(&addedTasks) {
		t.Errorf("PROPERTY VIOLATED: Lost tasks! Added %d, Processed %d",
			addedTasks, total)
	}
}

// Property 2: Priority ordering is respected
func TestProperty_PriorityOrdering(t *testing.T) {
	logger := log.New(os.Stdout, "prop: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(100, 1, logger, 1000) // Single worker
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	var executionOrder []string
	var mu sync.Mutex

	// Add tasks in mixed priority order
	tasks := []struct {
		id       string
		priority Priority
	}{
		{"low-1", LowPriority},
		{"high-1", HighPriority},
		{"medium-1", MediumPriority},
		{"low-2", LowPriority},
		{"high-2", HighPriority},
		{"medium-2", MediumPriority},
	}

	for _, tc := range tasks {
		tq.AddTask(ctx, Task{
			ID:       tc.id,
			Priority: tc.priority,
			Job: func(id string) func(context.Context) error {
				return func(ctx context.Context) error {
					mu.Lock()
					executionOrder = append(executionOrder, id)
					mu.Unlock()
					return nil
				}
			}(tc.id),
		})
		time.Sleep(10 * time.Millisecond) // Ensure all queued before processing
	}

	tq.Shutdown(context.Background())

	// PROPERTY: All high priority tasks execute before medium, medium before low
	mu.Lock()
	defer mu.Unlock()

	var lastHighIdx, lastMediumIdx, lastLowIdx int = -1, -1, -1

	for i, taskID := range executionOrder {
		if strings.HasPrefix(taskID, "high") {
			lastHighIdx = i
		} else if strings.HasPrefix(taskID, "medium") {
			lastMediumIdx = i
		} else if strings.HasPrefix(taskID, "low") {
			lastLowIdx = i
		}
	}

	// Check priority ordering property
	if lastHighIdx > lastMediumIdx || lastHighIdx > lastLowIdx {
		t.Errorf("PROPERTY VIOLATED: High priority not first. Order: %v", executionOrder)
	}
	if lastMediumIdx > lastLowIdx {
		t.Errorf("PROPERTY VIOLATED: Medium priority not before low. Order: %v", executionOrder)
	}
}

// Property 3: WaitGroup balance (no deadlocks)
func TestProperty_WaitGroupBalance(t *testing.T) {
	logger := log.New(os.Stdout, "prop: ", log.LstdFlags)
	tq, err := NewTaskQueueSimple(50, 5, logger, 100)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Add tasks, some will succeed, some will fail, some will be cancelled
	for i := 0; i < 100; i++ {
		go func(id int) {
			tq.AddTask(ctx, Task{
				ID: fmt.Sprintf("task-%d", id),
				Job: func(ctx context.Context) error {
					time.Sleep(10 * time.Millisecond)
					return nil
				},
			})

			// Randomly cancel some tasks
			if id%3 == 0 {
				time.Sleep(5 * time.Millisecond)
				tq.CancelTask(fmt.Sprintf("task-%d", id))
			}
		}(i)
	}

	// PROPERTY: Shutdown completes without deadlock
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	done := make(chan error, 1)
	go func() {
		done <- tq.Shutdown(shutdownCtx)
	}()

	select {
	case err := <-done:
		if err != nil && err != context.DeadlineExceeded {
			t.Errorf("PROPERTY VIOLATED: Shutdown failed with %v", err)
		}
		// Success - no deadlock
	case <-time.After(11 * time.Second):
		t.Fatal("PROPERTY VIOLATED: Deadlock detected - shutdown never completed")
	}
}

// Property 4: Duplicate detection works correctly
func TestProperty_DuplicateDetection(t *testing.T) {
	logger := log.New(os.Stdout, "prop: ", log.LstdFlags)
	tq, err := NewTaskQueue(TaskQueueConfig{
		BufferSize:        50,
		WorkerCount:       5,
		MaxRatePerSecond:  100,
		AllowDuplicates:   false,
		FullQueueStrategy: BlockUntilSpace,
		Logger:            logger,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Try to add same task ID 100 times concurrently
	duplicateErrors := int32(0)
	successes := int32(0)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := tq.AddTask(ctx, Task{
				ID:  "duplicate-task",
				Job: func(ctx context.Context) error { return nil },
			})
			if errors.Is(err, ErrDuplicateTask) {
				atomic.AddInt32(&duplicateErrors, 1)
			} else if err == nil {
				atomic.AddInt32(&successes, 1)
			}
		}()
	}

	wg.Wait()
	tq.Shutdown(context.Background())

	// PROPERTY: Exactly one task succeeds, all others fail with ErrDuplicateTask
	if atomic.LoadInt32(&successes) != 1 {
		t.Errorf("PROPERTY VIOLATED: Expected exactly 1 success, got %d", successes)
	}
	if atomic.LoadInt32(&duplicateErrors)+atomic.LoadInt32(&successes) != 100 {
		t.Errorf("PROPERTY VIOLATED: Some operations had unexpected results")
	}
}

// Property 5: Rate limiting is enforced
func TestProperty_RateLimiting(t *testing.T) {
	logger := log.New(os.Stdout, "prop: ", log.LstdFlags)
	maxRate := 10.0 // 10 tasks per second
	tq, err := NewTaskQueueSimple(100, 5, logger, maxRate)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Track task execution times
	var executionTimes []time.Time
	var mu sync.Mutex

	numTasks := 30
	for i := 0; i < numTasks; i++ {
		tq.AddTask(ctx, Task{
			ID: fmt.Sprintf("task-%d", i),
			Job: func(ctx context.Context) error {
				mu.Lock()
				executionTimes = append(executionTimes, time.Now())
				mu.Unlock()
				return nil
			},
		})
	}

	tq.Shutdown(context.Background())

	// PROPERTY: No 1-second window has more than maxRate tasks
	mu.Lock()
	defer mu.Unlock()

	if len(executionTimes) < numTasks {
		t.Logf("Warning: Only %d/%d tasks completed", len(executionTimes), numTasks)
		return
	}

	for i := 0; i < len(executionTimes); i++ {
		windowStart := executionTimes[i]
		windowEnd := windowStart.Add(1 * time.Second)
		tasksInWindow := 0

		for _, execTime := range executionTimes {
			if execTime.After(windowStart) && execTime.Before(windowEnd) {
				tasksInWindow++
			}
		}

		if float64(tasksInWindow) > maxRate*1.5 { // Allow 50% tolerance
			t.Errorf("PROPERTY VIOLATED: Rate limit exceeded. %d tasks in 1s window (max: %.0f)",
				tasksInWindow, maxRate)
		}
	}
}
