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
	tq := NewTaskQueue(10, 5, logger, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var processedTasks int32
	job := func(ctx context.Context) error {
		atomic.AddInt32(&processedTasks, 1)
		return nil
	}

	for i := 0; i < 5; i++ {
		err := tq.AddTask(ctx, Task{ID: fmt.Sprintf("task-%d", i), Job: job, MaxRetries: 3})
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	results, err := tq.ProcessTasksAsync(ctx, nil)
	if err != nil {
		t.Errorf("Failed to process tasks: %v", err)
	}

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
	tq := NewTaskQueue(10, 1, logger, 100)

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
		{ID: "low", Job: job("low"), MaxRetries: 1, Priority: LowPriority},
		{ID: "high", Job: job("high"), MaxRetries: 1, Priority: HighPriority},
		{ID: "medium", Job: job("medium"), MaxRetries: 1, Priority: MediumPriority},
	}

	for _, task := range tasks {
		err := tq.AddTask(ctx, task)
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	results, err := tq.ProcessTasksAsync(ctx, nil)
	if err != nil {
		t.Errorf("Failed to process tasks: %v", err)
	}

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
	tq := NewTaskQueue(10, 1, logger, 10) // 10 tasks per second, single worker

	ctx := context.Background()

	taskCount := 20
	start := time.Now()

	for i := 0; i < taskCount; i++ {
		err := tq.AddTask(ctx, Task{
			ID:         fmt.Sprintf("task-%d", i),
			Job:        func(ctx context.Context) error { return nil },
			MaxRetries: 1,
		})
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	err := tq.Shutdown(ctx)
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
	tq := NewTaskQueue(10, 5, logger, 100)

	ctx := context.Background()

	var processedTasks int32
	job := func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&processedTasks, 1)
		return nil
	}

	for i := 0; i < 10; i++ {
		err := tq.AddTask(ctx, Task{ID: fmt.Sprintf("task-%d", i), Job: job, MaxRetries: 1})
		if err != nil {
			t.Errorf("Failed to add task: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := tq.Shutdown(shutdownCtx)
	if err != nil {
		t.Errorf("Failed to shutdown: %v", err)
	}

	if atomic.LoadInt32(&processedTasks) != 10 {
		t.Errorf("Expected 10 processed tasks, got %d", processedTasks)
	}
}

func TestErrorHandling(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(10, 5, logger, 100)

	ctx := context.Background()

	errorJob := func(ctx context.Context) error {
		return errors.New("task error")
	}

	err := tq.AddTask(ctx, Task{ID: "error-task", Job: errorJob, MaxRetries: 3})
	if err != nil {
		t.Errorf("Failed to add task: %v", err)
	}

	results, err := tq.ProcessTasksAsync(ctx, nil)
	if err != nil {
		t.Errorf("Failed to process tasks: %v", err)
	}

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

	tq := NewTaskQueue(10000, 6000, logger, 5000) // 500 tasks/second, 50 workers

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
				MaxRetries: 3,
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
		for result := range tq.results {
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
						ID:         fmt.Sprintf("burst-task-%d", i),
						Priority:   HighPriority,
						Job:        func(ctx context.Context) error { return nil },
						MaxRetries: 1,
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
	err := tq.Shutdown(context.Background())
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
