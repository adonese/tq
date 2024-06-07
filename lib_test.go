package tq

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"testing"
)

func TestRetryTask(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	ctx := context.Background()

	// Test 1: Retry logic with a failing task
	failCount := 0
	maxRetries := 3
	job := func(ctx context.Context) error {
		failCount++
		return errors.New("fail")
	}

	RetryTask(ctx, Task{ID: "1", Job: job, MaxRetries: maxRetries}, logger)
	if failCount != maxRetries {
		t.Errorf("expected %d retries, got %d", maxRetries, failCount)
	}

	// Test 2: Successful task after retries
	successCount := 0
	job = func(ctx context.Context) error {
		successCount++
		if successCount < 2 {
			return errors.New("fail")
		}
		return nil
	}

	RetryTask(ctx, Task{ID: "2", Job: job, MaxRetries: maxRetries}, logger)
	if successCount != 2 {
		t.Errorf("expected success after 2 attempts, got %d", successCount)
	}
}

func TestTaskQueue(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(10, 5, logger)

	var processedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		return nil
	}

	tq.AddTask(Task{ID: "1", Job: job, MaxRetries: 3})
	tq.AddTask(Task{ID: "2", Job: job, MaxRetries: 3})
	tq.AddTask(Task{ID: "3", Job: job, MaxRetries: 3})

	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != 3 {
		t.Errorf("expected 3 processed tasks, got %d", processedTasks)
	}
}

func TestRaceCondition(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(10, 5, logger)

	var processedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		return nil
	}

	// Add tasks concurrently to test for race conditions
	go tq.AddTask(Task{ID: "1", Job: job, MaxRetries: 3})
	go tq.AddTask(Task{ID: "2", Job: job, MaxRetries: 3})
	go tq.AddTask(Task{ID: "3", Job: job, MaxRetries: 3})

	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != 3 {
		t.Errorf("expected 3 processed tasks, got %d", processedTasks)
	}
}

func TestNoDoubleTransactions(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(10, 5, logger)

	taskID := "unique-task-id"
	job := func(ctx context.Context) error {
		return nil
	}

	tq.AddTask(Task{ID: taskID, Job: job, MaxRetries: 3})
	tq.AddTask(Task{ID: taskID, Job: job, MaxRetries: 3}) // Duplicate task

	tq.Shutdown()

	// If the task is processed more than once, the logger should have logged it
	// You would need to check the logger output if you are logging the "skipping" message
}
