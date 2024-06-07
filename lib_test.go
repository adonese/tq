package tq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
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
		logger.Printf("Processed tasks count updated: %d", processedTasks)
		return nil
	}

	tasks := []Task{
		{ID: "1", Job: job, MaxRetries: 3},
		{ID: "2", Job: job, MaxRetries: 3},
		{ID: "3", Job: job, MaxRetries: 3},
	}

	logger.Println("Adding and processing tasks...")
	tq.AddAndProcessTasks(tasks)

	mu.Lock()
	defer mu.Unlock()
	logger.Printf("Final processed tasks count: %d", processedTasks)
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

func TestGracefulShutdownWithPendingTasks(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(10, 5, logger)

	var processedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		time.Sleep(100 * time.Millisecond) // Simulate long-running task
		return nil
	}

	tasks := []Task{
		{ID: "1", Job: job, MaxRetries: 3},
		{ID: "2", Job: job, MaxRetries: 3},
		{ID: "3", Job: job, MaxRetries: 3},
	}

	go tq.AddAndProcessTasks(tasks)
	time.Sleep(50 * time.Millisecond) // Let some tasks start processing

	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != 3 {
		t.Errorf("expected 3 processed tasks, got %d", processedTasks)
	}
}

func TestAllTasksProcessed(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(10, 5, logger)

	var processedTasks []string
	var mu sync.Mutex

	job := func(id string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			processedTasks = append(processedTasks, id)
			return nil
		}
	}

	tasks := []Task{
		{ID: "1", Job: job("1"), MaxRetries: 3},
		{ID: "2", Job: job("2"), MaxRetries: 3},
		{ID: "3", Job: job("3"), MaxRetries: 3},
	}

	tq.AddAndProcessTasks(tasks)

	mu.Lock()
	defer mu.Unlock()

	expectedTasks := []string{"1", "2", "3"}
	taskMap := make(map[string]bool)
	for _, id := range processedTasks {
		taskMap[id] = true
	}

	for _, id := range expectedTasks {
		if !taskMap[id] {
			t.Errorf("expected task %s to be processed, but it was not", id)
		}
	}
}

func TestTaskFailureHandling(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(10, 5, logger)

	failCount := 0
	maxRetries := 3
	job := func(ctx context.Context) error {
		failCount++
		return errors.New("fail")
	}

	tq.AddTask(Task{ID: "1", Job: job, MaxRetries: maxRetries})
	tq.Shutdown()

	if failCount != maxRetries {
		t.Errorf("expected %d retries, got %d", maxRetries, failCount)
	}
}

func TestHighLoad(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(100, 10, logger)

	var processedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		return nil
	}

	var tasks []Task
	for i := 0; i < 100; i++ {
		tasks = append(tasks, Task{ID: fmt.Sprintf("%d", i), Job: job, MaxRetries: 3})
	}

	tq.AddAndProcessTasks(tasks)

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != 100 {
		t.Errorf("expected 100 processed tasks, got %d", processedTasks)
	}
}

func TestAddTaskAfterShutdown(t *testing.T) {
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
	tq.Shutdown()

	tq.AddTask(Task{ID: "2", Job: job, MaxRetries: 3}) // Should be rejected

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != 1 {
		t.Errorf("expected 1 processed task, got %d", processedTasks)
	}
}

func TestConcurrentAddTasks(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(30, 15, logger)

	var processedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		return nil
	}

	var wg sync.WaitGroup
	numTasks := 100
	wg.Add(numTasks)

	for i := 0; i < numTasks; i++ {
		go func(taskID string) {
			defer wg.Done()
			tq.AddTask(Task{ID: taskID, Job: job, MaxRetries: 3})
		}(fmt.Sprintf("task-%d", i))
	}

	wg.Wait()
	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != numTasks {
		t.Errorf("expected %d processed tasks, got %d", numTasks, processedTasks)
	}
}

func TestAddTasksConcurrently_AllTasksProcessed(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(100, 10, logger)

	var processedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		return nil
	}

	var tasks []Task
	numTasks := 100
	for i := 0; i < numTasks; i++ {
		tasks = append(tasks, Task{ID: fmt.Sprintf("task-%d", i), Job: job, MaxRetries: 3})
	}

	tq.AddTasksConcurrently(tasks)
	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != numTasks {
		t.Errorf("expected %d processed tasks, got %d", numTasks, processedTasks)
	}
}

func TestAddTasksConcurrently_WithFailures(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(100, 80, logger)

	var processedTasks int
	var failedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		if processedTasks%2 == 0 { // Simulate failure for even tasks
			failedTasks++
			return errors.New("task failed")
		}
		return nil
	}

	var tasks []Task
	numTasks := 50
	for i := 0; i < numTasks; i++ {
		tasks = append(tasks, Task{ID: fmt.Sprintf("task-%d", i), Job: job, MaxRetries: 3})
	}

	tq.AddTasksConcurrently(tasks)
	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != numTasks*2 {
		t.Errorf("expected %d processed tasks (including retries), got %d", numTasks*2, processedTasks)
	}
	if failedTasks != numTasks/2 {
		t.Errorf("expected %d failed tasks, got %d", numTasks/2, failedTasks)
	}
}

func TestAddTasksConcurrently_ConcurrentAddAndShutdown(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(100, 10, logger)

	var processedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		return nil
	}

	var tasks []Task
	numTasks := 50
	for i := 0; i < numTasks; i++ {
		tasks = append(tasks, Task{ID: fmt.Sprintf("task-%d", i), Job: job, MaxRetries: 3})
	}

	go tq.AddTasksConcurrently(tasks)

	time.Sleep(50 * time.Millisecond) // Let some tasks start processing
	go tq.Shutdown()

	tq.Shutdown() // Ensure all tasks are processed before shutdown completes

	mu.Lock()
	defer mu.Unlock()
	if processedTasks == 0 {
		t.Errorf("expected some processed tasks, got %d", processedTasks)
	}
}

func TestAddTasksConcurrently_HTTPRequestTasks(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(100, 10, logger)

	var tasks []Task
	numTasks := 10
	for i := 0; i < numTasks; i++ {
		taskID := fmt.Sprintf("http-task-%d", i)
		task := Task{
			ID: taskID,
			Job: func(ctx context.Context) error {
				// Task logic to make an HTTP request using curl
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
		tasks = append(tasks, task)
	}

	tq.AddTasksConcurrently(tasks)
	tq.Shutdown()
}

func TestAddTasksConcurrently_MixedTaskTypes(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(100, 50, logger)

	var processedSimpleTasks int
	var processedHttpTasks int
	var mu sync.Mutex

	simpleJob := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		processedSimpleTasks++
		return nil
	}

	httpJob := func(ctx context.Context) error {
		// Task logic to make an HTTP request using curl
		cmd := exec.CommandContext(ctx, "curl", "-s", "https://example.com")
		output, err := cmd.Output()
		if err != nil {
			log.Printf("HTTP task failed: %v", err)
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		processedHttpTasks++
		log.Printf("HTTP task succeeded: %s", output)
		return nil
	}

	var tasks []Task
	numSimpleTasks := 50
	numHttpTasks := 10
	for i := 0; i < numSimpleTasks; i++ {
		tasks = append(tasks, Task{ID: fmt.Sprintf("simple-task-%d", i), Job: simpleJob, MaxRetries: 3})
	}
	for i := 0; i < numHttpTasks; i++ {
		tasks = append(tasks, Task{ID: fmt.Sprintf("http-task-%d", i), Job: httpJob, MaxRetries: 3})
	}

	tq.AddTasksConcurrently(tasks)
	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedSimpleTasks != numSimpleTasks {
		t.Errorf("expected %d processed simple tasks, got %d", numSimpleTasks, processedSimpleTasks)
	}
	if processedHttpTasks != numHttpTasks {
		t.Errorf("expected %d processed HTTP tasks, got %d", numHttpTasks, processedHttpTasks)
	}
}

func TestAddTask(t *testing.T) {
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

	task := Task{ID: "task-1", Job: job, MaxRetries: 3}
	tq.AddTask(task)
	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != 1 {
		t.Errorf("expected 1 processed task, got %d", processedTasks)
	}
}

func TestAddHttpRequestTask(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(10, 5, logger)

	var processedTasks int
	var mu sync.Mutex

	job := func(ctx context.Context) error {
		cmd := exec.CommandContext(ctx, "curl", "-s", "https://example.com")
		output, err := cmd.Output()
		if err != nil {
			log.Printf("HTTP request task failed: %v", err)
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		processedTasks++
		log.Printf("HTTP request task succeeded: %s", output)
		return nil
	}

	task := Task{ID: "http-task-1", Job: job, MaxRetries: 3}
	tq.AddTask(task)
	tq.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processedTasks != 1 {
		t.Errorf("expected 1 processed task, got %d", processedTasks)
	}
}

func TestHighConcurrencyAddTasks(t *testing.T) {
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)
	tq := NewTaskQueue(1000, 50, logger)

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
			task := Task{
				ID:         taskID,
				Job:        job,
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
