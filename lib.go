package tq

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/rand"
	"golang.org/x/time/rate"
)

var (
	ErrQueueShuttingDown = errors.New("task queue is shutting down")
	ErrQueueFull         = errors.New("task queue is full")
	ErrInvalidConfig     = errors.New("invalid task queue configuration")
	ErrTaskNotFound      = errors.New("task not found")
	ErrDuplicateTask     = errors.New("duplicate task ID")
)

type Priority int

const (
	LowPriority Priority = iota
	MediumPriority
	HighPriority
)

// BackoffStrategy defines how retry delays are calculated
type BackoffStrategy int

const (
	ExponentialBackoff BackoffStrategy = iota
	LinearBackoff
	ConstantBackoff
)

// FullQueueStrategy defines behavior when queue is full
type FullQueueStrategy int

const (
	BlockUntilSpace FullQueueStrategy = iota // Block until space available (default)
	ReturnError                              // Return ErrQueueFull immediately
	DropOldest                               // Drop oldest task and add new one
)

// RetryConfig defines retry behavior for a task
type RetryConfig struct {
	MaxRetries      int
	BackoffStrategy BackoffStrategy
	BaseDelay       time.Duration // default 1 second
}

// Task represents a unit of work to be executed
type Task struct {
	ID       string
	Job      func(ctx context.Context) error
	Priority Priority
	Retry    *RetryConfig // nil means no retry
}

// TaskResult contains the outcome of task execution
type TaskResult struct {
	TaskID    string
	Error     error
	StartTime time.Time
	EndTime   time.Time
	Retries   int
}

// TaskQueueHooks provides callbacks for observability
type TaskQueueHooks struct {
	OnTaskStarted   func(taskID string)
	OnTaskCompleted func(taskID string, duration time.Duration)
	OnTaskFailed    func(taskID string, err error, retries int)
	OnTaskRetry     func(taskID string, attempt int, err error)
	OnTaskCancelled func(taskID string)
}

// TaskQueueConfig configures the task queue
type TaskQueueConfig struct {
	BufferSize        int
	WorkerCount       int
	MaxRatePerSecond  float64
	AllowDuplicates   bool              // default true
	FullQueueStrategy FullQueueStrategy // default BlockUntilSpace
	Hooks             *TaskQueueHooks   // optional observability hooks
	Logger            *log.Logger
	TaskTimeout       time.Duration // default 5 minutes
}

// Validate checks if the configuration is valid
func (c *TaskQueueConfig) Validate() error {
	if c.BufferSize <= 0 {
		return fmt.Errorf("%w: bufferSize must be > 0", ErrInvalidConfig)
	}
	if c.WorkerCount <= 0 {
		return fmt.Errorf("%w: workerCount must be > 0", ErrInvalidConfig)
	}
	if c.MaxRatePerSecond <= 0 {
		return fmt.Errorf("%w: maxRatePerSecond must be > 0", ErrInvalidConfig)
	}
	if c.TaskTimeout <= 0 {
		c.TaskTimeout = 5 * time.Minute
	}
	return nil
}

// priorityQueueItem wraps a task with heap index for efficient updates
type priorityQueueItem struct {
	task  *Task
	index int
}

type priorityQueue []*priorityQueueItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].task.Priority > pq[j].task.Priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*priorityQueueItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // mark as removed
	*pq = old[0 : n-1]
	return item
}

// TaskQueue manages concurrent task execution with priorities
type TaskQueue struct {
	config       TaskQueueConfig
	tasks        priorityQueue
	taskIndex    map[string]*priorityQueueItem // for O(log n) lookups and cancellation
	results      chan TaskResult
	wg           sync.WaitGroup
	shuttingDown atomic.Int32
	mu           sync.RWMutex
	processed    map[string]struct{}
	limiter      *rate.Limiter
	taskChan     chan struct{} // signal channel for task availability
	cancelFuncs  map[string]context.CancelFunc // for task cancellation
	workerWg     sync.WaitGroup                // separate WaitGroup for workers
}

// NewTaskQueue creates a new task queue with the given configuration
func NewTaskQueue(config TaskQueueConfig) (*TaskQueue, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	tq := &TaskQueue{
		config:      config,
		tasks:       make(priorityQueue, 0),
		taskIndex:   make(map[string]*priorityQueueItem),
		results:     make(chan TaskResult, config.BufferSize),
		processed:   make(map[string]struct{}),
		limiter:     rate.NewLimiter(rate.Limit(config.MaxRatePerSecond), 1), // burst size of 1 for consistent rate limiting
		taskChan:    make(chan struct{}, config.BufferSize),
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	heap.Init(&tq.tasks)

	// Start worker pool
	for i := 0; i < config.WorkerCount; i++ {
		tq.workerWg.Add(1)
		go tq.worker()
	}

	return tq, nil
}

// NewTaskQueueSimple creates a task queue with simple parameters (backward compatible)
func NewTaskQueueSimple(bufferSize, workerCount int, logger *log.Logger, maxRatePerSecond float64) (*TaskQueue, error) {
	return NewTaskQueue(TaskQueueConfig{
		BufferSize:        bufferSize,
		WorkerCount:       workerCount,
		MaxRatePerSecond:  maxRatePerSecond,
		AllowDuplicates:   true,
		FullQueueStrategy: BlockUntilSpace,
		Logger:            logger,
		TaskTimeout:       5 * time.Minute,
	})
}

func (tq *TaskQueue) worker() {
	defer tq.workerWg.Done()

	for range tq.taskChan {
		tq.processNextTask()
	}
}

func (tq *TaskQueue) processNextTask() {
	// Pop the highest priority task
	tq.mu.Lock()
	if tq.tasks.Len() == 0 {
		tq.mu.Unlock()
		tq.wg.Done() // Balance the wg.Add from AddTask
		return
	}
	item := heap.Pop(&tq.tasks).(*priorityQueueItem)
	delete(tq.taskIndex, item.task.ID)
	task := item.task
	tq.mu.Unlock()

	defer tq.wg.Done()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), tq.config.TaskTimeout)
	defer cancel()

	// Store cancel function for potential task cancellation
	tq.mu.Lock()
	tq.cancelFuncs[task.ID] = cancel
	tq.mu.Unlock()

	// Clean up cancel function when done
	defer func() {
		tq.mu.Lock()
		delete(tq.cancelFuncs, task.ID)
		tq.mu.Unlock()
	}()

	// Rate limiting before execution
	if err := tq.limiter.Wait(ctx); err != nil {
		tq.sendResult(TaskResult{
			TaskID:    task.ID,
			Error:     err,
			StartTime: time.Now(),
			EndTime:   time.Now(),
		})
		return
	}

	// Call hook if available
	if tq.config.Hooks != nil && tq.config.Hooks.OnTaskStarted != nil {
		tq.config.Hooks.OnTaskStarted(task.ID)
	}

	startTime := time.Now()
	err, retries := tq.executeTaskWithRetry(ctx, task)
	endTime := time.Now()

	// Mark as processed if successful
	if err == nil {
		tq.mu.Lock()
		tq.processed[task.ID] = struct{}{}
		tq.mu.Unlock()

		if tq.config.Hooks != nil && tq.config.Hooks.OnTaskCompleted != nil {
			tq.config.Hooks.OnTaskCompleted(task.ID, endTime.Sub(startTime))
		}
	} else {
		if tq.config.Hooks != nil && tq.config.Hooks.OnTaskFailed != nil {
			tq.config.Hooks.OnTaskFailed(task.ID, err, retries)
		}
	}

	tq.sendResult(TaskResult{
		TaskID:    task.ID,
		Error:     err,
		StartTime: startTime,
		EndTime:   endTime,
		Retries:   retries,
	})
}

func (tq *TaskQueue) executeTaskWithRetry(ctx context.Context, task *Task) (error, int) {
	// If no retry config, execute once
	if task.Retry == nil {
		return task.Job(ctx), 0
	}

	var err error
	baseDelay := task.Retry.BaseDelay
	if baseDelay == 0 {
		baseDelay = time.Second
	}

	maxRetries := task.Retry.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err(), attempt
		default:
		}

		err = task.Job(ctx)
		if err == nil {
			return nil, attempt
		}

		// Call retry hook if available
		if tq.config.Hooks != nil && tq.config.Hooks.OnTaskRetry != nil {
			tq.config.Hooks.OnTaskRetry(task.ID, attempt+1, err)
		}

		if tq.config.Logger != nil {
			tq.config.Logger.Printf("Task %s failed, attempt %d/%d: %v", task.ID, attempt+1, maxRetries, err)
		}

		// Don't sleep after last attempt
		if attempt < maxRetries-1 {
			delay := tq.calculateBackoff(task.Retry.BackoffStrategy, baseDelay, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err(), attempt + 1
			case <-time.After(delay):
			}
		}
	}

	return err, maxRetries
}

func (tq *TaskQueue) calculateBackoff(strategy BackoffStrategy, baseDelay time.Duration, attempt int) time.Duration {
	var backoff time.Duration

	switch strategy {
	case ExponentialBackoff:
		backoff = baseDelay * time.Duration(1<<uint(attempt))
	case LinearBackoff:
		backoff = baseDelay * time.Duration(attempt+1)
	case ConstantBackoff:
		backoff = baseDelay
	default:
		backoff = baseDelay * time.Duration(1<<uint(attempt))
	}

	// Add jitter to prevent thundering herd
	jitter := time.Duration(rand.Int63n(int64(baseDelay)))
	return backoff + jitter
}

func (tq *TaskQueue) sendResult(result TaskResult) {
	// Non-blocking send to avoid deadlock if results channel is full
	select {
	case tq.results <- result:
	default:
		if tq.config.Logger != nil {
			tq.config.Logger.Printf("Warning: results channel full, dropping result for task %s", result.TaskID)
		}
	}
}

// AddTask adds a task to the queue
func (tq *TaskQueue) AddTask(ctx context.Context, task Task) error {
	if tq.shuttingDown.Load() == 1 {
		return ErrQueueShuttingDown
	}

	// Check for duplicates if not allowed
	if !tq.config.AllowDuplicates {
		tq.mu.RLock()
		if _, exists := tq.taskIndex[task.ID]; exists {
			tq.mu.RUnlock()
			return ErrDuplicateTask
		}
		if _, processed := tq.processed[task.ID]; processed {
			tq.mu.RUnlock()
			return ErrDuplicateTask
		}
		tq.mu.RUnlock()
	}

	// Handle full queue based on strategy
	tq.mu.Lock()
	if len(tq.taskChan) >= cap(tq.taskChan) && tq.tasks.Len() >= cap(tq.taskChan) {
		switch tq.config.FullQueueStrategy {
		case ReturnError:
			tq.mu.Unlock()
			return ErrQueueFull
		case DropOldest:
			// Drop oldest (lowest priority) task
			if tq.tasks.Len() > 0 {
				oldest := heap.Pop(&tq.tasks).(*priorityQueueItem)
				delete(tq.taskIndex, oldest.task.ID)
				tq.wg.Done() // This task won't be processed
			}
		case BlockUntilSpace:
			// Will block below
		}
	}

	// Add to priority queue
	item := &priorityQueueItem{task: &task}
	heap.Push(&tq.tasks, item)
	tq.taskIndex[task.ID] = item
	tq.mu.Unlock()

	// Increment wait group
	tq.wg.Add(1)

	// Notify that a task is available
	select {
	case <-ctx.Done():
		// Context cancelled, remove task and decrement wait group
		tq.mu.Lock()
		if item, exists := tq.taskIndex[task.ID]; exists && item.index >= 0 {
			heap.Remove(&tq.tasks, item.index)
			delete(tq.taskIndex, task.ID)
		}
		tq.mu.Unlock()
		tq.wg.Done()
		return ctx.Err()
	case tq.taskChan <- struct{}{}:
		return nil
	}
}

// AddTasksConcurrently adds multiple tasks concurrently
func (tq *TaskQueue) AddTasksConcurrently(ctx context.Context, tasks []Task) error {
	errChan := make(chan error, len(tasks))

	for _, task := range tasks {
		go func(t Task) {
			errChan <- tq.AddTask(ctx, t)
		}(task)
	}

	var firstErr error
	for range tasks {
		if err := <-errChan; err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Results returns a read-only channel of task results
// Note: This is the shared results channel. Consider using DrainResults() for specific batches.
func (tq *TaskQueue) Results() <-chan TaskResult {
	return tq.results
}

// CancelTask cancels a running task by its ID
func (tq *TaskQueue) CancelTask(taskID string) error {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	// Check if task is currently running
	if cancel, exists := tq.cancelFuncs[taskID]; exists {
		cancel()
		if tq.config.Hooks != nil && tq.config.Hooks.OnTaskCancelled != nil {
			tq.config.Hooks.OnTaskCancelled(taskID)
		}
		return nil
	}

	// Check if task is in queue and remove it
	if item, exists := tq.taskIndex[taskID]; exists {
		heap.Remove(&tq.tasks, item.index)
		delete(tq.taskIndex, taskID)
		tq.wg.Done() // Balance the wg.Add from AddTask
		if tq.config.Hooks != nil && tq.config.Hooks.OnTaskCancelled != nil {
			tq.config.Hooks.OnTaskCancelled(taskID)
		}
		return nil
	}

	return ErrTaskNotFound
}

// GetQueueLength returns the current number of pending tasks
func (tq *TaskQueue) GetQueueLength() int {
	tq.mu.RLock()
	defer tq.mu.RUnlock()
	return tq.tasks.Len()
}

// IsProcessed checks if a task has been successfully processed
func (tq *TaskQueue) IsProcessed(taskID string) bool {
	tq.mu.RLock()
	defer tq.mu.RUnlock()
	_, exists := tq.processed[taskID]
	return exists
}

// Shutdown gracefully shuts down the task queue
func (tq *TaskQueue) Shutdown(ctx context.Context) error {
	if !tq.shuttingDown.CompareAndSwap(0, 1) {
		return errors.New("already shutting down")
	}

	// Wait for all tasks to complete
	doneChan := make(chan struct{})
	go func() {
		tq.wg.Wait()
		close(tq.taskChan) // Signal workers to stop
		tq.workerWg.Wait() // Wait for workers to finish
		close(doneChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneChan:
		// Drain remaining results
		tq.drainResults()
		close(tq.results)
		return nil
	}
}

// drainResults processes any remaining results in the buffer
func (tq *TaskQueue) drainResults() {
	// Give a short time for any in-flight results to arrive
	time.Sleep(100 * time.Millisecond)

	// This is a helper for shutdown - results are already in the channel
	// Just logging that we're closing with potentially unread results
	remaining := len(tq.results)
	if remaining > 0 && tq.config.Logger != nil {
		tq.config.Logger.Printf("Shutdown: %d results remaining in buffer", remaining)
	}
}

// DrainResults reads all available results from the channel without blocking
func (tq *TaskQueue) DrainResults() []TaskResult {
	var results []TaskResult
	for {
		select {
		case result := <-tq.results:
			results = append(results, result)
		default:
			return results
		}
	}
}
