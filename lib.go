package tq

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	ID         string
	Job        func(ctx context.Context) error
	MaxRetries int
}

type TaskQueue struct {
	tasks          chan Task
	logger         *log.Logger
	wg             sync.WaitGroup
	processedTasks map[string]bool
	mu             sync.Mutex
	closeOnce      sync.Once
	shuttingDown   int32
	addTaskLock    sync.Mutex
}

func NewTaskQueue(bufferSize int, workerCount int, logger *log.Logger) *TaskQueue {
	tq := &TaskQueue{
		tasks:          make(chan Task, bufferSize),
		logger:         logger,
		processedTasks: make(map[string]bool),
	}

	for i := 0; i < workerCount; i++ {
		go tq.worker()
	}

	return tq
}

func (tq *TaskQueue) worker() {
	for task := range tq.tasks {
		tq.processTask(task)
	}
}

func (tq *TaskQueue) processTask(task Task) {
	defer tq.wg.Done() // Ensure wg.Done() is called regardless of how the function exits

	tq.mu.Lock()
	if tq.processedTasks[task.ID] {
		tq.logger.Printf("Task %s already processed, skipping", task.ID)
		tq.mu.Unlock()
		return
	}
	tq.processedTasks[task.ID] = true
	tq.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	RetryTask(ctx, task, tq.logger)
}

func (tq *TaskQueue) AddTask(task Task) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	if atomic.LoadInt32(&tq.shuttingDown) == 1 {
		tq.logger.Printf("Task queue is shutting down, rejecting task %s", task.ID)
		return
	}
	tq.wg.Add(1)
	tq.tasks <- task
}

func (tq *TaskQueue) Shutdown() {
	atomic.StoreInt32(&tq.shuttingDown, 1)
	tq.mu.Lock()
	tq.closeOnce.Do(func() {
		close(tq.tasks)
	})
	tq.mu.Unlock()
	tq.wg.Wait()
}

func RetryTask(ctx context.Context, task Task, logger *log.Logger) {
	baseDelay := time.Second
	for i := 0; i < task.MaxRetries; i++ {
		select {
		case <-ctx.Done():
			logger.Printf("Task %s canceled: %v", task.ID, ctx.Err())
			return
		default:
		}

		if err := task.Job(ctx); err != nil {
			logger.Printf("Task %s failed, attempt %d: %v", task.ID, i+1, err)
			backoff := baseDelay * time.Duration(1<<i) // Exponential backoff
			jitter := time.Duration(rand.Int63n(int64(baseDelay)))
			time.Sleep(backoff + jitter) // Add jitter to avoid collisions
			continue
		}
		// Success, break out of the loop
		break
	}
}
