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
	addingTasks    sync.WaitGroup
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
	defer func() {
		tq.logger.Printf("Task %s done processing", task.ID)
		tq.wg.Done()
		tq.checkForShutdown()
	}()

	tq.mu.Lock()
	tq.logger.Printf("Processing task %s", task.ID)
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
	tq.logger.Printf("Attempting to add task %s", task.ID)
	tq.addingTasks.Add(1)
	defer tq.addingTasks.Done()

	tq.addTaskLock.Lock()
	defer tq.addTaskLock.Unlock()

	if atomic.LoadInt32(&tq.shuttingDown) == 1 {
		tq.logger.Printf("Task queue is shutting down, rejecting task %s", task.ID)
		return
	}

	tq.mu.Lock()
	tq.wg.Add(1)
	tq.tasks <- task
	tq.logger.Printf("Task %s added to the queue", task.ID)
	tq.mu.Unlock()
}

func (tq *TaskQueue) Shutdown() {
	tq.logger.Printf("Shutdown initiated")
	atomic.StoreInt32(&tq.shuttingDown, 1)

	tq.logger.Printf("Waiting for all AddTask operations to complete")
	tq.addingTasks.Wait()

	tq.addTaskLock.Lock()
	defer tq.addTaskLock.Unlock()

	tq.closeOnce.Do(func() {
		tq.logger.Printf("Closing task channel")
		close(tq.tasks)
	})

	tq.logger.Printf("Waiting for all tasks to complete")
	tq.wg.Wait()
	tq.logger.Printf("Shutdown complete, all tasks processed")
}

func (tq *TaskQueue) checkForShutdown() {
	if atomic.LoadInt32(&tq.shuttingDown) == 1 {
		tq.closeOnce.Do(func() {
			tq.logger.Printf("Closing task channel")
			close(tq.tasks)
		})
		tq.logger.Printf("Waiting for all tasks to complete")
		tq.wg.Wait()
		tq.logger.Printf("Shutdown complete, all tasks processed")
	}
}

func (tq *TaskQueue) AddAndProcessTasks(tasks []Task) {
	for _, task := range tasks {
		tq.AddTask(task)
	}
	tq.Shutdown()
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
			backoff := baseDelay * time.Duration(1<<i)
			jitter := time.Duration(rand.Int63n(int64(baseDelay)))
			time.Sleep(backoff + jitter)
			continue
		}
		break
	}
	logger.Printf("Task %s completed successfully", task.ID)
}

func (tq *TaskQueue) AddTasksConcurrently(tasks []Task) {
	var wg sync.WaitGroup
	wg.Add(len(tasks))

	for _, task := range tasks {
		go func(task Task) {
			defer wg.Done()
			tq.AddTask(task)
		}(task)
	}

	wg.Wait()
}
