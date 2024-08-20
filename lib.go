package tq

import (
	"container/heap"
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/rand"
	"golang.org/x/time/rate"
)

type Priority int

const (
	LowPriority Priority = iota
	MediumPriority
	HighPriority
)

type Task struct {
	ID         string
	Job        func(ctx context.Context) error
	MaxRetries int
	Priority   Priority
}

type TaskResult struct {
	TaskID string
	Error  error
}

type priorityQueue []*Task

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Priority > pq[j].Priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*Task)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

type TaskQueue struct {
	tasks        priorityQueue
	results      chan TaskResult
	logger       *log.Logger
	wg           sync.WaitGroup
	shuttingDown int32
	mu           sync.RWMutex
	processed    map[string]struct{}
	limiter      *rate.Limiter
	taskChan     chan struct{}
}

func NewTaskQueue(bufferSize, workerCount int, logger *log.Logger, maxRatePerSecond float64) *TaskQueue {
	tq := &TaskQueue{
		tasks:     make(priorityQueue, 0),
		results:   make(chan TaskResult, bufferSize),
		logger:    logger,
		processed: make(map[string]struct{}),
		limiter:   rate.NewLimiter(rate.Limit(maxRatePerSecond), int(maxRatePerSecond)),
		taskChan:  make(chan struct{}, bufferSize),
	}

	heap.Init(&tq.tasks)

	for i := 0; i < workerCount; i++ {
		go tq.worker()
	}

	return tq
}

func (tq *TaskQueue) worker() {
	for range tq.taskChan {
		tq.processNextTask()
	}
}

func (tq *TaskQueue) processNextTask() {
	tq.mu.Lock()
	if tq.tasks.Len() == 0 {
		tq.mu.Unlock()
		return
	}
	task := heap.Pop(&tq.tasks).(*Task)
	tq.mu.Unlock()

	defer tq.wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err := tq.limiter.Wait(ctx)
	if err != nil {
		tq.results <- TaskResult{TaskID: task.ID, Error: err}
		return
	}

	err = RetryTask(ctx, *task, tq.logger)
	if err == nil {
		tq.mu.Lock()
		tq.processed[task.ID] = struct{}{}
		tq.mu.Unlock()
	}

	tq.results <- TaskResult{TaskID: task.ID, Error: err}
}

func (tq *TaskQueue) AddTask(ctx context.Context, task Task) error {
	if atomic.LoadInt32(&tq.shuttingDown) == 1 {
		return errors.New("task queue is shutting down")
	}

	tq.mu.Lock()
	heap.Push(&tq.tasks, &task)
	tq.mu.Unlock()

	tq.wg.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case tq.taskChan <- struct{}{}:
		return nil
	}
}

func (tq *TaskQueue) AddTasksConcurrently(ctx context.Context, tasks []Task) error {
	errChan := make(chan error, len(tasks))

	for _, task := range tasks {
		go func(t Task) {
			errChan <- tq.AddTask(ctx, t)
		}(task)
	}

	var err error
	for range tasks {
		if e := <-errChan; e != nil && err == nil {
			err = e
		}
	}

	return err
}

func (tq *TaskQueue) ProcessTasksAsync(ctx context.Context, tasks []Task) (<-chan TaskResult, error) {
	err := tq.AddTasksConcurrently(ctx, tasks)
	if err != nil {
		return nil, err
	}

	return tq.results, nil
}

func (tq *TaskQueue) Shutdown(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&tq.shuttingDown, 0, 1) {
		return errors.New("already shutting down")
	}

	close(tq.taskChan) // Signal workers to stop

	doneChan := make(chan struct{})
	go func() {
		tq.wg.Wait()
		close(doneChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneChan:
		close(tq.results)
		return nil
	}
}

func RetryTask(ctx context.Context, task Task, logger *log.Logger) error {
	var err error
	baseDelay := time.Second

	for i := 0; i < task.MaxRetries; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err = task.Job(ctx); err == nil {
			return nil
		}

		logger.Printf("Task %s failed, attempt %d: %v", task.ID, i+1, err)
		backoff := baseDelay * time.Duration(1<<uint(i))
		jitter := time.Duration(rand.Int63n(int64(baseDelay)))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + jitter):
		}
	}

	return err
}
