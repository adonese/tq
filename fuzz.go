package tq

import (
	"context"
	"errors"
	"log"
	"testing"
)

func FuzzTaskQueue(f *testing.F) {
	logger := log.New(nil, "test: ", log.LstdFlags)

	// Seed the fuzzer with initial data
	f.Add("task1", 3, false)
	f.Add("task2", 2, true)

	f.Fuzz(func(t *testing.T, taskID string, maxRetries int, shouldFail bool) {
		tq := NewTaskQueue(10, 5, logger)

		job := func(ctx context.Context) error {
			if shouldFail {
				return errors.New("fail")
			}
			return nil
		}

		tq.AddTask(Task{ID: taskID, Job: job, MaxRetries: maxRetries})
		tq.Shutdown()
	})
}
