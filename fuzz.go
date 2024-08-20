package tq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
)

func FuzzTaskQueue(f *testing.F) {
	logger := log.New(nil, "test: ", log.LstdFlags)

	// Seed the fuzzer with initial data
	f.Add("task1", 3, false)
	f.Add("task2", 2, true)

	f.Fuzz(func(t *testing.T, taskID string, maxRetries int, shouldFail bool) {
		tq := NewTaskQueue(10, 5, logger, 10)

		job := func(ctx context.Context) error {
			if shouldFail {
				return errors.New("fail")
			}
			return nil
		}

		tq.AddTask(context.TODO(), Task{ID: fmt.Sprintf("task-%d", 4), Job: job, MaxRetries: 3})
		tq.Shutdown(context.TODO())
	})
}
