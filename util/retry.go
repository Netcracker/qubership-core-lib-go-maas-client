package util

import (
	"fmt"
	"time"
)

var (
	DefaultRetryAttempts = 30
	DefaultRetryInterval = time.Second
)

// Retry repeats a task a fixed number of times with a fixed pause between them.
type Retry struct {
	Attempts int
	Interval time.Duration
}

func NewRetry(attempts int, interval time.Duration) *Retry {
	return &Retry{Attempts: attempts, Interval: interval}
}

// Run repeats task until it succeeds or the attempts run out.
func (r *Retry) Run(task func() error) (err error) {
	for i := 0; i < r.Attempts; i++ {
		err = task()
		if err == nil {
			return
		}
		time.Sleep(r.Interval)
	}
	return fmt.Errorf("failed after %d attempts: %w", r.Attempts, err)
}
