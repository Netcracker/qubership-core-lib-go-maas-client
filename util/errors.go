package util

import (
	"fmt"
	"time"
)

// HttpError is the answer maas-agent gave when a call ended on a response the
// client cannot use. Reach it with errors.As to read the status.
type HttpError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *HttpError) Error() string {
	return fmt.Sprintf("maas-agent responded with status: %s, body: %s", e.Status, e.Body)
}

// RetriesExhaustedError ends a call that kept failing until the time allowed
// for it ran out. It separates an agent that never recovered from a request
// that was wrong to begin with, which fails on the first attempt.
type RetriesExhaustedError struct {
	Duration time.Duration
	Cause    error
}

func (e *RetriesExhaustedError) Error() string {
	return fmt.Sprintf("maas-agent call did not succeed within %s: %v", e.Duration, e.Cause)
}

func (e *RetriesExhaustedError) Unwrap() error { return e.Cause }
