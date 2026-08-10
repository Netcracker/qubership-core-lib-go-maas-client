package util

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	DefaultRetryAttempts = 30
	DefaultRetryInterval = time.Second
	// DefaultMaxRetryInterval caps growing backoff strategies built on top of
	// DefaultRetryInterval, so a long outage cannot stretch the delay without bound.
	DefaultMaxRetryInterval = 30 * time.Second
)

// Retry runs a task with a bounded number of attempts.
//
// Non-positive Attempts or Interval fall back to the package defaults: a zero
// value must never degrade into "make no attempt at all", which would silently
// skip the request instead of performing it once.
type Retry struct {
	Attempts int
	Interval time.Duration
}

func NewRetry(attempts int, interval time.Duration) *Retry {
	return &Retry{Attempts: attempts, Interval: interval}
}

// AttemptsOrDefault reports the effective attempt count, see Retry.
func (r *Retry) AttemptsOrDefault() int {
	if r.Attempts <= 0 {
		return DefaultRetryAttempts
	}
	return r.Attempts
}

// IntervalOrDefault reports the effective interval, see Retry.
func (r *Retry) IntervalOrDefault() time.Duration {
	if r.Interval <= 0 {
		return DefaultRetryInterval
	}
	return r.Interval
}

// nonRetryableError marks err as final so Run/RunCtx stop retrying.
type nonRetryableError struct {
	err error
}

func (e *nonRetryableError) Error() string { return e.err.Error() }
func (e *nonRetryableError) Unwrap() error { return e.err }

// MarkNonRetryable wraps err so Run/RunCtx stop retrying immediately instead
// of exhausting the retry budget on it.
func MarkNonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

// IsNonRetryable reports whether err (or anything it wraps) was marked via
// MarkNonRetryable.
func IsNonRetryable(err error) bool {
	var nre *nonRetryableError
	return errors.As(err, &nre)
}

// IsRetryableStatus reports whether an HTTP status code from maas-agent
// should be retried.
//
// Two 4xx codes are deliberately included. Both are transient in this specific
// chain rather than permanent client errors:
//
//   - 405: maas-service maps PG error 25006 (READ ONLY SQL TRANSACTION) to
//     StatusMethodNotAllowed, so a write against a demoted Patroni node during
//     a leader switchover arrives here as 405, not as 5xx.
//   - 401: the M2M token is re-fetched on every attempt (addAuthToken runs
//     inside the retry closure), so an expired token or a briefly unavailable
//     token provider resolves itself on the next attempt.
//
// Keep the codes and their reasons together: both look obviously wrong to
// anyone applying the usual "retry 5xx, fail fast on 4xx" rule, and will be
// removed as dead weight if the reasons are not right here.
func IsRetryableStatus(statusCode int) bool {
	if statusCode >= 500 {
		return true
	}
	switch statusCode {
	case 429, 405, 401:
		return true
	default:
		return false
	}
}

// ClassifyResponseError turns a non-2xx maas-agent response into an error,
// marking it non-retryable unless the status is retryable per
// IsRetryableStatus.
//
// Takes the response fields rather than a *resty.Response so this package
// stays free of HTTP client dependencies.
func ClassifyResponseError(statusCode int, status, body string) error {
	err := fmt.Errorf("response with error code received. Status: %s, body: %s", status, body)
	if IsRetryableStatus(statusCode) {
		return err
	}
	return MarkNonRetryable(err)
}

// Run executes task up to Attempts times, sleeping Interval between
// attempts, until it succeeds or the budget is exhausted. Errors marked via
// MarkNonRetryable stop the loop immediately.
func (r *Retry) Run(task func() error) error {
	return r.RunCtx(context.Background(), func(context.Context) error {
		return task()
	})
}

// RunCtx is like Run but also aborts as soon as ctx is done, and does not
// sleep after the last attempt.
func (r *Retry) RunCtx(ctx context.Context, task func(ctx context.Context) error) error {
	attempts := r.AttemptsOrDefault()
	interval := r.IntervalOrDefault()

	var err error
	for i := 0; i < attempts; i++ {
		select {
		case <-ctx.Done():
			return ctxDoneErr(i, attempts, ctx.Err(), err)
		default:
		}

		err = task(ctx)
		if err == nil {
			return nil
		}
		if IsNonRetryable(err) {
			return err
		}
		if i == attempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return ctxDoneErr(i+1, attempts, ctx.Err(), err)
		case <-time.After(interval):
		}
	}
	return fmt.Errorf("failed after %d retries: %w", attempts, err)
}

// ctxDoneErr keeps both causes in the chain: callers may match either the
// context cause (deadline/cancellation) or the last transport/status error.
func ctxDoneErr(attemptsMade, attemptsTotal int, ctxErr, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("retry aborted before first attempt (%d/%d): %w", attemptsMade, attemptsTotal, ctxErr)
	}
	return fmt.Errorf("retry aborted after %d/%d attempts: %w", attemptsMade, attemptsTotal, errors.Join(ctxErr, lastErr))
}
