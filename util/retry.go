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
	// DefaultMaxRetryInterval caps growing backoff strategies.
	DefaultMaxRetryInterval = 30 * time.Second
	// DefaultAttemptTimeout bounds a single request, see AttemptContext.
	DefaultAttemptTimeout = 30 * time.Second
)

// AttemptContext derives the context for one attempt, so that a caller passing
// context.Background() is still bounded. A shorter caller deadline wins.
func AttemptContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = DefaultAttemptTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

// Retry runs a task with a bounded number of attempts.
// Non-positive Attempts or Interval fall back to the package defaults.
type Retry struct {
	Attempts int
	Interval time.Duration
}

func NewRetry(attempts int, interval time.Duration) *Retry {
	return &Retry{Attempts: attempts, Interval: interval}
}

// AttemptsOrDefault reports the effective attempt count.
func (r *Retry) AttemptsOrDefault() int {
	if r.Attempts <= 0 {
		return DefaultRetryAttempts
	}
	return r.Attempts
}

// IntervalOrDefault reports the effective interval.
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
// of spending every remaining attempt on it.
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

// IsRetryableStatus reports whether a maas-agent status code should be retried.
//
// Two 4xx are transient here rather than permanent: 405 is how maas-service
// reports a read-only Postgres during a leader switchover, and 401 clears when
// the token is re-fetched on the next attempt.
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

// MaxAuthRetries bounds how many times a single call retries a 401.
//
// One is enough: it covers a token that expired in flight. A token the provider
// still considers valid but the server rejects comes back identical on every
// further attempt, since TokenProvider cannot be told it was rejected.
const MaxAuthRetries = 1

// ResponseClassifier turns non-2xx maas-agent responses into errors, marking
// them non-retryable where retrying cannot help.
//
// Stateful because 401 has its own attempt limit, so create one per call.
type ResponseClassifier struct {
	authAttempts int
}

func NewResponseClassifier() *ResponseClassifier {
	return &ResponseClassifier{}
}

func (c *ResponseClassifier) Classify(statusCode int, status, body string) error {
	err := fmt.Errorf("response with error code received. Status: %s, body: %s", status, body)
	if statusCode == 401 {
		c.authAttempts++
		if c.authAttempts > MaxAuthRetries {
			return MarkNonRetryable(err)
		}
		return err
	}
	if IsRetryableStatus(statusCode) {
		return err
	}
	return MarkNonRetryable(err)
}

// Run executes task up to Attempts times, sleeping Interval between
// attempts, until it succeeds or the attempts run out. Errors marked via
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

// ctxDoneErr builds the abort error, keeping both the context cause and the
// last attempt error matchable via errors.Is.
func ctxDoneErr(attemptsMade, attemptsTotal int, ctxErr, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("retry aborted before first attempt (%d/%d): %w", attemptsMade, attemptsTotal, ctxErr)
	}
	return fmt.Errorf("retry aborted after %d/%d attempts: %w", attemptsMade, attemptsTotal, errors.Join(ctxErr, lastErr))
}
