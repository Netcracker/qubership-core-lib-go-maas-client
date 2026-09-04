package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
)

var (
	DefaultRetryAttempts = 30
	DefaultRetryInterval = time.Second
	// DefaultMaxRetryInterval caps growing backoff strategies.
	DefaultMaxRetryInterval = 30 * time.Second
	// DefaultAttemptTimeout bounds a single request, see AttemptContext.
	DefaultAttemptTimeout = 30 * time.Second
	// DefaultMaxTotalDuration bounds a whole call, retries included.
	DefaultMaxTotalDuration = time.Minute
)

const (
	// maxDelayFractionOfTotal caps a single pause, so none can eat the total.
	maxDelayFractionOfTotal = 4
	// minRetryInterval keeps a very short total duration from becoming a busy loop.
	minRetryInterval = 10 * time.Millisecond
	// retryJitterFactor keeps callers that failed together from returning together.
	retryJitterFactor = 0.2
)

// AttemptContext bounds one attempt, so a caller passing context.Background()
// is still bounded. A shorter caller deadline wins.
func AttemptContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = DefaultAttemptTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

// Retry runs a task until it succeeds or its bound is reached. MaxTotal bounds
// the whole call and derives the pauses; Attempts and Interval are the older
// count-and-fixed-delay pair, used only when MaxTotal is unset.
type Retry struct {
	Attempts int
	Interval time.Duration
	MaxTotal time.Duration
}

// NewRetry builds a count-bounded Retry. Non-positive values fall back to the
// package defaults.
func NewRetry(attempts int, interval time.Duration) *Retry {
	return &Retry{Attempts: attempts, Interval: interval}
}

// NewRetryWithin builds a Retry bounded by the total duration of one call. A
// non-positive value falls back to DefaultMaxTotalDuration.
func NewRetryWithin(maxTotal time.Duration) *Retry {
	if maxTotal <= 0 {
		maxTotal = DefaultMaxTotalDuration
	}
	return &Retry{MaxTotal: maxTotal}
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

// MarkNonRetryable wraps err so Run/RunCtx stop retrying immediately.
func MarkNonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

// IsNonRetryable reports whether err, or anything it wraps, was marked.
func IsNonRetryable(err error) bool {
	var nre *nonRetryableError
	return errors.As(err, &nre)
}

// IsRetryableStatus reports whether a maas-agent response should be retried.
// 405 is the one 4xx worth repeating, because maas-service reports a read-only
// database that way; a plain 405 is permanent.
func IsRetryableStatus(statusCode int, body string) bool {
	if statusCode >= 500 || statusCode == 429 {
		return true
	}
	return statusCode == 405 && isDatabaseUnavailable(body)
}

// databaseUnavailableMarkers cover the two maas-service errors a leader
// switchover produces, and rewordings of them.
var databaseUnavailableMarkers = []string{"read-only", "read only", "not in 'active' mode", "not active"}

// isDatabaseUnavailable reads the reason of a maas-service error envelope. The
// word "database" is required next to the marker, so an unrelated 405 that
// happens to mention read-only data stays permanent.
func isDatabaseUnavailable(body string) bool {
	var envelope struct {
		Reason string `json:"reason"`
	}
	if json.Unmarshal([]byte(body), &envelope) != nil {
		return false
	}
	reason := strings.ToLower(envelope.Reason)
	if !strings.Contains(reason, "database") {
		return false
	}
	return slices.ContainsFunc(databaseUnavailableMarkers, func(marker string) bool {
		return strings.Contains(reason, marker)
	})
}

// ResponseClassifier turns non-2xx maas-agent responses into errors, marking
// them non-retryable where retrying cannot help.
type ResponseClassifier struct{}

func NewResponseClassifier() *ResponseClassifier {
	return &ResponseClassifier{}
}

func (c *ResponseClassifier) Classify(statusCode int, status, body string) error {
	err := fmt.Errorf("response with error code received. Status: %s, body: %s", status, body)
	if IsRetryableStatus(statusCode, body) {
		return err
	}
	return MarkNonRetryable(err)
}

// Run repeats task until it succeeds or the configured bound is reached.
// Errors marked via MarkNonRetryable stop it immediately.
func (r *Retry) Run(task func() error) error {
	return r.RunCtx(context.Background(), func(context.Context) error {
		return task()
	})
}

// RunCtx is like Run but also aborts as soon as ctx is done.
func (r *Retry) RunCtx(ctx context.Context, task func(ctx context.Context) error) error {
	if r.MaxTotal <= 0 {
		return r.run(ctx, withAttempts(r.AttemptsOrDefault(), r.IntervalOrDefault()), task)
	}
	// the deadline rides on the context, so a per-attempt timeout derived from it
	// cannot overrun what is left
	ctx, cancel := context.WithTimeout(ctx, r.MaxTotal)
	defer cancel()
	return r.run(ctx, withinDuration(r.MaxTotal), task)
}

func (r *Retry) run(ctx context.Context, policy retrypolicy.RetryPolicy[any],
	task func(ctx context.Context) error) error {
	return failsafe.With[any](policy).WithContext(ctx).Run(func() error {
		return task(ctx)
	})
}

// withinDuration grows and jitters the pause, capping it at a fraction of the
// total and stopping once the total is spent.
func withinDuration(total time.Duration) retrypolicy.RetryPolicy[any] {
	maxDelay := max(total/maxDelayFractionOfTotal, minRetryInterval)
	builder := retryable().
		WithJitterFactor(retryJitterFactor).
		WithMaxAttempts(-1).
		WithMaxDuration(total)
	// a growing pause needs room to grow; a short total gets a flat one
	if DefaultRetryInterval < maxDelay {
		builder = builder.WithBackoff(DefaultRetryInterval, maxDelay)
	} else {
		builder = builder.WithDelay(maxDelay)
	}
	return builder.Build()
}

// withAttempts is the older bound: a fixed pause, a fixed attempt count.
func withAttempts(attempts int, interval time.Duration) retrypolicy.RetryPolicy[any] {
	return retryable().
		WithDelay(interval).
		WithMaxAttempts(attempts).
		Build()
}

func retryable() retrypolicy.Builder[any] {
	return retrypolicy.NewBuilder[any]().
		HandleIf(func(_ any, err error) bool { return err != nil && !IsNonRetryable(err) }).
		ReturnLastFailure()
}
