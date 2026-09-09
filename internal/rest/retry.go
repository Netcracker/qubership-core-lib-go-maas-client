package rest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
)

const (
	// defaultMaxTotalDuration bounds a whole call, retries included. It is meant to
	// outlast a database leader switchover and still fail before a real outage
	// starts to look like a hang.
	defaultMaxTotalDuration = time.Minute
	// defaultAttemptTimeout bounds a single request, see attemptContext.
	defaultAttemptTimeout = 30 * time.Second
	// initialRetryInterval is the first pause; each next one doubles.
	initialRetryInterval = time.Second
	// maxDelayFractionOfTotal caps a single pause, so none can eat the total.
	maxDelayFractionOfTotal = 4
	// minRetryInterval keeps a very short total duration from becoming a busy loop.
	minRetryInterval = 10 * time.Millisecond
	// jitterFactor keeps callers that failed together from returning together.
	jitterFactor = 0.2
)

// attemptContext bounds one attempt, so a caller passing context.Background() is
// still bounded. A shorter caller deadline wins.
func attemptContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = defaultAttemptTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

// retry repeats task until it succeeds, parent is done, or maxTotal is spent.
// An error marked non-retryable stops it immediately; running out of the
// duration is reported as util.RetriesExhaustedError.
func retry(parent context.Context, maxTotal time.Duration, task func(ctx context.Context) error) error {
	if maxTotal <= 0 {
		maxTotal = defaultMaxTotalDuration
	}
	// the deadline rides on the context, so a per-attempt timeout derived from it
	// cannot overrun what is left
	ctx, cancel := context.WithTimeout(parent, maxTotal)
	defer cancel()

	err := failsafe.With[any](retryPolicy(maxTotal)).WithContext(ctx).Run(func() error {
		return task(ctx)
	})
	// what is left once a non-retryable error and the caller's own cancellation
	// are excluded is the duration running out
	if err == nil || isNonRetryable(err) || parent.Err() != nil {
		return err
	}
	return &util.RetriesExhaustedError{Duration: maxTotal, Cause: err}
}

// retryPolicy grows and jitters the pause, capping it at a fraction of the total
// and stopping once the total is spent.
func retryPolicy(maxTotal time.Duration) retrypolicy.RetryPolicy[any] {
	maxDelay := max(maxTotal/maxDelayFractionOfTotal, minRetryInterval)
	builder := retrypolicy.NewBuilder[any]().
		HandleIf(func(_ any, err error) bool { return err != nil && !isNonRetryable(err) }).
		ReturnLastFailure().
		WithJitterFactor(jitterFactor).
		WithMaxAttempts(-1).
		WithMaxDuration(maxTotal)
	// a growing pause needs room to grow; a short total gets a flat one
	if initialRetryInterval < maxDelay {
		return builder.WithBackoff(initialRetryInterval, maxDelay).Build()
	}
	return builder.WithDelay(maxDelay).Build()
}

// nonRetryableError marks err as final so retry stops repeating it.
type nonRetryableError struct {
	err error
}

func (e *nonRetryableError) Error() string { return e.err.Error() }
func (e *nonRetryableError) Unwrap() error { return e.err }

func markNonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

func isNonRetryable(err error) bool {
	var marked *nonRetryableError
	return errors.As(err, &marked)
}

// classifyResponse turns a non-2xx maas-agent response into an error, marking it
// non-retryable where retrying cannot help.
func classifyResponse(statusCode int, status, body string) error {
	err := &util.HttpError{StatusCode: statusCode, Status: status, Body: body}
	if isRetryableStatus(statusCode, body) {
		return err
	}
	return markNonRetryable(err)
}

// isRetryableStatus reports whether a maas-agent response should be retried. 405
// is the one 4xx worth repeating, because maas-service reports a read-only
// database that way; a plain 405 is permanent.
func isRetryableStatus(statusCode int, body string) bool {
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
