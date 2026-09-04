package util

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_NewRetry(t *testing.T) {
	assertions := require.New(t)
	retry := NewRetry(1, 10*time.Millisecond)
	assertions.Equal(1, retry.Attempts)
	assertions.Equal(10*time.Millisecond, retry.Interval)
}

func Test_NewRetryWithin(t *testing.T) {
	assertions := require.New(t)
	assertions.Equal(5*time.Second, NewRetryWithin(5*time.Second).MaxTotal)
	assertions.Equal(DefaultMaxTotalDuration, NewRetryWithin(0).MaxTotal,
		"an unset total duration must fall back to the package default")
}

func Test_RetryRun(t *testing.T) {
	assertions := require.New(t)
	attempts := 10
	retry := NewRetry(attempts, 10*time.Millisecond)
	attempt := attempts
	err := retry.Run(func() error {
		attempt--
		if attempt > 0 {
			return errors.New("test")
		}
		return nil
	})
	assertions.NoError(err)
}

// A zero-valued Retry must still perform the request rather than skip it.
func Test_RetryRun_ZeroAttemptsFallsBackToDefault(t *testing.T) {
	assertions := require.New(t)
	retry := &Retry{} // built as a struct literal, no values set
	assertions.Equal(DefaultRetryAttempts, retry.AttemptsOrDefault())
	assertions.Equal(DefaultRetryInterval, retry.IntervalOrDefault())

	calls := 0
	err := retry.Run(func() error {
		calls++
		return nil
	})
	assertions.NoError(err)
	assertions.Equal(1, calls, "a zero-valued Retry must still run the task once")
}

func Test_RetryRunError(t *testing.T) {
	assertions := require.New(t)
	retry := NewRetry(3, 10*time.Millisecond)
	calls := 0
	err := retry.Run(func() error {
		calls++
		return errors.New("test")
	})
	assertions.EqualError(err, "test", "the last failure is what the caller gets")
	assertions.Equal(3, calls)
}

// MarkNonRetryable must end the loop on the first failure.
func Test_RetryRun_NonRetryableStopsImmediately(t *testing.T) {
	assertions := require.New(t)
	calls := 0
	err := NewRetry(5, time.Millisecond).Run(func() error {
		calls++
		return MarkNonRetryable(errors.New("bad request"))
	})
	assertions.Error(err)
	assertions.Equal(1, calls, "a non-retryable error must not be repeated")
}

// The total duration is the only stop condition on the MaxTotal path.
func Test_RetryRunCtx_StopsWhenTheTotalDurationIsSpent(t *testing.T) {
	assertions := require.New(t)
	calls := 0
	start := time.Now()
	err := NewRetryWithin(300*time.Millisecond).RunCtx(context.Background(), func(context.Context) error {
		calls++
		return errors.New("unavailable")
	})
	elapsed := time.Since(start)

	assertions.Error(err)
	assertions.Greater(calls, 1, "the total duration must fit more than the first attempt")
	assertions.Less(elapsed, 3*time.Second, "retries must stop with the total duration, took %s", elapsed)
}

// The deadline rides on the context, so the task can derive an attempt timeout
// that cannot overrun it.
func Test_RetryRunCtx_TaskContextCarriesTheDeadline(t *testing.T) {
	assertions := require.New(t)
	err := NewRetryWithin(time.Second).RunCtx(context.Background(), func(ctx context.Context) error {
		_, ok := ctx.Deadline()
		assertions.True(ok, "the task context must carry the deadline")
		return nil
	})
	assertions.NoError(err)
}

func Test_ResponseClassifier_RetryableStatuses(t *testing.T) {
	assertions := require.New(t)

	for _, code := range []int{500, 503, 429} {
		assertions.False(IsNonRetryable(NewResponseClassifier().Classify(code, "status", "body")),
			"status %d must stay retryable", code)
	}
	for _, code := range []int{400, 401, 403, 404, 405, 409} {
		assertions.True(IsNonRetryable(NewResponseClassifier().Classify(code, "status", "body")),
			"status %d must be marked non-retryable", code)
	}
}

// 405 is retryable only when the reason names a database that cannot be written.
func Test_ResponseClassifier_405IsGatedByTheReason(t *testing.T) {
	assertions := require.New(t)

	// the two errors maas-service maps to 405, verbatim, and reworded
	retryable := []string{
		`{"code":"MAAS-0600","reason":"database is in read-only mode"}`,
		`{"code":"MAAS-0600","reason":"database is not in 'active' mode"}`,
		`{"reason":"Database is read only"}`,
		`{"reason":"the database is not active"}`,
	}
	for _, body := range retryable {
		assertions.True(IsRetryableStatus(405, body), "must be retryable: %s", body)
	}

	notRetryable := []string{
		`{"code":"MAAS-0600","reason":"Method Not Allowed"}`,
		`{"code":"MAAS-0600","reason":"classifier is invalid"}`,
		`{"reason":"the read-only field cannot be updated"}`, // no database in the reason
		`{"message":"database is in read-only mode"}`,        // not the reason field
		`<html>405 Method Not Allowed</html>`,                // not maas-service at all
		``,
	}
	for _, body := range notRetryable {
		assertions.False(IsRetryableStatus(405, body), "must not be retryable: %s", body)
	}
}
