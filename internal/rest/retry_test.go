package rest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/netcracker/qubership-core-lib-go-maas-client/v3/util"
	"github.com/stretchr/testify/require"
)

// A call that ran out of its duration is told apart from one that failed on the
// first attempt, and still carries the last failure.
func Test_Retry_ReportsRetriesExhausted(t *testing.T) {
	assertions := require.New(t)
	last := errors.New("test")
	calls := 0
	err := retry(context.Background(), 200*time.Millisecond, func(context.Context) error {
		calls++
		return last
	})

	var exhausted *util.RetriesExhaustedError
	assertions.ErrorAs(err, &exhausted)
	assertions.Equal(200*time.Millisecond, exhausted.Duration)
	assertions.ErrorIs(err, last, "the last failure must stay reachable")
	assertions.Greater(calls, 1, "the total duration must fit more than the first attempt")
}

// A caller that cancels gets its own error, not an exhausted duration.
func Test_Retry_CallerCancellationIsNotExhaustion(t *testing.T) {
	assertions := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := retry(ctx, time.Minute, func(context.Context) error {
		return errors.New("test")
	})

	var exhausted *util.RetriesExhaustedError
	assertions.Error(err)
	assertions.False(errors.As(err, &exhausted), "a cancelled caller is not an exhausted call")
}

// markNonRetryable must end the loop on the first failure.
func Test_Retry_NonRetryableStopsImmediately(t *testing.T) {
	assertions := require.New(t)
	calls := 0
	err := retry(context.Background(), time.Minute, func(context.Context) error {
		calls++
		return markNonRetryable(errors.New("bad request"))
	})
	assertions.Error(err)
	assertions.Equal(1, calls, "a non-retryable error must not be repeated")
}

// An unset duration falls back to the default rather than to no retrying.
func Test_Retry_UnsetDurationUsesTheDefault(t *testing.T) {
	assertions := require.New(t)
	err := retry(context.Background(), 0, func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		assertions.True(ok, "the task context must carry the deadline")
		assertions.WithinDuration(time.Now().Add(defaultMaxTotalDuration), deadline, time.Second)
		return nil
	})
	assertions.NoError(err)
}

// attemptContext bounds an attempt even when the caller passed no deadline, and
// yields to a shorter one.
func Test_AttemptContext(t *testing.T) {
	assertions := require.New(t)

	ctx, cancel := attemptContext(context.Background(), 0)
	defer cancel()
	deadline, ok := ctx.Deadline()
	assertions.True(ok, "an unbounded caller context must still get a deadline")
	assertions.WithinDuration(time.Now().Add(defaultAttemptTimeout), deadline, time.Second)

	callerCtx, cancelCaller := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelCaller()
	ctx, cancel = attemptContext(callerCtx, time.Hour)
	defer cancel()
	deadline, _ = ctx.Deadline()
	assertions.WithinDuration(time.Now().Add(50*time.Millisecond), deadline, time.Second,
		"a shorter caller deadline wins")
}

// The status survives into the returned error, so a caller can tell a 404 from a
// 409 without parsing the message.
func Test_ClassifyResponse_CarriesTheResponse(t *testing.T) {
	assertions := require.New(t)

	var httpErr *util.HttpError
	assertions.ErrorAs(classifyResponse(409, "409 Conflict", "taken"), &httpErr)
	assertions.Equal(409, httpErr.StatusCode)
	assertions.Equal("409 Conflict", httpErr.Status)
	assertions.Equal("taken", httpErr.Body)
}

func Test_ClassifyResponse_RetryableStatuses(t *testing.T) {
	assertions := require.New(t)

	for _, code := range []int{500, 503, 429} {
		assertions.False(isNonRetryable(classifyResponse(code, "status", "body")),
			"status %d must stay retryable", code)
	}
	for _, code := range []int{400, 401, 403, 404, 405, 409} {
		assertions.True(isNonRetryable(classifyResponse(code, "status", "body")),
			"status %d must be marked non-retryable", code)
	}
}

// 405 is retryable only when the reason names a database that cannot be written.
func Test_ClassifyResponse_405IsGatedByTheReason(t *testing.T) {
	assertions := require.New(t)

	// the two errors maas-service maps to 405, verbatim, and reworded
	retryable := []string{
		`{"code":"MAAS-0600","reason":"database is in read-only mode"}`,
		`{"code":"MAAS-0600","reason":"database is not in 'active' mode"}`,
		`{"reason":"Database is read only"}`,
		`{"reason":"the database is not active"}`,
	}
	for _, body := range retryable {
		assertions.True(isRetryableStatus(405, body), "must be retryable: %s", body)
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
		assertions.False(isRetryableStatus(405, body), "must not be retryable: %s", body)
	}
}
