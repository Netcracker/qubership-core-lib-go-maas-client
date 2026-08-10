package util

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_NewRetry(t *testing.T) {
	assertions := require.New(t)
	retry := NewRetry(1, 10*time.Millisecond)
	assertions.Equal(1, retry.Attempts)
	assertions.Equal(10*time.Millisecond, retry.Interval)
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
		} else {
			return nil
		}
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

func Test_ResponseClassifier_RetryableStatuses(t *testing.T) {
	assertions := require.New(t)

	// 405 and 401 are transient in the maas-agent chain, see IsRetryableStatus.
	for _, code := range []int{500, 503, 429, 405, 401} {
		assertions.False(IsNonRetryable(NewResponseClassifier().Classify(code, "status", "body")),
			"status %d must stay retryable", code)
	}
	for _, code := range []int{400, 403, 404, 409} {
		assertions.True(IsNonRetryable(NewResponseClassifier().Classify(code, "status", "body")),
			"status %d must be marked non-retryable", code)
	}
}

// 401 has its own budget, so the classifier must count them within one call.
func Test_ResponseClassifier_AuthBudgetIsTighter(t *testing.T) {
	assertions := require.New(t)

	c := NewResponseClassifier()
	for i := 0; i < MaxAuthRetries; i++ {
		assertions.False(IsNonRetryable(c.Classify(401, "status", "body")),
			"401 #%d must still be retryable, MaxAuthRetries is %d", i+1, MaxAuthRetries)
	}
	assertions.True(IsNonRetryable(c.Classify(401, "status", "body")),
		"401 must stop being retryable once MaxAuthRetries is used up")

	// The 401 budget must not eat into the generic one.
	assertions.False(IsNonRetryable(c.Classify(500, "status", "body")),
		"a 5xx after exhausted auth retries must still be retryable")

	// State is per call: a fresh classifier starts over.
	assertions.False(IsNonRetryable(NewResponseClassifier().Classify(401, "status", "body")),
		"a new classifier must not inherit the previous call's auth attempts")
}

func Test_RetryRunError(t *testing.T) {
	assertions := require.New(t)
	attempts := 10
	retry := NewRetry(attempts, 10*time.Millisecond)
	attempt := attempts + 1
	err := retry.Run(func() error {
		attempt--
		if attempt > 0 {
			return errors.New("test")
		} else {
			return nil
		}
	})
	assertions.Equal("failed after 10 retries: test", err.Error())
}
