package util

import (
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

func Test_RetryRunError(t *testing.T) {
	assertions := require.New(t)
	retry := NewRetry(3, 10*time.Millisecond)
	calls := 0
	err := retry.Run(func() error {
		calls++
		return errors.New("test")
	})
	assertions.EqualError(err, "failed after 3 attempts: test")
	assertions.Equal(3, calls)
}
