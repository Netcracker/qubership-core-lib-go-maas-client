package util

import "time"

// Options are the bounds a client applies to every call to maas-agent. The zero
// value means the defaults, so a caller that sets nothing gets them.
type Options struct {
	// MaxTotalDuration bounds a whole call, retries included. Default 60s.
	MaxTotalDuration time.Duration
	// AttemptTimeout bounds one request of a call. Default 30s.
	AttemptTimeout time.Duration
}

// Option is one setting of a client.
type Option func(*Options)

// NewOptions applies options over the defaults.
func NewOptions(options ...Option) Options {
	var built Options
	for _, apply := range options {
		apply(&built)
	}
	return built
}

// WithMaxTotalDuration bounds a whole call, retries included. A non-positive
// value keeps the default.
func WithMaxTotalDuration(duration time.Duration) Option {
	return func(options *Options) { options.MaxTotalDuration = duration }
}

// WithAttemptTimeout bounds one request, so an agent that stops answering does
// not hold the whole duration. A non-positive value keeps the default.
func WithAttemptTimeout(duration time.Duration) Option {
	return func(options *Options) { options.AttemptTimeout = duration }
}
