package retry

import "time"

// Config holds the configuration for retry behavior.
type Config struct {
	// MaxAttempts is the maximum number of attempts before giving up.
	MaxAttempts int
	// InitialBackoff is the initial backoff duration before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff is the maximum backoff duration between retries.
	MaxBackoff time.Duration
	// BackoffMultiplier is the factor by which backoff increases each attempt.
	BackoffMultiplier float64
}

// DefaultConfig returns the default retry configuration.
func DefaultConfig() Config {
	return Config{
		MaxAttempts:       5,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

// Option is a functional option for configuring retry behavior.
type Option func(*Config)

// WithMaxAttempts sets the maximum number of retry attempts.
func WithMaxAttempts(n int) Option {
	return func(c *Config) {
		c.MaxAttempts = n
	}
}

// WithInitialBackoff sets the initial backoff duration.
func WithInitialBackoff(d time.Duration) Option {
	return func(c *Config) {
		c.InitialBackoff = d
	}
}

// WithMaxBackoff sets the maximum backoff duration.
func WithMaxBackoff(d time.Duration) Option {
	return func(c *Config) {
		c.MaxBackoff = d
	}
}

// WithBackoffMultiplier sets the backoff multiplier.
func WithBackoffMultiplier(m float64) Option {
	return func(c *Config) {
		c.BackoffMultiplier = m
	}
}

// Apply applies all options to the config.
func (c *Config) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(c)
	}
}
