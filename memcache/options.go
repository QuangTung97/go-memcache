package memcache

import "time"

type memcacheOptions struct {
	retryDuration time.Duration
	bufferSize    int

	tcpKeepAliveDuration time.Duration
	dialErrorLogger      func(err error)
}

// Option ...
type Option func(opts *memcacheOptions)

func computeOptions(options ...Option) *memcacheOptions {
	opts := &memcacheOptions{
		retryDuration: 10 * time.Second,
		bufferSize:    16 * 1024,

		tcpKeepAliveDuration: 5 * time.Minute,
		dialErrorLogger:      func(err error) {},
	}
	for _, o := range options {
		o(opts)
	}
	return opts
}

// WithRetryDuration duration between TCP connection retry
func WithRetryDuration(d time.Duration) Option {
	return func(opts *memcacheOptions) {
		opts.retryDuration = d
	}
}

// WithBufferSize change receiving & sending buffer size
func WithBufferSize(size int) Option {
	return func(opts *memcacheOptions) {
		opts.bufferSize = size
	}
}

// WithDialErrorLogger set the dial error logger
func WithDialErrorLogger(fn func(err error)) Option {
	return func(opts *memcacheOptions) {
		opts.dialErrorLogger = fn
	}
}

// WithTCPKeepAliveDuration sets the tcp keep alive duration
func WithTCPKeepAliveDuration(d time.Duration) Option {
	return func(opts *memcacheOptions) {
		opts.tcpKeepAliveDuration = d
	}
}
