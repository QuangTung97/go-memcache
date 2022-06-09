package memcache

import "time"

type memcacheOptions struct {
	retryDuration time.Duration
	bufferSize    int
}

// Option ...
type Option func(opts *memcacheOptions)

func computeOptions(options ...Option) *memcacheOptions {
	opts := &memcacheOptions{
		retryDuration: 10 * time.Second,
		bufferSize:    16 * 1024,
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
