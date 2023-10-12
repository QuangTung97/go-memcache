package memcache

import (
	"log"
	"net"
	"time"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
)

type memcacheOptions struct {
	retryDuration time.Duration
	bufferSize    int
	writeLimit    int

	maxCommandsPerBatch int

	healthCheckDuration time.Duration

	dialErrorLogger func(err error)

	connOptions []netconn.Option
}

func (o *memcacheOptions) addConnOption(options ...netconn.Option) {
	o.connOptions = append(o.connOptions, options...)
}

// Option ...
type Option func(opts *memcacheOptions)

func computeOptions(options ...Option) *memcacheOptions {
	opts := &memcacheOptions{
		retryDuration: 10 * time.Second,
		bufferSize:    16 * 1024,
		writeLimit:    500,

		maxCommandsPerBatch: 100,

		healthCheckDuration: 15 * time.Second,

		dialErrorLogger: func(err error) {
			log.Println("[ERROR] Memcache dial error:", err)
		},
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
		opts.addConnOption(netconn.WithBufferSize(size))
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
		opts.addConnOption(netconn.WithTCPKeepAliveDuration(d))
	}
}

// WithDialFunc ...
func WithDialFunc(dialFunc func(network, address string, timeout time.Duration) (net.Conn, error)) Option {
	return func(opts *memcacheOptions) {
		opts.addConnOption(netconn.WithDialFunc(dialFunc))
	}
}

// WithNetConnOptions ...
func WithNetConnOptions(options ...netconn.Option) Option {
	return func(opts *memcacheOptions) {
		opts.addConnOption(options...)
	}
}

// WithWriteLimit limit the number of concurrent operations send to memcached on one go
func WithWriteLimit(limit int) Option {
	return func(opts *memcacheOptions) {
		opts.writeLimit = limit
	}
}

// WithMaxCommandsPerBatch specifies the number of commands each batch will contain, if a pipeline has more command
// than this value, it will be split to multiple commands to avoid starvation
func WithMaxCommandsPerBatch(maxCommands int) Option {
	return func(opts *memcacheOptions) {
		opts.maxCommandsPerBatch = maxCommands
	}
}

// WithHealthCheckDuration specifies duration in which health check will be called after connections have no activity
// default is 15 seconds
func WithHealthCheckDuration(duration time.Duration) Option {
	return func(opts *memcacheOptions) {
		opts.healthCheckDuration = duration
	}
}
