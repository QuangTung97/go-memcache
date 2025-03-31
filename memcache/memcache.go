package memcache

import (
	"errors"
	"sync/atomic"
)

// Client represents a pool of TCP connections to a memcached server
type Client struct {
	conns []*clientConn // pool of TCP connections
	next  atomic.Uint64 // increase by one for each time a new **Pipeline** is created

	health *healthCheckService
}

// New creates a Client that contains a pool of TCP connections.
// Number of TCP connections specified by **numConns** param.
func New(addr string, numConns int, options ...Option) (*Client, error) {
	if numConns <= 0 {
		return nil, errors.New("numConns must > 0")
	}

	cmdPool := newPipelineCommandListPool(options...)

	conns := make([]*clientConn, 0, numConns)
	for i := 0; i < numConns; i++ {
		c := newConn(addr, cmdPool, options...)
		conns = append(conns, c)
	}

	client := &Client{
		conns: conns,
	}

	opts := computeOptions(options...)

	client.health = newHealthCheckService(
		conns,
		client.next.Load,
		client.next.Add,
		opts.healthCheckDuration,
	)
	client.health.runInBackground()

	return client, nil
}

// Close shuts down Client.
// It waits for all the background goroutines to finish before returning
func (c *Client) Close() error {
	c.health.shutdown()

	for _, conn := range c.conns {
		conn.shutdown()
	}
	for _, conn := range c.conns {
		conn.waitCloseCompleted()
	}
	return nil
}

func (c *Client) getNextConn() *clientConn {
	next := c.next.Add(1)
	return c.conns[next%uint64(len(c.conns))]
}
