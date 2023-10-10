package memcache

import (
	"errors"
	"sync/atomic"
)

// Client ...
type Client struct {
	conns []*clientConn
	next  atomic.Uint64

	health *healthCheckService
}

// New ...
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

	client.health = newHealthCheckService(conns, client.next.Load, opts.healthCheckDuration)
	client.health.runInBackground()

	return client, nil
}

// Close shut down Client
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
