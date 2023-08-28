package memcache

import (
	"errors"
	"sync/atomic"
)

// Client ...
type Client struct {
	conns []*clientConn
	next  uint64
}

// New ...
func New(addr string, numConns int, options ...Option) (*Client, error) {
	if numConns <= 0 {
		return nil, errors.New("numConns must > 0")
	}

	conns := make([]*clientConn, 0, numConns)

	for i := 0; i < numConns; i++ {
		c := newConn(addr, options...)
		conns = append(conns, c)
	}

	return &Client{
		conns: conns,
		next:  0,
	}, nil
}

// Close shut down Client
func (c *Client) Close() error {
	for _, conn := range c.conns {
		conn.shutdown()
	}
	for _, conn := range c.conns {
		conn.waitCloseCompleted()
	}
	return nil
}

func (c *Client) getNextConn() *clientConn {
	next := atomic.AddUint64(&c.next, 1)
	return c.conns[next%uint64(len(c.conns))]
}
