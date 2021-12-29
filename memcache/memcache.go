package memcache

import (
	"context"
	"sync/atomic"
)

// Client ...
type Client struct {
	conns []*conn
	next  uint64
}

// New ...
func New(addr string, numConns int) (*Client, error) {
	conns := make([]*conn, 0, numConns)

	for i := 0; i < numConns; i++ {
		c, err := newConn(addr)
		if err != nil {
			return nil, err
		}
		conns = append(conns, c)
	}

	return &Client{
		conns: conns,
		next:  0,
	}, nil
}

func (c *Client) getNextConn() *conn {
	next := atomic.AddUint64(&c.next, 1)
	return c.conns[next%uint64(len(c.conns))]
}

// Set ...
func (c *Client) Set(_ context.Context, _ string, _ []byte) error {
	return nil
}
