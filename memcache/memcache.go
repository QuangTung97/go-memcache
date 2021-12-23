package memcache

import (
	"context"
	"sync"
)

// Client ...
type Client struct {
	mut sync.Mutex

	conns []*conn
}

// New ...
func New(addr string, numConns int) (*Client, error) {
	conns := make([]*conn, 0, numConns)
	freeList := make([]int, numConns)

	for i := 0; i < numConns; i++ {
		c, err := newConn(addr)
		if err != nil {
			return nil, err
		}
		conns = append(conns, c)

		freeList[i] = i + 1
		if i == numConns-1 {
			freeList[i] = -1
		}
	}
	return &Client{
		conns: conns,
	}, nil
}

// Set ...
func (c *Client) Set(_ context.Context, _ string, _ []byte) error {
	return nil
}
