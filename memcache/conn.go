package memcache

import (
	"bufio"
	"net"
)

type conn struct {
	nc   net.Conn
	core *coreConnection
}

func newConn(addr string) (*conn, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(nc)
	return &conn{
		nc:   nc,
		core: newCoreConnection(nc, writer),
	}, nil
}

func (c *conn) pushCommand(cmd *commandData) error {
	return c.core.publish(cmd)
}
