package memcache

import (
	"bufio"
	"fmt"
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
		nc: nc,
		core: newCoreConnection(netConn{
			reader: nc,
			writer: writer,
			closer: nc,
		}),
	}, nil
}

func (c *conn) pushCommand(cmd *commandData) {
	c.core.publish(cmd)
}

func (c *conn) shutdown() error {
	err := c.core.sender.closeNetConn()
	fmt.Println("CLOSE CONN:", err)
	c.core.shutdown()
	return err
}
