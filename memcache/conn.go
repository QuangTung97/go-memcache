package memcache

import (
	"bufio"
	"net"
	"time"
)

type conn struct {
	core *coreConnection
}

func netDialNewConn(addr string) (netConn, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return netConn{}, err
	}

	writer := bufio.NewWriter(nc)
	return netConn{
		reader: nc,
		writer: writer,
		closer: nc,
	}, nil
}

func newConn(addr string) (*conn, error) {
	nc, err := netDialNewConn(addr)
	if err != nil {
		return nil, err
	}

	c := &conn{
		core: newCoreConnection(nc),
	}

	go func() {
		for {
			c.core.waitForError()
			if c.core.isShuttingDown() {
				return
			}

			nc, err := netDialNewConn(addr)
			if err != nil {
				time.Sleep(20 * time.Second)
				continue
			}

			c.core.resetNetConn(nc)
		}
	}()

	return c, nil
}

func (c *conn) pushCommand(cmd *commandData) {
	c.core.publish(cmd)
}

func (c *conn) shutdown() error {
	err := c.core.sender.closeNetConn()
	c.core.shutdown()
	return err
}
