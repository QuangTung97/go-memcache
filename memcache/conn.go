package memcache

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type clientConn struct {
	wg   sync.WaitGroup
	core *coreConnection
}

// for testing
var globalNetDial = net.Dial

func netDialNewConn(addr string) (netConn, error) {
	nc, err := globalNetDial("tcp", addr)
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

func newConn(addr string, options ...Option) (*clientConn, error) {
	opts := computeOptions(options...)

	nc, err := netDialNewConn(addr)
	if err != nil {
		return nil, err
	}

	c := &clientConn{
		core: newCoreConnection(nc),
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			c.core.waitForError()
			if c.core.isShuttingDown() {
				return
			}

			nc, err := netDialNewConn(addr)
			if err != nil {
				time.Sleep(opts.retryDuration)
				continue
			}

			c.core.resetNetConn(nc)
		}
	}()

	return c, nil
}

func (c *clientConn) pushCommand(cmd *commandData) {
	c.core.publish(cmd)
}

func (c *clientConn) shutdown() error {
	c.core.shutdown()
	err := c.core.sender.closeNetConn()
	return err
}

func (c *clientConn) waitCloseCompleted() {
	c.wg.Wait()
	c.core.waitReceiverShutdown()
}
