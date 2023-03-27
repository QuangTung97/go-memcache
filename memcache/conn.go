package memcache

import (
	"github.com/QuangTung97/go-memcache/memcache/netconn"
	"sync"
	"time"
)

type clientConn struct {
	wg   sync.WaitGroup
	core *coreConnection

	closeChan chan struct{}
}

func sleepWithCloseChan(d time.Duration, closeChan <-chan struct{}) {
	select {
	case <-closeChan:
	case <-time.After(d):
	}
}

func newConn(addr string, options ...Option) *clientConn {
	opts := computeOptions(options...)

	nc, err := netconn.DialNewConn(addr, opts.connOptions...)
	if err != nil {
		opts.dialErrorLogger(err)
		nc = netconn.ErrorNetConn(err)
	}

	c := &clientConn{
		core:      newCoreConnection(nc, opts),
		closeChan: make(chan struct{}),
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		retryErrorCount := 0
		for {
			c.core.waitForError()
			if c.core.isShuttingDown() {
				return
			}

			nc, err = netconn.DialNewConn(addr, opts.connOptions...)
			if err != nil {
				opts.dialErrorLogger(err)

				if retryErrorCount == 0 {
					c.core.sender.increaseEpochAndSetError(err)
				}
				retryErrorCount++

				sleepWithCloseChan(opts.retryDuration, c.closeChan)
				continue
			}

			c.core.resetNetConn(nc)
			retryErrorCount = 0
			continue
		}
	}()

	return c
}

func (c *clientConn) pushCommand(cmd *commandData) {
	c.core.publish(cmd)
}

func (c *clientConn) shutdown() error {
	close(c.closeChan)
	c.core.shutdown()
	err := c.core.sender.closeNetConn()
	return err
}

func (c *clientConn) waitCloseCompleted() {
	c.wg.Wait()
	c.core.waitReceiverShutdown()
}
