package memcache

import (
	"sync"
	"time"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
)

// clientConn for controlling the TCP connection.
// It starts a background goroutine to reconnect to the memcached server after timeout.
type clientConn struct {
	wg   sync.WaitGroup
	core *coreConnection

	cmdPool *pipelineCommandListPool

	maxCommandsPerBatch int

	// following fields are used for shutdown process only
	mut       sync.Mutex
	closed    bool // to avoid closing closeChan more than once
	closeChan chan struct{}
}

func sleepWithCloseChan(d time.Duration, closeChan <-chan struct{}) (closed bool) {
	select {
	case _, ok := <-closeChan:
		return !ok
	case <-time.After(d):
		return false
	}
}

func newConn(
	addr string, cmdPool *pipelineCommandListPool, options ...Option,
) *clientConn {
	opts := computeOptions(options...)

	nc, err := netconn.DialNewConn(addr, opts.connOptions...)
	if err != nil {
		opts.dialErrorLogger(err)
		nc = netconn.ErrorNetConn(err)
	}

	c := &clientConn{
		core:      newCoreConnection(nc, opts),
		closeChan: make(chan struct{}),

		cmdPool: cmdPool,

		maxCommandsPerBatch: opts.maxCommandsPerBatch,
	}

	// start the background goroutine for reconnecting when the underling connection is broken
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			closed := c.core.waitForError()
			if closed {
				return
			}

			nc, err = netconn.DialNewConn(addr, opts.connOptions...)
			if err != nil {
				opts.dialErrorLogger(err)

				sleepWithCloseChan(opts.retryDuration, c.closeChan)
				continue
			}

			c.core.resetNetConn(nc)
			continue
		}
	}()

	return c
}

func (c *clientConn) pushCommand(cmd *commandListData) {
	c.core.publish(cmd)
}

func (c *clientConn) shutdown() {
	c.mut.Lock()
	if !c.closed {
		c.closed = true
		close(c.closeChan)
	}
	c.mut.Unlock()

	c.core.sender.closeSendJob()
}

func (c *clientConn) waitCloseCompleted() {
	c.wg.Wait()
	c.core.waitReceiverShutdown()
}
