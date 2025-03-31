package memcache

import (
	"sync"
	"time"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
)

type clientConn struct {
	wg   sync.WaitGroup
	core *coreConnection

	cmdPool *pipelineCommandListPool

	maxCommandsPerBatch int

	mut       sync.Mutex
	closed    bool
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
