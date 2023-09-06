package memcache

import (
	"sync"
)

type sendBuffer struct {
	firstCmd   *commandData
	nextCmdPtr **commandData
	closed     bool
	mut        sync.Mutex
	cond       *sync.Cond
}

func initSendBuffer(b *sendBuffer) {
	b.cond = sync.NewCond(&b.mut)
	b.clearPointer()
}

func (b *sendBuffer) clearPointer() {
	b.firstCmd = nil
	b.nextCmdPtr = &b.firstCmd
}

func (b *sendBuffer) push(cmd *commandData) (closed bool) {
	b.mut.Lock()

	if b.closed {
		b.mut.Unlock()
		return true
	}

	cmd.link = nil
	*b.nextCmdPtr = cmd
	b.nextCmdPtr = &cmd.link

	b.mut.Unlock()

	b.cond.Signal()

	return false
}

func (b *sendBuffer) popAll() (cmdList *commandData, closed bool) {
	b.mut.Lock()

	for !b.closed && b.firstCmd == nil {
		b.cond.Wait()
	}

	closed = b.closed
	result := b.firstCmd

	b.clearPointer()

	b.mut.Unlock()
	return result, closed
}

func (b *sendBuffer) close() {
	b.closed = true
}