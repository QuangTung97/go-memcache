package memcache

import (
	"sync"
)

// sendBuffer is a singly-linked list of commandListData.
// It's the buffer that receives command from pipelineSession when flushing.
type sendBuffer struct {
	firstCmd   *commandListData
	nextCmdPtr **commandListData
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

func (b *sendBuffer) push(cmd *commandListData) (closed bool) {
	b.mut.Lock()

	if b.closed {
		b.mut.Unlock()
		return true
	}

	needSignal := b.firstCmd == nil

	cmd.link = nil
	*b.nextCmdPtr = cmd
	b.nextCmdPtr = &cmd.link

	b.mut.Unlock()

	if needSignal {
		b.cond.Signal()
	}

	return false
}

func (b *sendBuffer) popAll(waiting bool) (cmdList *commandListData, closed bool) {
	b.mut.Lock()

	for waiting && !b.closed && b.firstCmd == nil {
		b.cond.Wait()
	}

	closed = b.closed
	result := b.firstCmd

	b.clearPointer()

	b.mut.Unlock()
	return result, closed
}

func (b *sendBuffer) close() {
	b.mut.Lock()
	b.closed = true
	b.mut.Unlock()
	b.cond.Signal()
}
