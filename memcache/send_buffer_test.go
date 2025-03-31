package memcache

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newSendBuffer() *sendBuffer {
	b := &sendBuffer{}
	initSendBuffer(b)
	return b
}

func TestSendBuffer(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		b := newSendBuffer()
		var closed bool

		closed = b.push(newCommandFromString("mg key01"))
		assert.Equal(t, false, closed)

		closed = b.push(newCommandFromString("mg key02"))
		assert.Equal(t, false, closed)

		cmdList, closed := b.popAll(true)
		assert.Equal(t, false, closed)

		// check cmd list
		assert.Equal(t, "mg key01", string(cmdList.requestData))

		cmdList = cmdList.link
		assert.Equal(t, "mg key02", string(cmdList.requestData))

		assert.Nil(t, cmdList.link)

		// push again
		closed = b.push(newCommandFromString("mg key03"))
		assert.Equal(t, false, closed)

		cmdList, closed = b.popAll(true)
		assert.Equal(t, false, closed)

		assert.Equal(t, "mg key03", string(cmdList.requestData))
		assert.Nil(t, cmdList.link)

		// do close
		b.close()

		closed = b.push(newCommandFromString("mg key04"))
		assert.Equal(t, true, closed)

		cmdList, closed = b.popAll(true)
		assert.Equal(t, true, closed)

		assert.Nil(t, cmdList)
	})

	t.Run("close when having pending commands", func(t *testing.T) {
		b := newSendBuffer()
		var closed bool

		closed = b.push(newCommandFromString("mg key01"))
		assert.Equal(t, false, closed)

		closed = b.push(newCommandFromString("mg key02"))
		assert.Equal(t, false, closed)

		b.close()

		cmdList, closed := b.popAll(true)
		assert.Equal(t, true, closed)

		// check cmd list
		assert.Equal(t, "mg key01", string(cmdList.requestData))

		cmdList = cmdList.link
		assert.Equal(t, "mg key02", string(cmdList.requestData))

		assert.Nil(t, cmdList.link)
	})

	t.Run("pop all do wait", func(t *testing.T) {
		b := newSendBuffer()

		var popped atomic.Bool
		closeCh := make(chan struct{})

		var cmdList *commandListData
		var popClosed bool

		go func() {
			cmdList, popClosed = b.popAll(true)
			popped.Store(true)
			close(closeCh)
		}()

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, false, popped.Load())

		closed := b.push(newCommandFromString("mg key01"))
		assert.Equal(t, false, closed)

		<-closeCh

		assert.Equal(t, "mg key01", string(cmdList.requestData))
		assert.Nil(t, cmdList.link)

		assert.Equal(t, false, popClosed)
	})

	t.Run("pop all no wait", func(t *testing.T) {
		b := newSendBuffer()

		cmdList, closed := b.popAll(false)
		assert.Equal(t, false, closed)
		assert.Nil(t, cmdList)
	})

	t.Run("pop all do wait on closing", func(t *testing.T) {
		b := newSendBuffer()

		var popped atomic.Bool
		closeCh := make(chan struct{})

		var cmdList *commandListData
		var popClosed bool

		go func() {
			cmdList, popClosed = b.popAll(true)
			popped.Store(true)
			close(closeCh)
		}()

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, false, popped.Load())

		b.close()

		<-closeCh

		assert.Nil(t, cmdList)
		assert.Equal(t, true, popClosed)

		// pop again
		cmdList, popClosed = b.popAll(true)
		assert.Nil(t, cmdList)
		assert.Equal(t, true, popClosed)
	})
}
