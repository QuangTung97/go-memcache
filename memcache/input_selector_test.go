package memcache

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newInputSelector(send *sendBuffer, limit int) *inputSelector {
	s := &inputSelector{}
	initInputSelector(s, send, limit)
	return s
}

func newCommandWithCount(s string, count int) *commandData {
	cmd := newCommandFromString(s)
	cmd.cmdCount = count
	return cmd
}

func newCommandChain(list ...string) *commandData {
	var last *commandData
	var first *commandData
	for _, s := range list {
		cmd := newCommandFromString(s)
		if last == nil {
			first = cmd
		} else {
			last.sibling = cmd
		}
		last = cmd
	}
	return first
}

func TestNewCommandChain(t *testing.T) {
	chain := newCommandChain("mg key01", "mg key02", "mg key03")
	assert.Equal(t, "mg key01", string(chain.requestData))
	assert.Equal(t, "mg key02", string(chain.sibling.requestData))
	assert.Equal(t, "mg key03", string(chain.sibling.sibling.requestData))
	assert.Nil(t, chain.sibling.sibling.sibling)
}

func TestInputSelector(t *testing.T) {
	t.Run("single command", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 2)

		sendBuf.push(newCommandWithCount("mg key01", 1))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 1, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
	})

	t.Run("wait for single command", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 2)

		var cmds []*commandData
		var closed bool
		var finished atomic.Bool
		finishCh := make(chan struct{})

		go func() {
			cmds, closed = s.readCommands(nil)
			finished.Store(true)
			close(finishCh)
		}()

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, false, finished.Load())

		sendBuf.push(newCommandWithCount("mg key01", 1))

		<-finishCh

		assert.Equal(t, true, finished.Load())

		assert.Equal(t, false, closed)
		assert.Equal(t, 1, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
	})

	t.Run("three commands", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandWithCount("mg key01", 1))
		sendBuf.push(newCommandWithCount("mg key02", 1))
		sendBuf.push(newCommandWithCount("mg key03", 1))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key02", string(cmds[1].requestData))
		assert.Equal(t, "mg key03", string(cmds[2].requestData))
	})

	t.Run("three commands reach write limit", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 2)

		sendBuf.push(newCommandWithCount("mg key01", 1))
		sendBuf.push(newCommandWithCount("mg key02", 1))
		sendBuf.push(newCommandWithCount("mg key03", 1))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 2, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key02", string(cmds[1].requestData))

		// push another
		sendBuf.push(newCommandWithCount("mg key04", 2))

		s.addReadCount(2)

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 1, len(cmds))
		assert.Equal(t, "mg key03", string(cmds[0].requestData))

		// read remaining command 4
		s.addReadCount(1)

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 1, len(cmds))
		assert.Equal(t, "mg key04", string(cmds[0].requestData))
	})

	t.Run("5 commands reach write limit", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandWithCount("mg key01", 1))
		sendBuf.push(newCommandWithCount("mg key02", 1))
		sendBuf.push(newCommandWithCount("mg key03", 1))
		sendBuf.push(newCommandWithCount("mg key04", 1))
		sendBuf.push(newCommandWithCount("mg key05", 1))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key02", string(cmds[1].requestData))
		assert.Equal(t, "mg key03", string(cmds[2].requestData))

		s.addReadCount(3)

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 2, len(cmds))
		assert.Equal(t, "mg key04", string(cmds[0].requestData))
		assert.Equal(t, "mg key05", string(cmds[1].requestData))
	})

	t.Run("should waiting on write limiter", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandWithCount("mg key01", 1))
		sendBuf.push(newCommandWithCount("mg key02", 1))
		sendBuf.push(newCommandWithCount("mg key03", 1))
		sendBuf.push(newCommandWithCount("mg key04", 1))
		sendBuf.push(newCommandWithCount("mg key05", 1))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key02", string(cmds[1].requestData))
		assert.Equal(t, "mg key03", string(cmds[2].requestData))

		var finished atomic.Bool
		finishCh := make(chan struct{})
		go func() {
			cmds, closed = s.readCommands(nil)
			finished.Store(true)
			close(finishCh)
		}()

		time.Sleep(5 * time.Millisecond)
		assert.Equal(t, false, finished.Load())

		s.addReadCount(3)

		<-finishCh

		assert.Equal(t, false, closed)
		assert.Equal(t, 2, len(cmds))
		assert.Equal(t, "mg key04", string(cmds[0].requestData))
		assert.Equal(t, "mg key05", string(cmds[1].requestData))
	})

	t.Run("waiting on write limiter, new command is pushed in that time", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandWithCount("mg key01", 1))
		sendBuf.push(newCommandWithCount("mg key02", 1))
		sendBuf.push(newCommandWithCount("mg key03", 1))
		sendBuf.push(newCommandWithCount("mg key04", 1))
		sendBuf.push(newCommandWithCount("mg key05", 1))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key02", string(cmds[1].requestData))
		assert.Equal(t, "mg key03", string(cmds[2].requestData))

		var finished atomic.Bool
		finishCh := make(chan struct{})
		go func() {
			cmds, closed = s.readCommands(nil)
			finished.Store(true)
			close(finishCh)
		}()

		time.Sleep(5 * time.Millisecond)
		assert.Equal(t, false, finished.Load())

		sendBuf.push(newCommandWithCount("mg key06", 1))

		s.addReadCount(3)

		<-finishCh

		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))
		assert.Equal(t, "mg key04", string(cmds[0].requestData))
		assert.Equal(t, "mg key05", string(cmds[1].requestData))
		assert.Equal(t, "mg key06", string(cmds[2].requestData))
	})

	t.Run("do close after push", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandWithCount("mg key01", 1))
		sendBuf.push(newCommandWithCount("mg key02", 1))
		sendBuf.push(newCommandWithCount("mg key03", 1))
		sendBuf.push(newCommandWithCount("mg key04", 1))
		sendBuf.push(newCommandWithCount("mg key05", 1))

		sendBuf.close()

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key02", string(cmds[1].requestData))
		assert.Equal(t, "mg key03", string(cmds[2].requestData))

		var finished atomic.Bool
		finishCh := make(chan struct{})
		go func() {
			cmds, closed = s.readCommands(nil)
			finished.Store(true)
			close(finishCh)
		}()

		time.Sleep(5 * time.Millisecond)
		assert.Equal(t, false, finished.Load())

		s.addReadCount(3)

		<-finishCh

		assert.Equal(t, true, closed)
		assert.Equal(t, 2, len(cmds))
		assert.Equal(t, "mg key04", string(cmds[0].requestData))
		assert.Equal(t, "mg key05", string(cmds[1].requestData))
	})

	t.Run("with remaining", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandWithCount("mg key01", 1))
		sendBuf.push(newCommandWithCount("mg key02", 1))
		sendBuf.push(newCommandWithCount("mg key03", 1))
		sendBuf.push(newCommandWithCount("mg key04", 1))
		sendBuf.push(newCommandWithCount("mg key05", 1))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key02", string(cmds[1].requestData))
		assert.Equal(t, "mg key03", string(cmds[2].requestData))

		s.addReadCount(3)

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 2, len(cmds))
		assert.Equal(t, "mg key04", string(cmds[0].requestData))
		assert.Equal(t, "mg key05", string(cmds[1].requestData))

		s.addReadCount(2)

		sendBuf.push(newCommandWithCount("mg key06", 1))
		sendBuf.push(newCommandWithCount("mg key07", 1))

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 2, len(cmds))
		assert.Equal(t, "mg key06", string(cmds[0].requestData))
		assert.Equal(t, "mg key07", string(cmds[1].requestData))
	})

	t.Run("should not wait for first command with cmd count > write limit", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandWithCount("mg key01", 4))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 1, len(cmds))
		assert.Equal(t, "mg key01", string(cmds[0].requestData))
	})

	t.Run("with sibling total 2 commands", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandChain("mg key01", "mg key02"))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 2, len(cmds))

		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.NotNil(t, cmds[0].sibling)
		assert.Nil(t, cmds[0].link)

		assert.Equal(t, "mg key02", string(cmds[1].requestData))
		assert.Nil(t, cmds[1].sibling)
		assert.Nil(t, cmds[1].link)
	})

	t.Run("with sibling total 4 commands", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandChain("mg key01", "mg key02"))
		sendBuf.push(newCommandChain("mg key03", "mg key04"))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))

		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key03", string(cmds[1].requestData))
		assert.Equal(t, "mg key02", string(cmds[2].requestData))

		s.addReadCount(3)

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 1, len(cmds))

		assert.Equal(t, "mg key04", string(cmds[0].requestData))
	})

	t.Run("with multiple commands with multiple siblings", func(t *testing.T) {
		sendBuf := newSendBuffer()
		s := newInputSelector(sendBuf, 3)

		sendBuf.push(newCommandChain("mg key01", "mg key02"))
		sendBuf.push(newCommandChain("mg key03", "mg key04", "mg key05"))
		sendBuf.push(newCommandChain("mg key06"))
		sendBuf.push(newCommandChain("mg key07"))
		sendBuf.push(newCommandChain("mg key08"))
		sendBuf.push(newCommandChain("mg key09"))
		sendBuf.push(newCommandChain("mg key10"))

		cmds, closed := s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))

		assert.Equal(t, "mg key01", string(cmds[0].requestData))
		assert.Equal(t, "mg key03", string(cmds[1].requestData))
		assert.Equal(t, "mg key06", string(cmds[2].requestData))

		s.addReadCount(3)

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))

		assert.Equal(t, "mg key07", string(cmds[0].requestData))
		assert.Equal(t, "mg key02", string(cmds[1].requestData))
		assert.Equal(t, "mg key08", string(cmds[2].requestData))

		s.addReadCount(3)

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 3, len(cmds))

		assert.Equal(t, "mg key09", string(cmds[0].requestData))
		assert.Equal(t, "mg key04", string(cmds[1].requestData))
		assert.Equal(t, "mg key10", string(cmds[2].requestData))

		s.addReadCount(3)

		sendBuf.push(newCommandChain("mg key11"))

		cmds, closed = s.readCommands(nil)
		assert.Equal(t, false, closed)
		assert.Equal(t, 2, len(cmds))

		assert.Equal(t, "mg key11", string(cmds[0].requestData))
		assert.Equal(t, "mg key05", string(cmds[1].requestData))
	})
}

func TestInputSelector_Concurrent(t *testing.T) {
	sendBuf := newSendBuffer()
	s := newInputSelector(sendBuf, 3)

	var wg sync.WaitGroup
	wg.Add(2)

	const numLoops = 10_000

	go func() {
		defer wg.Done()

		for i := 0; i < numLoops; i++ {
			sendBuf.push(newCommandWithCount("mg key01", 1))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numLoops; i++ {
			sendBuf.push(newCommandWithCount("mg key02", 1))
		}
	}()

	total := 0
	readFinish := make(chan struct{})
	go func() {
		defer close(readFinish)

		for {
			cmds, closed := s.readCommands(nil)

			total += len(cmds)
			s.addReadCount(uint64(len(cmds)))

			if closed {
				return
			}
		}
	}()

	wg.Wait()
	sendBuf.close()

	<-readFinish
	assert.Equal(t, 2*numLoops, total)
}

func newConnWriteLimiter(limit int) *connWriteLimiter {
	l := &connWriteLimiter{}
	initConnWriteLimiter(l, limit)
	return l
}

func TestConnWriteLimiter(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		l := newConnWriteLimiter(3)

		l.addWriteCount(2)

		allow := l.allowMoreWrite(1, true)
		assert.Equal(t, true, allow)
		assert.Equal(t, false, l.justWaited)

		closed := make(chan struct{})
		var finished atomic.Bool

		go func() {
			l.allowMoreWrite(2, true)
			finished.Store(true)
			close(closed)
		}()

		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, false, finished.Load())

		l.addReadCount(1)

		<-closed
	})

	t.Run("normal with bigger add read count", func(t *testing.T) {
		l := newConnWriteLimiter(3)

		l.addWriteCount(2)

		allow := l.allowMoreWrite(1, true)
		assert.Equal(t, true, allow)
		allow = false

		closed := make(chan struct{})
		var finished atomic.Bool

		go func() {
			allow = l.allowMoreWrite(4, true)
			finished.Store(true)
			close(closed)
		}()

		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, false, finished.Load())

		l.addReadCount(3)

		<-closed

		assert.Equal(t, true, allow)
		assert.Equal(t, true, l.justWaited)

		l.addWriteCount(4) // write: 6, read: 3
		l.addReadCount(2)  // write: 6, read: 5

		l.allowMoreWrite(2, true)

		assert.Equal(t, false, l.justWaited)
	})

	t.Run("no wait", func(t *testing.T) {
		l := newConnWriteLimiter(3)

		l.addWriteCount(2)

		allow := l.allowMoreWrite(2, false)
		assert.Equal(t, false, allow)

		l.addReadCount(1)

		allow = l.allowMoreWrite(2, false)
		assert.Equal(t, true, allow)

		l.addWriteCount(2)

		allow = l.allowMoreWrite(1, false)
		assert.Equal(t, false, allow)
	})

	t.Run("concurrent", func(t *testing.T) {
		l := newConnWriteLimiter(3)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()

			for i := 0; i < 10000; i++ {
				n := uint64(rand.Intn(2) + 1)
				l.allowMoreWrite(n, true)
				l.addWriteCount(n)
			}
		}()

		go func() {
			defer wg.Done()

			for i := 0; i < 20000; i++ {
				n := uint64(rand.Intn(2) + 1)
				l.addReadCount(n)
			}
		}()

		wg.Wait()

		fmt.Println("WRITE COUNT:", l.cmdWriteCount)
		fmt.Println("READ COUNT:", l.cmdReadCount.Load())
	})
}
