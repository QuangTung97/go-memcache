package memcache

import (
	"sync"
	"sync/atomic"
)

type inputSelector struct {
	sendBuf      *sendBuffer
	writeLimiter connWriteLimiter

	remaining *commandData
}

func initInputSelector(s *inputSelector, sendBuf *sendBuffer, limit int) {
	s.sendBuf = sendBuf
	initConnWriteLimiter(&s.writeLimiter, limit)
}

func (s *inputSelector) traverseCommandList(
	next *commandData, result []*commandData,
) []*commandData {
	waiting := true
	for next != nil {
		writeCount := uint64(next.cmdCount)

		if !s.writeLimiter.allowMoreWrite(writeCount, waiting) {
			s.remaining = next
			return result
		}
		s.writeLimiter.addWriteCount(writeCount)

		waiting = false

		result = append(result, next)
		next = next.link
	}
	return result
}

func (s *inputSelector) linkRemainingWithInput(inputCmdList *commandData) *commandData {
	if s.remaining != nil {
		last := s.remaining
		for last.link != nil {
			last = last.link
		}
		last.link = inputCmdList
		return s.remaining
	}
	return inputCmdList
}

func (s *inputSelector) readCommands(placeholder []*commandData) ([]*commandData, bool) {
	waiting := s.remaining == nil
	cmdList, closed := s.sendBuf.popAll(waiting)

	next := s.linkRemainingWithInput(cmdList)
	return s.traverseCommandList(next, placeholder), closed
}

func (s *inputSelector) addReadCount(num uint64) {
	s.writeLimiter.addReadCount(num)
}

// ==================================
// Conn Write Limiter
// ==================================

func initConnWriteLimiter(l *connWriteLimiter, limit int) {
	l.writeLimit = uint64(limit)
	l.readCond = sync.NewCond(&l.readMut)
}

type connWriteLimiter struct {
	writeLimit uint64

	cmdWriteCount uint64

	readMut      sync.Mutex
	readCond     *sync.Cond
	cmdReadCount atomic.Uint64
}

func (l *connWriteLimiter) addWriteCount(num uint64) {
	l.cmdWriteCount += num
}

func (l *connWriteLimiter) allowMoreWrite(num uint64, waiting bool) bool {
	newWriteCount := l.cmdWriteCount + num

	if newWriteCount <= l.cmdReadCount.Load()+l.writeLimit {
		return true
	}
	if !waiting {
		return false
	}

	l.readMut.Lock()
	for newWriteCount > l.cmdReadCount.Load()+l.writeLimit {
		l.readCond.Wait()
	}
	l.readMut.Unlock()

	return true
}

func (l *connWriteLimiter) addReadCount(num uint64) {
	l.readMut.Lock()
	l.cmdReadCount.Add(num)
	l.readMut.Unlock()
	l.readCond.Signal()
}
