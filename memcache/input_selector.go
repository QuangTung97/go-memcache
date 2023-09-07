package memcache

import (
	"sync"
	"sync/atomic"
)

type inputSelector struct {
}

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
