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

func TestInputSelector(t *testing.T) {
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

		l.addWriteCount(4) // write: 6, read: 3
		l.addReadCount(2)  // write: 6, read: 5

		l.allowMoreWrite(2, true)
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

		fmt.Println(l.cmdWriteCount)
		fmt.Println(l.cmdReadCount.Load())
	})
}
