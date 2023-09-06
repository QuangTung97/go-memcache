package memcache

import (
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

		l.allowMoreWrite(1)

		closed := make(chan struct{})
		var finished atomic.Bool

		go func() {
			l.allowMoreWrite(2)
			finished.Store(true)
			close(closed)
		}()

		time.Sleep(10 * time.Millisecond)

		assert.Equal(t, false, finished.Load())

		l.addReadCount(1)

		<-closed
	})
}
