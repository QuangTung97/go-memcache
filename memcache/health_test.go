package memcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
)

type healthCheckTest struct {
	recorder1 *connRecorder
	recorder2 *connRecorder
	recorder3 *connRecorder

	nextCalls int
	nextVal   uint64

	addCalls []uint64

	conns []*clientConn

	svc *healthCheckService
}

func newHealthCheckTest(sleepDuration time.Duration) *healthCheckTest {
	cmdPool := newPipelineCommandListPool()

	dialFunc1, recorder1, startCh1 := newDialFuncWithRecorder()
	close(startCh1)

	dialFunc2, recorder2, startCh2 := newDialFuncWithRecorder()
	close(startCh2)

	dialFunc3, recorder3, startCh3 := newDialFuncWithRecorder()
	close(startCh3)

	conn1 := newConn("localhost:11211", cmdPool, WithNetConnOptions(
		netconn.WithDialFunc(dialFunc1),
	))
	conn2 := newConn("localhost:11211", cmdPool, WithNetConnOptions(
		netconn.WithDialFunc(dialFunc2),
	))
	conn3 := newConn("localhost:11211", cmdPool, WithNetConnOptions(
		netconn.WithDialFunc(dialFunc3),
	))

	conns := []*clientConn{conn1, conn2, conn3}

	s := &healthCheckTest{
		recorder1: recorder1,
		recorder2: recorder2,
		recorder3: recorder3,

		conns: conns,
	}

	s.svc = newHealthCheckService(
		conns,
		func() uint64 {
			s.nextCalls++
			return s.nextVal
		}, func(delta uint64) uint64 {
			s.addCalls = append(s.addCalls, delta)
			s.nextVal++
			return s.nextVal
		},
		sleepDuration,
	)

	return s
}

func (s *healthCheckTest) closeConns() {
	for _, conn := range s.conns {
		conn.shutdown()
	}

	for _, conn := range s.conns {
		conn.waitCloseCompleted()
	}
}

func TestHealthCheckService_SingleLoop(t *testing.T) {
	t.Run("ping all conns", func(t *testing.T) {
		s := newHealthCheckTest(100 * time.Millisecond)

		s.svc.runSingleLoop()

		time.Sleep(30 * time.Millisecond)
		s.closeConns()

		assert.Equal(t, 1, s.nextCalls)

		assert.Equal(t, "version\r\n", string(s.recorder1.data))
		assert.Equal(t, "version\r\n", string(s.recorder2.data))
		assert.Equal(t, "version\r\n", string(s.recorder3.data))

		assert.Equal(t, []uint64{1, 1, 1}, s.addCalls)
		assert.Equal(t, uint64(3), s.nextVal)
	})

	t.Run("next func returns 3, not call any", func(t *testing.T) {
		s := newHealthCheckTest(100 * time.Millisecond)
		s.nextVal = 3

		s.svc.runSingleLoop()

		time.Sleep(30 * time.Millisecond)
		s.closeConns()

		assert.Equal(t, 1, s.nextCalls)

		assert.Equal(t, "", string(s.recorder1.data))
		assert.Equal(t, "", string(s.recorder2.data))
		assert.Equal(t, "", string(s.recorder3.data))

		assert.Equal(t, []uint64(nil), s.addCalls)
	})

	t.Run("next func returns 2, call first conn", func(t *testing.T) {
		s := newHealthCheckTest(100 * time.Millisecond)
		s.nextVal = 2

		s.svc.runSingleLoop()

		time.Sleep(30 * time.Millisecond)
		s.closeConns()

		assert.Equal(t, 1, s.nextCalls)

		assert.Equal(t, "version\r\n", string(s.recorder1.data))
		assert.Equal(t, "", string(s.recorder2.data))
		assert.Equal(t, "", string(s.recorder3.data))

		assert.Equal(t, []uint64{1}, s.addCalls)
	})

	t.Run("get next func returns 3, call again, then return 4", func(t *testing.T) {
		s := newHealthCheckTest(100 * time.Millisecond)

		s.nextVal = 3
		s.svc.runSingleLoop()
		time.Sleep(30 * time.Millisecond)

		s.nextVal = 4
		s.svc.runSingleLoop()
		time.Sleep(30 * time.Millisecond)

		s.closeConns()

		assert.Equal(t, 2, s.nextCalls)

		assert.Equal(t, "version\r\n", string(s.recorder1.data)) // seq = 6
		assert.Equal(t, "", string(s.recorder2.data))
		assert.Equal(t, "version\r\n", string(s.recorder3.data)) // seq = 5

		assert.Equal(t, []uint64{1, 1}, s.addCalls)
	})

	t.Run("next func returns 2, call again, then return 3", func(t *testing.T) {
		s := newHealthCheckTest(100 * time.Millisecond)

		s.nextVal = 2
		s.svc.runSingleLoop()
		time.Sleep(30 * time.Millisecond)

		s.nextVal = 3
		s.svc.runSingleLoop()
		time.Sleep(30 * time.Millisecond)

		s.closeConns()

		assert.Equal(t, 2, s.nextCalls)

		assert.Equal(t, "version\r\nversion\r\n", string(s.recorder1.data)) // seq = 3, = 6
		assert.Equal(t, "version\r\n", string(s.recorder2.data))            // seq = 4
		assert.Equal(t, "version\r\n", string(s.recorder3.data))            // seq = 5

		assert.Equal(t, []uint64{1, 1, 1, 1}, s.addCalls)
	})

	t.Run("next func returns 7, call again, then return 8", func(t *testing.T) {
		s := newHealthCheckTest(100 * time.Millisecond)

		s.nextVal = 7
		s.svc.runSingleLoop()
		time.Sleep(30 * time.Millisecond)

		s.nextVal = 8
		s.svc.runSingleLoop()
		time.Sleep(30 * time.Millisecond)

		s.closeConns()

		assert.Equal(t, 2, s.nextCalls)

		assert.Equal(t, "version\r\n", string(s.recorder1.data)) // seq = 9
		assert.Equal(t, "version\r\n", string(s.recorder2.data)) // seq = 10
		assert.Equal(t, "", string(s.recorder3.data))            // seq = 8

		assert.Equal(t, []uint64{1, 1}, s.addCalls)
	})
}

func TestHealthCheckService_InBackground(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		s := newHealthCheckTest(200 * time.Millisecond)
		s.nextVal = 1

		s.svc.runInBackground()

		time.Sleep(250 * time.Millisecond)

		s.svc.shutdown()

		assert.Equal(t, 1, s.nextCalls)

		assert.Equal(t, "version\r\n", string(s.recorder1.data)) // seq = 3
		assert.Equal(t, "", string(s.recorder2.data))            // seq = 1
		assert.Equal(t, "version\r\n", string(s.recorder3.data)) // seq = 2
	})

	t.Run("call two times", func(t *testing.T) {
		s := newHealthCheckTest(200 * time.Millisecond)
		s.nextVal = 122

		s.svc.runInBackground()

		time.Sleep(450 * time.Millisecond)

		s.svc.shutdown()

		assert.Equal(t, 2, s.nextCalls)

		assert.Equal(t, "version\r\n", string(s.recorder1.data))
		assert.Equal(t, "version\r\n", string(s.recorder2.data))
		assert.Equal(t, "version\r\n", string(s.recorder3.data))
	})

	t.Run("shutdown two times", func(t *testing.T) {
		s := newHealthCheckTest(200 * time.Millisecond)
		s.nextVal = 122

		s.svc.runInBackground()

		time.Sleep(250 * time.Millisecond)

		s.svc.shutdown()
		s.svc.shutdown()
	})
}

func TestSleepWithCloseChan(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		ch := make(chan struct{})
		closed := sleepWithCloseChan(100*time.Millisecond, ch)
		assert.Equal(t, false, closed)
	})

	t.Run("closed", func(t *testing.T) {
		ch := make(chan struct{})
		close(ch)
		closed := sleepWithCloseChan(100*time.Millisecond, ch)
		assert.Equal(t, true, closed)
	})
}
