package memcache

import (
	"sync"
	"sync/atomic"
	"time"
)

type healthCheckService struct {
	conns []*clientConn

	getNextFunc func() uint64
	addNextFunc func(delta uint64) uint64

	sleepDuration time.Duration

	prevSequence uint64

	wg sync.WaitGroup

	closed  atomic.Bool
	closeCh chan struct{}
}

func newHealthCheckService(
	conns []*clientConn,
	getNextFunc func() uint64,
	addNextFunc func(delta uint64) uint64,
	sleepDuration time.Duration,
) *healthCheckService {
	return &healthCheckService{
		conns: conns,

		getNextFunc: getNextFunc,
		addNextFunc: addNextFunc,

		sleepDuration: sleepDuration,

		prevSequence: 0,

		closeCh: make(chan struct{}),
	}
}

func (s *healthCheckService) runSingleLoop() {
	connLen := uint64(len(s.conns))
	nextBound := s.prevSequence + connLen

	nextSeq := s.getNextFunc()
	if nextSeq >= nextBound {
		s.prevSequence = nextSeq
		return
	}

	for s.prevSequence < nextBound {
		s.prevSequence = s.addNextFunc(1)

		index := s.prevSequence % connLen
		conn := s.conns[index]

		pipe := newPipeline(conn, nil)
		_, _ = pipe.Version()()
		pipe.Finish()
	}
}

func (s *healthCheckService) runInBackground() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			closed := sleepWithCloseChan(s.sleepDuration, s.closeCh)
			if closed {
				return
			}
			s.runSingleLoop()
		}
	}()
}

func (s *healthCheckService) shutdown() {
	prevClosed := s.closed.Swap(true)
	if !prevClosed {
		close(s.closeCh)
		s.wg.Wait()
	}
}
