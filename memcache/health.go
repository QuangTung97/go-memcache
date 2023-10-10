package memcache

import (
	"sync"
	"time"
)

type healthCheckService struct {
	conns         []*clientConn
	getNextFunc   func() uint64
	sleepDuration time.Duration

	prevSequence uint64

	wg      sync.WaitGroup
	closeCh chan struct{}
}

func newHealthCheckService(
	conns []*clientConn,
	getNextFunc func() uint64,
	sleepDuration time.Duration,
) *healthCheckService {
	return &healthCheckService{
		conns:         conns,
		getNextFunc:   getNextFunc,
		sleepDuration: sleepDuration,

		prevSequence: 0,

		closeCh: make(chan struct{}),
	}
}

func (s *healthCheckService) runSingleLoop() {
	nextSeq := s.getNextFunc()
	if nextSeq >= s.prevSequence+uint64(len(s.conns)) {
		s.prevSequence = nextSeq
		return
	}

	startIndex := nextSeq - s.prevSequence

	for _, conn := range s.conns[startIndex:] {
		pipe := newPipeline(conn)
		_, _ = pipe.Version()()
		pipe.Finish()
	}

	s.prevSequence = s.getNextFunc()
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
	close(s.closeCh)
	s.wg.Wait()
}
