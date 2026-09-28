package analytics

import (
	"sync"
	"time"
)

// semaphore bounds the analytical commands that run at once. Waiters are
// served in arrival order. The limit is a function, read on every acquire
// and release, so CONFIG SET nildb.analytics-max-concurrent applies to the
// next command without a restart; a raised limit admits waiters as running
// commands finish.
type semaphore struct {
	mu      sync.Mutex
	running int
	waiters []chan struct{}
}

// acquire takes a slot, waiting at most timeout. waited reports that the
// caller had to queue; ok is false when the wait timed out.
func (s *semaphore) acquire(limit func() int, timeout time.Duration) (waited, ok bool) {
	s.mu.Lock()
	if s.running < max(limit(), 1) && len(s.waiters) == 0 {
		s.running++
		s.mu.Unlock()
		return false, true
	}
	ch := make(chan struct{})
	s.waiters = append(s.waiters, ch)
	s.mu.Unlock()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ch:
		return true, true
	case <-t.C:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, w := range s.waiters {
		if w == ch {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return true, false
		}
	}
	// release granted the slot while the timer fired.
	return true, true
}

// release frees a slot and hands free slots to waiters.
func (s *semaphore) release(limit func() int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running--
	for len(s.waiters) > 0 && s.running < max(limit(), 1) {
		ch := s.waiters[0]
		s.waiters = s.waiters[1:]
		s.running++
		close(ch)
	}
}

// load returns the running count and the queue length.
func (s *semaphore) load() (running, queued int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running, len(s.waiters)
}
