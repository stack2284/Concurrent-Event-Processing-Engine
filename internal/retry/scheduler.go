package retry

import (
	"sync"
	"time"

	"eventengine/internal/event"
)

type item struct {
	due int64
	ev  event.Event
}

type Scheduler struct {
	mu      sync.Mutex
	h       []item
	wake    chan struct{}
	done    chan struct{}
	wg      sync.WaitGroup
	now     func() int64
	deliver func(event.Event)
}

func New(now func() int64, deliver func(event.Event)) *Scheduler {
	s := &Scheduler{
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		now:     now,
		deliver: deliver,
	}
	s.wg.Add(1)
	go s.run()
	return s
}

func (s *Scheduler) Add(ev event.Event, delay time.Duration) {
	s.mu.Lock()
	s.h = append(s.h, item{due: s.now() + int64(delay), ev: ev})
	root := s.up(len(s.h)-1) == 0
	s.mu.Unlock()
	if root {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.h)
}

func (s *Scheduler) Close() {
	close(s.done)
	s.wg.Wait()
}

func (s *Scheduler) run() {
	defer s.wg.Done()
	t := time.NewTimer(time.Hour)
	t.Stop()
	for {
		wait := int64(-1)
		s.mu.Lock()
		for len(s.h) > 0 {
			if d := s.h[0].due - s.now(); d > 0 {
				wait = d
				break
			}
			it := s.pop()
			s.mu.Unlock()
			s.deliver(it.ev)
			s.mu.Lock()
		}
		s.mu.Unlock()
		if wait < 0 {
			select {
			case <-s.wake:
			case <-s.done:
				return
			}
			continue
		}
		t.Reset(time.Duration(wait))
		select {
		case <-s.wake:
			t.Stop()
		case <-t.C:
		case <-s.done:
			return
		}
	}
}

func (s *Scheduler) up(i int) int {
	for i > 0 {
		p := (i - 1) / 2
		if s.h[p].due <= s.h[i].due {
			break
		}
		s.h[p], s.h[i] = s.h[i], s.h[p]
		i = p
	}
	return i
}

func (s *Scheduler) pop() item {
	top := s.h[0]
	n := len(s.h) - 1
	s.h[0] = s.h[n]
	s.h = s.h[:n]
	i := 0
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		if r := l + 1; r < n && s.h[r].due < s.h[l].due {
			l = r
		}
		if s.h[i].due <= s.h[l].due {
			break
		}
		s.h[i], s.h[l] = s.h[l], s.h[i]
		i = l
	}
	return top
}
