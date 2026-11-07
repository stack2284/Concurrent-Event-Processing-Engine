package ring

import (
	"math/bits"
	"runtime"
	"sync/atomic"
	"time"

	"eventengine/internal/event"
)

const spinBeforePark = 128

type slot struct {
	seq atomic.Uint64
	ev  event.Event
	_   [8]byte
}

type Queue struct {
	slots  []slot
	mask   uint64
	_      [64]byte
	tail   atomic.Uint64
	_      [64]byte
	head   atomic.Uint64
	_      [64]byte
	parked atomic.Uint32
	closed atomic.Bool
	wake   chan struct{}
}

func New(capacity int) *Queue {
	n := 1 << bits.Len(uint(max(capacity, 2)-1))
	q := &Queue{
		slots: make([]slot, n),
		mask:  uint64(n - 1),
		wake:  make(chan struct{}, 1),
	}
	for i := range q.slots {
		q.slots[i].seq.Store(uint64(i))
	}
	return q
}

func (q *Queue) Cap() int { return len(q.slots) }

func (q *Queue) Len() int {
	return int(q.tail.Load() - q.head.Load())
}

func (q *Queue) TryPush(ev event.Event) bool {
	for {
		pos := q.tail.Load()
		s := &q.slots[pos&q.mask]
		diff := int64(s.seq.Load()) - int64(pos)
		switch {
		case diff == 0:
			if q.tail.CompareAndSwap(pos, pos+1) {
				s.ev = ev
				s.seq.Store(pos + 1)
				if q.parked.Load() != 0 {
					q.signal()
				}
				return true
			}
		case diff < 0:
			return false
		}
	}
}

func (q *Queue) Push(ev event.Event) {
	for i := 0; !q.TryPush(ev); i++ {
		if i < 64 {
			runtime.Gosched()
		} else {
			time.Sleep(20 * time.Microsecond)
		}
	}
}

func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *Queue) Close() {
	q.closed.Store(true)
	q.signal()
}

func (q *Queue) drain(dst []event.Event) int {
	pos := q.head.Load()
	n := 0
	for n < len(dst) {
		s := &q.slots[pos&q.mask]
		if s.seq.Load() != pos+1 {
			break
		}
		dst[n] = s.ev
		s.seq.Store(pos + q.mask + 1)
		pos++
		n++
	}
	if n > 0 {
		q.head.Store(pos)
	}
	return n
}

func (q *Queue) PopBatch(dst []event.Event) int {
	for {
		for i := 0; i < spinBeforePark; i++ {
			if n := q.drain(dst); n > 0 {
				return n
			}
			if i > spinBeforePark/2 {
				runtime.Gosched()
			}
		}
		q.parked.Store(1)
		if n := q.drain(dst); n > 0 {
			q.parked.Store(0)
			return n
		}
		if q.closed.Load() {
			q.parked.Store(0)
			return q.drain(dst)
		}
		<-q.wake
		q.parked.Store(0)
	}
}
