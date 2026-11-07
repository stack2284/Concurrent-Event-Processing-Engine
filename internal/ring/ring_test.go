package ring

import (
	"sync"
	"testing"

	"eventengine/internal/event"
)

func TestFullAndEmpty(t *testing.T) {
	q := New(4)
	for i := 0; i < 4; i++ {
		if !q.TryPush(event.Event{ID: uint64(i)}) {
			t.Fatalf("push %d failed", i)
		}
	}
	if q.TryPush(event.Event{}) {
		t.Fatal("push into full queue succeeded")
	}
	buf := make([]event.Event, 8)
	if n := q.PopBatch(buf); n != 4 {
		t.Fatalf("popped %d", n)
	}
	for i := 0; i < 4; i++ {
		if buf[i].ID != uint64(i) {
			t.Fatal("fifo violated")
		}
	}
	if !q.TryPush(event.Event{}) {
		t.Fatal("slot not reusable after drain")
	}
}

func TestCloseDrains(t *testing.T) {
	q := New(8)
	q.Push(event.Event{ID: 1})
	q.Close()
	buf := make([]event.Event, 4)
	if n := q.PopBatch(buf); n != 1 {
		t.Fatalf("popped %d", n)
	}
	if n := q.PopBatch(buf); n != 0 {
		t.Fatalf("popped %d after close", n)
	}
}

func TestMultiProducer(t *testing.T) {
	const producers, per = 8, 100000
	q := New(256)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p uint64) {
			defer wg.Done()
			for i := uint64(0); i < per; i++ {
				q.Push(event.Event{ID: p<<32 | i})
			}
		}(uint64(p))
	}
	go func() {
		wg.Wait()
		q.Close()
	}()

	last := make([]int64, producers)
	for i := range last {
		last[i] = -1
	}
	total := 0
	buf := make([]event.Event, 64)
	for {
		n := q.PopBatch(buf)
		if n == 0 {
			break
		}
		for _, ev := range buf[:n] {
			p, seq := ev.ID>>32, int64(ev.ID&0xffffffff)
			if seq != last[p]+1 {
				t.Fatalf("producer %d out of order: %d after %d", p, seq, last[p])
			}
			last[p] = seq
		}
		total += n
	}
	if total != producers*per {
		t.Fatalf("received %d want %d", total, producers*per)
	}
}
