package retry

import (
	"testing"
	"time"

	"eventengine/internal/event"
)

func TestDeliveryOrderAndDelay(t *testing.T) {
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	out := make(chan event.Event, 8)
	s := New(now, func(e event.Event) { out <- e })
	defer s.Close()

	s.Add(event.Event{ID: 3}, 30*time.Millisecond)
	s.Add(event.Event{ID: 1}, 5*time.Millisecond)
	s.Add(event.Event{ID: 2}, 15*time.Millisecond)

	for _, want := range []uint64{1, 2, 3} {
		select {
		case e := <-out:
			if e.ID != want {
				t.Fatalf("got %d want %d", e.ID, want)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout")
		}
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("delivered early")
	}
	if s.Len() != 0 {
		t.Fatal("heap not empty")
	}
}
