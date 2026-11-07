package dedupe

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestAddOnce(t *testing.T) {
	s := New(1024)
	if ok, _ := s.Add(7); !ok {
		t.Fatal("first add must win")
	}
	if ok, _ := s.Add(7); ok {
		t.Fatal("second add must lose")
	}
	if !s.Has(7) || s.Has(8) {
		t.Fatal("membership wrong")
	}
}

func TestRemoveThenAdd(t *testing.T) {
	s := New(64)
	s.Add(1)
	s.Add(2)
	if !s.Remove(1) {
		t.Fatal("remove failed")
	}
	if s.Has(1) || !s.Has(2) {
		t.Fatal("tombstone broke chain")
	}
	if ok, _ := s.Add(1); !ok {
		t.Fatal("re-add after remove must win")
	}
}

func TestConcurrentSingleWinner(t *testing.T) {
	const ids, goroutines = 50000, 8
	s := New(ids)
	var wins atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := uint64(0); id < ids; id++ {
				if ok, err := s.Add(id); err != nil {
					t.Error(err)
					return
				} else if ok {
					wins.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if wins.Load() != ids {
		t.Fatalf("wins=%d want %d", wins.Load(), ids)
	}
}

func TestFull(t *testing.T) {
	s := New(1)
	var err error
	for id := uint64(0); err == nil && id < 1<<20; id++ {
		_, err = s.Add(id)
	}
	if err != ErrFull {
		t.Fatalf("want ErrFull, got %v", err)
	}
}
