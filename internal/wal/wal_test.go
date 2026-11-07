package wal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"eventengine/internal/event"
)

type rec struct {
	accepts  []event.Event
	outcomes map[uint64]byte
}

func load(t *testing.T, path string) rec {
	t.Helper()
	r := rec{outcomes: map[uint64]byte{}}
	err := Recover(path, func(ev event.Event) { r.accepts = append(r.accepts, ev) },
		func(k byte, id uint64) { r.outcomes[id] = k })
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, err := Open(path, Group)
	if err != nil {
		t.Fatal(err)
	}
	evs := []event.Event{{ID: 1, From: 2, To: 3, Amount: 40}, {ID: 9, From: 5, To: 6, Amount: 7}}
	tk, err := l.AppendAccepts(evs)
	if err != nil || l.Wait(tk) != nil {
		t.Fatal("append failed")
	}
	l.AppendOutcomes(KindCommit, []uint64{1})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	r := load(t, path)
	if len(r.accepts) != 2 || r.accepts[0] != evs[0] || r.accepts[1] != evs[1] {
		t.Fatalf("accepts %+v", r.accepts)
	}
	if r.outcomes[1] != KindCommit || len(r.outcomes) != 1 {
		t.Fatalf("outcomes %+v", r.outcomes)
	}
}

func TestTornTailTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, _ := Open(path, Buffered)
	l.AppendAccepts([]event.Event{{ID: 1, From: 1, To: 2, Amount: 1}})
	l.Close()
	good, _ := os.Stat(path)

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{recAccept, 1, 2, 3, 4, 5})
	f.Close()

	r := load(t, path)
	if len(r.accepts) != 1 {
		t.Fatalf("accepts %d", len(r.accepts))
	}
	if st, _ := os.Stat(path); st.Size() != good.Size() {
		t.Fatalf("size %d want %d", st.Size(), good.Size())
	}
}

func TestCorruptRecordStopsReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, _ := Open(path, Buffered)
	l.AppendAccepts([]event.Event{{ID: 1, From: 1, To: 2, Amount: 1}, {ID: 2, From: 1, To: 2, Amount: 1}})
	l.Close()
	b, _ := os.ReadFile(path)
	b[acceptSize+3] ^= 0xff
	os.WriteFile(path, b, 0o644)
	if r := load(t, path); len(r.accepts) != 1 {
		t.Fatalf("accepts %d", len(r.accepts))
	}
}

func TestConcurrentAppendersAllDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, _ := Open(path, Ack)
	const writers, per = 8, 250
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				tk, err := l.AppendAccepts([]event.Event{{ID: uint64(w*per + i), From: 1, To: 2, Amount: 1}})
				if err != nil || l.Wait(tk) != nil {
					t.Error("append failed")
					return
				}
			}
		}(w)
	}
	wg.Wait()
	l.Abort()
	if r := load(t, path); len(r.accepts) != writers*per {
		t.Fatalf("recovered %d want %d", len(r.accepts), writers*per)
	}
}

func TestAppendAfterAbort(t *testing.T) {
	l, _ := Open(filepath.Join(t.TempDir(), "log"), Group)
	l.Abort()
	if _, err := l.AppendAccepts([]event.Event{{ID: 1}}); err == nil {
		t.Fatal("append after abort succeeded")
	}
}
