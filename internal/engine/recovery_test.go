package engine

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"eventengine/internal/event"
	"eventengine/internal/wal"
)

func walConfig(path string, n, accounts int, mode wal.Mode) Config {
	return Config{
		Accounts: accounts, InitialBalance: 1 << 40, Workers: 4, QueueSize: 256,
		DedupeCapacity: n, MaxAttempts: 8, BaseBackoff: 100 * time.Microsecond,
		WALPath: path, WALMode: mode,
	}
}

func TestCrashMidRunRecoversExactBalances(t *testing.T) {
	const n, accounts = 3000, 200
	path := filepath.Join(t.TempDir(), "wal")
	cfg := walConfig(path, n, accounts, wal.Ack)
	cfg.FaultRate, cfg.CommitFaultRate = 0.05, 0.05

	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	evs := gen(n, accounts)

	var acked atomic.Int64
	var wg sync.WaitGroup
	const producers = 64
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := p; i < n; i += producers {
				if e.Ingest(evs[i]) != nil {
					return
				}
				acked.Add(1)
			}
		}(p)
	}
	for acked.Load() < n/6 {
		time.Sleep(time.Millisecond)
	}
	e.Crash()
	wg.Wait()
	e.Stop()
	if acked.Load() >= n {
		t.Fatal("crash happened after all events were acked")
	}

	cfg.FaultRate, cfg.CommitFaultRate = 0, 0
	cfg.WALMode = wal.Group
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	rec := r.Recovery()
	if rec.Committed+rec.Requeued+rec.Rejected+rec.Dead < int(acked.Load()) {
		t.Fatalf("recovered %+v but %d events were acked", rec, acked.Load())
	}
	drain(t, r)

	for _, ev := range evs {
		r.Ingest(ev)
	}
	drain(t, r)
	s := r.Snapshot()
	if s.Committed != n {
		t.Fatalf("committed %d want %d (recovery %+v)", s.Committed, n, rec)
	}
	if s.Duplicates < uint64(acked.Load()) {
		t.Fatalf("acked events were not deduplicated after recovery: %d < %d", s.Duplicates, acked.Load())
	}
	check(t, r, expected(evs, accounts, 1<<40))
}

func TestCleanRestartRestoresState(t *testing.T) {
	const n, accounts = 20000, 300
	path := filepath.Join(t.TempDir(), "wal")
	cfg := walConfig(path, n, accounts, wal.Group)
	cfg.FaultRate, cfg.CommitFaultRate = 0.05, 0.05

	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	evs := gen(n, accounts)
	if k, err := e.IngestBatch(evs); err != nil || k != n {
		t.Fatalf("batch ingest %d %v", k, err)
	}
	drain(t, e)
	e.Stop()

	cfg.FaultRate, cfg.CommitFaultRate = 0, 0
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	rec := r.Recovery()
	if rec.Committed != n || rec.Requeued != 0 {
		t.Fatalf("recovery %+v", rec)
	}
	check(t, r, expected(evs, accounts, 1<<40))
	for _, ev := range evs[:100] {
		if r.Ingest(ev) != ErrDuplicate {
			t.Fatal("recovered engine accepted a duplicate")
		}
	}
}

func TestDeadLettersSurviveRestart(t *testing.T) {
	const n, accounts = 500, 50
	path := filepath.Join(t.TempDir(), "wal")
	cfg := walConfig(path, n, accounts, wal.Group)
	cfg.MaxAttempts = 2
	cfg.CommitFaultRate = 1

	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	evs := gen(n, accounts)
	e.IngestBatch(evs)
	drain(t, e)
	e.Stop()

	cfg.CommitFaultRate = 0
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if len(r.DeadLetters()) != n {
		t.Fatalf("dead letters %d want %d", len(r.DeadLetters()), n)
	}
	check(t, r, expected(nil, accounts, 1<<40))
	if r.ReplayDeadLetters() != n {
		t.Fatal("replay count")
	}
	drain(t, r)
	check(t, r, expected(evs, accounts, 1<<40))
}

func TestBackpressureVoidsLoggedEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	const total = 200000
	cfg := walConfig(path, total, 4, wal.Buffered)
	cfg.Workers, cfg.QueueSize = 1, 2
	cfg.FaultRate = 1
	cfg.MaxAttempts = 1000
	cfg.BaseBackoff = time.Hour
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	throttled := 0
	var accepted []event.Event
	for i := 1; i <= total; i++ {
		ev := event.Event{ID: uint64(i), From: 0, To: 1, Amount: 1}
		switch err := e.TryIngest(ev); err {
		case nil:
			accepted = append(accepted, ev)
		case ErrBackpressure:
			throttled++
		}
		if throttled >= 5 {
			break
		}
	}
	e.Stop()
	if throttled == 0 {
		t.Skip("queue never filled on this machine")
	}
	cfg.FaultRate = 0
	cfg.BaseBackoff = time.Millisecond
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if got := r.Recovery().Requeued; got != len(accepted) {
		t.Fatalf("requeued %d want %d accepted", got, len(accepted))
	}
}
