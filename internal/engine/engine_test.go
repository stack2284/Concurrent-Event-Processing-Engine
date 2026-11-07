package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"eventengine/internal/event"
)

func drain(t *testing.T, e *Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := e.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

func gen(n, accounts int) []event.Event {
	evs := make([]event.Event, n)
	for i := range evs {
		from := uint32(i*7+1) % uint32(accounts)
		to := uint32(i*13+5) % uint32(accounts)
		if to == from {
			to = (to + 1) % uint32(accounts)
		}
		evs[i] = event.Event{ID: uint64(i + 1), From: from, To: to, Amount: int64(i%50 + 1)}
	}
	return evs
}

func expected(evs []event.Event, accounts int, initial int64) []int64 {
	bal := make([]int64, accounts)
	for i := range bal {
		bal[i] = initial
	}
	for _, ev := range evs {
		bal[ev.From] -= ev.Amount
		bal[ev.To] += ev.Amount
	}
	return bal
}

func check(t *testing.T, e *Engine, want []int64) {
	t.Helper()
	for a := range want {
		if got := e.Ledger().Balance(uint32(a)); got != want[a] {
			t.Fatalf("account %d balance %d want %d", a, got, want[a])
		}
	}
}

func TestExactlyOnceWithDuplicatesAndFaults(t *testing.T) {
	const n, accounts, initial = 40000, 500, 1 << 40
	e, err := New(Config{
		Accounts: accounts, InitialBalance: initial, Workers: 4, QueueSize: 256,
		DedupeCapacity: n, MaxAttempts: 12, BaseBackoff: 100 * time.Microsecond,
		FaultRate: 0.1, CommitFaultRate: 0.1, TraceSample: 16, TraceBuffer: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Stop()

	evs := gen(n, accounts)
	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pass := 0; pass < 3; pass++ {
				for _, ev := range evs {
					e.Ingest(ev)
				}
			}
		}()
	}
	wg.Wait()
	drain(t, e)
	for e.ReplayDeadLetters() > 0 {
		drain(t, e)
	}

	s := e.Snapshot()
	if s.Committed != n {
		t.Fatalf("committed %d want %d", s.Committed, n)
	}
	if s.Duplicates != uint64(11*n) {
		t.Fatalf("duplicates %d want %d", s.Duplicates, 11*n)
	}
	if s.Compensated == 0 || s.Retried == 0 {
		t.Fatal("fault paths not exercised")
	}
	check(t, e, expected(evs, accounts, initial))
	if len(e.Traces(10)) == 0 {
		t.Fatal("no traces captured")
	}
}

func TestCompensationLeavesLedgerUntouched(t *testing.T) {
	const n, accounts, initial = 2000, 50, 1 << 30
	e, err := New(Config{
		Accounts: accounts, InitialBalance: initial, Workers: 2,
		DedupeCapacity: n, MaxAttempts: 3, BaseBackoff: 50 * time.Microsecond,
		CommitFaultRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	for _, ev := range gen(n, accounts) {
		e.Ingest(ev)
	}
	drain(t, e)
	s := e.Snapshot()
	if s.Committed != 0 || s.DeadLettered != n || s.Compensated != 3*n {
		t.Fatalf("unexpected counters %+v", s)
	}
	check(t, e, expected(nil, accounts, initial))
	if len(e.DeadLetters()) != n {
		t.Fatal("dead letter queue incomplete")
	}
}

func TestRejectOnInsufficientFunds(t *testing.T) {
	e, err := New(Config{Accounts: 2, InitialBalance: 10, Workers: 1, DedupeCapacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	e.Ingest(event.Event{ID: 1, From: 0, To: 1, Amount: 10})
	e.Ingest(event.Event{ID: 2, From: 0, To: 1, Amount: 1})
	drain(t, e)
	s := e.Snapshot()
	if s.Committed != 1 || s.Rejected != 1 {
		t.Fatalf("unexpected counters %+v", s)
	}
	if e.Ledger().Total() != 20 {
		t.Fatal("conservation violated")
	}
}

func TestValidationAndBackpressure(t *testing.T) {
	e, err := New(Config{Accounts: 4, InitialBalance: 100, Workers: 1, QueueSize: 1, DedupeCapacity: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	for _, ev := range []event.Event{
		{ID: 1, From: 0, To: 0, Amount: 1},
		{ID: 2, From: 0, To: 9, Amount: 1},
		{ID: 3, From: 0, To: 1, Amount: 0},
	} {
		if e.Ingest(ev) != ErrInvalid {
			t.Fatalf("event %d should be invalid", ev.ID)
		}
	}
	if e.Ingest(event.Event{ID: 4, From: 0, To: 1, Amount: 1}) != nil {
		t.Fatal("valid event rejected")
	}
	if e.Ingest(event.Event{ID: 4, From: 0, To: 1, Amount: 1}) != ErrDuplicate {
		t.Fatal("duplicate not detected")
	}
}
