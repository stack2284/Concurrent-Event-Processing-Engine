package ledger

import (
	"sync"
	"testing"
)

func TestDebitInsufficient(t *testing.T) {
	l := New(2, 100)
	if !l.Debit(0, 100) {
		t.Fatal("exact debit must succeed")
	}
	if l.Debit(0, 1) {
		t.Fatal("overdraft must fail")
	}
	if l.Balance(0) != 0 {
		t.Fatal("balance changed on failed debit")
	}
}

func TestConcurrentConservation(t *testing.T) {
	const accounts, workers, ops = 64, 8, 200000
	l := New(accounts, 1_000_000)
	want := l.Total()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint32) {
			defer wg.Done()
			x := seed*2654435761 + 1
			for i := 0; i < ops; i++ {
				x ^= x << 13
				x ^= x >> 17
				x ^= x << 5
				from, to := x%accounts, (x>>8)%accounts
				if l.Debit(from, 7) {
					l.Credit(to, 7)
				}
			}
		}(uint32(w + 1))
	}
	wg.Wait()
	if l.Total() != want {
		t.Fatalf("total %d want %d", l.Total(), want)
	}
	for a := uint32(0); a < accounts; a++ {
		if l.Balance(a) < 0 {
			t.Fatalf("negative balance on %d", a)
		}
	}
}
