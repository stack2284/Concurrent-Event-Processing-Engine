package ledger

import "sync/atomic"

type slot struct {
	bal atomic.Int64
	_   [56]byte
}

type Ledger struct {
	acc []slot
}

func New(accounts int, initial int64) *Ledger {
	l := &Ledger{acc: make([]slot, accounts)}
	for i := range l.acc {
		l.acc[i].bal.Store(initial)
	}
	return l
}

func (l *Ledger) Size() int { return len(l.acc) }

func (l *Ledger) Debit(a uint32, amt int64) bool {
	p := &l.acc[a].bal
	for {
		cur := p.Load()
		if cur < amt {
			return false
		}
		if p.CompareAndSwap(cur, cur-amt) {
			return true
		}
	}
}

func (l *Ledger) Credit(a uint32, amt int64) {
	l.acc[a].bal.Add(amt)
}

func (l *Ledger) Balance(a uint32) int64 {
	return l.acc[a].bal.Load()
}

func (l *Ledger) Total() int64 {
	var t int64
	for i := range l.acc {
		t += l.acc[i].bal.Load()
	}
	return t
}

func (l *Ledger) Apply(from, to uint32, amt int64) {
	l.acc[from].bal.Add(-amt)
	l.acc[to].bal.Add(amt)
}
