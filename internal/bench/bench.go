package bench

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"eventengine/internal/engine"
	"eventengine/internal/event"
	"eventengine/internal/hash"
	"eventengine/internal/wal"
)

const (
	initialBalance = 1 << 40
	ingestChunk    = 128
)

type Scenario struct {
	Name      string
	Events    int
	Accounts  int
	Workers   int
	Producers int
	Queue     int
	DupRate   float64
	FaultRate float64
	Skew      float64
	Rate      int
	Seed      uint64
	WAL       string
}

type Result struct {
	Scenario
	Offered      int
	Elapsed      time.Duration
	PerSec       float64
	UniquePerSec float64
	Duplicates   uint64
	Retried      uint64
	Compensated  uint64
	DeadLettered uint64
	Rejected     uint64
	P50, P99     time.Duration
	P999         time.Duration
	AllocsPerEv  float64
	HeapMB       float64
	Consistent   bool
	Mismatches   int
	Recovery     time.Duration
	Recovered    int
}

func generate(s Scenario) (all []event.Event, delta []int64) {
	r := rand.New(rand.NewPCG(s.Seed, s.Seed^0xdeadbeef))
	pick := func() uint32 {
		if s.Skew > 0 && r.Float64() < s.Skew {
			return uint32(r.IntN(10))
		}
		return uint32(r.IntN(s.Accounts))
	}
	uniq := make([]event.Event, s.Events)
	delta = make([]int64, s.Accounts)
	for i := range uniq {
		from, to := pick(), pick()
		for to == from {
			to = pick()
		}
		amt := int64(r.IntN(1000) + 1)
		uniq[i] = event.Event{ID: hash.Mix(uint64(i) + s.Seed<<32), From: from, To: to, Amount: amt}
		delta[from] -= amt
		delta[to] += amt
	}
	dups := int(float64(s.Events) * s.DupRate)
	all = make([]event.Event, 0, s.Events+dups)
	all = append(all, uniq...)
	for i := 0; i < dups; i++ {
		all = append(all, uniq[r.IntN(s.Events)])
	}
	r.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	return all, delta
}

func Run(s Scenario) (Result, error) {
	if s.Producers <= 0 {
		s.Producers = 4
	}
	if s.Seed == 0 {
		s.Seed = 1
	}
	all, delta := generate(s)

	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	cfg := engine.Config{
		Accounts:        s.Accounts,
		InitialBalance:  initialBalance,
		Workers:         s.Workers,
		QueueSize:       s.Queue,
		DedupeCapacity:  len(all),
		MaxAttempts:     6,
		BaseBackoff:     200 * time.Microsecond,
		FaultRate:       s.FaultRate,
		CommitFaultRate: s.FaultRate,
		TraceSample:     1024,
		TraceBuffer:     1 << 14,
	}
	if s.WAL != "" {
		dir, err := os.MkdirTemp("", "engine-wal")
		if err != nil {
			return Result{}, err
		}
		defer os.RemoveAll(dir)
		cfg.WALPath = filepath.Join(dir, "wal.log")
		switch s.WAL {
		case "buffered":
			cfg.WALMode = wal.Buffered
		case "group":
			cfg.WALMode = wal.Group
		case "ack":
			cfg.WALMode = wal.Ack
		default:
			return Result{}, fmt.Errorf("bench: unknown wal mode %q", s.WAL)
		}
	}
	e, err := engine.New(cfg)
	if err != nil {
		return Result{}, err
	}
	defer e.Stop()

	chunk := (len(all) + s.Producers - 1) / s.Producers
	perProducer := float64(s.Rate) / float64(s.Producers)
	var wg sync.WaitGroup
	t0 := time.Now()
	for p := 0; p < s.Producers; p++ {
		lo, hi := p*chunk, min((p+1)*chunk, len(all))
		wg.Add(1)
		go func(part []event.Event) {
			defer wg.Done()
			begin := time.Now()
			for i := 0; i < len(part); i += ingestChunk {
				if perProducer > 0 {
					due := begin.Add(time.Duration(float64(i) / perProducer * float64(time.Second)))
					if d := time.Until(due); d > 0 {
						time.Sleep(d)
					}
				}
				e.IngestBatch(part[i:min(i+ingestChunk, len(part))])
			}
		}(all[lo:hi])
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := e.Drain(ctx); err != nil {
		return Result{}, err
	}
	elapsed := time.Since(t0)
	runtime.ReadMemStats(&m1)

	snap := e.Snapshot()
	deadLettered := snap.DeadLettered
	for e.ReplayDeadLetters() > 0 {
		if err := e.Drain(ctx); err != nil {
			return Result{}, err
		}
	}
	final := e.Snapshot()

	mismatches := 0
	for a := range delta {
		if e.Ledger().Balance(uint32(a)) != initialBalance+delta[a] {
			mismatches++
		}
	}
	dupWant := uint64(len(all) - s.Events)
	ok := final.Committed+final.Rejected == uint64(s.Events) &&
		final.Duplicates == dupWant &&
		final.Pending == 0 &&
		final.Invalid == 0 &&
		e.Ledger().Total() == int64(s.Accounts)*initialBalance &&
		(final.Rejected != 0 || mismatches == 0)

	var recovery time.Duration
	recovered := 0
	if s.WAL != "" {
		before := make([]int64, s.Accounts)
		for a := range before {
			before[a] = e.Ledger().Balance(uint32(a))
		}
		e.Stop()
		r, err := engine.New(cfg)
		if err != nil {
			return Result{}, err
		}
		defer r.Stop()
		rec := r.Recovery()
		recovery, recovered = rec.Elapsed, rec.Records
		for a := range before {
			if r.Ledger().Balance(uint32(a)) != before[a] {
				ok = false
				break
			}
		}
		ok = ok && rec.Requeued == 0
	}

	return Result{
		Scenario:     s,
		Offered:      len(all),
		Elapsed:      elapsed,
		PerSec:       float64(len(all)) / elapsed.Seconds(),
		UniquePerSec: float64(s.Events) / elapsed.Seconds(),
		Duplicates:   snap.Duplicates,
		Retried:      snap.Retried,
		Compensated:  snap.Compensated,
		DeadLettered: deadLettered,
		Rejected:     final.Rejected,
		P50:          snap.Latency.P50,
		P99:          snap.Latency.P99,
		P999:         snap.Latency.P999,
		AllocsPerEv:  float64(m1.Mallocs-m0.Mallocs) / float64(len(all)),
		HeapMB:       float64(m1.HeapAlloc-m0.HeapAlloc) / (1 << 20),
		Consistent:   ok,
		Mismatches:   mismatches,
		Recovery:     recovery,
		Recovered:    recovered,
	}, nil
}
