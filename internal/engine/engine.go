package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"eventengine/internal/dedupe"
	"eventengine/internal/event"
	"eventengine/internal/hash"
	"eventengine/internal/ledger"
	"eventengine/internal/metrics"
	"eventengine/internal/retry"
	"eventengine/internal/ring"
	"eventengine/internal/trace"
	"eventengine/internal/wal"
)

var (
	ErrInvalid      = errors.New("engine: invalid event")
	ErrDuplicate    = errors.New("engine: duplicate event")
	ErrBackpressure = errors.New("engine: queue full")
)

type Config struct {
	Accounts        int
	InitialBalance  int64
	Workers         int
	QueueSize       int
	DedupeCapacity  int
	MaxAttempts     int
	BaseBackoff     time.Duration
	MaxBackoff      time.Duration
	FaultRate       float64
	CommitFaultRate float64
	TraceSample     int
	TraceBuffer     int
	WALPath         string
	WALMode         wal.Mode
}

func (c *Config) normalize() error {
	if c.Accounts <= 0 {
		return fmt.Errorf("engine: accounts must be positive")
	}
	if c.Workers <= 0 {
		c.Workers = runtime.GOMAXPROCS(0)
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 4096
	}
	if c.DedupeCapacity <= 0 {
		c.DedupeCapacity = 1 << 20
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = time.Millisecond
	}
	if c.MaxBackoff < c.BaseBackoff {
		c.MaxBackoff = 64 * c.BaseBackoff
	}
	if c.FaultRate < 0 || c.FaultRate > 1 || c.CommitFaultRate < 0 || c.CommitFaultRate > 1 {
		return fmt.Errorf("engine: fault rates must be within [0,1]")
	}
	return nil
}

type rng uint64

func (r *rng) next() uint64 {
	x := uint64(*r)
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	*r = rng(x)
	return x
}

type Engine struct {
	cfg   Config
	start time.Time
	led   *ledger.Ledger
	dd    *dedupe.Set
	qs    []*ring.Queue
	reg   *metrics.Registry
	tr    *trace.Tracer
	sch   *retry.Scheduler
	wg    sync.WaitGroup
	pre   uint64
	post  uint64

	wal      *wal.Log
	stopOnce sync.Once
	recovery Recovery

	dlqMu sync.Mutex
	dlq   []event.Event
}

type Recovery struct {
	Records   int
	Committed int
	Rejected  int
	Dead      int
	Requeued  int
	Elapsed   time.Duration
}

func New(cfg Config) (*Engine, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	e := &Engine{
		cfg:   cfg,
		start: time.Now(),
		led:   ledger.New(cfg.Accounts, cfg.InitialBalance),
		dd:    dedupe.New(cfg.DedupeCapacity),
		qs:    make([]*ring.Queue, cfg.Workers),
		reg:   metrics.NewRegistry(cfg.Workers),
		tr:    trace.New(cfg.TraceSample, cfg.TraceBuffer),
		pre:   uint64(cfg.FaultRate * (1 << 32)),
		post:  uint64(cfg.CommitFaultRate * (1 << 32)),
	}
	e.sch = retry.New(e.now, func(ev event.Event) { e.qs[e.part(ev.From)].Push(ev) })
	for i := range e.qs {
		e.qs[i] = ring.New(cfg.QueueSize)
	}
	for i := range e.qs {
		e.wg.Add(1)
		go e.work(i, e.qs[i])
	}
	if cfg.WALPath != "" {
		if err := e.recover(); err != nil {
			e.Stop()
			return nil, err
		}
	}
	return e, nil
}

func (e *Engine) now() int64 { return int64(time.Since(e.start)) }

func (e *Engine) part(from uint32) int {
	return int(hash.Range(hash.Mix(uint64(from)), uint32(len(e.qs))))
}

func (e *Engine) Ledger() *ledger.Ledger { return e.led }

func (e *Engine) Ingest(ev event.Event) error {
	_, err := e.admit([]event.Event{ev}, true)
	return err
}

func (e *Engine) TryIngest(ev event.Event) error {
	_, err := e.admit([]event.Event{ev}, false)
	return err
}

func (e *Engine) IngestBatch(evs []event.Event) (int, error) {
	accepted := 0
	for len(evs) > 0 {
		n := min(len(evs), batchSize)
		k, err := e.admit(evs[:n], true)
		accepted += k
		if err != nil && err != ErrInvalid && err != ErrDuplicate {
			return accepted, err
		}
		evs = evs[n:]
	}
	return accepted, nil
}

func (e *Engine) admit(evs []event.Event, block bool) (int, error) {
	var acc [batchSize]event.Event
	n := 0
	var first error
	now := e.now()
	for i := range evs {
		ev := evs[i]
		st := e.reg.Stripe(ev.ID)
		if ev.ID > dedupe.MaxID || ev.Amount <= 0 || ev.From == ev.To ||
			int(ev.From) >= e.cfg.Accounts || int(ev.To) >= e.cfg.Accounts {
			st.Invalid.Add(1)
			first = firstErr(first, ErrInvalid)
			continue
		}
		ok, err := e.dd.Add(ev.ID)
		if err != nil {
			e.release(acc[:n])
			return 0, err
		}
		if !ok {
			st.Duplicates.Add(1)
			first = firstErr(first, ErrDuplicate)
			continue
		}
		ev.Born, ev.Enq, ev.Attempt = now, now, 0
		st.Accepted.Add(1)
		acc[n] = ev
		n++
	}
	if n == 0 {
		return 0, first
	}
	if e.wal != nil {
		tk, err := e.wal.AppendAccepts(acc[:n])
		if err == nil && e.cfg.WALMode == wal.Ack {
			err = e.wal.Wait(tk)
		}
		if err != nil {
			e.release(acc[:n])
			return 0, err
		}
	}
	if block {
		for i := 0; i < n; i++ {
			e.qs[e.part(acc[i].From)].Push(acc[i])
		}
		return n, first
	}
	for i := 0; i < n; i++ {
		if !e.qs[e.part(acc[i].From)].TryPush(acc[i]) {
			for _, ev := range acc[i:n] {
				e.void(ev)
			}
			return i, ErrBackpressure
		}
	}
	return n, first
}

func firstErr(cur, err error) error {
	if cur != nil {
		return cur
	}
	return err
}

func (e *Engine) release(evs []event.Event) {
	for i := range evs {
		e.dd.Remove(evs[i].ID)
		e.reg.Stripe(evs[i].ID).Accepted.Add(^uint64(0))
	}
}

func (e *Engine) void(ev event.Event) {
	st := e.reg.Stripe(ev.ID)
	if e.wal != nil {
		e.wal.AppendOutcomes(wal.KindVoid, []uint64{ev.ID})
	}
	e.dd.Remove(ev.ID)
	st.Accepted.Add(^uint64(0))
	st.Backpressure.Add(1)
}

func (e *Engine) recover() error {
	t0 := time.Now()
	type entry struct {
		ev   event.Event
		kind byte
	}
	state := make(map[uint64]entry, e.cfg.DedupeCapacity/4)
	var rec Recovery
	err := wal.Recover(e.cfg.WALPath,
		func(ev event.Event) {
			rec.Records++
			state[ev.ID] = entry{ev: ev}
		},
		func(kind byte, id uint64) {
			rec.Records++
			en, ok := state[id]
			switch {
			case !ok:
			case kind == wal.KindVoid:
				delete(state, id)
			default:
				en.kind = kind
				state[id] = en
			}
		})
	if err != nil {
		return err
	}
	w := &e.reg.W[0]
	var requeue []event.Event
	for id, en := range state {
		if ok, err := e.dd.Add(id); err != nil {
			return err
		} else if !ok {
			continue
		}
		e.reg.Stripe(id).Accepted.Add(1)
		switch en.kind {
		case wal.KindCommit:
			e.led.Apply(en.ev.From, en.ev.To, en.ev.Amount)
			w.Committed.Add(1)
			rec.Committed++
		case wal.KindReject:
			w.Rejected.Add(1)
			rec.Rejected++
		case wal.KindDead:
			e.dlq = append(e.dlq, en.ev)
			w.DeadLettered.Add(1)
			rec.Dead++
		default:
			requeue = append(requeue, en.ev)
		}
	}
	l, err := wal.Open(e.cfg.WALPath, e.cfg.WALMode)
	if err != nil {
		return err
	}
	e.wal = l
	now := e.now()
	for _, ev := range requeue {
		ev.Born, ev.Enq = now, now
		e.qs[e.part(ev.From)].Push(ev)
	}
	rec.Requeued = len(requeue)
	rec.Elapsed = time.Since(t0)
	e.recovery = rec
	return nil
}

func (e *Engine) Recovery() Recovery { return e.recovery }

func (e *Engine) Crash() {
	if e.wal != nil {
		e.wal.Abort()
	}
}

const batchSize = 64

func (e *Engine) work(id int, q *ring.Queue) {
	defer e.wg.Done()
	w := &e.reg.W[id]
	r := rng(uint64(id+1) * 0x9e3779b97f4a7c15)
	var buf [batchSize]event.Event
	var born [batchSize]int64
	var ids [batchSize]uint64
	for {
		n := q.PopBatch(buf[:])
		if n == 0 {
			return
		}
		done := 0
		for i := 0; i < n; i++ {
			if e.process(id, w, &r, &buf[i]) {
				born[done] = buf[i].Born
				ids[done] = buf[i].ID
				done++
			}
		}
		if done > 0 {
			t := e.now()
			for _, b := range born[:done] {
				w.Lat.Observe(t - b)
			}
			w.Committed.Add(uint64(done))
			if e.wal != nil {
				e.wal.AppendOutcomes(wal.KindCommit, ids[:done])
			}
		}
	}
}

func (e *Engine) process(wid int, w *metrics.Worker, r *rng, ev *event.Event) bool {
	sampled := e.tr.Sampled(ev.ID)
	var t0 int64
	if sampled {
		t0 = e.now()
		e.span(*ev, trace.Queue, wid, ev.Enq, t0-ev.Enq)
	}
	if e.pre != 0 && r.next()>>32 < e.pre {
		w.Faults.Add(1)
		e.fail(wid, w, r, *ev, sampled)
		return false
	}
	if !e.led.Debit(ev.From, ev.Amount) {
		t := e.now()
		if e.wal != nil {
			e.wal.AppendOutcomes(wal.KindReject, []uint64{ev.ID})
		}
		w.Rejected.Add(1)
		w.Lat.Observe(t - ev.Born)
		if sampled {
			e.span(*ev, trace.Reject, wid, t, 0)
		}
		return false
	}
	var t1 int64
	if sampled {
		t1 = e.now()
		e.span(*ev, trace.Debit, wid, t0, t1-t0)
	}
	if e.post != 0 && r.next()>>32 < e.post {
		e.led.Credit(ev.From, ev.Amount)
		w.Compensated.Add(1)
		w.Faults.Add(1)
		e.fail(wid, w, r, *ev, sampled)
		return false
	}
	e.led.Credit(ev.To, ev.Amount)
	if sampled {
		t2 := e.now()
		e.span(*ev, trace.Credit, wid, t1, t2-t1)
		e.span(*ev, trace.Commit, wid, t2, 0)
	}
	return true
}

func (e *Engine) fail(wid int, w *metrics.Worker, r *rng, ev event.Event, sampled bool) {
	t := e.now()
	next := ev.Attempt + 1
	if int(next) >= e.cfg.MaxAttempts {
		e.dlqMu.Lock()
		e.dlq = append(e.dlq, ev)
		e.dlqMu.Unlock()
		if e.wal != nil {
			e.wal.AppendOutcomes(wal.KindDead, []uint64{ev.ID})
		}
		w.Lat.Observe(t - ev.Born)
		w.DeadLettered.Add(1)
		if sampled {
			e.span(ev, trace.Dead, wid, t, 0)
		}
		return
	}
	d := int64(e.cfg.BaseBackoff) << (next - 1)
	if d <= 0 || d > int64(e.cfg.MaxBackoff) {
		d = int64(e.cfg.MaxBackoff)
	}
	d = d/2 + int64(r.next()%uint64(d))
	w.Retried.Add(1)
	if sampled {
		e.span(ev, trace.Retry, wid, t, d)
	}
	ev.Attempt = next
	ev.Enq = t + d
	e.sch.Add(ev, time.Duration(d))
}

func (e *Engine) span(ev event.Event, st trace.Stage, wid int, start, dur int64) {
	e.tr.Record(trace.Span{
		Trace:   ev.ID,
		Start:   start,
		Dur:     dur,
		Stage:   st,
		Worker:  uint16(wid),
		Attempt: uint8(ev.Attempt),
	})
}

func (e *Engine) Drain(ctx context.Context) error {
	for e.reg.Pending() != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Microsecond):
		}
	}
	return nil
}

func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		e.sch.Close()
		for _, q := range e.qs {
			q.Close()
		}
		e.wg.Wait()
		if e.wal != nil {
			e.wal.Close()
		}
	})
}

func (e *Engine) DeadLetters() []event.Event {
	e.dlqMu.Lock()
	defer e.dlqMu.Unlock()
	return append([]event.Event(nil), e.dlq...)
}

func (e *Engine) ReplayDeadLetters() int {
	e.dlqMu.Lock()
	evs := e.dlq
	e.dlq = nil
	e.dlqMu.Unlock()
	for _, ev := range evs {
		ev.Attempt = 0
		ev.Born = e.now()
		ev.Enq = ev.Born
		e.reg.Stripe(ev.ID).Accepted.Add(1)
		e.qs[e.part(ev.From)].Push(ev)
	}
	return len(evs)
}

func (e *Engine) Snapshot() metrics.Snapshot {
	depth := 0
	for _, q := range e.qs {
		depth += q.Len()
	}
	return e.reg.Snapshot(depth)
}

func (e *Engine) Traces(limit int) []trace.Trace { return e.tr.Traces(limit) }
