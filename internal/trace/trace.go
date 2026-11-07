package trace

import (
	"slices"
	"sync"

	"eventengine/internal/hash"
)

type Stage uint8

const (
	Queue Stage = iota
	Debit
	Credit
	Commit
	Retry
	Reject
	Dead
)

var names = [...]string{"queue", "debit", "credit", "commit", "retry", "reject", "dead"}

func (s Stage) String() string { return names[s] }

type Span struct {
	Trace   uint64
	Start   int64
	Dur     int64
	Stage   Stage
	Worker  uint16
	Attempt uint8
}

type Trace struct {
	ID    uint64
	Start int64
	Total int64
	Spans []Span
}

type Tracer struct {
	on   bool
	mask uint64
	mu   sync.Mutex
	ring []Span
	pos  uint64
}

func New(sampleEvery, capacity int) *Tracer {
	t := &Tracer{}
	if sampleEvery <= 0 || capacity <= 0 {
		return t
	}
	n := 1
	for n < sampleEvery {
		n <<= 1
	}
	t.on = true
	t.mask = uint64(n - 1)
	t.ring = make([]Span, capacity)
	return t
}

func (t *Tracer) Sampled(id uint64) bool {
	return t.on && (hash.Mix(id)>>32)&t.mask == 0
}

func (t *Tracer) Record(s Span) {
	t.mu.Lock()
	t.ring[t.pos%uint64(len(t.ring))] = s
	t.pos++
	t.mu.Unlock()
}

func (t *Tracer) Traces(limit int) []Trace {
	if !t.on {
		return nil
	}
	t.mu.Lock()
	n := min(t.pos, uint64(len(t.ring)))
	spans := make([]Span, n)
	copy(spans, t.ring[:n])
	t.mu.Unlock()

	byID := make(map[uint64]*Trace)
	for _, s := range spans {
		tr := byID[s.Trace]
		if tr == nil {
			tr = &Trace{ID: s.Trace, Start: s.Start}
			byID[s.Trace] = tr
		}
		tr.Spans = append(tr.Spans, s)
	}
	out := make([]Trace, 0, len(byID))
	for _, tr := range byID {
		slices.SortFunc(tr.Spans, func(a, b Span) int {
			switch {
			case a.Start < b.Start:
				return -1
			case a.Start > b.Start:
				return 1
			}
			return int(a.Stage) - int(b.Stage)
		})
		first, last := tr.Spans[0], tr.Spans[len(tr.Spans)-1]
		tr.Start = first.Start
		tr.Total = last.Start + last.Dur - first.Start
		out = append(out, *tr)
	}
	slices.SortFunc(out, func(a, b Trace) int {
		switch {
		case a.Start > b.Start:
			return -1
		case a.Start < b.Start:
			return 1
		}
		return 0
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}
