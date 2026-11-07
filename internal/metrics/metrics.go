package metrics

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

const (
	subBits  = 3
	subCount = 1 << subBits
	nBuckets = (64 - subBits + 1) * subCount
	stripes  = 16
)

func bucket(v uint64) int {
	if v < subCount {
		return int(v)
	}
	e := bits.Len64(v) - 1
	return (e-subBits+1)*subCount + int((v>>(e-subBits))&(subCount-1))
}

func upper(i int) uint64 {
	if i < subCount {
		return uint64(i)
	}
	shift := uint(i/subCount - 1)
	lo := uint64(subCount+i%subCount) << shift
	return lo + (1 << shift) - 1
}

type Hist struct {
	b   [nBuckets]atomic.Uint64
	sum atomic.Uint64
	max atomic.Uint64
}

func (h *Hist) Observe(v int64) {
	u := uint64(max(v, 0))
	h.b[bucket(u)].Add(1)
	h.sum.Add(u)
	for {
		m := h.max.Load()
		if u <= m || h.max.CompareAndSwap(m, u) {
			return
		}
	}
}

type Quantiles struct {
	Count                          uint64
	Mean, P50, P90, P99, P999, Max time.Duration
}

func Merge(hs []*Hist) Quantiles {
	var agg [nBuckets]uint64
	var n, sum, mx uint64
	for _, h := range hs {
		for i := range agg {
			agg[i] += h.b[i].Load()
		}
		sum += h.sum.Load()
		mx = max(mx, h.max.Load())
	}
	for _, c := range agg {
		n += c
	}
	if n == 0 {
		return Quantiles{}
	}
	q := func(p float64) time.Duration {
		rank := max(uint64(math.Ceil(p*float64(n))), 1)
		var cum uint64
		for i, c := range agg {
			cum += c
			if cum >= rank {
				return time.Duration(min(upper(i), mx))
			}
		}
		return time.Duration(mx)
	}
	return Quantiles{
		Count: n,
		Mean:  time.Duration(sum / n),
		P50:   q(0.50),
		P90:   q(0.90),
		P99:   q(0.99),
		P999:  q(0.999),
		Max:   time.Duration(mx),
	}
}

type Worker struct {
	Committed    atomic.Uint64
	Rejected     atomic.Uint64
	Retried      atomic.Uint64
	DeadLettered atomic.Uint64
	Compensated  atomic.Uint64
	Faults       atomic.Uint64
	Lat          Hist
	_            [64]byte
}

type IngestStripe struct {
	Accepted     atomic.Uint64
	Duplicates   atomic.Uint64
	Invalid      atomic.Uint64
	Backpressure atomic.Uint64
	_            [32]byte
}

type Registry struct {
	start time.Time
	ing   [stripes]IngestStripe
	W     []Worker
}

func NewRegistry(workers int) *Registry {
	return &Registry{start: time.Now(), W: make([]Worker, workers)}
}

func (r *Registry) Stripe(id uint64) *IngestStripe {
	return &r.ing[id&(stripes-1)]
}

func (r *Registry) Accepted() uint64 {
	var n uint64
	for i := range r.ing {
		n += r.ing[i].Accepted.Load()
	}
	return n
}

func (r *Registry) Finished() uint64 {
	var n uint64
	for i := range r.W {
		w := &r.W[i]
		n += w.Committed.Load() + w.Rejected.Load() + w.DeadLettered.Load()
	}
	return n
}

func (r *Registry) Pending() int64 {
	f := r.Finished()
	return int64(r.Accepted() - f)
}

type Snapshot struct {
	Uptime       time.Duration
	Accepted     uint64
	Duplicates   uint64
	Invalid      uint64
	Backpressure uint64
	Committed    uint64
	Rejected     uint64
	Retried      uint64
	DeadLettered uint64
	Compensated  uint64
	Faults       uint64
	Pending      int64
	QueueDepth   int
	Latency      Quantiles
}

func (r *Registry) Snapshot(queueDepth int) Snapshot {
	s := Snapshot{Uptime: time.Since(r.start), QueueDepth: queueDepth}
	fin := r.Finished()
	for i := range r.ing {
		st := &r.ing[i]
		s.Accepted += st.Accepted.Load()
		s.Duplicates += st.Duplicates.Load()
		s.Invalid += st.Invalid.Load()
		s.Backpressure += st.Backpressure.Load()
	}
	hs := make([]*Hist, len(r.W))
	for i := range r.W {
		w := &r.W[i]
		s.Committed += w.Committed.Load()
		s.Rejected += w.Rejected.Load()
		s.Retried += w.Retried.Load()
		s.DeadLettered += w.DeadLettered.Load()
		s.Compensated += w.Compensated.Load()
		s.Faults += w.Faults.Load()
		hs[i] = &w.Lat
	}
	s.Pending = int64(s.Accepted - fin)
	s.Latency = Merge(hs)
	return s
}
