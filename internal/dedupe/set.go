package dedupe

import (
	"errors"
	"math/bits"
	"sync/atomic"

	"eventengine/internal/hash"
)

const (
	tomb     = ^uint64(0)
	maxProbe = 1024
	MaxID    = tomb - 2
)

var ErrFull = errors.New("dedupe: probe limit reached")

type Set struct {
	keys []uint64
	mask uint64
}

func New(capacity int) *Set {
	n := 1 << bits.Len(uint(capacity*2-1))
	if n < 16 {
		n = 16
	}
	return &Set{keys: make([]uint64, n), mask: uint64(n - 1)}
}

func (s *Set) Add(id uint64) (bool, error) {
	k := id + 1
	i := hash.Mix(id) & s.mask
	for p := 0; p < maxProbe; p++ {
		switch atomic.LoadUint64(&s.keys[i]) {
		case k:
			return false, nil
		case 0:
			if atomic.CompareAndSwapUint64(&s.keys[i], 0, k) {
				return true, nil
			}
			if atomic.LoadUint64(&s.keys[i]) == k {
				return false, nil
			}
		}
		i = (i + 1) & s.mask
	}
	return false, ErrFull
}

func (s *Set) Remove(id uint64) bool {
	k := id + 1
	i := hash.Mix(id) & s.mask
	for p := 0; p < maxProbe; p++ {
		switch atomic.LoadUint64(&s.keys[i]) {
		case k:
			return atomic.CompareAndSwapUint64(&s.keys[i], k, tomb)
		case 0:
			return false
		}
		i = (i + 1) & s.mask
	}
	return false
}

func (s *Set) Has(id uint64) bool {
	k := id + 1
	i := hash.Mix(id) & s.mask
	for p := 0; p < maxProbe; p++ {
		switch atomic.LoadUint64(&s.keys[i]) {
		case k:
			return true
		case 0:
			return false
		}
		i = (i + 1) & s.mask
	}
	return false
}
