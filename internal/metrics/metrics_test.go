package metrics

import "testing"

func TestBucketBounds(t *testing.T) {
	for _, v := range []uint64{0, 1, 7, 8, 9, 15, 16, 1000, 123456789, 1 << 40} {
		i := bucket(v)
		if hi := upper(i); hi < v || float64(hi) > float64(v)*1.126+1 {
			t.Fatalf("v=%d bucket=%d upper=%d", v, i, hi)
		}
	}
}

func TestQuantiles(t *testing.T) {
	var h Hist
	for v := int64(1); v <= 10000; v++ {
		h.Observe(v)
	}
	q := Merge([]*Hist{&h})
	if q.Count != 10000 || q.Max != 10000 {
		t.Fatalf("bad summary %+v", q)
	}
	if p := float64(q.P50); p < 5000 || p > 5000*1.13 {
		t.Fatalf("p50=%v", q.P50)
	}
	if p := float64(q.P99); p < 9900 || p > 9900*1.13 {
		t.Fatalf("p99=%v", q.P99)
	}
}
