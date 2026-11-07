package hash

func Mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func Range(h uint64, n uint32) uint32 {
	return uint32(((h >> 32) * uint64(n)) >> 32)
}
