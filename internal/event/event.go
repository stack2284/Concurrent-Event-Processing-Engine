package event

type Event struct {
	ID      uint64
	Amount  int64
	Born    int64
	Enq     int64
	From    uint32
	To      uint32
	Attempt uint32
}
