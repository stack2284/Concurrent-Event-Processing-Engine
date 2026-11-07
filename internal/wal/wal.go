package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"

	"eventengine/internal/event"
)

type Mode int

const (
	Buffered Mode = iota
	Group
	Ack
)

const (
	recAccept  byte = 1
	KindCommit byte = 2
	KindReject byte = 3
	KindDead   byte = 4
	KindVoid   byte = 5

	acceptSize  = 1 + 8 + 4 + 4 + 8 + 4
	outcomeSize = 1 + 8 + 4
	maxBuffered = 64 << 20
	initialBuf  = 1 << 20
)

var (
	ErrClosed = errors.New("wal: closed")
	table     = crc32.MakeTable(crc32.Castagnoli)
)

type Log struct {
	f    *os.File
	mode Mode
	kick chan struct{}
	done chan struct{}

	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	spare    []byte
	appended uint64
	synced   uint64
	closed   bool
	err      error
}

func Open(path string, mode Mode) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{
		f:     f,
		mode:  mode,
		kick:  make(chan struct{}, 1),
		done:  make(chan struct{}),
		buf:   make([]byte, 0, initialBuf),
		spare: make([]byte, 0, initialBuf),
	}
	l.cond = sync.NewCond(&l.mu)
	go l.run()
	return l, nil
}

func (l *Log) lock() error {
	l.mu.Lock()
	for len(l.buf) >= maxBuffered && l.err == nil && !l.closed {
		l.cond.Wait()
	}
	if l.err != nil {
		l.mu.Unlock()
		return l.err
	}
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	return nil
}

func (l *Log) release() uint64 {
	t := l.appended
	l.mu.Unlock()
	select {
	case l.kick <- struct{}{}:
	default:
	}
	return t
}

func (l *Log) AppendAccepts(evs []event.Event) (uint64, error) {
	if err := l.lock(); err != nil {
		return 0, err
	}
	for i := range evs {
		ev := &evs[i]
		start := len(l.buf)
		l.buf = append(l.buf, recAccept)
		l.buf = binary.LittleEndian.AppendUint64(l.buf, ev.ID)
		l.buf = binary.LittleEndian.AppendUint32(l.buf, ev.From)
		l.buf = binary.LittleEndian.AppendUint32(l.buf, ev.To)
		l.buf = binary.LittleEndian.AppendUint64(l.buf, uint64(ev.Amount))
		l.buf = binary.LittleEndian.AppendUint32(l.buf, crc32.Checksum(l.buf[start:], table))
	}
	l.appended += uint64(len(evs) * acceptSize)
	return l.release(), nil
}

func (l *Log) AppendOutcomes(kind byte, ids []uint64) (uint64, error) {
	if err := l.lock(); err != nil {
		return 0, err
	}
	for _, id := range ids {
		start := len(l.buf)
		l.buf = append(l.buf, kind)
		l.buf = binary.LittleEndian.AppendUint64(l.buf, id)
		l.buf = binary.LittleEndian.AppendUint32(l.buf, crc32.Checksum(l.buf[start:], table))
	}
	l.appended += uint64(len(ids) * outcomeSize)
	return l.release(), nil
}

func (l *Log) Wait(ticket uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.synced < ticket && l.err == nil {
		if l.closed && len(l.buf) == 0 && l.synced >= l.appended {
			break
		}
		l.cond.Wait()
	}
	return l.err
}

func (l *Log) run() {
	defer close(l.done)
	for range l.kick {
		l.mu.Lock()
		if len(l.buf) == 0 {
			end := l.closed
			l.mu.Unlock()
			if end {
				return
			}
			continue
		}
		b, target := l.buf, l.appended
		l.buf = l.spare[:0]
		l.cond.Broadcast()
		l.mu.Unlock()

		_, err := l.f.Write(b)
		if err == nil && l.mode != Buffered {
			err = l.f.Sync()
		}

		l.mu.Lock()
		l.spare = b[:0]
		l.synced = target
		if err != nil && l.err == nil {
			l.err = err
		}
		end := l.closed && len(l.buf) == 0
		l.cond.Broadcast()
		l.mu.Unlock()
		if end {
			return
		}
	}
}

func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return l.err
	}
	l.closed = true
	l.mu.Unlock()
	l.kick <- struct{}{}
	<-l.done
	l.mu.Lock()
	l.cond.Broadcast()
	l.mu.Unlock()
	err := l.f.Close()
	if l.err != nil {
		return l.err
	}
	return err
}

func (l *Log) Abort() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.buf = l.buf[:0]
	l.err = ErrClosed
	l.cond.Broadcast()
	l.mu.Unlock()
	select {
	case l.kick <- struct{}{}:
	default:
	}
	<-l.done
	l.f.Close()
}

func Recover(path string, accept func(event.Event), outcome func(kind byte, id uint64)) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	off := 0
loop:
	for off < len(b) {
		switch kind := b[off]; kind {
		case recAccept:
			if len(b)-off < acceptSize || crc32.Checksum(b[off:off+acceptSize-4], table) != binary.LittleEndian.Uint32(b[off+acceptSize-4:]) {
				break loop
			}
			p := b[off+1:]
			accept(event.Event{
				ID:     binary.LittleEndian.Uint64(p),
				From:   binary.LittleEndian.Uint32(p[8:]),
				To:     binary.LittleEndian.Uint32(p[12:]),
				Amount: int64(binary.LittleEndian.Uint64(p[16:])),
			})
			off += acceptSize
		case KindCommit, KindReject, KindDead, KindVoid:
			if len(b)-off < outcomeSize || crc32.Checksum(b[off:off+outcomeSize-4], table) != binary.LittleEndian.Uint32(b[off+outcomeSize-4:]) {
				break loop
			}
			outcome(kind, binary.LittleEndian.Uint64(b[off+1:]))
			off += outcomeSize
		default:
			break loop
		}
	}
	if off < len(b) {
		return os.Truncate(path, int64(off))
	}
	return nil
}
