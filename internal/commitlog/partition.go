package commitlog

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Message is one record to append.
type Message struct {
	Key, Value []byte
}

// Partition is an ordered, append-only sequence of records split into segments.
//
// Appends are serialized by mu. Durability is decoupled from appends: a flusher
// fsyncs the active segment on a timer or as soon as a producer waits for acks=all,
// so many concurrent producers share one fsync (group commit).
type Partition struct {
	dir string

	mu       sync.RWMutex
	segs     []*segment
	buf      []byte
	pos      []int
	notify   chan struct{} // closed and replaced on every append (long-poll wakeup)
	durable  int64         // highest offset known to be fsynced
	flushReq chan struct{}
	durCond  *sync.Cond

	SegmentBytes int64
}

func OpenPartition(dir string) (*Partition, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range ents {
		if b, ok := strings.CutSuffix(e.Name(), ".log"); ok {
			if n, err := strconv.ParseInt(b, 10, 64); err == nil {
				bases = append(bases, n)
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	if len(bases) == 0 {
		bases = []int64{0}
	}
	p := &Partition{dir: dir, notify: make(chan struct{}), flushReq: make(chan struct{}, 1), SegmentBytes: SegmentBytes}
	p.durCond = sync.NewCond(&p.mu)
	for i, b := range bases {
		s, err := openSegment(dir, b, i < len(bases)-1)
		if err != nil {
			return nil, err
		}
		// a crash can leave a later segment whose base does not continue the previous one
		if i > 0 && b != p.segs[len(p.segs)-1].next {
			s.close()
			os.Remove(s.path)
			continue
		}
		p.segs = append(p.segs, s)
	}
	p.durable = p.active().next - 1
	return p, nil
}

func (p *Partition) active() *segment { return p.segs[len(p.segs)-1] }

// HighWatermark is the next offset that will be assigned.
func (p *Partition) HighWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active().next
}

func (p *Partition) LowWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.segs[0].base
}

// Append writes msgs contiguously and returns the first assigned offset.
func (p *Partition) Append(msgs []Message) (int64, error) {
	ts := time.Now().UnixNano()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active().size >= p.SegmentBytes {
		if err := p.roll(); err != nil {
			return 0, err
		}
	}
	a := p.active()
	first := a.next
	p.buf, p.pos = p.buf[:0], p.pos[:0]
	for i, m := range msgs {
		if len(m.Key)+len(m.Value) > MaxRecord || len(m.Key) > 0xffff {
			return 0, ErrTooLarge
		}
		p.pos = append(p.pos, len(p.buf))
		p.buf = encode(p.buf, first+int64(i), ts, m.Key, m.Value)
	}
	if err := a.write(p.buf, first, p.pos, ts); err != nil {
		return 0, err
	}
	close(p.notify)
	p.notify = make(chan struct{})
	return first, nil
}

func (p *Partition) roll() error {
	a := p.active()
	if err := a.f.Sync(); err != nil {
		return err
	}
	if err := a.seal(); err != nil {
		return err
	}
	s, err := openSegment(p.dir, a.next, false)
	if err != nil {
		return err
	}
	p.segs = append(p.segs, s)
	return nil
}

// WaitDurable blocks until offset off has been fsynced.
func (p *Partition) WaitDurable(off int64) {
	select {
	case p.flushReq <- struct{}{}:
	default:
	}
	p.mu.Lock()
	for p.durable < off {
		p.durCond.Wait()
	}
	p.mu.Unlock()
}

// Flusher fsyncs the active segment every interval, or immediately when a
// producer is waiting. One fsync covers every append made before it started.
func (p *Partition) Flusher(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.flush()
			return
		case <-t.C:
		case <-p.flushReq:
		}
		p.flush()
	}
}

func (p *Partition) flush() {
	p.mu.RLock()
	a := p.active()
	upto := a.next - 1
	p.mu.RUnlock()
	if upto <= p.durableSnapshot() {
		return
	}
	a.f.Sync() // an append racing with Sync is covered by the next flush
	p.mu.Lock()
	if upto > p.durable {
		p.durable = upto
	}
	p.durCond.Broadcast()
	p.mu.Unlock()
}

func (p *Partition) durableSnapshot() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.durable
}

// Chunk is a byte range of a segment file holding whole records.
type Chunk struct {
	Path       string
	Pos, Len   int64
	Records    int64
	NextOffset int64
	HighWater  int64
}

// Read locates records starting at off. If off == high watermark and wait > 0,
// it long-polls for new data. The caller streams Chunk from disk (sendfile-friendly).
func (p *Partition) Read(ctx context.Context, off, maxBytes int64, wait time.Duration) (Chunk, error) {
	deadline := time.Now().Add(wait)
	for {
		p.mu.RLock()
		hw := p.active().next
		low := p.segs[0].base
		if off < low || off > hw {
			p.mu.RUnlock()
			return Chunk{HighWater: hw}, ErrOutOfRange
		}
		if off < hw {
			i := sort.Search(len(p.segs), func(i int) bool { return p.segs[i].base > off }) - 1
			s := p.segs[i]
			pos, err := s.locate(off)
			if err != nil {
				p.mu.RUnlock()
				return Chunk{}, err
			}
			n, cnt, err := s.span(pos, maxBytes)
			p.mu.RUnlock()
			return Chunk{Path: s.path, Pos: pos, Len: n, Records: cnt, NextOffset: off + cnt, HighWater: hw}, err
		}
		ch := p.notify
		p.mu.RUnlock()
		left := time.Until(deadline)
		if left <= 0 {
			return Chunk{NextOffset: off, HighWater: hw}, nil
		}
		select {
		case <-ch:
		case <-time.After(left):
		case <-ctx.Done():
			return Chunk{NextOffset: off, HighWater: hw}, ctx.Err()
		}
	}
}

// Retain deletes whole segments whose newest record is older than cutoff.
// The active segment is never deleted.
func (p *Partition) Retain(cutoff time.Time) (removed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.segs) > 1 && p.segs[0].maxTS < cutoff.UnixNano() {
		s := p.segs[0]
		s.close()
		os.Remove(s.path)
		os.Remove(idxPath(s.path))
		p.segs = p.segs[1:]
		removed++
	}
	return
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.segs {
		s.f.Sync()
		s.close()
	}
	return nil
}

func (p *Partition) Dir() string { return filepath.Clean(p.dir) }
