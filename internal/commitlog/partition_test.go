package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// openT closes the partition before TempDir cleanup (Windows cannot delete open files).
func openT(tb testing.TB) *Partition {
	tb.Helper()
	p, err := OpenPartition(tb.TempDir())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { p.Close() })
	return p
}

func readAll(t *testing.T, p *Partition, from int64) []string {
	t.Helper()
	var out []string
	off := from
	for off < p.HighWatermark() {
		c, err := p.Read(context.Background(), off, 64<<10, 0)
		if err != nil {
			t.Fatal(err)
		}
		b := make([]byte, c.Len)
		f, _ := os.Open(c.Path)
		f.ReadAt(b, c.Pos)
		f.Close()
		want := off
		if err := Decode(b, func(o, _ int64, k, v []byte) {
			if o != want {
				t.Fatalf("offset %d, want %d", o, want)
			}
			want++
			out = append(out, string(k)+"="+string(v))
		}); err != nil {
			t.Fatal(err)
		}
		off = c.NextOffset
	}
	return out
}

func TestAppendReadAcrossSegments(t *testing.T) {
	p := openT(t)
	p.SegmentBytes = 10 << 10 // force many segments
	var want []string
	for i := 0; i < 3000; i++ {
		k, v := fmt.Sprintf("k%d", i%7), fmt.Sprintf("value-%05d", i)
		if _, err := p.Append([]Message{{[]byte(k), []byte(v)}}); err != nil {
			t.Fatal(err)
		}
		want = append(want, k+"="+v)
	}
	if len(p.segs) < 5 {
		t.Fatalf("expected rolling, got %d segments", len(p.segs))
	}
	got := readAll(t, p, 0)
	if len(got) != len(want) {
		t.Fatalf("read %d records, want %d", len(got), len(want))
	}
	for _, from := range []int64{1, 777, 1500, 2999} { // random starts hit the sparse index
		if g := readAll(t, p, from); g[0] != want[from] {
			t.Fatalf("read from %d: got %s want %s", from, g[0], want[from])
		}
	}
}

func TestRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPartition(dir)
	for i := 0; i < 100; i++ {
		p.Append([]Message{{nil, []byte(fmt.Sprintf("m%d", i))}})
	}
	path := p.active().path
	p.Close()
	st, _ := os.Stat(path)
	os.Truncate(path, st.Size()-5) // crash in the middle of the last record

	p2, err := OpenPartition(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p2.Close() })
	if hw := p2.HighWatermark(); hw != 99 {
		t.Fatalf("high watermark after recovery = %d, want 99", hw)
	}
	off, _ := p2.Append([]Message{{nil, []byte("after")}})
	if off != 99 {
		t.Fatalf("next offset %d, want 99", off)
	}
	if got := readAll(t, p2, 98); got[0] != "=m98" || got[1] != "=after" {
		t.Fatalf("tail after recovery: %v", got)
	}
}

func TestConcurrentProducersGetUniqueContiguousOffsets(t *testing.T) {
	p := openT(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[int64]bool{}
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				batch := []Message{{nil, []byte("a")}, {nil, []byte("b")}, {nil, []byte("c")}}
				off, err := p.Append(batch)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for j := int64(0); j < 3; j++ {
					if seen[off+j] {
						t.Errorf("offset %d assigned twice", off+j)
					}
					seen[off+j] = true
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	if int64(len(seen)) != p.HighWatermark() || p.HighWatermark() != 16*200*3 {
		t.Fatalf("seen %d offsets, hw %d", len(seen), p.HighWatermark())
	}
	if n := len(readAll(t, p, 0)); n != 9600 {
		t.Fatalf("read back %d", n)
	}
}

func TestLongPollWakesOnAppend(t *testing.T) {
	p := openT(t)
	done := make(chan Chunk)
	go func() {
		c, _ := p.Read(context.Background(), 0, 1<<20, 5*time.Second)
		done <- c
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	p.Append([]Message{{nil, []byte("wake")}})
	c := <-done
	if c.Records != 1 || time.Since(start) > time.Second {
		t.Fatalf("long poll returned %+v after %v", c, time.Since(start))
	}
}

func TestDurableAckWaitsForFsync(t *testing.T) {
	p := openT(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Flusher(ctx, time.Hour) // timer never fires: only producer requests flush
	off, _ := p.Append([]Message{{nil, []byte("x")}})
	ok := make(chan struct{})
	go func() { p.WaitDurable(off); close(ok) }()
	select {
	case <-ok:
	case <-time.After(2 * time.Second):
		t.Fatal("acks=all never completed")
	}
}

func TestRetentionKeepsActive(t *testing.T) {
	p := openT(t)
	p.SegmentBytes = 1 << 10
	for i := 0; i < 500; i++ {
		p.Append([]Message{{nil, []byte("0123456789")}})
	}
	n := len(p.segs)
	removed := p.Retain(time.Now().Add(time.Hour))
	if removed != n-1 || len(p.segs) != 1 {
		t.Fatalf("removed %d of %d segments", removed, n)
	}
	if p.LowWatermark() != p.active().base {
		t.Fatal("low watermark not advanced")
	}
}

func BenchmarkAppend1KB(b *testing.B) {
	p := openT(b)
	val := make([]byte, 1024)
	batch := make([]Message, 100)
	for i := range batch {
		batch[i] = Message{Value: val}
	}
	b.SetBytes(int64(len(batch) * len(val)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Append(batch)
	}
}

func TestRestartLoadsSealedIndexes(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPartition(dir)
	p.SegmentBytes = 8 << 10
	for i := 0; i < 2000; i++ {
		p.Append([]Message{{nil, []byte(fmt.Sprintf("v%d", i))}})
	}
	sealed := len(p.segs) - 1
	p.Close()

	idx, _ := filepath.Glob(filepath.Join(dir, "*.idx"))
	if len(idx) != sealed || sealed < 3 {
		t.Fatalf("%d index files for %d sealed segments", len(idx), sealed)
	}
	p2, err := OpenPartition(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p2.Close() })
	got := readAll(t, p2, 0)
	if len(got) != 2000 || got[1234] != "=v1234" {
		t.Fatalf("after restart read %d records", len(got))
	}
}
