package broker

import (
	"context"
	"testing"
	"time"
)

func TestTopicsAndGroupsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	b, err := Open(ctx, dir, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	tp, err := b.CreateTopic("orders", 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.CreateTopic("orders", 8); err != ErrExists {
		t.Fatalf("recreate with other partition count: %v", err)
	}
	if _, err := b.CreateTopic("bad name", 1); err != ErrBadName {
		t.Fatalf("bad name accepted: %v", err)
	}
	for i := 0; i < 40; i++ {
		p := tp.PartitionFor([]byte("user-42"))
		tp.Partitions[p].Append(nil)
	}
	b.Groups().Commit("billing", "orders", 1, 17)
	b.Close()
	cancel()

	b2, err := Open(context.Background(), dir, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	tp2, err := b2.Topic("orders")
	if err != nil || len(tp2.Partitions) != 4 {
		t.Fatalf("topic after restart: %v", err)
	}
	if got := b2.Groups().Offsets("billing")["orders/1"]; got != 17 {
		t.Fatalf("group offset after restart = %d", got)
	}
}

func TestSameKeySamePartition(t *testing.T) {
	b, _ := Open(context.Background(), t.TempDir(), time.Second)
	defer b.Close()
	tp, _ := b.CreateTopic("t", 16)
	first := tp.PartitionFor([]byte("k"))
	for i := 0; i < 100; i++ {
		if tp.PartitionFor([]byte("k")) != first {
			t.Fatal("key moved between partitions")
		}
	}
	seen := map[int]bool{}
	for i := 0; i < 64; i++ {
		seen[tp.PartitionFor(nil)] = true
	}
	if len(seen) != 16 {
		t.Fatalf("round-robin hit %d of 16 partitions", len(seen))
	}
}
