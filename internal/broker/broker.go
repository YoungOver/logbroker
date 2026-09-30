// Package broker holds topics (sets of partitions) and consumer-group offsets.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YoungOver/logbroker/internal/commitlog"
)

var (
	ErrNoTopic = errors.New("topic not found")
	ErrBadName = errors.New("topic name must match [a-zA-Z0-9._-]{1,64}")
	ErrExists  = errors.New("topic already exists with a different partition count")
	validName  = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,64}$`)
)

type Topic struct {
	Name       string
	Partitions []*commitlog.Partition
	rr         atomic.Uint64
}

// PartitionFor sends equal keys to the same partition (ordering per key);
// keyless messages are spread round-robin.
func (t *Topic) PartitionFor(key []byte) int {
	if len(key) == 0 {
		return int(t.rr.Add(1) % uint64(len(t.Partitions)))
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(len(t.Partitions)))
}

type Broker struct {
	dir    string
	ctx    context.Context
	flush  time.Duration
	mu     sync.RWMutex
	topics map[string]*Topic
	groups *Groups
}

func Open(ctx context.Context, dir string, flush time.Duration) (*Broker, error) {
	b := &Broker{dir: dir, ctx: ctx, flush: flush, topics: map[string]*Topic{}}
	if err := os.MkdirAll(filepath.Join(dir, "topics"), 0o755); err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(filepath.Join(dir, "topics"))
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		parts, _ := os.ReadDir(filepath.Join(dir, "topics", e.Name()))
		if _, err := b.open(e.Name(), len(parts)); err != nil {
			return nil, err
		}
	}
	if b.groups, err = openGroups(filepath.Join(dir, "groups.json")); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) open(name string, n int) (*Topic, error) {
	t := &Topic{Name: name}
	for i := 0; i < n; i++ {
		p, err := commitlog.OpenPartition(filepath.Join(b.dir, "topics", name, strconv.Itoa(i)))
		if err != nil {
			return nil, err
		}
		go p.Flusher(b.ctx, b.flush)
		t.Partitions = append(t.Partitions, p)
	}
	b.topics[name] = t
	return t, nil
}

// CreateTopic is idempotent for the same partition count.
func (b *Broker) CreateTopic(name string, partitions int) (*Topic, error) {
	if !validName.MatchString(name) {
		return nil, ErrBadName
	}
	if partitions < 1 || partitions > 256 {
		return nil, fmt.Errorf("partitions must be 1..256")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		if len(t.Partitions) != partitions {
			return nil, ErrExists
		}
		return t, nil
	}
	return b.open(name, partitions)
}

func (b *Broker) Topic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	if !ok {
		return nil, ErrNoTopic
	}
	return t, nil
}

type TopicInfo struct {
	Name       string  `json:"name"`
	Partitions []PInfo `json:"partitions"`
}

type PInfo struct {
	ID   int   `json:"id"`
	Low  int64 `json:"low_watermark"`
	High int64 `json:"high_watermark"`
}

func (b *Broker) Describe() []TopicInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []TopicInfo
	for _, t := range b.topics {
		ti := TopicInfo{Name: t.Name}
		for i, p := range t.Partitions {
			ti.Partitions = append(ti.Partitions, PInfo{i, p.LowWatermark(), p.HighWatermark()})
		}
		out = append(out, ti)
	}
	return out
}

func (b *Broker) Groups() *Groups { return b.groups }

// Retain applies time-based retention to every partition.
func (b *Broker) Retain(cutoff time.Time) (removed int) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, t := range b.topics {
		for _, p := range t.Partitions {
			removed += p.Retain(cutoff)
		}
	}
	return
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.topics {
		for _, p := range t.Partitions {
			p.Close()
		}
	}
	b.groups.Save()
}

// Groups stores committed consumer offsets: group -> "topic/partition" -> next offset.
// Commits are in memory and snapshotted atomically (write temp + rename) by Saver.
type Groups struct {
	path  string
	mu    sync.Mutex
	m     map[string]map[string]int64
	dirty bool
}

func openGroups(path string) (*Groups, error) {
	g := &Groups{path: path, m: map[string]map[string]int64{}}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return g, nil
	}
	if err != nil {
		return nil, err
	}
	return g, json.Unmarshal(b, &g.m)
}

func (g *Groups) Commit(group, topic string, partition int, offset int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m[group] == nil {
		g.m[group] = map[string]int64{}
	}
	g.m[group][topic+"/"+strconv.Itoa(partition)] = offset
	g.dirty = true
}

func (g *Groups) Offsets(group string) map[string]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]int64, len(g.m[group]))
	for k, v := range g.m[group] {
		out[k] = v
	}
	return out
}

func (g *Groups) Save() error {
	g.mu.Lock()
	if !g.dirty {
		g.mu.Unlock()
		return nil
	}
	b, err := json.Marshal(g.m)
	g.dirty = false
	g.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := g.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, g.path)
}

func (g *Groups) Saver(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.Save()
		}
	}
}
