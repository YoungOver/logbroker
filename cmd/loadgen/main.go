// loadgen runs producers and long-polling consumers at the same time and reports
// throughput and end-to-end latency (produce call start -> consumer decode).
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YoungOver/logbroker/pkg/client"
)

func main() {
	base := flag.String("url", "http://127.0.0.1:9092", "broker URL")
	topic := flag.String("topic", "bench", "topic")
	parts := flag.Int("p", 8, "partitions")
	producers := flag.Int("producers", 16, "producer goroutines")
	batch := flag.Int("batch", 100, "messages per produce request")
	size := flag.Int("size", 1024, "message size in bytes")
	acks := flag.Bool("acks-all", false, "wait for fsync on every produce")
	dur := flag.Duration("d", 20*time.Second, "duration")
	flag.Parse()

	c := client.New(*base)
	if err := c.CreateTopic(*topic, *parts); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var produced, consumed, errs atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			val := make([]byte, *size)
			vals := make([][]byte, *batch)
			buf := make([]byte, 0, *batch*(*size+6))
			for n := 0; ctx.Err() == nil; n++ {
				// first 8 bytes carry the send time for end-to-end latency
				binary.LittleEndian.PutUint64(val, uint64(time.Now().UnixNano()))
				for j := range vals {
					vals[j] = val
				}
				buf = client.EncodeBatch(buf[:0], nil, vals...)
				if err := c.Produce(ctx, *topic, (i+n)%*parts, buf, *acks); err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				produced.Add(int64(*batch))
			}
		}(i)
	}

	lat := make([][]time.Duration, *parts)
	for p := 0; p < *parts; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			off := int64(0)
			buf := make([]byte, 0, 4<<20)
			for ctx.Err() == nil {
				b, next, err := c.Fetch(ctx, *topic, p, off, 4<<20, 500*time.Millisecond, buf)
				if err != nil {
					continue
				}
				now := time.Now().UnixNano()
				var n int64
				client.Decode(b, func(_, _ int64, _, v []byte) {
					if n%97 == 0 { // sample latency, not every record
						lat[p] = append(lat[p], time.Duration(now-int64(binary.LittleEndian.Uint64(v))))
					}
					n++
				})
				consumed.Add(n)
				off, buf = next, b
			}
			c.Commit(context.Background(), "loadgen", *topic, p, off)
		}(p)
	}
	t0 := time.Now()
	wg.Wait()
	el := time.Since(t0).Seconds()
	all := slices.Concat(lat...)
	slices.Sort(all)
	mb := float64(produced.Load()) * float64(*size) / (1 << 20) / el
	fmt.Printf("produced %d msgs (%.0f msg/s, %.0f MB/s), consumed %d, errors %d, acks=all %v\n",
		produced.Load(), float64(produced.Load())/el, mb, consumed.Load(), errs.Load(), *acks)
	if len(all) > 0 {
		fmt.Printf("end-to-end latency p50 %v p99 %v p99.9 %v\n", all[len(all)/2], all[len(all)*99/100], all[len(all)*999/1000])
	}
}
