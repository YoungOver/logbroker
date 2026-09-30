# logbroker

A Kafka-style message broker in Go: partitioned append-only logs, sparse offset
indexes, long-poll consumers streaming straight from segment files, consumer-group
offsets and group-commit durability. About 1 500 lines of Go, no dependencies beyond the
Prometheus client.

![benchmarks](docs/bench.png)

| | |
|---|---|
| Throughput, 100 B messages | **1.22M msg/s** while consumers read everything back |
| Throughput, 1 KB messages | **573k msg/s, 559 MB/s**, end-to-end p50 4.4 ms |
| `acks=all` (fsync before ack) | **149k msg/s**, p99 12.5 ms: one fsync covers many producers |
| Restart with 9.4 GB of log | **2.4 s** (sealed segments load a persisted index, only the active one is scanned) |
| Produce path | 1.2 GB/s into one partition in-process, 1 allocation per batch |

Measured with `cmd/loadgen`: 16 producers and 8 long-polling consumers against one
broker on a laptop (i7-13620H, NVMe), over HTTP on localhost.

## Design

```
producer ──POST /produce──► route by key hash ──► Partition.Append (one write per batch)
                                                        │
                            flusher: fsync on timer, or at once if acks=all waits
                                                        │
 segments:  00000000000000000000.log  .idx   (sealed, 128 MB)
            00000000000001203911.log         (active)
                                                        │
consumer ──GET /fetch?offset=&wait_ms=──► locate via sparse index ──► sendfile(segment) ──► client
```

**Log format.** Each record is `len | crc32c | offset | ts | klen | key | value`.
The offset inside the record lets consumers detect gaps. A segment is named after
its first offset and rolls at 128 MB.

**Sparse index.** One entry per 4 KiB of log. A fetch binary-searches the index and
scans at most 4 KiB of headers to reach the exact offset. On roll the index is
written next to the segment (temp file + rename). At startup it is trusted only if
the recorded size matches the file, so a crash can never make a stale index authoritative.

**Crash recovery.** The active segment is scanned record by record; the first record
with a bad length, bad CRC or unexpected offset marks a torn write and the file is
truncated there. A later segment that does not continue the previous one is dropped.

**Durability as a separate axis.** Appends only write to the page cache. A flusher
per partition fsyncs on a timer (100 ms by default) or immediately when a producer
asks for `acks=all`, and wakes every producer whose offset is now covered. Under load
one fsync acknowledges dozens of batches, which is why durable mode keeps 149k msg/s.

**Zero-copy reads.** A fetch returns whole records as they lie on disk. The handler
copies from an `*os.File` into the response, which `net/http` turns into `sendfile(2)`
on Linux: payload bytes never pass through the Go heap. Consumers decode the same
format the broker writes, including CRC checks.

**Long polling.** Each partition keeps a channel that is closed and replaced on every
append. A consumer at the head waits on it with a timeout, so a new message reaches
waiting consumers without polling delay.

**Consumer groups.** Committed offsets live in memory and are snapshotted to
`groups.json` every second with write + fsync + rename.

## API

```bash
curl -X PUT 'localhost:9092/topics/orders?partitions=8'

# produce: repeated  klen u16 | vlen u32 | key | value  (see pkg/client.EncodeBatch)
#   ?partition=N to pin a batch, otherwise routed by key hash; &acks=all to wait for fsync
POST /topics/orders/produce

# fetch whole records from an offset, waiting up to wait_ms for new data
GET /topics/orders/partitions/3/fetch?offset=1200&max_bytes=1048576&wait_ms=500
#   headers: X-Next-Offset, X-High-Watermark, X-Records

POST /groups/billing/commit   {"topic":"orders","partition":3,"offset":1450}
GET  /groups/billing
GET  /topics                  # low/high watermarks per partition
GET  /metrics                 # produced messages/bytes, fetched bytes, produce latency by acks
```

Go client in `pkg/client`: `EncodeBatch`, `Produce`, `Fetch`, `Decode`, `Commit`.

## Run

```bash
make run            # broker on :9092, data in ./data
make load           # 16 producers + 8 consumers for 20 s
make load-durable   # the same with acks=all
make test
docker build -t logbroker . && docker run -p 9092:9092 -v lb:/data logbroker
```

## Tests

Offsets stay unique and contiguous under 16 concurrent producers. Reads from random
offsets work across many small segments. A torn tail is truncated and appends continue
at the right offset. `acks=all` completes through the flusher. Retention never removes
the active segment. Topics and group offsets survive a restart. After a restart, sealed
segments are read through their persisted indexes.

## What a production version would add

- Replication: a leader-follower protocol with an in-sync replica set and a high
  watermark that advances only after followers catch up.
- A binary TCP protocol with pipelining. HTTP/1.1 per batch is the bottleneck for
  small messages here.
- Consumer-group membership with partition rebalancing. Right now clients choose partitions.
