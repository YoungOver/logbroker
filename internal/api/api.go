// Package api exposes the broker over HTTP.
package api

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/YoungOver/logbroker/internal/broker"
	"github.com/YoungOver/logbroker/internal/commitlog"
)

var (
	producedMsgs  = prometheus.NewCounter(prometheus.CounterOpts{Name: "logbroker_produced_messages_total", Help: "Messages appended."})
	producedBytes = prometheus.NewCounter(prometheus.CounterOpts{Name: "logbroker_produced_bytes_total", Help: "Payload bytes appended."})
	fetchedBytes  = prometheus.NewCounter(prometheus.CounterOpts{Name: "logbroker_fetched_bytes_total", Help: "Bytes streamed to consumers."})
	produceDur    = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "logbroker_produce_seconds", Help: "Produce latency by acks mode.",
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 18),
	}, []string{"acks"})
)

func init() { prometheus.MustRegister(producedMsgs, producedBytes, fetchedBytes, produceDur) }

type API struct {
	B       *broker.Broker
	MaxBody int64
}

func (a *API) Routes(mux *http.ServeMux) {
	mux.HandleFunc("PUT /topics/{topic}", a.createTopic)
	mux.HandleFunc("GET /topics", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, a.B.Describe()) })
	mux.HandleFunc("POST /topics/{topic}/produce", a.produce)
	mux.HandleFunc("GET /topics/{topic}/partitions/{p}/fetch", a.fetch)
	mux.HandleFunc("POST /groups/{group}/commit", a.commit)
	mux.HandleFunc("GET /groups/{group}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, a.B.Groups().Offsets(r.PathValue("group")))
	})
}

func (a *API) createTopic(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.Atoi(r.URL.Query().Get("partitions"))
	if err != nil {
		n = 1
	}
	if _, err := a.B.CreateTopic(r.PathValue("topic"), n); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// decodeBatch parses klen u16 | vlen u32 | key | value, repeated. Messages alias body.
func decodeBatch(body []byte) ([]commitlog.Message, error) {
	var out []commitlog.Message
	for len(body) > 0 {
		if len(body) < 6 {
			return nil, errBadBatch
		}
		kl, vl := int(binary.LittleEndian.Uint16(body)), int(binary.LittleEndian.Uint32(body[2:]))
		if len(body) < 6+kl+vl {
			return nil, errBadBatch
		}
		out = append(out, commitlog.Message{Key: body[6 : 6+kl], Value: body[6+kl : 6+kl+vl]})
		body = body[6+kl+vl:]
	}
	return out, nil
}

var errBadBatch = errors.New("malformed batch")

type produceResp struct {
	Partition int   `json:"partition"`
	Base      int64 `json:"base_offset"`
	Count     int   `json:"count"`
}

// produce appends a batch. With ?partition= the whole batch goes there; otherwise
// messages are routed by key hash and grouped so each partition gets one append.
func (a *API) produce(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	t, err := a.B.Topic(r.PathValue("topic"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, a.MaxBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	msgs, err := decodeBatch(body)
	if err != nil || len(msgs) == 0 {
		http.Error(w, "empty or malformed batch", http.StatusBadRequest)
		return
	}
	groups := map[int][]commitlog.Message{}
	if ps := r.URL.Query().Get("partition"); ps != "" {
		p, err := strconv.Atoi(ps)
		if err != nil || p < 0 || p >= len(t.Partitions) {
			http.Error(w, "bad partition", http.StatusBadRequest)
			return
		}
		groups[p] = msgs
	} else {
		for _, m := range msgs {
			p := t.PartitionFor(m.Key)
			groups[p] = append(groups[p], m)
		}
	}
	acksAll := r.URL.Query().Get("acks") == "all"
	var out []produceResp
	for p, ms := range groups {
		base, err := t.Partitions[p].Append(ms)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if acksAll {
			t.Partitions[p].WaitDurable(base + int64(len(ms)) - 1)
		}
		out = append(out, produceResp{p, base, len(ms)})
	}
	producedMsgs.Add(float64(len(msgs)))
	producedBytes.Add(float64(len(body)))
	mode := "1"
	if acksAll {
		mode = "all"
	}
	produceDur.WithLabelValues(mode).Observe(time.Since(start).Seconds())
	writeJSON(w, out)
}

// fetch streams whole records straight from the segment file. With an *os.File
// source, net/http uses sendfile(2) on Linux, so payload bytes never enter user space.
func (a *API) fetch(w http.ResponseWriter, r *http.Request) {
	t, err := a.B.Topic(r.PathValue("topic"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil || p < 0 || p >= len(t.Partitions) {
		http.Error(w, "bad partition", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	off, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	maxBytes, err := strconv.ParseInt(q.Get("max_bytes"), 10, 64)
	if err != nil || maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	waitMs, _ := strconv.Atoi(q.Get("wait_ms"))
	c, err := t.Partitions[p].Read(r.Context(), off, maxBytes, time.Duration(min(waitMs, 30_000))*time.Millisecond)
	w.Header().Set("X-High-Watermark", strconv.FormatInt(c.HighWater, 10))
	if errors.Is(err, commitlog.ErrOutOfRange) {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Next-Offset", strconv.FormatInt(c.NextOffset, 10))
	w.Header().Set("X-Records", strconv.FormatInt(c.Records, 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(c.Len, 10))
	if c.Len == 0 {
		return
	}
	f, err := os.Open(c.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	if _, err := f.Seek(c.Pos, io.SeekStart); err != nil {
		return
	}
	n, _ := io.CopyN(w, f, c.Len)
	fetchedBytes.Add(float64(n))
}

func (a *API) commit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.B.Groups().Commit(r.PathValue("group"), req.Topic, req.Partition, req.Offset)
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
