// Package client is a small Go client for logbroker: batched producer and a
// long-polling consumer that commits offsets to a consumer group.
package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/YoungOver/logbroker/internal/commitlog"
)

type Client struct {
	Base string
	HTTP *http.Client
}

func New(base string) *Client {
	return &Client{Base: base, HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256}}}
}

// EncodeBatch builds the produce wire format: repeated klen u16 | vlen u32 | key | value.
func EncodeBatch(dst []byte, key []byte, vals ...[]byte) []byte {
	for _, v := range vals {
		dst = binary.LittleEndian.AppendUint16(dst, uint16(len(key)))
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(v)))
		dst = append(dst, key...)
		dst = append(dst, v...)
	}
	return dst
}

func (c *Client) CreateTopic(topic string, partitions int) error {
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/topics/%s?partitions=%d", c.Base, topic, partitions), nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("create topic: %s", resp.Status)
	}
	return nil
}

// Produce sends a pre-encoded batch. acksAll waits until the batch is fsynced.
func (c *Client) Produce(ctx context.Context, topic string, partition int, batch []byte, acksAll bool) error {
	u := fmt.Sprintf("%s/topics/%s/produce?partition=%d", c.Base, topic, partition)
	if acksAll {
		u += "&acks=all"
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(batch))
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("produce: %s", resp.Status)
	}
	return nil
}

type Record struct {
	Offset, TS int64
	Key, Value []byte
}

// Fetch long-polls one partition and returns records plus the next offset to ask for.
func (c *Client) Fetch(ctx context.Context, topic string, partition int, offset int64, maxBytes int, wait time.Duration, buf []byte) ([]byte, int64, error) {
	u := fmt.Sprintf("%s/topics/%s/partitions/%d/fetch?offset=%d&max_bytes=%d&wait_ms=%d",
		c.Base, url.PathEscape(topic), partition, offset, maxBytes, wait.Milliseconds())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return buf, offset, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return buf, offset, fmt.Errorf("fetch: %s", resp.Status)
	}
	next, _ := strconv.ParseInt(resp.Header.Get("X-Next-Offset"), 10, 64)
	w := bytes.NewBuffer(buf[:0])
	_, err = io.Copy(w, resp.Body)
	return w.Bytes(), next, err
}

// Decode iterates records returned by Fetch.
func Decode(b []byte, fn func(off, ts int64, key, val []byte)) error { return commitlog.Decode(b, fn) }

func (c *Client) Commit(ctx context.Context, group, topic string, partition int, offset int64) error {
	body, _ := json.Marshal(map[string]any{"topic": topic, "partition": partition, "offset": offset})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+"/groups/"+group+"/commit", bytes.NewReader(body))
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("commit: %s", resp.Status)
	}
	return nil
}
