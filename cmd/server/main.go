package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/YoungOver/logbroker/internal/api"
	"github.com/YoungOver/logbroker/internal/broker"
)

func main() {
	addr := flag.String("addr", ":9092", "HTTP listen address")
	dir := flag.String("data", "data", "data directory")
	flush := flag.Duration("flush", 100*time.Millisecond, "background fsync interval (acks=all flushes immediately)")
	keep := flag.Duration("retention", 7*24*time.Hour, "delete segments older than this")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	start := time.Now()
	b, err := broker.Open(ctx, *dir, *flush)
	if err != nil {
		log.Error("open", "err", err)
		os.Exit(1)
	}
	log.Info("recovered", "topics", len(b.Describe()), "took", time.Since(start).String())
	go b.Groups().Saver(ctx, time.Second)
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				if n := b.Retain(now.Add(-*keep)); n > 0 {
					log.Info("retention", "segments_removed", n)
				}
			}
		}
	}()

	mux := http.NewServeMux()
	(&api.API{B: b, MaxBody: 64 << 20}).Routes(mux)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("listening", "addr", *addr, "data", *dir)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http", "err", err)
	}
	b.Close()
}
