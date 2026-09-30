.PHONY: run test bench load load-durable docker
run:
	go run ./cmd/server -data data
test:
	go test -race -count=1 ./...
bench:
	go test -run x -bench . -benchmem ./internal/...
load:
	go run ./cmd/loadgen -d 20s
load-durable:
	go run ./cmd/loadgen -d 20s -topic durable -acks-all -producers 64 -batch 20
docker:
	docker build -t logbroker .
