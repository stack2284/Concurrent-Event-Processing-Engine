.PHONY: build test race bench suite

build:
	go build -o bin/engine ./cmd/engine

test:
	go test ./...

race:
	go test -race ./...

bench: build
	./bin/engine bench

suite: build
	./bin/engine suite
