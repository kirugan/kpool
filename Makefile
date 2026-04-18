.PHONY: lint format test

lint:
	golangci-lint run ./...

format:
	golangci-lint fmt ./...

test:
	go test ./...
