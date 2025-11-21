.PHONY: fmt vet lint test build example

GOLANGCI_LINT ?= golangci-lint

fmt:
	go fmt ./...

vet:
	go vet ./...

lint:
	$(GOLANGCI_LINT) run ./...

test:
	go test ./...

build:
	go build ./...

example:
	go run ./examples
