.PHONY: fmt vet lint test build example broker broker-run generate clean

GOLANGCI_LINT ?= golangci-lint
BROKER_BIN ?= bin/broker
MODULES ?= core broker client contracts

fmt:
	@set -e; for module in $(MODULES); do \
		echo "==> go fmt $$module"; \
		(cd $$module && go fmt ./...); \
	done

vet:
	@set -e; for module in $(MODULES); do \
		echo "==> go vet $$module"; \
		(cd $$module && go vet ./...); \
	done

lint:
	@set -e; for module in $(MODULES); do \
		echo "==> golangci-lint $$module"; \
		(cd $$module && $(GOLANGCI_LINT) run ./...); \
	done

test:
	@set -e; for module in $(MODULES); do \
		echo "==> go test $$module"; \
		(cd $$module && go test ./...); \
	done

build:
	@set -e; for module in $(MODULES); do \
		echo "==> go build $$module"; \
		(cd $$module && go build ./...); \
	done

broker:
	@mkdir -p bin
	@echo "Building broker..."
	cd broker && go build -o ../$(BROKER_BIN) ./cmd/broker
	@echo "Broker built: $(BROKER_BIN)"

broker-run: broker
	@echo "Starting broker on :50051..."
	./$(BROKER_BIN)

generate:
	@for config in $$(find . -name "buf.gen.yaml"); do \
		dir=$$(dirname $$config); \
		echo "Generating protos in $$dir..."; \
		(cd $$dir && buf generate); \
	done

clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	rm -rf data/
	rm -rf dir/
