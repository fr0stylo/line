.PHONY: fmt vet lint test build example broker broker-run

GOLANGCI_LINT ?= golangci-lint
BROKER_BIN ?= bin/broker
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

broker:
	@mkdir -p bin
	@echo "Building broker..."
	cd broker && go build -o ../$(BROKER_BIN) ./cmd/broker
	@echo "Broker built: $(BROKER_BIN)"

broker-run: broker
	@echo "Starting broker on :50051..."
	./$(BROKER_BIN)

.PHONY: generate
generate:
	@for config in $$(find . -name "buf.gen.yaml"); do \
		dir=$$(dirname $$config); \
		echo "Generating protos in $$dir..."; \
		(cd $$dir && buf generate); \
	done

.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	rm -rf data/
	rm -rf dir/
