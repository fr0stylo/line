# line

Durable Go queue built on append-only stores. `line` provides a minimal push/pop/stream API intended for services that need simple persistence without an external broker.

## Table of Contents
1. [Features](#features)
2. [Architecture](#architecture)
3. [Project Layout](#project-layout)
4. [Getting Started](#getting-started)
5. [Usage](#usage)
6. [Development Workflow](#development-workflow)
7. [Testing & Quality](#testing--quality)
8. [Persistence & Configuration](#persistence--configuration)
9. [Troubleshooting](#troubleshooting)
10. [Contributing](#contributing)

## Features
- **Pluggable storage**: File-based store and segmented store implement the shared `store.Store` interface.
- **Durable metadata**: `_meta.json` snapshots track read/write offsets so queues survive restarts.
- **Back-pressure aware streaming**: `Stream(ctx)` yields messages on a channel until cancelled.
- **Concurrency safety**: Writers and readers share mutex/condition primitives for predictable blocking semantics.
- **Example-driven**: `examples/example.go` simulates multiple producers and one streaming consumer.

## Architecture
The architecture revolves around three layers:

| Layer | Responsibility | Key Files |
| --- | --- | --- |
| Queue façade | Public API (`Push`, `Pop`, `Stream`) and lifecycle (`Close`) | `line.go` |
| Store interface | Contract that every durable store must fulfill (`store.Store`) | `store/store.go` |
| Store implementations | Disk-backed persistence with metadata, offsets, and segment rollover | `store/file.go`, `store/segment.go`, `store/utils.go` |

`Line` relies on a provided `store.Store` to push bytes by length-prefixing messages. Reads block using `sync.Cond` until data is available. Segment stores roll files over when they reach `segmentSize`, deleting fully-read segments to keep disk usage bounded.

## Project Layout
- `line.go` — queue logic on top of a `store.Store`.
- `store/` — persistence layer (file store, segmented store, helpers).
- `examples/` — runnable demonstration.
- `dir/` — default data directory; safe to delete when starting fresh.

## Getting Started
1. Ensure Go 1.25+ is installed (`go env GOVERSION`).
2. Clone the repository and change into it.
3. Run the example to confirm your environment:
   ```bash
   go run ./examples
   ```
   Two goroutines will enqueue timestamped messages while a consumer streams them for ~10 seconds. Logs and metadata accumulate under `dir/`.

### Installing in Another Module
Since `go.mod` declares `module line`, import using a module path that reflects your fork (e.g., `github.com/you/line`). Run `go get github.com/you/line` or use a replace directive while iterating locally.

## Usage
```go
store, err := store.NewSegmentStore("./dir/", 1024)
if err != nil { log.Fatal(err) }
defer store.Close()

queue, err := line.NewLine(store)
if err != nil { log.Fatal(err) }
defer queue.Close()

if err := queue.Push([]byte("hello")); err != nil { log.Fatal(err) }
msg, err := queue.Pop()
fmt.Println(string(msg))
```

For streaming:
```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

for blob := range queue.Stream(ctx) {
    slog.Info("received", "payload", string(blob))
}
```

## Development Workflow
`Makefile` shortcuts:
```bash
make fmt      # go fmt ./...
make vet      # go vet ./...
make lint     # golangci-lint run ./...
make test     # go test ./...
make build    # go build ./...
make example  # go run ./examples
```
Run `make fmt vet lint test build` before pushing to ensure formatting, vetting, and linters all pass.

## Testing & Quality
- Tests live alongside the code they cover (e.g., `store/file_test.go`).
- Favor table-driven tests and `TestType_Method` naming.
- Use `go test ./... -cover` to confirm meaningful coverage of push/pop paths, error handling, and segment rollover.
- `golangci-lint` is the canonical static-analysis entry point; configure it via `GOLANGCI_LINT` env var if installed in a custom path.

## Persistence & Configuration
- File store (`store.NewFile`) writes to a single append-only log; ideal for small deployments.
- Segment store (`store.NewSegmentStore(dir, segmentSize)`) caps each log file at `segmentSize` bytes and rolls forward, deleting old segments after successful reads.
- Metadata (`*_meta.json`) is written via atomic temp-file swaps. Deleting metadata resets offsets, so keep files together with their logs.
- When tweaking `segmentSize`, stop the process and remove old `dir/*.log` files to avoid offset mismatches.

## Troubleshooting
- **Queue blocks forever**: Ensure at least one producer is calling `Push`; otherwise `Stream` waits on the condition variable.
- **Metadata corruption**: Remove `dir/*.log` and `_meta.json` while the process is stopped, then restart to rebuild clean state.
- **golangci-lint missing**: Install via `brew install golangci-lint` or `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`.
- **Different module path**: Update `module` in `go.mod` to match your fork, then run `go mod tidy`.

## Contributing
- Run formatting, vetting, linting, tests, and builds (`make fmt vet lint test build`) before pushing.
- Add focused tests for every behavior change—especially around concurrency or persistence boundaries.
- Use concise, imperative commit messages (e.g., `add segment rollover guard`) and describe user-visible impact in the body when needed.
- Provide clear PR descriptions that cover what changed, why, how it was tested, and any manual steps (e.g., cleaning `dir/_meta.json`).

Questions or proposals? Open an issue or discussion with context about your use case so we can iterate together.
