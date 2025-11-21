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

## Performance Testing
- Run the push/pop throughput benchmark via `go test -bench=LinePushPopThroughput -run '^$' -benchmem -benchtime=5s .`.
- Each sub-benchmark exercises a payload size (`128B`, `512B`, `2KiB`, `8KiB`) across batch factors (`batch1`, `batch8`, `batch64`), so you get a grid of `msgs/s` metrics that reflect both message size and per-iteration depth.
- Narrow to a specific scenario with `go test -bench='LinePushPopThroughput/2048B_batch64' -run '^$' -benchmem -benchtime=10s .`.
- Benchmark data are written to a temporary directory, so every invocation is isolated and leaves no artifacts under `./dir`. Adjust `-benchtime` for longer sampling windows and inspect `-benchmem` output to compare allocation pressure across runs. Throughput is I/O bound, so SSDs vs HDDs (or tmpfs) can materially change results.

Sample output from an AMD Ryzen 7 5700U laptop (ext4 NVMe SSD, `-benchtime=5s`):

```
goos: linux
goarch: amd64
pkg: github.com/fr0stylo/line
cpu: AMD Ryzen 7 5700U with Radeon Graphics
BenchmarkLinePushPopThroughput/128B_batch1-16         	   43040	    138122 ns/op	   0.93 MB/s	      7240 msgs/s	    3464 B/op	      53 allocs/op
BenchmarkLinePushPopThroughput/128B_batch8-16         	    5006	   1098689 ns/op	   0.93 MB/s	      7281 msgs/s	   27714 B/op	     424 allocs/op
BenchmarkLinePushPopThroughput/128B_batch64-16        	     668	   8883796 ns/op	   0.92 MB/s	      7204 msgs/s	  229131 B/op	    3394 allocs/op
BenchmarkLinePushPopThroughput/512B_batch1-16         	   41470	    139358 ns/op	   3.67 MB/s	      7176 msgs/s	    3849 B/op	      53 allocs/op
BenchmarkLinePushPopThroughput/512B_batch8-16         	    5143	   1158070 ns/op	   3.54 MB/s	      6908 msgs/s	   30796 B/op	     424 allocs/op
BenchmarkLinePushPopThroughput/512B_batch64-16        	     589	  10951088 ns/op	   2.99 MB/s	      5844 msgs/s	  253762 B/op	    3394 allocs/op
BenchmarkLinePushPopThroughput/2048B_batch1-16        	   42495	    148920 ns/op	  13.75 MB/s	      6715 msgs/s	    5504 B/op	      53 allocs/op
BenchmarkLinePushPopThroughput/2048B_batch8-16        	    4538	   1398247 ns/op	  11.72 MB/s	      5721 msgs/s	   44034 B/op	     424 allocs/op
BenchmarkLinePushPopThroughput/2048B_batch64-16       	     650	   9126376 ns/op	  14.36 MB/s	      7013 msgs/s	  352879 B/op	    3394 allocs/op
BenchmarkLinePushPopThroughput/8192B_batch1-16        	   38162	    153873 ns/op	  53.24 MB/s	      6499 msgs/s	   11661 B/op	      53 allocs/op
BenchmarkLinePushPopThroughput/8192B_batch8-16        	    4818	   1219965 ns/op	  53.72 MB/s	      6558 msgs/s	   93284 B/op	     424 allocs/op
BenchmarkLinePushPopThroughput/8192B_batch64-16       	     604	   9740597 ns/op	  53.83 MB/s	      6570 msgs/s	  746866 B/op	    3397 allocs/op
```

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
