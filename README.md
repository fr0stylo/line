# line

Durable Go queue built on append-only stores. `line` provides a minimal push/pop/stream API intended for services that need simple persistence without an external broker.

## Table of Contents
1. [Features](#features)
2. [Architecture](#architecture)
3. [Project Layout](#project-layout)
4. [Getting Started](#getting-started)
5. [Core Usage](#core-usage)
6. [Broker Usage](#broker-usage)
7. [Development Workflow](#development-workflow)
8. [Testing & Quality](#testing--quality)
9. [Performance Testing](#performance-testing)
10. [Persistence & Configuration](#persistence--configuration)
11. [Troubleshooting](#troubleshooting)
12. [Contributing](#contributing)

## Features

- **Pluggable storage**: In-memory store for quick runs plus file/segment stores that satisfy the shared `store.Store`
  interface.
- **Durable metadata**: `_meta.json` snapshots track read/write offsets so queues survive restarts.
- **Back-pressure aware streaming**: `Stream(ctx)` yields messages on a channel until cancelled.
- **Telemetry-first**: Push/Pop metrics and traces emit via OpenTelemetry (OTLP/gRPC by default in examples) and
  propagate baggage across envelope headers.
- **gRPC broker**: Optional broker process exposes Publish/Subscribe over protobuf contracts with a matching Go client.
- **Example-driven**: `examples/01-core` stresses the queue; `examples/02-broker` wires the broker and clients together.
- **Name with a wink**: “Line” doubles as “queue” in several languages, so the project name is a tongue-in-cheek nod to its FIFO focus.

## Architecture
The architecture revolves around three layers:

| Layer                 | Responsibility                                                                  | Key Files                                                                                    |
|-----------------------|---------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| Queue façade          | Public API (`Push`, `PushContext`, `Pop`, `Stream`) and lifecycle (`Close`)     | `core/line.go`                                                                               |
| Store interface       | Contract that every durable store must fulfill (`store.Store`)                  | `core/store/store.go`                                                                        |
| Store implementations | In-memory store plus disk-backed persistence with metadata and segment rollover | `core/store/memory.go`, `core/store/file.go`, `core/store/segment.go`, `core/store/utils.go` |
| gRPC broker           | Server wiring a `core.Line` behind protobuf contracts                           | `broker/*.go`, `broker/cmd/broker`                                                           |
| Contracts & client    | Protobuf definitions and generated Go client                                    | `contracts/`, `client/`                                                                      |

`Line` relies on a provided `store.Store` to marshal envelopes (payload + tracing baggage) and length-prefix them before
persistence. Reads block using `sync.Cond` until data is available. Segment stores roll files over when they reach
`segmentSize`, deleting fully-read segments to keep disk usage bounded. Telemetry is emitted through the OpenTelemetry
SDK when configured.

## Project Layout

- `core/` — queue logic on top of a `store.Store` implementation.
- `core/store/` — persistence layer (memory, file, segmented store, helpers).
- `broker/` — gRPC broker server exposing Publish/Subscribe.
- `client/` — Go client for the broker API.
- `contracts/` — protobuf definitions and generated code.
- `examples/01-core` — instrumented queue demo writing to `./dir/`.
- `examples/02-broker` — in-process broker + client demo.

## Getting Started
1. Ensure Go 1.25+ is installed (`go env GOVERSION`).
2. Clone the repository and change into it.
3. The workspace `go.work` wires modules together; run examples from the repo root so paths resolve.
4. Core demo: `go run ./examples/01-core` (writes under `./dir/`).
5. Broker demo: `go run ./examples/02-broker -p 2 -c 3` (starts a broker on `:8080` and spawns clients).

### Installing in Another Module

Import the queue package as `github.com/fr0stylo/line/core` and the store package from
`github.com/fr0stylo/line/core/store`. If you fork the repo, update module paths accordingly or use a `replace`
directive while iterating locally.

## Core Usage
```go
import (
"context"
line "github.com/fr0stylo/line/core"
"github.com/fr0stylo/line/core/store"
)

s, err := store.NewSegmentStore("./dir/", 1024)
if err != nil { log.Fatal(err) }
defer s.Close()

queue, err := line.NewLine(s)
if err != nil { log.Fatal(err) }
defer queue.Close()

ctx := context.Background()
if err := queue.PushContext(ctx, []byte("hello")); err != nil { log.Fatal(err) }
env, err := queue.Pop()
fmt.Println(string(env.Payload))
```

For streaming:
```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

for env := range queue.Stream(ctx) {
slog.Info("received", "payload", string(env.Payload))
}
```

OpenTelemetry traces and metrics are emitted when a provider is configured (see `examples/01-core` for a ready-to-run
setup).

## Broker Usage

- Start the broker (persistent store by default):
  ```bash
  go run ./broker/cmd/broker -addr :50051 -data-dir ./data -segment-size $((64*1024*1024))
  ```
  Add `-memory` to use the in-memory store instead.
- Publish and consume with the Go client:
  ```go
  cli, err := client.NewClient("localhost:50051")
  if err != nil { log.Fatal(err) }
  defer cli.Close()

  if err := cli.Publish(context.Background(), []byte("hello via broker")); err != nil { log.Fatal(err) }

  _ = cli.Handle(func(ctx context.Context, payload []byte) error {
      fmt.Println(string(payload))
      return nil
  })
  ```
- A full demo combining broker + clients lives in `examples/02-broker` (`go run ./examples/02-broker -h` for flags).

## Development Workflow
`Makefile` shortcuts:
```bash
make fmt        # go fmt ./... across core, broker, client, contracts
make vet        # go vet ./... across modules
make lint       # golangci-lint run ./...
make test       # go test ./... across modules
make build      # go build ./... across modules
make broker     # build broker binary to ./bin/broker
make broker-run # build then start broker on :50051
make generate   # regenerate protobufs via buf
```
Run `make fmt vet lint test build` before pushing to ensure formatting, vetting, and linters all pass.

## Testing & Quality

- Tests live alongside the code they cover (e.g., `core/store/file_test.go`).
- Favor table-driven tests and `TestType_Method` naming.
- Use `go test ./... -cover` to confirm meaningful coverage of push/pop paths, error handling, and segment rollover.
- `golangci-lint` is the canonical static-analysis entry point; configure it via `GOLANGCI_LINT` env var if installed in a custom path.

## Performance Testing

- Run the push/pop throughput benchmark via
  `go test -bench=LinePushPopThroughput -run '^$' -benchmem -benchtime=5s ./core`.
- Each sub-benchmark exercises a payload size (`128B`, `512B`, `2KiB`, `8KiB`) across batch factors (`batch1`, `batch8`,
  `batch64`).
- Narrow to a specific scenario with
  `go test -bench='LinePushPopThroughput/2048B_batch64' -run '^$' -benchmem -benchtime=10s ./core`.
- Benchmark data are written to a temporary directory, so every invocation is isolated and leaves no artifacts under
  `./dir/`.

Sample output from an AMD Ryzen 7 5700U laptop (ext4 NVMe SSD, `-benchtime=5s`, buffered writes enabled):

```
goos: linux
goarch: amd64
pkg: github.com/fr0stylo/line/core
cpu: AMD Ryzen 7 5700U with Radeon Graphics
BenchmarkLinePushPopThroughput/128B_batch1-16          	   45787	    134881 ns/op	   0.95 MB/s	      7414 msgs/s	    3334 B/op	      53 allocs/op
BenchmarkLinePushPopThroughput/128B_batch8-16          	    5380	   1101614 ns/op	   0.93 MB/s	      7262 msgs/s	   26930 B/op	     424 allocs/op
BenchmarkLinePushPopThroughput/128B_batch64-16         	     699	   8705236 ns/op	   0.94 MB/s	      7352 msgs/s	  229122 B/op	    3393 allocs/op
BenchmarkLinePushPopThroughput/512B_batch1-16          	   42748	    139368 ns/op	   3.67 MB/s	      7175 msgs/s	    3849 B/op	      53 allocs/op
BenchmarkLinePushPopThroughput/512B_batch8-16          	    5384	   1089271 ns/op	   3.76 MB/s	      7344 msgs/s	   30797 B/op	     424 allocs/op
BenchmarkLinePushPopThroughput/512B_batch64-16         	     692	   8891392 ns/op	   3.69 MB/s	      7198 msgs/s	  253780 B/op	    3394 allocs/op
BenchmarkLinePushPopThroughput/2048B_batch1-16         	   42324	    140981 ns/op	  14.53 MB/s	      7093 msgs/s	    5505 B/op	      53 allocs/op
BenchmarkLinePushPopThroughput/2048B_batch8-16         	    4916	   1120916 ns/op	  14.62 MB/s	      7137 msgs/s	   44045 B/op	     424 allocs/op
BenchmarkLinePushPopThroughput/2048B_batch64-16        	     681	   8910302 ns/op	  14.71 MB/s	      7183 msgs/s	  352946 B/op	    3395 allocs/op
BenchmarkLinePushPopThroughput/8192B_batch1-16         	   38728	    153491 ns/op	  53.37 MB/s	      6515 msgs/s	   11665 B/op	      53 allocs/op
BenchmarkLinePushPopThroughput/8192B_batch8-16         	    4762	   1220220 ns/op	  53.71 MB/s	      6556 msgs/s	   93317 B/op	     424 allocs/op
BenchmarkLinePushPopThroughput/8192B_batch64-16        	     621	   9654863 ns/op	  54.30 MB/s	      6629 msgs/s	  747170 B/op	    3398 allocs/op
```

## Persistence & Configuration
- File store (`store.NewFile`) writes to a single append-only log; ideal for small deployments.
- Segment store (`store.NewSegmentStore(dir, segmentSize)`) caps each log file at `segmentSize` bytes and rolls forward, deleting old segments after successful reads.
- Metadata (`*_meta.json`) is written via atomic temp-file swaps. Deleting metadata resets offsets, so keep files together with their logs.
- When tweaking `segmentSize`, stop the process and remove old `dir/*.log` files to avoid offset mismatches. The broker
  uses `./data/` by default; the core example uses `./dir/`.

## Troubleshooting
- **Queue blocks forever**: Ensure at least one producer is calling `Push`; otherwise `Stream` waits on the condition variable.
- **Metadata corruption**: Remove `dir/*.log` and `_meta.json` while the process is stopped, then restart to rebuild clean state.
- **OTLP collector unavailable**: The core example logs exporter errors but continues running. Point
  `OTEL_EXPORTER_OTLP_ENDPOINT` at a reachable collector or run without telemetry.
- **Broker port in use**: Adjust `-addr` when starting the broker or shut down the conflicting service.
- **golangci-lint missing**: Install via `brew install golangci-lint` or
  `go install github.com/golangci-lint/cmd/golangci-lint@latest`.
- **Different module path**: Update `module` in each module’s `go.mod` to match your fork, then run `go mod tidy`.

## Contributing
- Run formatting, vetting, linting, tests, and builds (`make fmt vet lint test build`) before pushing.
- Add focused tests for every behavior change—especially around concurrency or persistence boundaries.
- Use concise, imperative commit messages (e.g., `add segment rollover guard`) and describe user-visible impact in the body when needed.
- Provide clear PR descriptions that cover what changed, why, how it was tested, and any manual steps (e.g., cleaning `dir/_meta.json`).

Questions or proposals? Open an issue or discussion with context about your use case so we can iterate together.
