# Examples

This directory houses runnable demonstrations for the queue core and the gRPC broker. Run them from the repo root so
shared assets (like `./dir/`) land in the expected place and the workspace `go.work` file wires local modules together.

## Prerequisites

- Go 1.25+
- Optional: an OpenTelemetry collector listening on `localhost:4317` if you want traces/metrics from `01-core` to
  export. The example still runs if the collector is absent; you will just see initialization errors logged.

## 01-core — instrumented queue demo

- What it shows: `core.Line` backed by `store.NewSegmentStore` writing to `./dir/` with two producers emitting at
  different intervals, plus `Stream(ctx)` consuming for ~2 minutes.
- Run it: `go run ./examples/01-core`
- Output: timestamped messages in logs; segment files and `_meta.json` accumulate under `./dir/`. Reuse the same
  directory between runs, or delete `./dir/*.log` and `./dir/_meta.json` to reset state.
- Telemetry: OTLP/gRPC by default (`localhost:4317`). Adjust with standard OTEL env vars (e.g.,
  `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_INSECURE`).

## 02-broker — in-process broker + clients

- What it shows: a gRPC broker backed by the in-memory store, plus producer and consumer clients in one process.
- Run it: `go run ./examples/02-broker -p 2 -c 3 -i 500ms -ir 4`
  - `-p`: number of producers
  - `-c`: number of consumers
  - `-i`: base publish interval
  - `-ir`: randomization factor applied to the interval
- Behavior: starts the broker on `:8080`, spins up the requested producers/consumers, and streams "Hello, World!"
  messages until you exit (Ctrl+C).

## Notes

- Ensure port `8080` is free before running `02-broker`.
- Both examples assume the default `./dir/` data directory stays out of version control.
