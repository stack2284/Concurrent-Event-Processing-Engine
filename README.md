# Concurrent Event Processing Engine

A high-throughput, in-memory transaction processing engine written in Go. Account-transfer events are validated, deduplicated, partitioned across worker goroutines, executed against an atomic ledger with compensation on failure, and recorded to an append-only write-ahead log (WAL) for crash recovery.

The engine runs within a single process. Concurrency is handled across worker goroutines using bounded lock-free ring queues, and state durability is backed by local disk storage.

---

## Directory Structure

```text
cmd/engine/          CLI entrypoints: bench, suite, serve
internal/
├── bench/           Synthetic workload generator and ledger verification
├── dedupe/          Lock-free open-addressing deduplication set
├── engine/          Core pipeline: ingestion, routing, workers, DLQ, recovery
├── event/           Event data structures
├── hash/            SplitMix64 mixing and fast range reduction
├── ledger/          Cache-line padded atomic account ledger
├── metrics/         Sharded counters and log-linear latency histogram
├── retry/           Single-goroutine timer heap for exponential backoff
├── ring/            Lock-free bounded MPSC queue with batch dequeue
├── server/          HTTP ingestion and observability handlers
├── trace/           Trace recorder with configurable sampling
└── wal/             Write-ahead log with group commit and torn-tail recovery
```

---

## Architecture & Mechanics

### Ingestion & Deduplication
- **Admission**: Ingestion accepts single events or batches. Each event is validated for non-zero amount, distinct source/destination accounts, and valid account bounds.
- **Deduplication (`internal/dedupe`)**: Fixed-capacity table of 64-bit keys using open addressing with linear probing. ID acquisition is executed via a single `CompareAndSwapUint64`. Duplicate events are dropped immediately. Removals write a tombstone to maintain probe chain integrity.
- **Backpressure**: In non-blocking mode (`TryIngest`), if target worker queues are full, admitted claims are voided, released from the dedupe table, logged as void in the WAL, and rejected with `429 Too Many Requests`.

### Partitioning & Worker Queues
- **Account Affinity**: Events are partitioned by hashing the source account ID (`hash.Mix(from)`). All debits for a given account are serialized onto a single worker goroutine, eliminating cross-worker balance contention on source accounts.
- **Ring Buffers (`internal/ring`)**: Multi-Producer Single-Consumer (MPSC) bounded ring queues using per-slot sequence numbers (Vyukov queue). Workers drain batches of up to 64 events per iteration, falling back to brief spinning before parking on a wake channel.

### Ledger & Consistency
- **Cache-Line Isolation (`internal/ledger`)**: Account balances are stored as atomic 64-bit integers padded to 64-byte boundaries (`[56]byte` padding) to prevent false sharing under high concurrency.
- **Two-Step Saga Execution**: 
  1. Source account is debited via CAS. If balance is insufficient, the transaction terminates with a reject.
  2. If the secondary stage (credit or downstream commit) fails, the engine issues a compensating credit back to the source account before queuing for retry, ensuring no half-applied state persists.

### Retries & Dead-Letter Queue
- **Backoff Scheduler (`internal/retry`)**: Failed events are pushed to an in-memory binary min-heap timer queue. Backoff delays follow exponential scaling with uniform random jitter.
- **Re-routing**: Expired retry events are re-enqueued onto the appropriate worker partition.
- **Dead-Lettering**: Events exhausting `MaxAttempts` are moved to an in-memory dead-letter queue (DLQ) and flagged in the WAL. The DLQ can be queried or replayed via HTTP.

### Durability & Recovery
- **Write-Ahead Log (`internal/wal`)**: Append-only log with CRC32C checksums covering header and payload fields.
- **Sync Modes**:
  - `buffered`: Writes to OS page cache without explicit flush.
  - `group`: Background flusher executes asynchronous group commits (`f.Sync()`), coalescing pending writes.
  - `ack`: Admission blocks until the batch is synchronized to persistent storage via `f.Sync()`.
- **Crash Recovery**: Startup replays the WAL from offset 0 up to the first corrupt or torn record, truncating trailing incomplete writes. The engine restores deduplication keys, reconstructs ledger balances from committed records, re-populates the DLQ, and re-enqueues accepted transactions that lacked terminal outcome records.

---

## Getting Started

### Prerequisites
- Go 1.24+

### Build & Test

```bash
# Build CLI binary
make build

# Run unit and integration tests
make test

# Run tests under Go race detector
make race
```

### Running the Server

```bash
./bin/engine serve \
  -addr :8080 \
  -accounts 10000 \
  -balance 1000000 \
  -workers 8 \
  -queue 4096 \
  -wal-path ./engine.wal \
  -wal-mode group
```

---

## HTTP API

| Method | Path | Description |
|---|---|---|
| `POST` | `/ingest` | Ingest batch of transfer events (JSON array) |
| `GET` | `/balance/{account}` | Return balance for account ID |
| `GET` | `/metrics` | Prometheus-compatible plain-text metrics |
| `GET` | `/traces` | Retrieve sampled transaction execution traces |
| `GET` | `/deadletters` | List dead-lettered events |
| `POST` | `/deadletters/replay` | Replay dead-lettered events back into processing |
| `GET` | `/healthz` | Health check endpoint |

### Example Ingestion

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '[
    {"id": 1001, "from": 12, "to": 45, "amount": 250},
    {"id": 1002, "from": 45, "to": 90, "amount": 100}
  ]'
```

Response:
```json
{
  "accepted": 2,
  "duplicates": 0,
  "invalid": 0,
  "throttled": 0
}
```

---

## Benchmarks & Verification

The CLI includes an integrated synthetic benchmark generator (`internal/bench`) that measures throughput, tail latencies, allocation rates, and verifies conservation of total currency across all accounts.

```bash
# Run standalone benchmark scenario
./bin/engine bench -events 1000000 -dup 0.1 -fault 0.02 -wal group

# Run full evaluation matrix
make suite
```

### Representative Benchmark Results

> Measured on Apple Silicon (14 cores, Go 1.24) with 2,000,000 unique events, 100,000 accounts, and 4 concurrent producer goroutines submitting batches of 128. Results reflect synthetic in-memory CPU and memory-bus scaling; disk durability under `ack` mode is bounded by disk fsync latency.

| Scenario | Workers | Offered Events | Throughput (ev/s) | Latency p50 | Latency p99 | Duplicates | Retries | Comp | DLQ | Ledger Consistent |
|---|---|---|---|---|---|---|---|---|---|---|
| Baseline | 14 | 2,000,000 | ~21.2M | 15 µs | 3.4 ms | 0 | 0 | 0 | 0 | Yes |
| 10% Duplicates | 14 | 2,200,000 | ~21.0M | 13 µs | 82 µs | 200,000 | 0 | 0 | 0 | Yes |
| 10% Dups, 2% Faults | 14 | 2,200,000 | ~24.3M | 20 µs | 9.4 ms | 200,000 | ~82k | ~41k | 0 | Yes |
| 10% Dups, 20% Faults | 14 | 2,200,000 | ~2.0M | 9.4 ms | 1.0 s | 200,000 | ~1.12M | ~499k | 4,398 | Yes |
| Skewed (50% on 10 accounts) | 14 | 2,200,000 | ~21.5M | 18 µs | 590 µs | 200,000 | ~82k | ~41k | 0 | Yes |
| Paced (300k target/s) | 14 | 2,200,000 | 300,000 | 25 µs | 426 µs | 200,000 | ~82k | ~41k | 0 | Yes |

### Durability Modes (10% Duplicates, 2% Faults)

| Mode | Throughput | Latency p99 | Recovery Behavior |
|---|---|---|---|
| `none` (in-memory) | ~24.3M ev/s | 9.4 ms | Non-durable |
| `buffered` | ~11.4M ev/s | 492 µs | OS page-cache dependent |
| `group` | ~12.4M ev/s | 426 µs | Asynchronous group `f.Sync` |
| `ack` (200k events) | ~31.7k ev/s | 14.7 ms | Synchronous fsync per batch |

---

## Limitations

- **Single Process**: Designed for single-node vertical scaling. Distributed consensus and inter-node replication are not included.
- **Log Compaction**: The WAL is currently append-only without checkpointing or compaction. Startup replay time scales with total log size.
- **Fixed Deduplication Window**: The deduplication table is pre-sized to a fixed power-of-two capacity.
- **Event ID Range**: Event IDs are uint64 with reserved sentinel values (`MaxUint64 - 2` maximum usable ID).
