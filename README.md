# Concurrent Event Processing Engine

A single-process, event-driven transaction processor written in Go. Account-transfer events are ingested, deduplicated, partitioned across worker goroutines, applied to a lock-free ledger with saga-style compensation, retried with jittered exponential backoff, and dead-lettered when attempts are exhausted. A write-ahead log makes the state recoverable after a crash. Every stage is counted and sampled for tracing.

This is not a multi-node system. Workers are goroutines in one process, and the only durable state is the write-ahead log on local disk.

## Layout

```
cmd/engine           CLI: bench, suite, serve
internal/dedupe      lock-free open-addressing id set
internal/ledger      cache-line padded atomic balances
internal/ring        lock-free bounded MPSC queue with batch dequeue
internal/wal         write-ahead log with group commit and torn-tail recovery
internal/engine      ingestion, partitioned queues, workers, DLQ, recovery
internal/retry       single-goroutine timer heap for backoff
internal/metrics     sharded counters, log-linear latency histogram
internal/trace       sampled span recorder
internal/server      HTTP ingestion and observability endpoints
internal/bench       workload generator and consistency verifier
```

## Design

- **Ingestion**: `IngestBatch` validates events, claims their ids in the dedupe set, appends the accepted ones to the log in a single locked section, and enqueues them on the partition owned by the source account. `TryIngest` is the non-blocking variant: when a queue is full it writes a void record and releases the claim.
- **Duplicate detection**: a fixed-size table of 64-bit keys. Claiming an id is a single CAS, so concurrent duplicates have exactly one winner. Removal uses tombstones so probe chains stay intact.
- **Partitioning and queues**: events hash by source account onto one queue per worker, so debits for an account are serialized on one goroutine. Queues are bounded lock-free rings (per-slot sequence numbers) that workers drain in batches of 64. Profiling showed Go channels spent most of their time on lock contention. Replacing them roughly doubled throughput.
- **Transaction consistency**: each transfer is a two-step saga. Debit is a CAS that fails on insufficient funds (terminal reject). If the commit stage fails after the debit, the debit is compensated before the event is retried, so a failed attempt never leaves a partial effect.
- **Retries**: failed events go to a timer heap with jittered exponential backoff and are re-enqueued on their partition. After `MaxAttempts` they land in the dead-letter queue, which can be replayed.
- **Durability**: accepted events are appended to the log before they are enqueued. Workers append commit, reject and dead-letter outcomes once per batch. A flusher goroutine group-commits: while one fsync runs, new records accumulate and share the next one. Records carry CRC32C checksums.
- **Recovery**: on startup the log is replayed up to the first torn or corrupt record, and the tail is truncated. The ledger is derived state: committed transfers are re-applied, rejected and dead-lettered events are restored, and events with no recorded outcome are re-enqueued. Because the ledger is rebuilt only from the log, an event is applied exactly once whether the crash came before or after its in-memory effect.
- **Sync modes**: `buffered` writes without fsync, `group` fsyncs asynchronously (a crash can lose the most recent unsynced events, never half-apply one), and `ack` makes ingestion wait until the event is on disk before it is enqueued.
- **Monitoring**: per-worker counters and histograms avoid shared cache lines. Latency is measured from ingest to terminal state with 12.5% bucket resolution.
- **Tracing**: events are sampled by id hash; queue, debit, credit, commit, retry, reject and dead-letter spans are recorded in a ring and grouped per trace.

## Usage

```
make test            # unit and integration tests
make race            # same under the race detector
make suite           # full evaluation table
./bin/engine bench -events 2000000 -dup 0.1 -fault 0.02 -wal group
./bin/engine serve -addr :8080 -wal-path ./engine.wal -wal-mode group
```

HTTP endpoints: `POST /ingest`, `GET /metrics`, `GET /traces`, `GET /deadletters`, `POST /deadletters/replay`, `GET /balance/{account}`, `GET /healthz`. Ingest body: `[{"id":1,"from":0,"to":1,"amount":50}]`. A full queue returns 429.

## Evaluation

Apple silicon, 14 cores, Go 1.24, 2M unique events and 100k accounts per scenario (200k for `ack`), 4 producers submitting batches of 128. Fault rate is the probability of failure at each of the two stages per attempt, retries are capped at 6 attempts, and dead letters are replayed at the end of each run.

Every run verifies: committed plus rejected equals unique events, duplicates dropped equals duplicates sent, the pending count is zero, total ledger supply is conserved, and every account balance equals its initial balance plus the exact net of the unique events. Runs with a log also stop the engine, recover a new one from the log, and check that every balance matches.

| scenario | workers | offered | events/s | p50 | p99 | p99.9 | dups | retries | compensations | dlq | consistent |
|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 14 | 2.0M | 21.2M | 15us | 3.4ms | 4.7ms | 0 | 0 | 0 | 0 | yes |
| 10% duplicates | 14 | 2.2M | 21.0M | 13us | 82us | 4.2ms | 200k | 0 | 0 | 0 | yes |
| dup 10%, fault 2% | 14 | 2.2M | 24.3M | 20us | 9.4ms | 10.5ms | 200k | 82k | 41k | 0 | yes |
| dup 10%, fault 20% | 14 | 2.2M | 2.0M | 9.4ms | 1.0s | 1.1s | 200k | 1.12M | 499k | 4398 | yes |
| skew 50% on 10 accounts | 14 | 2.2M | 21.5M | 18us | 590us | 1.8ms | 200k | 82k | 41k | 0 | yes |
| paced 300k/s | 14 | 2.2M | 300k | 25us | 426us | 918us | 200k | 82k | 41k | 0 | yes |

Allocations per event round to zero in every scenario except the 20% fault run, where retries allocate (0.01 per event).

Write-ahead log (10% duplicates, 2% faults, 14 workers):

| mode | events/s | p99 | recovery |
|---|---|---|---|
| none | 24.3M | 9.4ms | - |
| buffered | 11.4M | 492us | 400ms for 4.0M records |
| group commit | 12.4M | 426us | 395ms for 4.0M records |
| ack (200k events) | 31.7k | 14.7ms | 50ms for 400k records |

In `ack` mode every batch waits for a full fsync (F_FULLFSYNC on macOS), so throughput is bounded by producers divided by fsync latency.

Worker scaling (10% duplicates, 2% faults, no log):

| workers | events/s | speedup |
|---|---|---|
| 1 | 7.18M | 1.0x |
| 2 | 10.7M | 1.5x |
| 4 | 16.5M | 2.3x |
| 8 | 19.8M | 2.8x |
| 14 | 22.7M | 3.2x |

Notes: at 8+ workers the four producer goroutines and memory bandwidth, not worker capacity, are the limit, and latencies in unbounded runs mostly reflect batching and queue depth. The paced run is the representative latency figure. The 20% fault case shows tail latency from backoff, where retried events wait up to the capped delay.

## Crash testing

`TestCrashMidRunRecoversExactBalances` runs 64 producers against an `ack` log with 5% faults at each stage, aborts the log mid-run (unflushed buffers are discarded and later appends fail), then opens a new engine on the same file. It checks that every acknowledged event was recovered, that re-ingesting everything deduplicates the acknowledged ones, and that final balances equal the exact net of all events applied once. Further tests cover clean restarts, dead letters across restarts, torn tails, corrupt records and void records after backpressure.

## Limits

- Single process, single node. Nothing here replicates the log or coordinates between machines.
- The log grows without bound. There is no snapshotting or compaction, so recovery time grows with the log.
- In `buffered` and `group` modes, events acknowledged after the last fsync can be lost in a crash. Only `ack` guarantees that an acknowledged event is on disk.
- The dedupe table has fixed capacity and returns an error when a probe sequence is exhausted. Size it to the number of ids you expect to retain, including those recovered from the log.
- Event ids must be at most `MaxUint64 - 3`.
