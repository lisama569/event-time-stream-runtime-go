# Event-Time Stream Runtime (local, bounded-state)

A small **real-time event-stream processing engine** that runs entirely on the
local machine: multiple local input sources are ingested concurrently with
bounded-memory backpressure; events are aggregated into **keyed tumbling
event-time windows** with watermarks, out-of-order and late-event handling;
state is persisted with **consistent, atomic checkpoints** so a process
restart continues from the latest valid progress with results that are
**unique and deterministic**.

No external services, network or credentials are required — only the Go
standard library.

## Build and verify

```bash
go build ./...
go test -race ./...           # unit + integration tests
go vet ./...

# deterministic local demo (generates its own in-memory data)
go run ./cmd/stream-engine demo --dir ./.checkpoint-demo

# simulated crash (stops after N envelopes WITHOUT a trailing checkpoint)
go run ./cmd/stream-engine demo --dir ./.checkpoint-demo --crash-after 4
go run ./cmd/stream-engine demo --dir ./.checkpoint-demo --restart

# run against local newline-delimited JSON files
go run ./cmd/stream-engine run \
  --inputs orders=./data/a.jsonl,clicks=./data/b.jsonl \
  --dir ./.checkpoint-run --window 10s --lateness 5s
```

Input line format (one JSON object per line; `ts` is RFC3339 or unix-ms):

```json
{"id":"e1","key":"k1","ts":"2026-09-23T10:00:00.123Z","value":3}
{"id":"e2","key":"k1","ts":1790157600200,"value":4}
```

## Event time, watermarks and lateness

* Internal time is **int64 unix milliseconds** so window math is deterministic
  and independent of wall clocks.
* Each source has a high-water mark = `max(event time seen) - OutOfOrderBound`.
  `OutOfOrderBound` (default 2s) says how much out-of-order skew the system
  tolerates before declaring an event time "passed".
* The **global watermark** is the **minimum over all active sources**. A source
  that is registered but has produced nothing yet holds the watermark at
  `-infinity`, so windows never close before every source has started. A source
  that reached EOF is **not** idle — its maximum stays a permanent bound. A
  source that has produced no data for `IdleTimeout` may be marked idle and is
  then excluded from the min until it emits again.
* Window lifecycle (size `WindowSize`, e.g. 10s):
  * **open** — watermark `< windowEnd`: events apply; out-of-order events
    (older timestamps arriving after newer ones) still land in the correct
    window as long as that window is open and the watermark has not passed it.
  * **closed** — once watermark `>= windowEnd` the aggregate is **immutable**;
    a genuinely new event for that window is classified `late_dropped` and can
    never change the published result. (A *redelivery* of an event that already
    contributed is recognized as a duplicate, not a late event.)
  * **finalized/purged** — at watermark `>= windowEnd + AllowedLateness` the
    result is drained, published with the next checkpoint and the window state
    is deleted; state therefore stays bounded to live windows.
  * **AllowedLateness** (default 5s) bounds how long finalized-but-relevant
    window state is retained around the close frontier; the aggregate itself is
    immutable from the close point.

### Determinism and unique results

* Window identity is `(key, windowStart)`. Contributions inside one window are
  deduplicated by **both** `(sourceID, per-source offset/seq)` and
  `(sourceID, eventID)`, covering transport redelivery and post-crash replay as
  well as identical business events re-enqueued with a new position.
* The aggregate (`count`, `sum`) is therefore computed once from a unique input
  set. Finalized results are emitted in a deterministic order
  (windowStart, key). Cross-source interleaving may change the *timing* of
  internal work, but never the final result set.

## Backpressure and bounded memory

* Every source is read by its own goroutine into a **per-source bounded
  channel** (`PerSourceBuffer`, default 32). A merged channel feeds the single
  processing loop.
* When the processing loop is slow, a source blocks once **its own** buffer is
  full; faster sources keep ingesting up to their own capacity. One slow source
  cannot stall the other sources beyond that bounded capacity, and total
  in-flight memory is capped at `numSources * PerSourceBuffer` envelopes — no
  unbounded queue, and events are never silently dropped.
* Invalid input is **classified, counted and surfaced** (never dropped as if it
  were valid data):
  * `malformed` — not parseable JSON / missing `id` or `key`
  * `bad_time` — missing/unparsable/non-positive timestamp
  * `out_of_span` — timestamp outside the configured `MinEventTime/MaxEventTime`
  * `late_dropped` — valid event for a closed/finalized window (reported with
    event time, current watermark, window bounds and the decision basis)
  Invalid envelopes still advance their source offset (they were processed), so
  a poison record never blocks the stream.

## Checkpoints and recovery

Checkpoint file `checkpoint.bin` (single local directory):

```
magic "ETSC" | uint32 version | uint32 crc32(payload) | gob(Checkpoint)
```

Contents: engine id, **next-to-read offset per source**, per-source watermark
maxima, all live window states (including both dedup sets), and a small
`Pending` list of finalized results durably committed but not yet published.

* Save is crash-safe: temp file → fsync → atomic rename → fsync directory.
* Consistency rule: an input offset is advanced **only together with** the
  window state it produced, inside one saved checkpoint. On restart every
  source resumes exactly at its saved offset, so uncheckpointed work is replayed
  and checkpointed work is never re-aggregated (window dedup is a second
  defence).
* Publishing protocol: finalized windows are (1) made durable in the
  checkpoint, (2) published to the sink, (3) cleared from `Pending` by a second
  durable save. A crash after (1) but before (3) re-publishes on recovery, so
  **sinks must be idempotent per `(key, windowStart)`**; the input set itself is
  always unique. During recovery nothing is published before the checkpoint is
  validated and state restored.
* Load validation rejects a checkpoint with a distinguishable cause —
  `missing_file` (treated as cold start), `bad_magic`, `bad_version`
  (version field reported; only version `1` is accepted by this binary),
  `bad_crc`, `bad_payload` (incl. gob errors and engine-id mismatch). The
  engine refuses to start rather than risk processing against unknown state.

## Configuration

| Flag / field            | Default      | Meaning                                             |
|-------------------------|--------------|-----------------------------------------------------|
| `--window`              | `10s`        | event-time tumbling window size                     |
| `--lateness`            | `5s`         | allowed lateness after window end (state retention) |
| `--ooo`                 | `2s`         | per-source watermark lag (out-of-order bound)       |
| `--checkpoint-every`    | `500ms`      | wall-clock checkpoint interval                      |
| `--dir`                 | `./.checkpoint-demo` | local checkpoint directory                  |
| `PerSourceBuffer`       | `32`         | bounded in-flight envelopes per source              |
| `IdleTimeout`           | `30s`        | quiet-source grace before it is treated idle        |
| `MinEventTime/MaxEventTime` | unbounded | reject events outside this event-time range      |
| `--crash-after N`       | `0` (off)    | stop after N envelopes with no trailing checkpoint  |

## Test coverage

`go test -race ./...` covers: parsing/classification; watermark min/lag/idle
semantics; out-of-order windows, closed-window immutability, allowed-lateness
boundary, dedup by offset and by event id, snapshot/restore round-trips;
checkpoint save/load, CRC/version/magic/payload/engine-id/truncation
rejections; bounded-buffer backpressure and slow-source isolation; engine
end-to-end determinism, duplicate redelivery across restarts, crash before
publish, crash mid-stream, and concurrent multi-source ingestion. Failing
assertions print event time, watermark, checkpoint version and the decision
basis.
