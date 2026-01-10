
# ShunyaDB

**ShunyaDB** is a correctness-first, embedded storage engine written in **Rust**.

It is designed for applications that require **deterministic, crash-safe local persistence** with explicit durability guarantees. ShunyaDB provides a stable database core built around write-ahead logging, immutable on-disk data, snapshot-consistent reads, and safe compaction.

ShunyaDB prioritizes **data integrity, recoverability, and predictability** over aggressive performance optimizations. The system is intended to be used as a reliable embedded persistence layer inside larger applications and infrastructure components.

---

## Problem Statement

Many embedded databases trade clarity and determinism for performance or abstraction. When failures occur, it becomes difficult to reason about correctness and recovery behavior.

ShunyaDB takes a different approach:
- Every durability boundary is explicit
- All on-disk data is immutable
- Crash recovery behavior is deterministic and testable
- Internal invariants are enforced, not implied

The result is a storage engine whose behavior can be reasoned about under failure conditions.

---

## Core Guarantees

ShunyaDB provides the following guarantees:

### Durable Writes
- All writes are appended to a Write-Ahead Log (WAL)
- Writes are acknowledged only after durability is ensured
- Sequence numbers enforce strict ordering

### Crash-Safe Recovery
- WAL replay restores the database after crashes
- Partial writes are safely detected and discarded
- Recovery always converges to a consistent state

### Snapshot-Consistent Reads
- Multi-version concurrency control (MVCC)
- Reads observe a stable snapshot at a chosen sequence number
- No locks are required for reads

### Immutable On-Disk Storage
- Data is flushed to immutable page files
- Pages are never modified in place
- New data always results in new pages

### Deterministic Behavior
- No background threads
- No hidden concurrency
- All state transitions are explicit and observable

---

## Storage Architecture

![ShunyaDB Architecture](image.png)

### Write Path
```
Client
  → Write-Ahead Log (WAL)
  → MemTable
  → Immutable L0 Pages
  → Compaction → L1 Pages
```

### Read Path
```
MemTable
  → L0 Pages
  → L1 Pages
  → Disk (via LRU page cache)
```

---

## LSM Design

- **L0**
  - May contain overlapping key ranges
  - Optimized for fast flushes from memory

- **L1+**
  - Non-overlapping, sorted key ranges
  - Created exclusively through compaction

- **Compaction**
  - Merges overlapping pages
  - Retains the latest visible version of each key
  - Tombstones suppress older values
  - Obsolete files are deleted only after metadata is safely persisted

---

## WAL and Checkpointing

- Every write is assigned a monotonically increasing sequence number
- Pages track the highest sequence number they contain (`max_seqno`)
- WAL checkpointing follows a strict invariant:

```
checkpoint_seqno = min(max_seqno across all persisted pages)
```

- The WAL is rewritten only after data is fully durable
- The WAL may become empty when all data is safely persisted

This guarantees that WAL truncation never results in data loss.

---

## Page Cache

- Page-level LRU cache
- Reduces disk reads on repeated access
- Cache eviction never affects correctness
- Cache behavior is fully observable through metrics

---

## Metrics and Observability

ShunyaDB exposes internal engine metrics, including:

- Reads and writes
- WAL appends and rewrites
- Memtable flushes
- Compactions
- Page cache hits, misses, and evictions
- Pages read from disk

Metrics are used to validate correctness and performance trends.

---

## Example Usage

```rust
use shunyadb::engine::engine::Engine;
use std::collections::BTreeMap;

let mut engine = Engine::open("./data")?;

let mut value = BTreeMap::new();
value.insert("name".to_string(), "shunya".into());

engine.put("key1".to_string(), value)?;

let snapshot = u64::MAX;
let record = engine.get("key1", snapshot);

assert!(record.is_some());
```

---

## Testing and Reliability

ShunyaDB is tested against real on-disk persistence.

The test suite includes:
- WAL durability tests
- Crash and restart recovery tests
- Compaction correctness tests
- WAL checkpoint safety tests
- Cache effectiveness tests
- Performance trend tests

All tests validate behavior across process restarts.

---

## Performance Characteristics

- ~10,000 durable writes in ~4 seconds (debug build, fsync per write)
- Performance is intentionally conservative
- Optimizations are applied only when they do not compromise correctness

---

## Intended Use Cases

ShunyaDB is suitable for:

- Embedded persistence inside applications
- Local state storage for infrastructure components
- Systems requiring deterministic crash recovery
- Applications where correctness is more critical than peak throughput

---

## Roadmap

Planned additions include:

- Batched writes and group commit
- Background compaction
- Iterators and range scans
- Secondary indexes
- Concurrency support

---

## Design Philosophy

ShunyaDB is built with the assumption that **data correctness is non-negotiable**.

Performance optimizations are applied only when they preserve durability and recoverability. This ensures predictable behavior across crashes, restarts, and partial failures.
