# 📘ShunyaDB Core Engine (v1.0-core)

> **Phase:** 1 – Core Engine
> **Version:** v1.0-core
> **Days Covered:** 1–12
> **Status:** ✅ Stable, WAL-backed, cache-optimized engine

---

## 📝 Overview

The **ShunyaDB Core Engine** provides a typed, crash-safe, and cache-optimized local storage layer for hybrid database operations.
It forms the foundation of the system, supporting SQL-like CRUD operations and preparing for future **Graph** and **Vector** query layers.

The core focuses on:

* **Typed storage** using `Record` and `FieldValue`
* **Crash safety** via Write-Ahead Logging (WAL)
* **Performance** through LRU page caching
* **Extensibility** for indexing, graph, and vector layers

---

## 💉 Architecture

```
CLI  →  Engine::execute_command()
         │
         ├──> WriteAheadLog (durability)
         ├──> PageCache (fast reads)
         └──> Page I/O (disk persistence)
```

Each operation flows through the **Engine**, which coordinates:

* **Persistence** (via `storage/io.rs`)
* **Durability** (via `storage/wal.rs`)
* **Performance** (via `storage/cache.rs`)

---

## 💮 Core Components

### 1. Record and FieldValue System

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FieldValue {
  Int(i64),
  Float(f64),
  Bool(bool),
  Text(String),
}
```

Each record stores typed data:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Record {
  pub id: u64,
  pub data: BTreeMap<String, FieldValue>,
}
```

Filtering is done via the `Filter` system:

```rust
pub enum Filter {
  ById(u64),
  ByKeyValueEq(String, FieldValue),
  ByKeyValueOp(String, String, FieldValue),
}
```

---

### 2. Page System (`storage/page.rs`)

A **Page** represents a 4 KB physical unit on disk containing a vector of records.

```rust
pub struct Page {
  pub id: u64,
  pub records: Vec<Record>,
  pub capacity: usize,
}
```

* Stored as binary via **bincode**
* Supports basic CRUD in-memory
* Serialized to `data/<table>/page_<n>.bin`

---

### 3. Write-Ahead Log (`storage/wal.rs`)

Ensures **durability and crash recovery**.

```rust
pub struct WalEntry {
  pub operation: String,
  pub table: String,
  pub record_id: u64,
  pub data: Vec<u8>,
}
```

Each operation (`INSERT`, `UPDATE`, `DELETE`) is logged before disk writes.

On startup, `Engine::replay_wal_at_startup()` replays entries sequentially to rebuild consistent state.

---

### 4. Page Cache (LRU) — `storage/cache.rs`

Thread-safe **in-memory page cache** using `lru` crate and `Arc<Mutex<_>>`:

```rust
#[derive(Clone)]
pub struct PageCache {
  cache: Arc<Mutex<LruCache<String, Page>>>,
}
```

**Key Methods**

```rust
pub fn new(capacity: usize) -> Self
pub fn get(&self, key: &str) -> Option<Page>
pub fn put(&self, key: &str, page: Page)
pub fn invalidate(&self, key: &str)
```

* Stores recently accessed pages by `"<table>_page_<id>"`
* Automatically evicts least recently used pages
* Ensures thread-safe concurrent access
* Prepares for future REST API and multi-threaded engine

---

### 5. Engine — The Coordinator

```rust
pub struct Engine {
  wal: WriteAheadLog,
  cache: PageCache,
}
```

Core methods:

```rust
impl Engine {
  pub fn insert_record(&mut self, table: &str, record: Record) -> Result<()>;
  pub fn get(&mut self, table: &str, filter: Filter) -> Page;
  pub fn get_all(&mut self, table: &str) -> Page;
  pub fn update(&mut self, table: &str, filter: Filter, patch: BTreeMap<String, FieldValue>) -> Result<usize>;
  pub fn delete(&mut self, table: &str, filter: Filter) -> Result<usize>;
  pub fn replay_wal_at_startup(&mut self) -> Result<()>;
}
```

**Data Flow Example (Insert):**

1. CLI parses input → `Record`
2. Engine logs operation to WAL
3. Loads or creates `page_1.bin`
4. Inserts record into page
5. Updates cache → saves page to disk

**Recovery Path:**

* On restart, WAL is replayed.
* Missing pages/tables are recreated automatically.

---

## ⚙️ CRUD Operation Flow

| Operation  | Description                                                                          |
| ---------- | ------------------------------------------------------------------------------------ |
| **Insert** | Appends to WAL → Loads/creates page → Inserts record → Saves to disk → Updates cache |
| **Get**    | Checks cache → Loads page if miss → Returns records                                  |
| **Update** | Loads from disk → Applies patch → WAL entry per update → Cache refresh               |
| **Delete** | Filters by ID → WAL entry → Removes from page → Cache invalidated                    |

---

## 🧪 Testing & Verification

| Test                     | Purpose                             | Status           |
| ------------------------ | ----------------------------------- | ---------------- |
| **Record Serialization** | Validates serde + bincode roundtrip | ✅                |
| **WAL Recovery**         | Replays log after simulated crash   | ✅                |
| **CLI CRUD Tests**       | End-to-end functional test          | ✅                |
| **Cache Benchmark**      | Validates read optimization         | ✅ (3.75× faster) |

**Cache Benchmark Results**

```
Uncached: 34900 ms
Cached: 9306 ms
Speedup: 3.75× (Debug Mode)
```

> ⚙️ Note: Under `--release`, expected speedup ≈ 5–10×.
> Bottlenecks mainly from `Mutex` locking and cloning overhead (planned optimization in Day 13–15).

---

## Performance Benchmark (Day 20)

4090 records benchmark (cache cleared before scan):

| Operation | Cache | Time (µs) | Speedup |
|------------|-------|-----------|----------|
| Linear Scan | ❌ | 8245 | — |
| Indexed Lookup | ✅ | 1857 | 4.44× |

**Insights**
- Index + Cache integration validated.
- Cold read dominated by disk I/O.
- Hot read hits cache; achieves >4× speedup.
- With multi-page tables, expected 10–20× gains.


## 🧠 Design Highlights

* **Typed data layer** (`FieldValue` system)
* **Crash-safe** via WAL and recovery
* **Thread-safe cache** ready for concurrency
* **Binary serialization** for performance
* **Modular architecture** with minimal