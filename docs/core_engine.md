# 🧠 ShunyaDB Core Engine — Architecture & Internals

> **File:** `src/engine/mod.rs`  
> **Author:** Shreyash Mogaveera  
> **Status:** Phase 1 — Complete  

---

## 🧩 Overview

The **Engine** module is the central coordinator of ShunyaDB.  
It bridges the CLI layer with persistence subsystems — Write-Ahead Log (WAL), Page Storage, Cache, Index, and Metadata — ensuring **typed, crash-safe, and indexed** CRUD operations.

---

## ⚙️ Engine Structure

```rust
pub struct Engine {
    pub wal: WriteAheadLog,
    pub cache: PageCache,
    pub index: HashMap<String, HashIndex>,
    pub replaying: bool,
    pub pagecapacity: usize,
}
```

| Field | Purpose |
|--------|----------|
| **wal** | Global append-only WAL (`wal.log`) for durability |
| **cache** | LRU page cache for hot-path reads/writes |
| **index** | Per-table `HashIndex` for O(1) key lookups |
| **replaying** | Flag set during WAL recovery to skip duplicate logging |
| **pagecapacity** | Page size target (~4 KB) |

---

## 🔄 Lifecycle & Initialization

### `Engine::new(wal_path: &str) -> Self`

1. Creates a `WriteAheadLog` at `wal_path`.  
2. Initializes a `PageCache` (64 entries).  
3. Loads existing tables via `util::list_tables()`.  
4. Loads or rebuilds each table’s `HashIndex`.  

---

## 🧱 CRUD Methods

### `insert_record(&mut self, table, record) -> Result<()>`

**Steps**
1. Ensure `data/<table>/` exists.  
2. Load `TableMeta`; create new page if the last one is full.  
3. WAL-log `INSERT`.  
4. Insert record → save page to disk.  
5. Update cache + metadata.  
6. Add entry to `HashIndex` and persist.  

**Guarantees:** WAL-first atomic commit, idempotent replay.

---

### `get_all(&mut self, table) -> Result<Vec<Page>>`

Loads all pages defined in `TableMeta`, using cache when possible.  
Returns every `Page` object for the table.

---

### `get(&mut self, table, filter) -> Result<Vec<Page>>`

1. If filter = `ByKeyValueEq`, try **HashIndex** lookup.  
2. If hit → load only those pages and retain matching records.  
3. Otherwise perform full table scan with `Record::matches()`.  

Returns vector of filtered `Page`s.

---

### `update(&mut self, table, filter, patch) -> Result<usize>`

- Iterates pages, applies patch to matching records.  
- WAL-logs `UPDATE` entries.  
- Persists pages and refreshes cache.  
- Updates metadata + rebuilds index.  
- Returns count of records updated.

---

### `delete(&mut self, table, filter) -> Result<usize>`

- Identifies records matching filter.  
- WAL-logs `DELETE` with IDs to remove.  
- Saves non-empty pages / deletes empty ones.  
- Updates metadata and rebuilds index.  
- Returns deleted record count.

---

## 🔁 Recovery & Maintenance

### `replay_wal_at_startup(&mut self) -> Result<()>`

**Crash Recovery Process**
1. Clear cache and set `replaying = true`.  
2. Load on-disk pages into buffer.  
3. Apply WAL entries (`INSERT / UPDATE / DELETE`) idempotently.  
4. Persist buffer back to disk.  
5. Rebuild indexes and metadata.  
6. Reset `replaying = false`.  

---

### `truncate_wal(&mut self)`

Truncates WAL after successful checkpoint or recovery.

### `clear_cache(&self)`

Empties LRU cache.

### `integrity_check(&mut self) -> Result<()>`

Rebuilds indexes and metadata for all tables,  
reports and removes missing or empty pages.

---

## 📊 Subsystem Responsibilities

| Module | Responsibility |
|---------|----------------|
| `storage/io.rs` | Binary page read/write |
| `storage/page.rs` | Page structure and record container |
| `storage/cache.rs` | LRU page cache |
| `storage/wal.rs` | WAL log/recover/truncate |
| `storage/meta.rs` | Table metadata |
| `engine/index.rs` | `HashIndex` per table |
| `engine/filter.rs` | Filter parser + logic |
| `util.rs` | Path and table utilities |

---

## ⚡ Crash Recovery Flow

```
Startup
 ├─ replay_wal_at_startup()
 │    ├─ Read wal.log
 │    ├─ Apply INSERT / UPDATE / DELETE
 │    ├─ Save pages
 │    ├─ Rebuild index + meta
 │    └─ Resume normal ops
 └─ Ready
```

---

## 🧪 Example

```bash
insert users name=Alice age=25
get users "age>20"
update users "name=Alice" age=26
delete users "id=1"
```

```rust
let mut engine = Engine::new("wal.log");
let rec = Record::from_pairs(vec!["name=Alice".into(), "age=25".into()]);
engine.insert_record("users", rec)?;
let result = engine.get("users", Filter::parse("age>20").unwrap())?;
```

---

## 🧠 Design Highlights

| Area | Choice |
|-------|--------|
| Durability | WAL + replay = crash safety |
| Performance | LRU cache + HashIndex |
| Type Safety | `FieldValue` typed records |
| Modularity | Engine ≠ direct I/O |
| Integrity | Built-in `integrity_check()` |

---

## 🔮 Future Extensions

| Feature | Planned |
|----------|----------|
| B-Tree index | Range queries |
| Graph engine | Nodes & edges |
| Vector search | Cosine similarity |
| Concurrency | `Arc<RwLock<Engine>>` |
| Checkpoints | WAL rotation + snapshots |

---

## ✅ Summary

The **Engine** guarantees that every record operation is:
- **Atomic** (via WAL),
- **Consistent** (typed data + index sync),
- **Durable** (fsync’d binary pages),
- **Efficient** (cached access).  

It is the backbone of ShunyaDB’s reliable storage layer.

---
