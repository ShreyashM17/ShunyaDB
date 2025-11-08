# 🖥️ ShunyaDB CLI Interface — Command Layer & Integration

> File: `src/cli.rs + src/main.rs`
>
> **Modules Used:** `engine::Engine`, `filter::Filter`, `storage::record::Record`
> 
> **Status:** Phase 1 — Complete
> 
> **Author:** Shreyash Mogaveera 

---

## 🧩 Overview

The ShunyaDB **Command-Line Interface (CLI)** acts as the *frontend entry point* for all database operations.

It translates user commands like `insert`, `get`, `update`, and `delete` into structured Rust method calls on the **Engine** — which handles WAL, persistence, caching, and indexing.

The CLI is built using the [`clap`](https://crates.io/crates/clap) crate for ergonomic parsing and supports commands that mimic SQL-like syntax.

---

## ⚙️ Architecture

```
User → CLI Parser → Engine API → Storage/WAL
```

### Files Involved

| File | Responsibility |
| --- | --- |
| `main.rs` | Entry point, initializes engine + parses commands |
| `cli.rs` | Defines CLI schema and argument parsing (via `clap`) |
| `engine/mod.rs` | Executes CRUD logic and recovery |
| `engine/filter.rs` | Parses WHERE-like conditions |
| `util.rs` | Converts CLI `key=value` pairs into structured data |

---

## 🚀 CLI Workflow

```bash
shunyadb insert users name=Alice age=25
shunyadb get users where age>20
shunyadb update users where name=Alice age=26
shunyadb delete users where id=1
```

1. Command is parsed into a `Commands` enum.
2. CLI constructs the right `Record`, `Filter`, or patch map.
3. Corresponding method (`insert_record`, `get`, etc.) is called on the `Engine`.
4. Results are printed in a readable debug format.
---

## 🧱 Command Structure

```rust
match cli.command {
    Commands::Insert { table, pairs } => { ... }
    Commands::Get { table, filter } => { ... }
    Commands::GetAll { table } => { ... }
    Commands::Update { table, filter, patch } => { ... }
    Commands::Delete { table, filter } => { ... }
    Commands::ReplayWal => { ... }
    Commands::TruncateWal => { ... }
    Commands::IntegrityCheck => { ... }
}

```

---

## 🧩 Command Reference

### 🔹 `insert`

**Syntax:**

```bash
shunyadb insert <table> <key=value ...>
```

**Example:**

```bash
shunyadb insert users name=Alice age=25 active=true
```

**Flow:**

1. CLI builds a `Record` via `Record::from_pairs(pairs)`.
2. Calls `Engine::insert_record(&table, record)`.
3. Engine logs to WAL and persists to page + index.
4. Prints confirmation.

---

### 🔹 `get`

**Syntax:**

```bash
shunyadb get <table> <filter>
```

**Example:**

```bash
shunyadb get users "age>20"
```

**Flow:**
1. CLI parses filter string using `Filter::parse(&filter)`.
2. Calls `Engine::get(&table, filter_value)`.
3. Engine decides whether to use **index lookup** or **full scan**.
4. Prints result with formatted pages and records.

**Output Example:**

```
Records in table = users
Filter = age>20
Records: [
  Page { id: 1, records: [ Record { id: 1, data: { "name": "Alice", "age": 25 } } ] }
]
```

---

### 🔹 `get-all`

**Syntax:**

```bash
shunyadb get-all <table>
```

**Example:**

```bash
shunyadb get-all users
```

**Flow:**

- Calls `Engine::get_all(&table)`.
- Returns all pages, leveraging cache.
- Useful for debugging or full dumps.

---

### 🔹 `update`

**Syntax:**

```bash
shunyadb update <table> <filter> <key=value ...>
```

**Example:**

```bash
shunyadb update users "name=Alice" age=26 city=Mumbai
```

**Flow:**

1. Parses filter via `Filter::parse`.
2. Converts patch pairs → `BTreeMap` via `util::from_pairs_to_btree()`.
3. Calls `Engine::update(&table, filter, patch)`.
4. Logs updates to WAL and persists page.
5. Prints update confirmation.

**Output:**

```
Records in users from values name=Alice updated
```

---

### 🔹 `delete`

**Syntax:**

```bash
shunyadb delete <table> <filter>
```

**Example:**

```bash
shunyadb delete users "id=1"
```

**Flow:**

1. Parses filter.
2. Calls `Engine::delete(&table, filter_value)`.
3. Deletes records safely and logs in WAL.
4. Prints deletion confirmation.

---

### 🔹 `replay-wal`

**Syntax:**

```bash
shunyadb replay-wal
```

**Flow:**

1. Calls `Engine::replay_wal_at_startup()`.
2. Replays inserts/updates/deletes sequentially.
3. Rebuilds metadata and indexes.
4. Prints: `WAL replay complete`.

---

### 🔹 `truncate-wal`

**Syntax:**

```bash
shunyadb truncate-wal
```

**Flow:**

- Calls `Engine::truncate_wal()`.
- Clears WAL file (use after recovery).
- Prints: `WAL truncated`.
---

### 🔹 `integrity-check`

**Syntax:**

```bash
shunyadb integrity-check
```

**Flow:**

1. Calls `Engine::integrity_check()`.
2. Rebuilds indexes and metadata.
3. Removes empty/missing pages.
4. Prints per-table diagnostics.

**Output Example:**

```
Integrity: checking table 'users'
  - Empty page found: 3
  - Missing page file for page 7
Integrity check completed
```

---

## 🔍 Filter Integration

- The CLI’s `get`, `update`, and `delete` commands rely on:
    
    ```rust
    let filter_value = Filter::parse(&filter).unwrap();
    ```
    
- The filter supports operators:
    `=`, `>`, `<`, `>=`, `<=`.
    

Example parsing:

```rust
Filter::parse("age>20")
→ Filter::ByKeyValueOp("age", ">", FieldValue::Int(20))
```

---

## 🧰 Utility Conversion

The helper:

```rust
util::from_pairs_to_btree(patch)
```

converts CLI `Vec<String>` inputs like:

```
["age=26", "city=Mumbai"]
```

into:

```rust
BTreeMap<String, FieldValue> {
  "age" => FieldValue::Int(26),
  "city" => FieldValue::Text("Mumbai")
}
```

---

## 🧱 Engine–CLI Bridge

| CLI Command | Engine Method | WAL Logged | Cache Updated | Index Updated |
| --- | --- | --- | --- | --- |
| `insert` | `insert_record()` | ✅ | ✅ | ✅ |
| `get` | `get()` | ❌ | ✅ (read) | ✅ (lookup) |
| `get-all` | `get_all()` | ❌ | ✅ | ❌ |
| `update` | `update()` | ✅ | ✅ | ✅ |
| `delete` | `delete()` | ✅ | ✅ | ✅ |
| `replay-wal` | `replay_wal_at_startup()` | — | — | ✅ |
| `truncate-wal` | `truncate_wal()` | — | — | — |
| `integrity-check` | `integrity_check()` | — | — | ✅ |

---

## 🧠 Developer Notes

- All CLI commands return `anyhow::Result<()>` — uniform error handling.
- Each operation uses the same `Engine` instance per execution.
- CLI is intentionally **stateless** — ShunyaDB CLI runs as a one-shot command tool (like SQLite CLI mode).
- Supports easy transition to REST interface later (`/api/query`, `/api/insert`).

---

## 💡 Future Plans (Phase 3)

| Feature | Description |
| --- | --- |
| Interactive mode | `shunyadb shell` for continuous queries |
| REST API layer | Axum-based HTTP endpoints |
| Batch mode | Execute `.sql`-like script files |
| JSON output | Pretty-print query results as JSON |

---

## ✅ Summary

The **CLI layer** in ShunyaDB:

- Acts as a simple SQL-like bridge between users and the `Engine`.
- Translates human commands → typed `Record`, `Filter`, and `Patch` structures.
- Ensures clean modular separation from storage internals.

It’s fast, crash-safe, and ready to evolve into an **HTTP or gRPC API** in Phase 3.
---