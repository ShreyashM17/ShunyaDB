# 📖 ShunyaDB Developer Documentation Index

Welcome to the internal documentation for **ShunyaDB v0.1** (Phase 1).

---

## 📘 Core Modules

| Module | Description | Link |
|---------|--------------|------|
| **Engine** | Core CRUD logic, WAL integration, and cache synchronization | [core_engine.md](./core_engine.md) |
| **Filters** | SQL-like query condition system | [filters.md](./filters.md) |
| **CLI** | Command-line interface bridging user input to the engine | [cli_interface.md](./cli_interface.md) |
| **Storage** | Handles Pages, I/O, WAL, and Caching | *(Planned Phase 2 detailed docs)* |

---

## 🧩 Design Roadmap
Key Milestones:
- **Phase 1:** Typed Engine + WAL + Cache + Index ✅  
- **Phase 2:** Graph + Vector Hybrid Engine 🚧  
- **Phase 3:** REST API + Concurrency + Logging + Benchmarks (Planned)  

---

## 🧠 Future Work

- Add **Graph module** (`node.rs`, `edge.rs`)  
- Implement **Vector similarity search** (`vector/similarity.rs`)  
- Introduce **B-Tree indexes** for range queries  
- Add **WAL checkpointing + rotation**  
- Extend CLI → REST via **Axum API layer**  
- Add **tracing logs** for cache hits and I/O events  

---

## 🧪 Developer Testing Commands

```bash
# Run full test suite
cargo test

# Run specific integration test
cargo test benchmark_crud

# Check formatting and lints
cargo fmt --all
cargo clippy
```

---

## 📦 Docs Summary

| File | Purpose |
|------|----------|
| `/docs/core_engine.md` | Engine architecture, CRUD flow, and recovery logic |
| `/docs/filters.md` | Filter parsing and evaluation details |
| `/docs/cli_interface.md` | CLI command workflow and Engine integration |
| `/docs/README_DEV.md` | This documentation index |

---

## 🏁 Version

- **Release:** v0.1-pre  
- **Status:** Phase 1 Completed — Ready for Hybrid Expansion  
- **Maintainer:** Shreyash Mogaveera  
- **Year:** 2025  

---

© 2025 Shreyash Mogaveera — *ShunyaDB Hybrid Database Engine*
