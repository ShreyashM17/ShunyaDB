# 🔍 ShunyaDB Filter System — Query Parsing & Evaluation

> **File:** `src/engine/filter.rs`  
> **Author:** Shreyash Mogaveera  
> **Status:** Phase 1 — Complete

---

## 🧩 Overview

The **Filter** module provides SQL-style conditional logic for record selection.  
It enables commands such as:

```
id=1
age>20
price<=500
name=Shreyash
```

Used by:  
- `Engine::get()`  
- `Engine::update()`  
- `Engine::delete()`

---

## ⚙️ Filter Enum

```rust
#[derive(Debug, Clone)]
pub enum Filter {
    ById(u64),
    ByKeyValueEq(String, FieldValue),
    ByKeyValueOp(String, String, FieldValue),
}
```

| Variant | Description | Example |
|----------|--------------|----------|
| `ById(u64)` | Match by record ID | `id=1` |
| `ByKeyValueEq` | Equality comparison | `name=Shreyash` |
| `ByKeyValueOp` | Comparison with operator (`>`, `<`, `>=`, `<=`) | `age>20` / `price<=500` |

---

## 🧠 Parsing Logic

```rust
pub fn parse(s: &str) -> Option<Self> {
    let ops = ["<=", ">=", "=", ">", "<"];
    for op in ops {
        if let Some((k, v)) = s.split_once(op) {
            let k = k.trim();
            let v = v.trim();
            if k == "id" && op == "=" {
                if let Ok(id) = v.parse::<u64>() {
                    return Some(Filter::ById(id));
                } else { return None; }
            }
            let field = FieldValue::from_str_infer(v);
            if op == "=" {
                return Some(Filter::ByKeyValueEq(k.to_string(), field));
            } else {
                return Some(Filter::ByKeyValueOp(k.to_string(), op.to_string(), field));
            }
        }
    }
    None
}
```

### Rules
1. Operators checked longest-first (`<=`, `>=`, `=`, `>`, `<`).  
2. `"id"` with `=` maps to `ById`.  
3. Other keys use `FieldValue::from_str_infer()` for type inference.  

### Examples

| Input | Output |
|--------|---------|
| `id=5` | `Filter::ById(5)` |
| `age>18` | `Filter::ByKeyValueOp("age", ">", Int(18))` |
| `name=Alice` | `Filter::ByKeyValueEq("name", Text("Alice"))` |
| `price<=99.5` | `Filter::ByKeyValueOp("price", "<=", Float(99.5))` |

---

## 🔍 Evaluation Integration

`Record::matches(&Filter)` applies filters to records:

```rust
pub fn matches(&self, filter: &Filter) -> bool {
    match filter {
        Filter::ById(id) => self.id == *id,
        Filter::ByKeyValueEq(k, v) => self.data.get(k) == Some(v),
        Filter::ByKeyValueOp(k, op, v) => compare_with_operator(self.data.get(k), op, v),
    }
}
```

`compare_with_operator()` handles numeric and lexicographic comparisons.

---

## ⚖️ Supported Operators

| Operator | Meaning | Types |
|-----------|----------|-------|
| `=` | Equality | All |
| `>` / `<` | Greater / Less | Int, Float, Text |
| `>=` / `<=` | Inclusive range | Int, Float, Text |

---

## 🧪 CLI Examples

```bash
shunyadb get users "age>20"
shunyadb update users "name=Alice" age=26
shunyadb delete users "price<50"
```

---

## 🧱 Integration Summary

| Layer | Role |
|--------|------|
| `cli.rs` | Parses user string → `Filter::parse()` |
| `engine/mod.rs` | Uses filter to select records/pages |
| `record.rs` | Executes `matches()` |
| `index.rs` | Accelerates `ByKeyValueEq` lookups |

---

## 🔮 Planned Extensions

| Feature | Description |
|----------|-------------|
| Logical operators | `AND`, `OR`, `NOT` |
| Nested filters | `(age>20 AND city=Mumbai)` |
| Pattern matching | `LIKE`, `REGEX` for Text |
| Multi-filter queries | Chained conditions |

---

## ✅ Summary

`filter.rs` converts human-readable conditions into typed `Filter` objects  
that can be evaluated quickly and safely across records and indexes.  
It is the logical core of conditional operations in ShunyaDB.

---
