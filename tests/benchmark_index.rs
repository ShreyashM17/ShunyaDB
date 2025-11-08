use shunyadb::engine::Engine;
use shunyadb::storage::record::{Record, FieldValue};
use std::collections::BTreeMap;
use std::fs;
use std::time::Instant;

#[test]
fn benchmark_index_vs_scan() {
    let mut engine = Engine::new("wal.log");
    let table = "bench_users";
    let n = 4090;

    // Insert N records
    for i in 0..n {
        let mut data = BTreeMap::new();
        data.insert("name".to_string(), FieldValue::Text(format!("User{}", i)));
        data.insert("age".to_string(), FieldValue::Int((20 + i % 50) as i64));
        let record = Record { id: i as u64, data };
        engine.insert_record(table, record).unwrap();
    }

    // Force index rebuild
    engine.index.insert(
        table.to_string(),
        shunyadb::engine::index::HashIndex::rebuild_index(table).unwrap(),
    );

    let filter = shunyadb::engine::filter::Filter::parse("name=User500").unwrap();

    // Uncached linear scan
    engine.clear_cache();
    let index_path = format!("data/{}/index.bin", table);
    let _ = fs::write(index_path, b"");
    let start = Instant::now();
    let _res1 = engine.get(table, filter.clone());
    let linear_time = start.elapsed().as_micros();

    // Indexed lookup
    let _ = engine.integrity_check();
    let start = Instant::now();
    let _res2 = engine.get(table, filter);
    let indexed_time = start.elapsed().as_micros();

    println!("\nBenchmark Results:");
    println!("Linear:  {} µs", linear_time);
    println!("Indexed: {} µs", indexed_time);
    println!("Speedup: {:.2}x", linear_time as f64 / indexed_time as f64);
}
