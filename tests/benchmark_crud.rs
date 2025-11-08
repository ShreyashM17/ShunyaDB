use shunyadb::engine::Engine;
use shunyadb::engine::filter::Filter;
use shunyadb::storage::meta::TableMeta;
use shunyadb::storage::record::{FieldValue, Record};
use std::collections::BTreeMap;
use std::time::Instant;

#[test]
fn benchmark_full_crud_suite() {
    let mut engine = Engine::new("wal.log");
    let table = "benchmark_crud";
    engine.truncate_wal();

    // Clean existing data
    std::fs::remove_dir_all(format!("data/{}", table)).ok();
    std::fs::create_dir_all(format!("data/{}", table)).unwrap();

    const N: usize = 10_000;
    println!("\n🚀 Starting full CRUD benchmark with {N} records");

    // --- INSERT BENCHMARK ---
    let start_insert = Instant::now();
    for i in 0..N {
        let mut data = BTreeMap::new();
        data.insert("name".into(), FieldValue::Text(format!("User{}", i)));
        data.insert("age".into(), FieldValue::Int((20 + (i % 50)) as i64));
        let record = Record { id: i as u64, data };
        engine.insert_record(table, record).unwrap();
    }
    let insert_time = start_insert.elapsed();
    println!(
        "✅ Inserted {N} records in {:.2?} ({:.2} ops/sec)",
        insert_time,
        N as f64 / insert_time.as_secs_f64()
    );

    // --- GET BENCHMARK (INDEXED) ---
    let filter = Filter::parse("name=User5000").unwrap();
    let start_get = Instant::now();
    let res = engine.get(table, filter).unwrap();
    let get_time = start_get.elapsed();
    let records_found: usize = res.iter().map(|p| p.records.len()).sum();
    assert_eq!(records_found, 1);
    println!(
        "🔍 GET query completed in {:.3?} (records found: {})",
        get_time, records_found
    );

    // --- UPDATE BENCHMARK ---
    let mut patch = BTreeMap::new();
    patch.insert("age".into(), FieldValue::Int(99));
    let filter_update = Filter::parse("age=25").unwrap();
    let start_update = Instant::now();
    let updated_count = engine.update(table, filter_update, patch).unwrap();
    let update_time = start_update.elapsed();
    println!(
        "✏️  Updated {} records in {:.2?} ({:.2} ops/sec)",
        updated_count,
        update_time,
        updated_count as f64 / update_time.as_secs_f64()
    );

    // --- DELETE BENCHMARK ---
    let filter_delete = Filter::parse("age=99").unwrap();
    let start_delete = Instant::now();
    let deleted_count = engine.delete(table, filter_delete).unwrap();
    let delete_time = start_delete.elapsed();
    println!(
        "🗑️  Deleted {} records in {:.2?} ({:.2} ops/sec)",
        deleted_count,
        delete_time,
        deleted_count as f64 / delete_time.as_secs_f64()
    );

    // --- WAL REPLAY BENCHMARK ---
    std::fs::remove_dir_all(format!("data/{}", table)).ok();
    std::fs::create_dir_all(format!("data/{}", table)).unwrap();
    let start_replay = Instant::now();
    engine.replay_wal_at_startup().unwrap();
    let replay_time = start_replay.elapsed();
    println!("💾 WAL replay completed in {:.3?}", replay_time);

    // --- VALIDATE METADATA ---
    let meta = TableMeta::load(table).unwrap();
    let total_pages = meta.pages.len();
    let total_records: u64 = meta.pages.iter().map(|p| p.record_count).sum();
    println!(
        "📊 Pages: {} | Records remaining: {} | Avg/page: {:.2}",
        total_pages,
        total_records,
        total_records as f64 / total_pages as f64
    );

    println!("\n🏁 CRUD Benchmark Summary:");
    println!("• Insert Time:  {:.2?}", insert_time);
    println!("• Get Time:     {:.3?}", get_time);
    println!("• Update Time:  {:.2?}", update_time);
    println!("• Delete Time:  {:.2?}", delete_time);
    println!("• Replay Time:  {:.2?}", replay_time);
}
