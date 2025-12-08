use shunyadb::engine::Engine;
use shunyadb::storage::{record::Record, wal::WriteAheadLog};
use shunyadb::storage::io::{save_page_to_disk, load_page_from_disk};
use shunyadb::storage::cache::PageCache;
use shunyadb::storage::record::FieldValue;
use std::collections::BTreeMap;
use std::fs;

#[test]
fn test_page_save_and_load_roundtrip() {
    let mut record_data = BTreeMap::new();
    record_data.insert("name".to_string(), FieldValue::Text("Alice".into()));
    let record = Record { id: 1, data: record_data };
    let page = shunyadb::storage::page::Page {
        id: 1,
        records: vec![record.clone()],
        capacity: 4096,
    };
    let path = "data/test_roundtrip_page.bin";

    save_page_to_disk(&page, path).unwrap();
    let loaded_page = load_page_from_disk(path).unwrap();
    assert_eq!(page.records.len(), loaded_page.records.len());
    assert_eq!(page.records[0].data, loaded_page.records[0].data);

    fs::remove_file(path).unwrap();
}

#[test]
fn test_wal_append_and_recover_consistency() {
    let wal_path = "wal_test.log";
    let mut wal = WriteAheadLog::new(wal_path);
    let record = bincode::serialize(&BTreeMap::from([
        ("name".to_string(), FieldValue::Text("Bob".into())),
    ])).unwrap();

    wal.log(&shunyadb::storage::wal::WalEntry {
        operation: "INSERT".to_string(),
        table: "users".to_string(),
        record_id: 1,
        data: record,
    });

    let entries = wal.recover();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].table, "users");
    assert_eq!(entries[0].operation, "INSERT");

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn test_cache_invalidation_on_write() {
    let cache = PageCache::new(2);
    let page_a = shunyadb::storage::page::Page {
        id: 1,
        records: vec![],
        capacity: 4096,
    };
    let key = format!("{}_page_{}", "users", 1);
    cache.put(&key, page_a.clone());
    assert!(cache.get(&key).is_some());

    // Simulate write
    cache.invalidate(&key);
    assert!(cache.get(&key).is_none());
}

#[test]
fn test_engine_replay_wal_applies_records() {
    let wal_path = "wal_replay_test.log";
    if fs::metadata(wal_path).is_ok() {
        fs::remove_file(wal_path).unwrap();
    }

    let mut engine = Engine::new(wal_path);
    let mut data = BTreeMap::new();
    data.insert("city".to_string(), FieldValue::Text("Mumbai".into()));
    let record = Record { id: 1, data };

    engine.insert_record("people", record.clone()).unwrap();
    drop(engine);

    // Restart engine and replay WAL
    let mut engine2 = Engine::new("wal_replay_test.log");
    engine2.replay_wal_at_startup().unwrap();

    let recovered = engine2.get_all("people").unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].records[0].data.get("city").unwrap(), &FieldValue::Text("Mumbai".into()));

    fs::remove_file(wal_path).unwrap();
}
