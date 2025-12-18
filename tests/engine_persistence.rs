use std::fs;
use std::path::Path;

fn clean_dir(path: &Path) {
    if path.exists() {
        fs::remove_dir_all(path).unwrap();
    }
    fs::create_dir_all(path).unwrap();
}

use std::collections::BTreeMap;
use shunyadb::storage::record::FieldValue;

fn value(i: usize) -> BTreeMap<String, FieldValue> {
    let mut map = BTreeMap::new();
    map.insert(
        "value".to_string(),
        FieldValue::Str(format!("val_{}", i)),
    );
    map
}

use shunyadb::engine::engine::Engine;
use shunyadb::engine::seqno::current;

#[test]
fn persist_1000_entries_to_disk() {
    let base = std::path::Path::new("test_data/shunyadb_engine_test");
    clean_dir(base);

    let mut engine = Engine::open(base).expect("engine open failed");

    for i in 0..1000 {
        engine
            .put(i.to_string(), value(i))
            .expect("put failed");
    }

    let snap_before_flush = current();

    engine.flush().expect("flush failed");

    // Verify files exist
    assert!(base.join("wal.log").exists());
    assert!(base.join("meta.json").exists());

    let page_count = std::fs::read_dir(base)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("page_")
        })
        .count();

    assert!(page_count > 0, "no pages written to disk");

    // Verify reads still work after flush
    for i in 0..1000 {
        let rec = engine
            .get(&i.to_string(), snap_before_flush)
            .expect("missing record");

        let v = rec.data.get("value").unwrap();
        assert_eq!(v, &FieldValue::Str(format!("val_{}", i)));
    }
}


#[test]
fn restart_engine_reads_from_disk() {
    let base = std::path::Path::new("test_data/shunyadb_engine_test");

    // Restart (new Engine instance)
    let engine = Engine::open(base).expect("engine reopen failed");

    let snapshot = shunyadb::engine::seqno::current();

    for i in 0..1000 {
        let rec = engine
            .get(&i.to_string(), snapshot)
            .expect("record missing after restart");

        let v = rec.data.get("value").unwrap();
        assert_eq!(v, &FieldValue::Str(format!("val_{}", i)));
    }
}


#[test]
fn update_and_delete_after_restart() {
    let base = std::path::Path::new("test_data/shunyadb_engine_test");
    let mut engine = Engine::open(base).unwrap();

    // Update half
    for i in 0..500 {
        engine
            .put(i.to_string(), value(i + 10_000))
            .unwrap();
    }

    // Delete quarter
    for i in 500..750 {
        engine.delete(i.to_string()).unwrap();
    }

    let snap = shunyadb::engine::seqno::current();
    engine.flush().unwrap();

    // Restart again
    let engine = Engine::open(base).unwrap();

    // Updated records
    for i in 0..500 {
        let rec = engine.get(&i.to_string(), snap).unwrap();
        let v = rec.data.get("value").unwrap();
        assert_eq!(v, &FieldValue::Str(format!("val_{}", i + 10_000)));
    }

    // Deleted records
    for i in 500..750 {
        assert!(engine.get(&i.to_string(), snap).is_none());
    }

    // Untouched records
    for i in 750..1000 {
        let rec = engine.get(&i.to_string(), snap).unwrap();
        let v = rec.data.get("value").unwrap();
        assert_eq!(v, &FieldValue::Str(format!("val_{}", i)));
    }
}
