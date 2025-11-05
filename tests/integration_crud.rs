use shunyadb::engine::Engine;
use shunyadb::storage::record::FieldValue;
use shunyadb::storage::{record::Record};
use shunyadb::engine::filter::Filter;
use shunyadb::storage::page::Page;
use shunyadb::storage::cache::PageCache;

#[test]
fn test_insert_and_get() {
    let mut engine = Engine::new("wal.log");
    let pairs = vec!["name=Alice".to_string(),"age=25".to_string()];
    let record = Record::from_pairs(pairs);
    engine.insert_record("user", record.clone()).unwrap();
    engine.clear_cache();
    let results = engine.get_all("user");
    assert_eq!(results.records.len(), 1);
    assert_eq!(results.records[0].data["name"], FieldValue::Text("Alice".into()));
    let results = engine.get("user", Filter::parse("age>24").unwrap());
    assert_eq!(results.records[0].data["name"], FieldValue::Text("Alice".into()));
    engine.truncate_wal();
}

#[test]
fn test_filter_and_delete() {
    let mut engine = Engine::new("wal.log");
    let pairs = vec!["name=Bob".to_string(),"age=30".to_string()];
    let record = Record::from_pairs(pairs);
    engine.insert_record("people", record.clone()).unwrap();
    engine.clear_cache();
    let filter = Filter::parse("name=Bob").unwrap();
    let deleted = engine.delete("people", filter).unwrap();
    assert_eq!(deleted, 1);
    engine.truncate_wal();
}

#[test]
fn test_cache_put_get_invalidate() {
  let cache = PageCache::new(2);
  let page = Page::new(2, 4096);
  cache.put("users_page_2", page.clone());
  assert!(cache.get("users_page_2").is_some());
  cache.invalidate("users_page_2");
  assert!(cache.get("users_page_2").is_none());
}

#[test]
fn test_wal_recovery_after_crash() {
    // Step 1: Insert record normally
    {
        let mut engine = Engine::new("wal.log");
        engine.truncate_wal(); // To truncate if there are any older versions
        let pairs = vec!["city=Mumbai".to_string()];
        let record = Record::from_pairs(pairs);
        engine.insert_record("places", record).unwrap();
        // Simulate abrupt crash (drop without saving cache)
    }

    // Step 2: New instance replays WAL
    {
        let mut engine = Engine::new("wal.log");
        engine.replay_wal_at_startup().unwrap();

        let results = engine.get_all("places");
        assert!(!results.records.is_empty());
        assert_eq!(results.records[0].data["city"], FieldValue::Text("Mumbai".into()));
    }
}
