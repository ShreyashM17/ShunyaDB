use std::collections::BTreeMap;
use tempfile::tempdir;
use shunyadb::{engine::engine::Engine, storage::record::FieldValue};
use shunyadb::engine::seqno::current;

fn value(i: usize) -> BTreeMap<String, FieldValue> {
    let mut map = BTreeMap::new();
    map.insert(
        "value".to_string(),
        FieldValue::Str(format!("value_{}", i)),
    );
    map
}

#[test]
fn compaction_and_wal_checkpoint_are_safe() -> anyhow::Result<()> {
    // 1️⃣ Setup isolated temp directory
    let dir = tempdir()?;
    let data_dir = dir.path();

    // 2️⃣ Create engine
    let mut engine = Engine::open(data_dir)?;

    // 3️⃣ Write enough entries to:
    //    - trigger flush
    //    - trigger compaction
    //    - advance WAL seqnos
    let total = 2_000;

    for i in 0..total {
        engine.put(i.to_string(), value(i))?;
    }

    // 3️⃣ Force durability boundaries
    engine.flush()?;
    engine.maybe_compact()?;

    // Capture checkpoint (for sanity, not logic)
    let checkpoint = engine.meta.checkpoint_seqno;

    // 4️⃣ Drop engine (simulate crash)
    drop(engine);

    // 5️⃣ Restart engine (recovery path)
    let mut engine = Engine::open(data_dir)?;
    let snapshot = current();

    // 6️⃣ Verify ALL data is present
    for i in 0..total {
      let record = engine.get(&i.to_string(), snapshot);
      assert!(
          record.is_some(),
          "missing record after restart for key={}",
          i
      );

      let record = record.unwrap();
      let value = record
          .data
          .get("value")
          .expect("missing 'value' field");

      assert_eq!(
          value.clone(),
          FieldValue::Str(format!("value_{}", i)),
          "incorrect value after restart for key={}",
          i
      );
    }

    // 7️⃣ Sanity check: checkpoint should not regress
    assert!(
        engine.meta.checkpoint_seqno >= checkpoint,
        "checkpoint_seqno regressed after restart"
    );

    Ok(())
}
