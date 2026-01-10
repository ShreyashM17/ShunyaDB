use tempfile::tempdir;
use shunyadb::engine::engine::Engine;
use std::collections::BTreeMap;
use shunyadb::storage::record::FieldValue;

#[test]
fn writes_trigger_flush_reasonably() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::open(dir.path())?;

    let writes = 2000;

    for i in 0..writes {
        let mut map = BTreeMap::new();
        map.insert("value".to_string(), FieldValue::Str(i.to_string()));
        engine.put(i.to_string(), map)?;
    }

    assert!(
        engine.metrics.flushes > 0,
        "writes should trigger memtable flushes"
    );

    assert!(
        engine.metrics.wal_appends >= writes as u64,
        "each write should append to WAL"
    );

    Ok(())
}
