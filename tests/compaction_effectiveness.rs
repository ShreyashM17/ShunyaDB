use tempfile::tempdir;
use shunyadb::engine::engine::Engine;
use std::collections::BTreeMap;
use shunyadb::storage::record::FieldValue;

#[test]
fn compaction_reduces_l0_pages() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::open(dir.path())?;

    // Force many flushes
    for i in 0..5000 {
        let mut map = BTreeMap::new();
        map.insert("value".to_string(), FieldValue::Str(i.to_string()));
        engine.put(i.to_string(), map)?;
    }

    engine.flush()?;
    engine.maybe_compact()?;

    let l0_pages = engine.meta.level[0].len();
    let l1_pages = engine.meta.level[1].len();

    assert!(
        l0_pages < 8,
        "L0 should be reduced after compaction"
    );

    assert!(
        l1_pages > 0,
        "compaction should produce L1 pages"
    );

    assert!(
        engine.metrics.compactions > 0,
        "compaction metric should increment"
    );

    Ok(())
}
