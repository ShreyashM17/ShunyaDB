use tempfile::tempdir;
use shunyadb::engine::engine::Engine;
use std::collections::BTreeMap;
use shunyadb::storage::record::FieldValue;

#[test]
fn page_cache_reduces_disk_reads() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::open(dir.path())?;

    // Insert data
    for i in 0..1000 {
        let mut map = BTreeMap::new();
        map.insert("value".to_string(), FieldValue::Str(i.to_string()));
        engine.put(i.to_string(), map)?;
    }

    engine.flush()?;
    engine.maybe_compact()?;

    // First read pass
    for i in 0..1000 {
        engine.get(&i.to_string(), u64::MAX);
    }

    let disk_reads_after_first = engine.metrics.pages_read_from_disk;
    let cache_hits_after_first = engine.metrics.page_cache_hits;

    assert!(disk_reads_after_first > 0);
    assert!(cache_hits_after_first > 0);

    // Second read pass
    for i in 0..1000 {
        engine.get(&i.to_string(), u64::MAX);
    }

    let disk_reads_after_second = engine.metrics.pages_read_from_disk;
    let cache_hits_after_second = engine.metrics.page_cache_hits;

    // Cache hits must increase
    assert!(
        cache_hits_after_second > cache_hits_after_first,
        "cache hits should increase on repeated reads"
    );

    // Disk reads should grow much slower on second pass
    let first_pass_disk_reads = disk_reads_after_first;
    let second_pass_disk_reads =
        disk_reads_after_second - disk_reads_after_first;

    assert!(
        second_pass_disk_reads < first_pass_disk_reads / 2,
        "disk reads should significantly reduce after cache warmup"
    );

    Ok(())
}

