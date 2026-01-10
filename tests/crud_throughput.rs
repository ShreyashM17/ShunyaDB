use shunyadb::engine::engine::Engine;
use std::collections::BTreeMap;
use shunyadb::storage::record::FieldValue;

#[test]
fn rough_put_throughput() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut engine = Engine::open(dir.path())?;

    let start = std::time::Instant::now();

    for i in 0..10_000 {
        let mut map = BTreeMap::new();
        map.insert("value".to_string(), FieldValue::Str(i.to_string()));
        engine.put(i.to_string(), map)?;
    }

    let elapsed = start.elapsed();
    println!("10k writes took {:?}", elapsed);

    Ok(())
}
