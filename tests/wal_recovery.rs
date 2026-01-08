use std::collections::BTreeMap;
use tempfile::TempDir;

use shunyadb::engine::engine::Engine;
use shunyadb::engine::seqno;
use shunyadb::storage::record::FieldValue;

fn sample_value(v: &str) -> BTreeMap<String, FieldValue> {
    let mut map = BTreeMap::new();
    map.insert("val".to_string(), FieldValue::Str(v.to_string()));
    map
}

#[test]
fn wal_recovery_works() {
    let dir = TempDir::new().unwrap();

    {
        let mut engine = Engine::open(dir.path()).unwrap();
        engine.put("1".to_string(), sample_value("a")).unwrap();
        engine.put("1".to_string(), sample_value("b")).unwrap();
        engine.flush().unwrap();
    }

    let mut engine = Engine::open(dir.path()).unwrap();
    let snapshot = seqno::current();

    let rec = engine.get("1", snapshot).unwrap();
    assert_eq!(
        rec.data.get("val").unwrap(),
         &FieldValue::Str("b".to_string())
    );
}
