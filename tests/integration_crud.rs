use shunyadb::engine::*;
use shunyadb::storage::record::{Record, FieldValue};
use std::collections::BTreeMap;

#[test]
fn test_crud_btree() {
  let mut engine = Engine::new("wal.log");

  // Insert
  let mut data = BTreeMap::new();
  data.insert("name".into(), FieldValue::Text("Shreyash".into()));
  data.insert("age".into(), FieldValue::Int(23));
  let rec = Record { id: 1, data };
  engine.insert_record("users", rec.clone()).unwrap();

  // Update
  let mut patch = BTreeMap::new();
  patch.insert("age".into(), FieldValue::Int(24));
  let filter = filter::Filter::ById(1);
  let n = engine.update("users", filter.clone(), patch).unwrap();
  assert_eq!(n, 1);

  // Delete
  let deleted = engine.delete("users", filter).unwrap();
  assert_eq!(deleted, 1);
}
