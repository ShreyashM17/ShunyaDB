use super::MemTable;
use crate::storage::record::{Record, FieldValue};

#[test]
fn memtable_put_and_get() {
    let mut mem = MemTable::new();

    let r = Record::from_pairs("a", 10, vec![("v", FieldValue::Int(1))]);
    mem.put(r);

    let got = mem.get("a", 10).unwrap();
    assert_eq!(got.id, "a");
}

#[test]
fn memtable_respects_snapshot_seqno() {
    let mut mem = MemTable::new();

    let r = Record::from_pairs("x", 50, vec![("v", FieldValue::Int(1))]);
    mem.put(r);

    assert!(mem.get("x", 10).is_none());
}

#[test]
fn memtable_delete_creates_tombstone() {
    let mut mem = MemTable::new();

    let r = Record::from_pairs("k", 5, vec![("v", FieldValue::Int(1))]);
    mem.put(r);

    //tombstone check
    let tom = Record::new_tombstone("k", 10);
    mem.put(tom);

    let rec = mem.get("k", 10);
    assert_eq!(rec, None);
}
