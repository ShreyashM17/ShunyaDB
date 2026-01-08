use tempfile::tempdir;
use shunyadb::storage::flush::flush_memtable;
use shunyadb::storage::memtable::MemTable;
use shunyadb::storage::record::{Record, FieldValue};
use shunyadb::storage::page::io::read_page_from_disk;

#[test]
fn memtable_flush_creates_page_and_clears_memtable() {
    let dir = tempdir().unwrap();
    let page_path = dir.path().join("page_0.db");

    let mut mem = MemTable::new();
    mem.put(Record::from_pairs("a", 1, vec![("v", FieldValue::Int(1))]));
    mem.put(Record::from_pairs("b", 2, vec![("v", FieldValue::Int(2))]));

    flush_memtable(&mut mem, &dir).unwrap();

    assert!(mem.is_empty());

    let page = read_page_from_disk(&page_path).unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].id, "a");
    assert_eq!(page.records[1].id, "b");
}

#[test]
fn flushing_empty_memtable_fails() {
    let dir = tempdir().unwrap();

    let mut mem = MemTable::new();
    let result = flush_memtable(&mut mem, &dir);

    assert!(result.is_err());
}
