use super::header::PageHeader;
use super::builder::*;
use crate::storage::record::{Record, FieldValue};
use crate::storage::page::lookup::PageLookupResult;
use crate::storage::page::io::*;
use tempfile::tempdir;

#[test]
fn page_header_validation_works() {
  let header = PageHeader::new(
    "a".into(),
    "z".into(),
    10,
    100,
  );
  assert!(header.validate().is_ok());
}

#[test]
fn page_header_validation_works_with_min_max_error() {
  let header = PageHeader::new(
    "z".into(),
    "a".into(),
    10,
    100,
  );
  assert!(header.validate().is_err());
}

#[test]
fn page_header_detects_invalid_magic() {
  let mut header = PageHeader::new(
    "a".into(),
    "z".into(),
    10,
    100,
  );
  header.magic = 0xdeadbeef;
  assert!(header.validate().is_err());
}

#[test]
fn checksum_is_deterministic() {
  let payload = b"hello world";
  let c1 = PageHeader::compute_checksum(payload);
  let c2 = PageHeader::compute_checksum(payload);
  assert_eq!(c1, c2);
}


#[test]
fn page_builder_creates_sorted_page() {
    let mut pb = PageBuilder::new();

    pb.add(Record::from_pairs(
        "b", 10, vec![("v", FieldValue::Int(2))]
    ));
    pb.add(Record::from_pairs(
        "a", 5, vec![("v", FieldValue::Int(1))]
    ));
    pb.add(Record::from_pairs(
        "c", 20, vec![("v", FieldValue::Int(3))]
    ));

    let page = pb.build();

    assert_eq!(page.header.min_id, "a");
    assert_eq!(page.header.max_id, "c");
    assert_eq!(page.header.num_records, 3);
    assert_eq!(page.header.page_seqno, 20);

    let ids: Vec<_> = page.records.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

#[test]
fn checksum_matches_payload() {
    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs(
        "x", 42, vec![("n", FieldValue::Int(100))]
    ));

    let page = pb.build();
    let checksum = PageHeader::compute_checksum(&page.payload);

    assert_eq!(checksum, page.header.checksum);
}

#[test]
#[should_panic]
fn empty_page_is_not_allowed() {
    let pb = PageBuilder::new();
    let _ = pb.build();
}

use crate::storage::page::reader::*;

#[test]
fn page_roundtrip_works() {
    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs("a", 1, vec![("x", FieldValue::Int(1))]));
    pb.add(Record::from_pairs("b", 2, vec![("x", FieldValue::Int(2))]));

    let page = pb.build();

    // simulate disk bytes: [header][payload]
    let mut bytes = Vec::new();
    bytes.extend(bincode::serialize(&page.header).unwrap());
    bytes.extend(&page.payload);

    let decoded = read_page(&bytes).unwrap();

    assert_eq!(decoded.header, page.header);
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].id, "a");
    assert_eq!(decoded.records[1].id, "b");
}

#[test]
fn detects_checksum_corruption() {
    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs("x", 10, vec![("v", FieldValue::Int(1))]));
    let page = pb.build();

    let mut bytes = Vec::new();
    bytes.extend(bincode::serialize(&page.header).unwrap());

    // corrupt payload
    let mut payload = page.payload.clone();
    payload[0] ^= 0xFF;
    bytes.extend(payload);

    let result = read_page(&bytes);
    assert!(result.is_err());
}

// Tests for page lookup
#[test]
fn lookup_finds_existing_record() {
    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs("a", 5, vec![("v", FieldValue::Int(1))]));
    pb.add(Record::from_pairs("b", 10, vec![("v", FieldValue::Int(2))]));

    let page = pb.build();

    let res = page.get("a", 100);
    assert!(matches!(res, PageLookupResult::Found(_)));
}

#[test]
fn lookup_respects_snapshot_seqno() {
    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs("x", 50, vec![("v", FieldValue::Int(1))]));

    let page = pb.build();

    let res = page.get("x", 10);
    assert_eq!(res, PageLookupResult::NotVisible);
}

#[test]
fn lookup_returns_not_found_for_missing_key() {
    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs("a", 1, vec![("v", FieldValue::Int(1))]));

    let page = pb.build();

    let res = page.get("z", 100);
    assert_eq!(res, PageLookupResult::NotFound);
}

#[test]
fn lookup_uses_page_pruning() {
    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs("m", 10, vec![("v", FieldValue::Int(1))]));
    pb.add(Record::from_pairs("n", 20, vec![("v", FieldValue::Int(2))]));

    let page = pb.build();

    // outside page range
    let res = page.get("a", 100);
    assert_eq!(res, PageLookupResult::NotFound);
}


// io tests
#[test]
fn page_write_and_read_roundtrip() {
    let dir = tempdir().unwrap();
    let page_path = dir.path().join("page_1.pg");

    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs("a", 1, vec![("v", FieldValue::Int(1))]));
    pb.add(Record::from_pairs("b", 2, vec![("v", FieldValue::Int(2))]));

    let page = pb.build();

    write_page(&page_path, &page).unwrap();
    let loaded = read_page_from_disk(&page_path).unwrap();

    assert_eq!(loaded.header, page.header);
    assert_eq!(loaded.records.len(), 2);
    assert_eq!(loaded.records[0].id, "a");
    assert_eq!(loaded.records[1].id, "b");
}

#[test]
fn page_is_immutable() {
    let dir = tempdir().unwrap();
    let page_path = dir.path().join("page.pg");

    let mut pb = PageBuilder::new();
    pb.add(Record::from_pairs("x", 1, vec![("v", FieldValue::Int(1))]));
    let page = pb.build();

    write_page(&page_path, &page).unwrap();

    // Writing again should fail
    let result = write_page(&page_path, &page);
    assert!(result.is_err());
}