use super::header::PageHeader;
use super::builder::*;
use crate::storage::record::{Record, FieldValue};

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