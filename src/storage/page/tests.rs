use super::header::PageHeader;

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