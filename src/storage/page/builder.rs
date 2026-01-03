use crate::storage::page::header::PageHeader;
use crate::storage::page::{header};
use crate::storage::record::Record;


// In-memory page representation
#[derive(Debug)]
pub struct Page {
  pub header: PageHeader,
  pub records: Vec<Record>,
  pub payload: Vec<u8>, // Serialized records
}

/// Builder for creating immutable pages
pub struct PageBuilder {
  records: Vec<Record>,
  current_size_bytes: usize
}

impl PageBuilder {
  pub fn new() -> Self {
    Self {
      records: Vec::new(),
      current_size_bytes: 100, // initial header size estimate (bytes)
    }
  }

  /// Add a record to the page builder
  pub fn add(&mut self, record: Record) {
    self.records.push(record);
  }

  /// Build an immutable page
  pub fn build(mut self) -> Page {
    assert!(!self.records.is_empty(), "cannot build empty page");

    self.records.sort_by(|a, b| a.id.cmp(&b.id));

    let min_id = self.records.first().unwrap().id.clone();
    let max_id = self.records.last().unwrap().id.clone();

    let num_records = self.records.len() as u32;
    let page_seqno = self.records.iter().map(|r| r.seqno).max().unwrap();

    let payload = bincode::serialize(&self.records).expect("record serialization failed");

    let mut header = header::PageHeader::new(
      min_id,
      max_id,
      num_records,
      page_seqno,
    );

    header.checksum = header::PageHeader::compute_checksum(&payload);

    Page {
      header,
      records: self.records,
      payload,
    }
  }

  pub fn estimate_size(&self, record: &Record) -> usize {
    let record_size = bincode::serialized_size(record).unwrap() as usize;
    record_size
  }

  pub fn estimate_size_with(&self, record: &Record) -> usize {
    self.current_size_bytes + self.estimate_size(record)
  }

  pub fn update_size(&mut self, record: &Record) {
    self.current_size_bytes = self.estimate_size_with(record);
  }
}