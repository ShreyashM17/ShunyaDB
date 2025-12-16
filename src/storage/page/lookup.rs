use crate::storage::page::builder::Page;
use crate::storage::record::Record;

/// Result of a page lookup operation.
#[derive(Debug, PartialEq)]
pub enum PageLookupResult<'page> {
  Found(&'page Record),
  NotFound,
  NotVisible, // exists but seqno > snapshot
}

impl Page {
  /// Lookup a record by id, respecting snapshot seqno
  pub fn get(&self, id: &str, snapshot_seqno: u64) -> PageLookupResult<'_> {
    // Fast prune using page metadata
    if id < self.header.min_id.as_str() || id > self.header.max_id.as_str() {
      return PageLookupResult::NotFound;
    }

    // Binary search for the record
    match self.records.binary_search_by(|r| r.id.as_str().cmp(id)) {
      Ok(idx) => {
        let record = &self.records[idx];

        if record.seqno <= snapshot_seqno {
          PageLookupResult::Found(record)
        } else {
          PageLookupResult::NotVisible
        }
      }
      Err(_) => PageLookupResult::NotFound,
    }
  }
}