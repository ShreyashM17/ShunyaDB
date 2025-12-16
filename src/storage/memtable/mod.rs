use std::collections::BTreeMap;
use crate::storage::record::Record;

#[derive(Debug)]
pub struct MemTable {
  pub data: BTreeMap<String, Record>,
}

impl MemTable {
  pub fn new() -> Self {
    Self {
      data: BTreeMap::new(),
    }
  }

  pub fn put(&mut self, record: Record) {
    self.data.insert(record.id.clone(), record);
  }

  pub fn delete(&mut self, id: String, seqno: u64) {
    let tombstone = Record::new_tombstone(id.clone(), seqno);
    self.data.insert(id, tombstone);
  }

  pub fn get(&self, id: &str, snapshot_seqno: u64) -> Option<&Record> {
    match self.data.get(id) {
      Some(rec) if rec.seqno <= snapshot_seqno => Some(rec),
      _ => None,
    }
  }

  pub fn len(&self) -> usize {
    self.data.len()
  }

  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }

  pub fn iter(&self) -> impl Iterator<Item = &Record> {
    self.data.values()
  }
}

#[cfg(test)]
mod tests;