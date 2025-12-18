use std::collections::BTreeMap;
use crate::storage::record::Record;

#[derive(Debug)]
pub struct MemTable {
  pub data: BTreeMap<String, Vec<Record>>,
}

impl MemTable {
  pub fn new() -> Self {
    Self {
      data: BTreeMap::new(),
    }
  }

  pub fn put(&mut self, record: Record) {
    self.data.entry(record.id.clone()).or_default().push(record);
  }

  pub fn get(&self, id: &str, snapshot_seqno: u64) -> Option<&Record> {
    let versions = self.data.get(id)?;

    for record in versions.iter().rev() {
        if record.seqno <= snapshot_seqno {
            if record.is_tombstone {
                return None;
            } else {
                return Some(record);
            }
        }
    }

    None
  }

  pub fn len(&self) -> usize {
    self.data.len()
  }

  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }

  pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<Record>)> {
    self.data.iter()
  }

  pub fn clear(&mut self) {
    self.data.clear();
  }
}

#[cfg(test)]
mod tests;