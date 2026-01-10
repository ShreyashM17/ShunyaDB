use std::collections::BTreeMap;
use crate::storage::record::{FieldValue, Record};

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

  pub fn approx_size_bytes(&self) -> usize {
    let mut size = 0;

    for (key, versions) in &self.data {
      // Key size
      size += key.len();

      // Value Size
      size += std::mem::size_of::<Vec<Record>>();

      for record in versions {
        size += std::mem::size_of::<Record>();

        size += record.id.len();

        for (field, value) in &record.data {
          size += field.len();

          size += match value {
            FieldValue::Str(s) => s.len(),
            FieldValue::Int(_) => std::mem::size_of::<i64>(),
            FieldValue::Bool(_) => std::mem::size_of::<bool>(),
            FieldValue::Float(_) => std::mem::size_of::<f64>(),
            FieldValue::Null => 0,
            FieldValue::UInt(_) => std::mem::size_of::<u64>(),
          };
        }
      }
    }
    size
  }

}

#[cfg(test)]
mod tests;