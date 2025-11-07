use std::collections::BTreeMap;
use crate::storage::record::{FieldValue, Record};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
  pub id: u64,
  pub records: Vec<Record>,
  pub capacity: usize,
}

impl Page {
  pub fn new(id: u64, capacity: usize) -> Self {
    Self {
      id,
      records: Vec::new(),
      capacity
    }
  }

  pub fn insert(&mut self, record: Record) -> Result<(), &'static str> {
    if self.records.len() < self.capacity {
      self.records.push(record);
      Ok(())
    } else {
      Err("Page full")
    }
  }

  pub fn get_all(&self) -> &Vec<Record> {
    &self.records
  }

  pub fn is_full(&self) -> bool {
    if self.records.len() >= self.capacity {
      return true;
    }
    return false;
  }
  // For testing purpose
  pub fn generate_mock_record(&self, id: u64) -> Record {
    let mut data = BTreeMap::new();
    data.insert("Name".to_string(), FieldValue::Text("Shadow".to_string()));
    data.insert("Age".to_string(), FieldValue::Int(30));
    data.insert("Alive".to_string(), FieldValue::Bool(true));
    let record = Record::new(id, data);
    record
  }
}