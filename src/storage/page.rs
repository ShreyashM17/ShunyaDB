use crate::storage::record::Record;
use serde::{Deserialize, Serialize};

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
            capacity,
        }
    }

    pub fn insert(&mut self, record: Record) -> Result<(), &'static str> {
        if !self.is_full() {
            self.records.push(record);
            Ok(())
        } else {
            Err("Page full")
        }
    }

    pub fn is_full(&self) -> bool {
        if self.records.len() >= self.capacity {
            return true;
        }
        false
    }

    pub fn from_records(&mut self, records: Vec<Record>) -> Result<(), &'static str> {
        self.records = records;
        Ok(())
    }
}
