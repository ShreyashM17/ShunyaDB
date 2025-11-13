use std::collections::BTreeMap;
use std::fs;
use crate::storage::record::Record;
use crate::storage::{io, page::Page};
use crate::util;
use anyhow::Result;

#[derive(Debug)]
pub struct MemTable {
  pub data: BTreeMap<u64, Record>,
  pub capacity: usize,
}

impl MemTable {
  pub fn new(capacity: usize) -> Self {
    Self { data: BTreeMap::new(), capacity}
  }

  pub fn insert(&mut self, record: Record) {
    self.data.insert(record.id, record);
  }

  pub fn is_full(&self) -> bool {
    self.data.len() >= self.capacity
  }

  /// Flush MemTable to Disk Page
  pub fn flush_to_page(&mut self, table: &str, page_id: u64, capacity: usize) -> Result<Vec<(u64,Page)>> {
    let records: Vec<Record> = self.data.values().cloned().collect();
    let mut file_path = util::page_file(table, page_id);
    let mut page = if fs::exists(&file_path)? {
      io::load_page_from_disk(&file_path)?
    } else {
      Page::new(page_id, capacity)
    };
    let mut saved: Vec<(u64, Page)> = Vec::new();
    let mut page_id = page_id;
    if page.is_full() {
      page_id = page_id + 1;
      page = Page::new(page_id, capacity);
      page.from_records(records).expect("Page building failed");
      file_path = util::page_file(table, page_id);
      io::save_page_to_disk(&page, &file_path)?;
      self.clear();
      saved.push((page_id, page));
      return Ok(saved);
    } 

    for record in records {
      page.insert(record).expect("Unable to insert record on Page");
      if page.is_full() {
        file_path = util::page_file(table, page_id);
        io::save_page_to_disk(&page, &file_path)?;
        saved.push((page_id, page.clone()));
        page_id = page_id + 1;
        page = Page::new(page_id, capacity);
      }
    }
    
    self.clear();
    Ok(saved)
  }

  pub fn clear(&mut self) {
    self.data.clear();
  }
}