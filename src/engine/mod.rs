pub mod filter;
use std::collections::BTreeMap;
use crate::engine::filter::Filter;
use crate::storage::io;
use crate::storage::page::Page;
use crate::storage::cache::PageCache;
use crate::storage::record::{FieldValue, Record};
use crate::storage::wal::{WalEntry, WriteAheadLog};
use std::fs;
use crate::util;
use anyhow::Result;

pub struct Engine {
  wal: WriteAheadLog,
  cache: PageCache,
}

impl Engine {
  pub fn new(wal_path: &str) -> Self {
    let wal = WriteAheadLog::new(wal_path);
    let cache = PageCache::new(8);
    Self { wal, cache }
  }

  pub fn insert_record(&mut self, table: &str, record: Record) -> std::io::Result<()> {
    // Create data folder for the table if not exists
    fs::create_dir_all(format!("data/{}", table))?;

    // Load or create first page
    let file_path = util::page_file(table, 1);
    let mut page = if std::path::Path::new(&file_path).exists() {
      io::load_page_from_disk(&file_path)?
    } else {
      Page::new(1, 4096)
    };

    let entry = WalEntry {
      operation: "INSERT".to_string(),
      table: table.to_string(),
      record_id: record.id,
      data: bincode::serialize(&page).unwrap(),
    };

    self.wal.log(&entry);
    self.cache.put(table, page.clone());
    page.insert(record).expect("Page insertion failed");
    io::save_page_to_disk(&page, &file_path)?;

    Ok(())
  }

  pub fn get(&mut self, table: &str) -> Page {
    let file_path = util::page_file(&table, 1);
    let page = if let Some(p) = self.cache.get(&table) {
      p
    } else {
      let p = io::load_page_from_disk(&file_path).expect("No Data Found");
      self.cache.put(table, p.clone());
      p
    };
    page
  }

  pub fn update(&mut self, table: &str, filter: Filter, patch: BTreeMap<String, FieldValue>) -> Result<usize> {
    let file_path = util::page_file(table, 1);
    let mut page = io::load_page_from_disk(&file_path).expect("Unable to load Page from Disk");
    let mut updated = 0;
    for record in &mut page.records {
      if record.matches(&filter) {
        record.apply_patch(&patch);
        updated += 1;
      }
      let entry = WalEntry {
        operation: "UPDATE".into(),
        table: table.into(),
        record_id: record.id,
        data: bincode::serialize(&record).expect("Unable to log data"),
      };
      self.wal.log(&entry);
    }
    self.cache.put(table, page.clone());
    io::save_page_to_disk(&page, &file_path).expect("Saving to disk failed");
    Ok(updated)
  }

  pub fn delete(&mut self, table: &str, filter: Filter) -> Result<usize> {
    let file_path = util::page_file(table, 1);
    let mut page = io::load_page_from_disk(&file_path)?;

    let before = page.get_all().len();
    page.records.retain(|r| !r.matches(&filter));
    let deleted = before - page.get_all().len();
    if deleted > 0 {
      let entry = WalEntry {
        operation: "DELETE".into(),
        table: table.into(),
        record_id: 0,
        data: vec![],
      };
      self.wal.log(&entry);
        io::save_page_to_disk(&page, &file_path)?;
      }
      self.cache.invalidate(table);
    Ok(deleted)
  }

  pub fn replay_wal_at_startup(&mut self) -> Result<()> {
    let entries = WriteAheadLog::recover("wal.log");
    for entry in entries {
      match entry.operation.as_str() {
        "INSERT" | "UPDATE" => {
          let record: Record = bincode::deserialize(&entry.data)?;
            self.insert_record(&entry.table, record)?;
        }
        "DELETE" => {
          // optional for now
        }
        _ => {}
      }
    }
    Ok(())
  }

  pub fn truncate_wal(&mut self) {
    WriteAheadLog::truncate(&self.wal);
  }
}