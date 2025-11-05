pub mod filter;
use std::collections::BTreeMap;
use crate::engine::filter::Filter;
use crate::storage::{io, 
  page::Page, 
  cache::PageCache, 
  record::{FieldValue, Record}, 
  wal::{WalEntry, WriteAheadLog}};
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
    let cache = PageCache::new(64);
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
      operation: "INSERT".into(),
      table: table.into(),
      record_id: record.id,
      data: bincode::serialize(&record).expect("Unable to log data"),
    };

    self.wal.log(&entry);
    let key = format!("{}_page_{}", table, page.id);
    self.cache.put(&key, page.clone());
    page.insert(record).expect("Page insertion failed");
    io::save_page_to_disk(&page, &file_path)?;

    Ok(())
  }

  pub fn get_all(&mut self, table: &str) -> Page {
    let file_path = util::page_file(&table, 1);
    let key = format!("{}_page_1", table);
    let page = if let Some(p) = self.cache.get(&key) {
      p
    } else {
      let p = io::load_page_from_disk(&file_path).expect("No Data Found");
      let key = format!("{}_page_{}", table, p.id);
      self.cache.put(&key, p.clone());
      p
    };
    page
  }

  pub fn get(&mut self, table: &str, filter: Filter) -> Page {
    let file_path = util::page_file(table, 1);
    let key = format!("{}_page_1", table);
    let mut page = if let Some(p) = self.cache.get(&key) {
      p
    } else {
      let p = io::load_page_from_disk(&file_path).expect("No Data Found");
      let key = format!("{}_page_{}", table, p.id);
      self.cache.put(&key, p.clone());
      p
    };
    page.records.retain(|r| r.matches(&filter));
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
    let key = format!("{}_page_{}", table, page.id);
    self.cache.put(&key, page.clone());
    io::save_page_to_disk(&page, &file_path).expect("Saving to disk failed");
    Ok(updated)
  }

  pub fn delete(&mut self, table: &str, filter: Filter) -> Result<usize> {
    let file_path = util::page_file(table, 1);
    let mut page = io::load_page_from_disk(&file_path)?;

    let before = page.get_all().len();
    let ids_to_delete: Vec<u64> = page.records.iter().filter(|r| r.matches(&filter)).map(|r| r.id).collect();
    page.records.retain(|r| !ids_to_delete.contains(&r.id));
    let deleted = before - page.get_all().len();
    if deleted > 0 {
      let delete_payload = bincode::serialize(&ids_to_delete).map_err(|e| anyhow::anyhow!("Failed to serialize delete ids: {:?}", e))?;
      let entry = WalEntry {
        operation: "DELETE".into(),
        table: table.into(),
        record_id: 0,
        data: delete_payload,
      };
      self.wal.log(&entry);
      io::save_page_to_disk(&page, &file_path)?;
      }
      let key = format!("{}_page_{}", table, page.id);
      self.cache.invalidate(&key);
    Ok(deleted)
  }

  pub fn replay_wal_at_startup(&mut self) -> Result<()> {
    let entries = WriteAheadLog::recover("wal.log");
    for entry in entries {
      match entry.operation.as_str() {
        "INSERT" => {
          let record: Record = bincode::deserialize(&entry.data)?;
          self.insert_record(&entry.table, record)?;
        }
        "UPDATE" => {
          let record: Record = bincode::deserialize(&entry.data)?;
          self.update(&entry.table, Filter::ById(record.id),record.data)?;
        }
        "DELETE" => {
          let ids: Vec<u64> = bincode::deserialize(&entry.data)?;
          let file_path = util::page_file(&entry.table, 1);
          let mut page = io::load_page_from_disk(&file_path).unwrap_or_else(|_| Page::new(1, 4096));
          page.records.retain(|r| !ids.contains(&r.id));
          io::save_page_to_disk(&page, &file_path)?;
        }
        _ => {}
      }
    }
    Ok(())
  }

  pub fn truncate_wal(&mut self) {
    WriteAheadLog::truncate(&self.wal);
  }

  pub fn clear_cache(&self) {
    self.cache.clear_cache();
  }
}