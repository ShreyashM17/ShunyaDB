pub mod filter;
pub mod index;

use std::collections::{BTreeMap, HashMap};
use crate::engine::{filter::Filter, index::HashIndex};
use crate::storage::{io, 
  page::Page, 
  cache::PageCache, 
  record::{FieldValue, Record}, 
  wal::{WalEntry, WriteAheadLog}};
use std::fs;
use crate::util;
use anyhow::Result;

pub struct Engine {
  pub wal: WriteAheadLog,
  pub cache: PageCache,
  pub index: HashMap<String, HashIndex>
}

impl Engine {
  pub fn new(wal_path: &str) -> Self {
    let wal = WriteAheadLog::new(wal_path);
    let cache = PageCache::new(64);
    let mut index = HashMap::new();
    let tables = util::list_tables().unwrap();
    for table in tables {
      let idx = HashIndex::load(&table).unwrap_or_else(
        |_| {
          println!("Rebuilding missing index for table '{}'", table);
          HashIndex::rebuild_index(&table).expect("Failed to rebuild index")
        }
      );
      index.insert(table, idx);
    }
    Self { wal, cache, index }
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

    // Log to WAL
    let entry = WalEntry {
      operation: "INSERT".into(),
      table: table.into(),
      record_id: record.id,
      data: bincode::serialize(&record).expect("Unable to log data"),
    };

    self.wal.log(&entry);

    // Insert to disk
    let key = format!("{}_page_{}", table, page.id);
    page.insert(record.clone()).expect("Page insertion failed");
    io::save_page_to_disk(&page, &file_path)?;
    
    // Update cache
    self.cache.put(&key, page.clone());

    // Update index
    self.index.entry(table.to_string()).or_insert_with(HashIndex::new).add_record(&record, page.id);
    self.index[table].save(table)?;

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
      self.cache.put(&key, p.clone());
      p
    };
    if let Filter::ByKeyValueEq(ref field, ref value) = filter {
      // Only attempt index lookup for Text fields
      let val_as_str = match value {
        FieldValue::Text(v) => v.clone(),
        FieldValue::Int(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::Bool(b) => b.to_string(),
      };
      if !val_as_str.is_empty() {  
        if let Some(index) = self.index.get(table) {
          if let Some(id_pairs) = index.lookup(field, &val_as_str) {
            let ids: Vec<u64> = id_pairs.iter().map(|(_, rid)| *rid).collect();
            page.records.retain(|r| ids.contains(&r.id));
            return page;
          }
        }
      }
    }

    //fallback
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

    // Rebuild index
    let new_index = HashIndex::rebuild_index(table)?;
    self.index.insert(table.to_string(), new_index);
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

    // Rebuild index
    let new_index = HashIndex::rebuild_index(table)?;
    self.index.insert(table.to_string(), new_index);
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

    // Rebuild all indexes after replay
    for table in util::list_tables()? {
      let idx = HashIndex::rebuild_index(&table)?;
      self.index.insert(table, idx);
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