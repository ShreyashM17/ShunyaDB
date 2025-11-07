pub mod filter;
pub mod index;

use std::collections::{BTreeMap, HashMap};
use crate::engine::{filter::Filter, index::HashIndex};
use crate::storage::{
  io,
  meta::TableMeta, 
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
  pub index: HashMap<String, HashIndex>,
  pub replaying: bool
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
    Self { wal, cache, index, replaying: false }
  }

  pub fn insert_record(&mut self, table: &str, record: Record) -> std::io::Result<()> {
    // Create data folder for the table if not exists
    fs::create_dir_all(format!("data/{}", table))?;

    // load meta data
    let mut meta = TableMeta::load(table)?;

    // Load or create first page
    let (page_id, mut page) = if let Some(last) = meta.latest_page() {
        let file_path = util::page_file(table, last.id);
        let page = io::load_page_from_disk(&file_path)?;
        if page.is_full() {
          let new_id = last.id + 1;
          (new_id, Page::new(new_id, 4096))
        } else {
          (last.id, page)
        }
      } else {
        (1, Page::new(1, 4096))
      };

    // Log to WAL
    if !self.replaying {
      let entry = WalEntry {
        operation: "INSERT".into(),
        table: table.into(),
        record_id: record.id,
        data: bincode::serialize(&record).expect("Unable to log data"),
      };
      self.wal.log(&entry);
    }

    // Insert to disk
    page.insert(record.clone()).expect("Page insertion failed");
    io::save_page_to_disk(&page, &util::page_file(table, page_id))?;
    
    // Update cache
    let key = format!("{}_page_{}", table, page_id);
    self.cache.put(&key, page.clone());

    // Update metadata
    meta.update_page(page_id, page.records.len() as u64);
    meta.save()?;

    // Update index
    self.index.entry(table.to_string()).or_insert_with(HashIndex::new).add_record(&record, page_id);
    self.index[table].save(table)?;

    Ok(())
  }

  pub fn get_all(&mut self, table: &str) -> std::io::Result<Vec<Page>> {
    let meta = TableMeta::load(table)?;
    let mut pages = Vec::new();
    for page in &meta.pages {
      let file_path = util::page_file(&table, page.id);
      let key = format!("{}_page_{}", table, page.id);
      let page = if let Some(cached) = self.cache.get(&key) {
        cached
      } else {
        let loaded = io::load_page_from_disk(&file_path)?;
        self.cache.put(&key, loaded.clone());
        loaded
      };
      pages.push(page);
    }
    Ok(pages)
  }

  pub fn get(&mut self, table: &str, filter: Filter) -> std::io::Result<Vec<Page>> {
    let mut index_record_ids = Vec::new();
    let mut page_ids: Vec<u64> = Vec::new();
    let mut pages = Vec::new();
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
            index_record_ids = id_pairs.iter().map(|(_, rid)| *rid).collect();
            page_ids = id_pairs.iter().map(|(pid, _)| *pid).collect();
          }
        }
      }
    }
    if index_record_ids.len() > 0 && page_ids.len() > 0 {
      page_ids.dedup();
      for page_number in page_ids {
        let file_path = util::page_file(table, page_number);
        let key = format!("{}_page_{}", table, page_number);
        let mut page = if let Some(p) = self.cache.get(&key) {
          p
        } else {
          let p = io::load_page_from_disk(&file_path).expect("No Data Found");
          self.cache.put(&key, p.clone());
          p
        };
        page.records.retain(|r| index_record_ids.contains(&r.id));
        pages.push(page);
      }
    } else {
      let meta = TableMeta::load(table)?;
      for p in &meta.pages {
        let file_path = util::page_file(table, p.id);
        let key = format!("{}_page_{}", table, p.id);
        let mut page = if let Some(p) = self.cache.get(&key) {
          p
        } else {
          let p = io::load_page_from_disk(&file_path).expect("No Data Found");
          self.cache.put(&key, p.clone());
          p
        };
        page.records.retain(|r| r.matches(&filter));
        pages.push(page);
      }
    }
    Ok(pages)
  }

  pub fn update(&mut self, table: &str, filter: Filter, patch: BTreeMap<String, FieldValue>) -> Result<usize> {
    let meta = TableMeta::load(table)?;
    let mut updated = 0;
    for p in &meta.pages {
      let file_path = util::page_file(table, p.id);
      let mut page = io::load_page_from_disk(&file_path).expect("Unable to load Page from Disk");
      for record in &mut page.records {
        if record.matches(&filter) {
          record.apply_patch(&patch);
          updated += 1;
        }
        if !self.replaying {
          let entry = WalEntry {
            operation: "UPDATE".into(),
            table: table.into(),
            record_id: record.id,
            data: bincode::serialize(&record).expect("Unable to log data"),
          };
          self.wal.log(&entry);
        }
      }
      let key = format!("{}_page_{}", table, p.id);
      self.cache.put(&key, page.clone());
      io::save_page_to_disk(&page, &file_path).expect("Saving to disk failed");
    }

    // Rebuild index
    let new_index = HashIndex::rebuild_index(table)?;
    self.index.insert(table.to_string(), new_index);
    Ok(updated)
  }

  pub fn delete(&mut self, table: &str, filter: Filter) -> Result<usize> {
    let mut meta = TableMeta::load(table)?;
    let mut total_deleted = 0;
    let mut updates = Vec::new();
    for p in &meta.pages {
      let pid = p.id;
      let file_path = util::page_file(table, pid);
      let mut page = io::load_page_from_disk(&file_path)?;
      let before = page.records.len();
      let ids_to_delete: Vec<u64> = page.records
            .iter()
            .filter(|r| r.matches(&filter))
            .map(|r| r.id)
            .collect();

      page.records.retain(|r| !ids_to_delete.contains(&r.id));
      let deleted = before - page.records.len();
      total_deleted += deleted;

      if deleted > 0 {
        if !self.replaying {
          let entry = WalEntry {
            operation: "DELETE".into(),
            table: table.into(),
            record_id: 0,
            data: bincode::serialize(&ids_to_delete)?,
          };
          self.wal.log(&entry);
        }
        io::save_page_to_disk(&page, &file_path)?;
        updates.push((pid, page.records.len() as u64));
      }
      self.cache.invalidate(&format!("{}_page_{}", table, p.id));
    }

    for (pid, len) in updates {
      meta.update_page(pid, len);
    }
    meta.save()?;

    // Rebuild index
    let new_index = HashIndex::rebuild_index(table)?;
    self.index.insert(table.to_string(), new_index);
    Ok(total_deleted)
  }

  pub fn replay_wal_at_startup(&mut self) -> Result<()> {
    self.clear_cache();
    self.replaying = true;
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

    // Resume val behaviour
    self.replaying = false;

    // Rebuild all indexes after replay
    for table in util::list_tables()? {
      let idx = HashIndex::rebuild_index(&table)?;
      self.index.insert(table, idx);
    }

    // Rebuild metadata
    for table in util::list_tables()? {
      let mut meta = TableMeta { table_name: table.clone(), pages: vec![] };
      for pid in util::list_pages(&table)? {
        let page = io::load_page_from_disk(&util::page_file(&table, pid))?;
        meta.update_page(pid, page.records.len() as u64);
      }
      meta.save()?;
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