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
  pub replaying: bool,
  pub pagecapacity: usize
}

impl Engine {
  pub fn new(wal_path: &str) -> Self {
    let wal = WriteAheadLog::new(wal_path);
    let cache = PageCache::new(64);
    let mut index = HashMap::new();
    let tables = util::list_tables().unwrap_or_default();
    for table in tables {
      let idx = HashIndex::load(&table).unwrap_or_else(
        |_| {
          println!("Rebuilding missing index for table '{}'", table);
          HashIndex::rebuild_index(&table).expect("Failed to rebuild index")
        }
      );
      index.insert(table, idx);
    }
    Self { wal, cache, index, replaying: false, pagecapacity: 4096 }
  }

  /// Inserts a new record into a given table.
  /// Logs the operation to WAL and updates the page + cache.
  pub fn insert_record(&mut self, table: &str, record: Record) -> Result<()> {
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
          (new_id, Page::new(new_id, self.pagecapacity))
        } else {
          (last.id, page)
        }
      } else {
        (1, Page::new(1, self.pagecapacity))
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
    let file_path = util::page_file(table, page_id);
    io::save_page_to_disk(&page, &file_path)?;

    // Update cache (ensure updated page is present)
    let key = format!("{}_page_{}", table, page_id);
    self.cache.put(&key, page.clone());

    // Update metadata
    meta.update_page(page_id, page.records.len() as u64);
    meta.save()?;

    // Update index and persist it
    let mut idx = self.index.entry(table.to_string()).or_insert_with(HashIndex::new).clone();
    idx.add_record(&record, page_id);
    idx.save(table)?;
    self.index.insert(table.to_string(), idx);

    Ok(())
  }

  /// Retrieves all records for a given table
  pub fn get_all(&mut self, table: &str) -> Result<Vec<Page>> {
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

  /// Retrieves filtered records using the provided filter.
  pub fn get(&mut self, table: &str, filter: Filter) -> Result<Vec<Page>> {
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

  /// Updates records matching a filter with the given patch map.
  pub fn update(&mut self, table: &str, filter: Filter, patch: BTreeMap<String, FieldValue>) -> Result<usize> {
    let mut meta = TableMeta::load(table)?;
    let mut updated = 0;
    for p in meta.pages.clone() {
      let file_path = util::page_file(table, p.id);
      let mut page = io::load_page_from_disk(&file_path).expect("Unable to load Page from Disk");
      for record in &mut page.records {
        if record.matches(&filter) {
          record.apply_patch(&patch);
          updated += 1;

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
      }
      // persist page
      io::save_page_to_disk(&page, &file_path).expect("Saving to disk failed");
      // update cache
      self.cache.invalidate(&format!("{}_page_{}", table, p.id));
      self.cache.put(&format!("{}_page_{}", table, p.id), page.clone());
      // update metadata for this page entry
      meta.update_page(p.id, page.records.len() as u64);
    }

    meta.save()?;

    // Rebuild index and persist it
    let new_index = HashIndex::rebuild_index(table)?;
    new_index.save(table)?;
    self.index.insert(table.to_string(), new_index);

    Ok(updated)
  }

  /// Deletes records matching the given filter.
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

      // invalidate cache and refresh if page still exists
      let key = format!("{}_page_{}", table, p.id);
      self.cache.invalidate(&key);
      if page.records.is_empty() {
        // remove page file if empty
        if std::path::Path::new(&file_path).exists() {
          std::fs::remove_file(&file_path)?;
        }
      } else {
        // if still has records, update cache
        self.cache.put(&key, page.clone());
      }
    }

    // apply metadata updates
    for (pid, len) in updates {
      if len > 0 {
        meta.update_page(pid, len);
      } else {
        meta.remove_page(pid);
      }
    }
    meta.save()?;

    // Rebuild index and persist it
    let new_index = HashIndex::rebuild_index(table)?;
    new_index.save(table)?;
    self.index.insert(table.to_string(), new_index);

    Ok(total_deleted)
  }

  /// Replays WAL entries at startup to recover from crashes.
  pub fn replay_wal_at_startup(&mut self) -> Result<()> {
    // Clear cache and mark replaying
    self.clear_cache();
    self.replaying = true;

    // Recover WAL entries
    let entries = WriteAheadLog::recover("wal.log");
    let mut buffer :HashMap<String, Vec<Page>> = HashMap::new();
    let table_vector = util::list_tables()?;
    if table_vector.len() > 0 {
      for table in table_vector {
        let pageid_vector = util::list_pages(&table)?;
        if pageid_vector.len() > 0 {
          let mut page_vector :Vec<Page> = Vec::new();
          for page_number in pageid_vector {
            let file_path = util::page_file(&table, page_number);
            let page = io::load_page_from_disk(&file_path)?;
            page_vector.push(page);
          }
          buffer.insert(table, page_vector);
        }
      }
    };

    // Apply WAL entries into the in-memory buffer (idempotent)
    for entry in entries {
      match entry.operation.as_str() {
        "INSERT" => {
          let record: Record = bincode::deserialize(&entry.data)?;
          if buffer.contains_key(&entry.table) {
            if let Some(pages) = buffer.get_mut(&entry.table) {
              // avoid duplicate inserts by checking existing ids
              if util::pages_contain_record(pages, record.id) {
                continue;
              }

              if let Some(last_page) = pages.last_mut() {
                if last_page.is_full() {
                  let mut new_page = Page::new(last_page.id + 1, self.pagecapacity);
                  new_page.insert(record).expect("Insertion failed to new record while replay");
                  pages.push(new_page);
                } else {
                  last_page.insert(record).expect("Insertion failed to existing file while replay");
                }
              } else {
                // no pages present (shouldn't happen if contains_key), create first page
                let mut new_page = Page::new(1, self.pagecapacity);
                new_page.insert(record).expect("Insertion failed to new record while replay");
                pages.push(new_page);
              }
            }
          } else {
            let mut new_page = Page::new(1, self.pagecapacity);
            new_page.insert(record).expect("Insertion failed to new record while replay");
            let pages = vec![new_page];
            buffer.insert(entry.table.clone(), pages);
          }
        }
        "UPDATE" => {
          let record: Record = bincode::deserialize(&entry.data)?;
          if let Some(pages) = buffer.get_mut(&entry.table) {
            for page in pages.iter_mut() {
              for oldrecord in page.records.iter_mut() {
                if oldrecord.id == record.id {
                  // replace the record entirely with the replayed record (idempotent)
                  *oldrecord = record.clone();
                }
              }
            }
          }
        }
        "DELETE" => {
          let ids: Vec<u64> = bincode::deserialize(&entry.data)?;
          if let Some(pages) = buffer.get_mut(&entry.table) {
            for page in pages.iter_mut() {
              page.records.retain(|r| !ids.contains(&r.id));
            }
          }
        }
        _ => {}
      }
    }

    // Persist buffer to disk (overwrite pages)
    for (table, pages) in buffer {
      fs::create_dir_all(format!("data/{}", table))?;
      for page in pages {
        let file_path = util::page_file(&table, page.id);
        // if page became empty, remove file; otherwise save
        if page.records.is_empty() {
          if std::path::Path::new(&file_path).exists() {
            std::fs::remove_file(&file_path)?;
          }
        } else {
          io::save_page_to_disk(&page, &file_path).expect("Unable to save page");
        }
      }
    }

    // Resume normal behaviour
    self.replaying = false;

    // Rebuild all indexes after replay and persist them
    for table in util::list_tables()? {
      let idx = HashIndex::rebuild_index(&table)?;
      idx.save(&table)?;
      self.index.insert(table.clone(), idx);
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

  /// Run a quick integrity check:
  /// - Rebuilds indexes for all tables and persists them
  /// - Recomputes metadata from on-disk pages
  /// - Reports empty pages/tables
  pub fn integrity_check(&mut self) -> Result<()> {
    let tables = util::list_tables()?;
    for table in tables {
      println!("Integrity: checking table '{}'", table);
      // Rebuild and persist index
      let idx = HashIndex::rebuild_index(&table)?;
      idx.save(&table)?;
      self.index.insert(table.clone(), idx);

      // Rebuild metadata from on-disk pages
      let mut meta = TableMeta { table_name: table.clone(), pages: vec![] };
      for pid in util::list_pages(&table)? {
        let page_path = util::page_file(&table, pid);
        if !std::path::Path::new(&page_path).exists() {
          println!("  - Missing page file for page {}", pid);
          continue;
        }
        let page = io::load_page_from_disk(&page_path)?;
        if page.records.is_empty() {
          println!("  - Empty page found: {}", pid);
          // remove empty page
          std::fs::remove_file(&page_path)?;
          continue;
        }
        meta.update_page(pid, page.records.len() as u64);
      }
      meta.save()?;
    }
    Ok(())
  }
}
