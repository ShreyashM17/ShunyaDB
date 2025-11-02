use crate::storage::io;
use crate::storage::page::Page;
use crate::storage::record::Record;
use crate::storage::wal::{WalEntry, WriteAheadLog};
use std::fs;

pub struct Engine {
  wal: WriteAheadLog,
}

impl Engine {
  pub fn new(wal_path: &str) -> Self {
    let wal = WriteAheadLog::new(wal_path);
    Self { wal }
  }

  pub fn insert_record(&mut self, table: &str, record: Record) -> std::io::Result<()> {
    // Create data folder for the table if not exists
    fs::create_dir_all(format!("data/{}", table))?;

    // Load or create first page
    let file_path = format!("data/{}/page_1.bin", table);
    let mut page = if std::path::Path::new(&file_path).exists() {
      io::load_page_from_disk(&file_path)?
    } else {
      Page::new(1, 4)
    };

    let entry = WalEntry {
      operation: "INSERT".to_string(),
      table: table.to_string(),
      record_id: record.id,
      data: bincode::serialize(&page).unwrap(),
    };

    self.wal.log(&entry);

    page.insert(record).expect("Page insertion failed");
    io::save_page_to_disk(&page, &file_path)?;

    Ok(())
  }
}