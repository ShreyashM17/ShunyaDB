use serde::{Serialize, Deserialize};
use std::fs::{OpenOptions};
use std::io::{Read, Write};

#[derive(Serialize, Deserialize, Debug)]
pub struct WalEntry {
  pub operation: String, //"INSERT", "UPDATE", "DELETE"
  pub table: String,
  pub record_id: u64,
  pub data: Vec<u8>, // Serialized record
}

pub struct WriteAheadLog {
  file_path: String,
  file: std::fs::File,
}

impl WriteAheadLog {
  pub fn new(path: &str) -> Self {
    let file = OpenOptions::new().create(true).append(true).read(true).open(path).expect("Failed to open WAL file");
    Self { file_path: path.to_string(), file }
  }

  pub fn log(&mut self, entry: &WalEntry) {
    let bytes = bincode::serialize(entry).expect("wal serialize failed");
    let len = bytes.len() as u64;
    self.file.write_all(&len.to_le_bytes()).unwrap();
    self.file.write_all(&bytes).unwrap();
    self.file.flush().unwrap();
  }

  pub fn recover(path: &str) -> Vec<WalEntry> {
    let mut file = OpenOptions::new().read(true).open(path).unwrap();
    let mut entries = Vec::new();
    loop {
      let mut size_buf = [0u8; 8];
      if file.read_exact(&mut size_buf).is_err() {
        break;
      }
      let size = u64::from_le_bytes(size_buf);
      let mut data = vec![0u8; size as usize];
      if file.read_exact(&mut data).is_err() {
        break;
      }
      let entry: WalEntry = bincode::deserialize(&data).expect("wal deserialize failed");
      entries.push(entry);
    }
    entries
  }

  pub fn truncate(&self) {
    let _ = std::fs::write(&self.file_path, b"");
  }
}