pub mod replay;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::storage::record::Record;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalOp {
  Insert,
  Update,
  Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalEntry {
  pub seqno: u64,
  pub op: WalOp,
  pub table: String,
  pub record_id: String,
  pub record: Option<Record>, // Delete -> None
}

impl WalEntry {
  pub fn new(op: WalOp, table: impl Into<String>, record_id: impl Into<String>, seqno: u64, record: Option<Record>) -> Self {
    WalEntry {
      seqno,
      op,
      table: table.into(),
      record_id: record_id.into(),
      record,
    }
  }
}

pub struct Wal {
  file: File,
  path: String,
}

impl Wal {
  pub fn open(path: impl AsRef<Path>) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .append(true)
      .create(true)
      .open(&path)
      .with_context(|| format!("failed to open WAL file"))?;
    Ok(Wal { file, path: path.as_ref().to_string_lossy().to_string() })
  }

  /// Append a WAL entry to the log. as [len][payload][len]
  pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
    let payload = bincode::serialize(entry)?;

    let len = payload.len() as u64;
    let len_bytes = len.to_le_bytes();

    self.file.write_all(&len_bytes)?;
    self.file.write_all(&payload)?;
    self.file.write_all(&len_bytes)?;

    // durability gurantee
    self.file.flush()?;
    self.file.sync_all()?;

    Ok(())
  }

  /// Replay all WAL entries in order
  pub fn read_all(&mut self) -> Result<Vec<WalEntry>> {
    let mut entries = Vec::new();
    self.file.seek(SeekFrom::Start(0))?;
    loop {
      let mut len_buf = [0u8; 8];
      let n = self.file.read(&mut len_buf)?;
      if n == 0 {
        break; // EOF
      }
      if n < 8 {
        // corrupted/truncated WAL
        break;
      }
      let len = u64::from_le_bytes(len_buf);

      // read payload
      let mut payload = vec![0u8; len as usize];
      let n = self.file.read(&mut payload)?;
      if n < len as usize {
        // corrupted/truncated WAL
        break;
      }

      // read trailing len
      let mut len_buf2 = [0u8; 8];
      let n = self.file.read(&mut len_buf2)?;
      if n < 8 {
        break;
      }
      let len2 = u64::from_le_bytes(len_buf2);
      if len != len2 {
        break; // corrupted WAL
      }

      // decode entry
      let entry: WalEntry = bincode::deserialize(&payload)?;
      entries.push(entry);
    }
    Ok(entries)
  }

  pub fn rewrite_to(&mut self, checkpoint: u64) -> Result<()> {
    let wal_path = Path::new(&self.path);
    let tmp_file = wal_path.with_extension("rewrite_wal");

    let mut old = File::open(wal_path)?;

    let mut new = OpenOptions::new()
                      .create(true)
                      .truncate(true)
                      .write(true)
                      .open(&tmp_file)?;
    
    loop {
      // Read length
      let mut len_buf = [0u8; 8];
      if old.read_exact(&mut len_buf).is_err() {
        break;
      }

      let len = u64::from_le_bytes(len_buf) as usize;

      // Read payload
      let mut payload = vec![0u8; len];
      old.read_exact(&mut payload)?;


      // Read trailing length
      let mut len2_buf = [0u8; 8];
      old.read_exact(&mut len2_buf)?;

      let len_2 = u64::from_le_bytes(len2_buf) as usize;

      if len != len_2 {
        anyhow::bail!("Wal Corruption: Length mismatch")
      }

      // Deserialize entry
      let entry: WalEntry = bincode::deserialize(&payload)?;

      if entry.seqno > checkpoint {
        new.write_all(&len_buf)?;
        new.write_all(&payload)?;
        new.write_all(&len2_buf)?;
      }
    }

    new.sync_all()?;
    drop(new);

    // Atomically replace old Wal
    std::fs::rename(&tmp_file, wal_path)?;

    #[cfg(unix)]
    {
      if let Some(dir) = wal_path.parent() {
        File::open(dir)?.sync_all()?;
      }
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests;