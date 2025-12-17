use anyhow::Result;
use crate::engine::seqno;
use crate::storage::memtable::{self, MemTable};
use crate::storage::record::{FieldValue, Record};
use crate::storage::flush::flush_memtable;
use crate::storage::wal::{Wal, WalEntry, WalOp};

pub struct Engine {
  wal: Wal,
  memtable: MemTable,
  data_dir: std::path::PathBuf,
}

impl Engine {
  pub fn open(data_dir: impl Into<std::path::PathBuf>) -> Result<Self> {
    let data_dir = data_dir.into();
    let wal_path = data_dir.join("wal.log");
    let wal = Wal::open(&wal_path)?;
    let memtable = MemTable::new();

    Ok(Self { wal, memtable, data_dir })
  }

  pub fn put(&mut self, table: String , id: String, fields: Vec<(String, FieldValue)>) -> Result<()> {
    let seqno = seqno::allocate();
    let record = Record::from_pairs(id, seqno, fields);
    let entry = WalEntry::new(WalOp::Insert, table, record.id.clone(), seqno, Some(record.clone()));
    self.wal.append(&entry)?;

    self.memtable.put(record);
    Ok(())
  }

  pub fn delete(&mut self, table: String, id: String) -> Result<()> {
    let seqno = seqno::allocate();
    let record = Record::new_tombstone(id, seqno);
    
    let entry = WalEntry::new(WalOp::Delete, table, record.id.clone(), seqno, Some(record.clone()));
    self.wal.append(&entry)?;

    self.memtable.put(record);
    Ok(())
  }

  pub fn get(&self, table: String, id: &str) -> Option<&Record> {
    let snapshot_seqno = seqno::current();

    if let Some(rec) = self.memtable.get(id, snapshot_seqno) {
      if rec.is_tombstone() {
        return None;
      } else {
        return Some(rec);
      }
    }

    None
  }


  pub fn update(&mut self, table: String, id: String, new_data: Vec<(String, FieldValue)>) -> Result<()> {
    let seqno = seqno::allocate();

    let record = Record::from_pairs(id, seqno, new_data);

    let wal_entry = WalEntry::new(WalOp::Update, table, record.id.clone(), seqno, Some(record.clone()));
    self.wal.append(&wal_entry)?;

    self.memtable.put(record);
    Ok(())
  }
}