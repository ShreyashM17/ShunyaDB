use anyhow::{Result, bail};
use crate::storage::wal::{Wal, WalEntry};

#[derive(Debug)]
pub struct ReplayResult {
  pub entries: Vec<WalEntry>,
  pub max_seqno: u64,
}

impl ReplayResult {
  pub fn new(entries: Vec<WalEntry>) -> Result<Self> {
    let mut max_seqno = 0;

    // Validate monotonic seqno order:
    for e in &entries {
      if e.seqno <= max_seqno {
        bail!(
          "WAL Replay Error: seqno order violated: current seqno {}, max seqno {}",
          e.seqno, max_seqno
        );
      }
      max_seqno = e.seqno;
    }
    Ok(Self { entries, max_seqno })
  }

  /// Full replay function used during DB startup
  pub fn replay_wal(wal: &mut Wal) -> Result<ReplayResult> {
    let entries = wal.read_all()?;
    ReplayResult::new(entries)
  } 
}

#[cfg(test)]
mod tests{
  use super::*;
use tempfile::tempdir;
use crate::storage::wal::{Wal, WalEntry, WalOp};
use crate::engine::seqno;

#[test]
fn replay_detects_seqno_ordering() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    let mut wal = Wal::open(&wal_path).unwrap();

    // Create correct order
    let seq1 = seqno::allocate();
    let e1 = WalEntry::new(WalOp::Insert, "tbl", "id1", seq1, None);
    wal.append(&e1).unwrap();

    let seq2 = seqno::allocate();
    let e2 = WalEntry::new(WalOp::Insert, "tbl", "id2", seq2, None);
    wal.append(&e2).unwrap();

    let mut wal = Wal::open(&wal_path).unwrap();
    let replay = ReplayResult::replay_wal(&mut wal).unwrap();

    assert_eq!(replay.entries.len(), 2);
    assert_eq!(replay.max_seqno, seq2);
}

#[test]
fn replay_rejects_out_of_order_seqno() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    // manually create WAL with bad seqno ordering
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&wal_path).unwrap();

        let bad = vec![
            // first entry: seqno 10
            bincode::serialize(&WalEntry {
                seqno: 10, op: WalOp::Insert, table: "x".into(), record_id: "1".into(), record: None
            }).unwrap(),
            // second entry: seqno 9 (invalid!)
            bincode::serialize(&WalEntry {
                seqno: 9, op: WalOp::Insert, table: "x".into(), record_id: "2".into(), record: None
            }).unwrap(),
        ];

        for p in bad {
            let len = p.len() as u64;
            f.write_all(&len.to_le_bytes()).unwrap();
            f.write_all(&p).unwrap();
            f.write_all(&len.to_le_bytes()).unwrap();
        }
    }

    let mut wal = Wal::open(&wal_path).unwrap();
    let replay = ReplayResult::replay_wal(&mut wal);

    assert!(replay.is_err()); // correctly rejects bad WAL order
}
}
