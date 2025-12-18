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
