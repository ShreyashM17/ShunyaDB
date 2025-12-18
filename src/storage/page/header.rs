use serde::{Serialize, Deserialize};
use crc32fast::Hasher;

/// Magic number to identify ShunyaDB pages on disk
pub const PAGE_MAGIC: u32 = 0x53484442; // 'SHDB'

/// Current page format version
pub const PAGE_VERSION: u16 = 1;

/// Immutable page header.
/// Stored at the beginning of every page file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PageHeader {
    pub magic: u32,
    pub version: u16,
    pub checksum: u32,
    pub min_id: String,
    pub max_id: String,
    pub num_records: u32,
    pub page_seqno: u64,
}

impl PageHeader {
    /// Create a new page header (checksum filled later)
    pub fn new(
        min_id: String,
        max_id: String,
        num_records: u32,
        page_seqno: u64,
    ) -> Self {
        Self {
            magic: PAGE_MAGIC,
            version: PAGE_VERSION,
            checksum: 0, // computed after payload is known
            min_id,
            max_id,
            num_records,
            page_seqno,
        }
    }

    /// Compute checksum for a given payload
    pub fn compute_checksum(payload: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(payload);
        hasher.finalize()
    }

    /// Validate header invariants
    pub fn validate(&self) -> Result<(), String> {
        if self.magic != PAGE_MAGIC {
            return Err("Invalid page magic".into());
        }
        if self.version != PAGE_VERSION {
            return Err("Unsupported page version".into());
        }
        if self.min_id > self.max_id {
            return Err("min_id > max_id".into());
        }
        Ok(())
    }
}
