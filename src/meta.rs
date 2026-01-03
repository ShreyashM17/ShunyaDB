use serde::{Serialize, Deserialize};
use anyhow::Result;
use std::fs;
use std::path::Path;
use crate::lsm::level::PageMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub version: u32,
    pub pages: Vec<PageMeta>,
    pub checkpoint_seqno: u64,
}

impl Default for TableMeta {
    fn default() -> Self {
        Self {
            version: 1,
            pages: Vec::new(),
            checkpoint_seqno: 0,
        }
    }
}

impl TableMeta {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read(path)?;
        Ok(serde_json::from_slice(&data)?)
    }

    pub fn persist(&self, path: impl AsRef<Path>) -> Result<()> {
        let data = serde_json::to_vec_pretty(self)?;
        fs::write(path, data)?;
        Ok(())
    }

    pub fn add_pages(&mut self, new_pages: Vec<PageMeta>) {
        for p in &new_pages {
            self.checkpoint_seqno = self.checkpoint_seqno.max(p.max_seqno);
        }
        self.pages.extend(new_pages);
    }
}
