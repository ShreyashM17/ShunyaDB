use serde::{Serialize, Deserialize};
use anyhow::Result;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageInfo {
    pub page_id: u64,
    pub file_name: String,
    pub min_id: String,
    pub max_id: String,
    pub num_records: usize,
    pub max_seqno: u64,
}

impl PageInfo {
    pub fn new(
        page_id: u64,
        min_id: String,
        max_id: String,
        num_records: usize,
        max_seqno: u64,
    ) -> Self {
        Self {
            page_id,
            file_name: format!("page_{}.db", page_id),
            min_id,
            max_id,
            num_records,
            max_seqno,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableMeta {
    pub pages: Vec<PageInfo>,
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

    pub fn add_pages(&mut self, new_pages: Vec<PageInfo>) {
        self.pages.extend(new_pages);
    }
}
