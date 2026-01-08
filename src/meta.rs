use serde::{Serialize, Deserialize};
use anyhow::Result;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageMeta {
    pub page_id: u64,
    pub file_name: String,
    pub min_id: String,
    pub max_id: String,
    pub number_of_records: usize,
    pub size_bytes: u64,
    pub max_seqno: u64,
}

impl PageMeta {
    pub fn new(
        page_id: u64,
        min_id: String,
        max_id: String,
        number_of_records: usize,
        size_bytes: u64,
        max_seqno: u64,
    ) -> Self {
        Self {
            page_id,
            file_name: format!("page_{}.db", page_id),
            min_id,
            max_id,
            number_of_records,
            size_bytes,
            max_seqno,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub version: u32,
    pub level: Vec<Vec<PageMeta>>,
    pub checkpoint_seqno: u64,
    pub current_page_id: u64,
}

impl Default for TableMeta {
    fn default() -> Self {
        Self {
            version: 1,
            level: vec![
                Vec::new(),
                Vec::new()
            ],
            checkpoint_seqno: 0,
            current_page_id: 0,
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
        self.level[0].extend(new_pages);
    }
}
