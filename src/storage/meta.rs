use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageMeta {
    pub id: u64,
    pub record_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub table_name: String,
    pub pages: Vec<PageMeta>,
}

impl TableMeta {
    pub fn load(table: &str) -> std::io::Result<Self> {
        let path = format!("data/{}/table.meta", table);
        if Path::new(&path).exists() {
            let file = fs::File::open(&path)?;
            Ok(bincode::deserialize_from(file).expect("Deserialization of meta data failed"))
        } else {
            Ok(Self {
                table_name: table.to_string(),
                pages: vec![],
            })
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        let path = format!("data/{}/table.meta", self.table_name);
        let file = fs::File::create(&path)?;
        bincode::serialize_into(file, self).expect("Serialization of meta data failed");
        Ok(())
    }

    pub fn update_page(&mut self, page_id: u64, new_len: u64) {
        if let Some(p) = self.pages.iter_mut().find(|p| p.id == page_id) {
            p.record_count = new_len;
        } else {
            self.pages.push(PageMeta {
                id: page_id,
                record_count: new_len,
            });
            self.pages.sort_by_key(|p| p.id);
        }
    }

    pub fn latest_page(&self) -> Option<&PageMeta> {
        self.pages.last()
    }

    pub fn remove_page(&mut self, page_id: u64) {
        self.pages.retain(|p| !p.id == page_id);
    }
}
