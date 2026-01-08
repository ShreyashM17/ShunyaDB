use crate::cache::lru::LruCache;
use crate::storage::memtable::MemTable;
use crate::storage::page::builder::Page;
use crate::storage::record::Record;
use crate::meta::TableMeta;
use crate::storage::page::io::read_page_from_disk;

use std::path::PathBuf;

pub struct Reader {
    data_dir: PathBuf,
}

impl Reader {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            data_dir: dir,
        }
    }

    pub fn get(
        &self,
        meta: &TableMeta,
        memtable: &MemTable,
        id: &str,
        snapshot: u64,
        page_cache: &mut LruCache<u64, Page>
    ) -> Option<Record> {
        // Memtable first
        if let Some(rec) = memtable.get(id, snapshot) {
            return Some(rec.clone());
        }

        // Immutable pages (newest → oldest)
        for pages_at_level in meta.level.iter() {
            for page_info in pages_at_level.iter().rev() {
                if id < page_info.min_id.as_str() || id > page_info.max_id.as_str() {
                    continue;
                }

                let page = if let Some(p) = page_cache.get(&page_info.page_id) {
                    p.clone()
                } else {
                    let path = self.data_dir.join(&page_info.file_name);
                    let p = read_page_from_disk(&path).ok()?;
                    page_cache.put(page_info.page_id, p.clone());
                    p
                };

                for rec in page.records.iter().rev() {
                    if rec.id == id && rec.seqno <= snapshot {
                        return if rec.is_tombstone {
                            None
                        } else {
                            Some(rec.clone())
                        };
                    }
                }
            }
        }

        None
    }
}
