use anyhow::Result;
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use crate::storage::record::Record;
use crate::engine::reader::Reader;
use crate::engine::writer::Writer;
use crate::engine::recovery::recover;
use crate::storage::memtable::MemTable;
use crate::storage::record::FieldValue;
use crate::storage::wal::Wal;
use crate::meta::{TableMeta, PageMeta};
use crate::lsm::compaction_plan::plan_l0_to_l1;
use crate::lsm::compaction::execute_l0_to_l1;
use crate::storage::page::io::delete_older_pages;

pub struct Engine {
    memtable: MemTable,
    wal: Wal,
    reader: Reader,
    writer: Writer,
    meta: TableMeta,
    data_dir: PathBuf,
}

impl Engine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut wal = Wal::open(path.join("wal.log"))?;
        let mut memtable = MemTable::new();
        let mut meta = TableMeta::load(path.join("meta.json"))?;

        let reader = Reader::new(path.clone());
        let writer = Writer::new();

        // Recovery
        recover(
            &mut wal,
            &mut memtable,
            &writer,
            &mut meta,
            &path,
        )?;

        for entry in std::fs::read_dir(&path)? {
            let p = entry?.path();
            if p.extension().and_then(|e| e.to_str()) == Some("new") {
                let _ = std::fs::remove_file(p);
            }
        }

        Ok(Self {
            memtable,
            wal,
            reader,
            writer,
            meta,
            data_dir: path,
        })
    }

    pub fn put(&mut self, id: String, value: BTreeMap<String, FieldValue>) -> Result<()> {
        self.writer.put(&mut self.memtable, &mut self.wal, id, value)
    }

    pub fn delete(&mut self, id: String) -> Result<()> {
        self.writer.delete(&mut self.memtable, &mut self.wal, id)
    }

    pub fn get(&self, id: &str, snapshot: u64) -> Option<Record> {
        self.reader.get(&self.meta, &self.memtable, id, snapshot)
    }

    pub fn flush(&mut self) -> Result<()> {
        let current_page_id = self.meta.current_page_id;
        let (next_page_id, pages_meta) = self.writer.flush(&mut self.memtable, &self.data_dir, &current_page_id)?;
        self.meta.add_pages(pages_meta);
        self.meta.current_page_id = next_page_id;
        self.meta.persist(self.data_dir.join("meta.json"))?;
        Ok(())
    }

    pub fn maybe_compact(&mut self) -> Result<()> {
        if let Some(plan) = plan_l0_to_l1(&self.meta) {
            let obsolete_pages: Vec<PageMeta> = plan.input_l0_pages
                                                    .iter()
                                                    .chain(plan.input_l1_pages.iter())
                                                    .cloned()
                                                    .collect();

            let (current_page_id,new_pages) = execute_l0_to_l1(plan, &self.data_dir)?;
            
            self.meta.level[0].clear();
            self.meta.level[1].retain(|p| {
                !new_pages.iter().any(|np| np.overlaps(p))
            });

            for p in new_pages {
                self.meta.level[1].push(p);
            }

            self.meta.current_page_id = current_page_id;
            self.meta.persist(self.data_dir.join("meta.json"))?;
            delete_older_pages(&self.data_dir, obsolete_pages)?;
        }
        Ok(())
    }
}
