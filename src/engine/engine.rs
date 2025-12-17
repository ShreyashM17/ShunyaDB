use anyhow::Result;
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use crate::storage::record::Record;
use crate::engine::reader::Reader;
use crate::engine::writer::Writer;
use crate::storage::memtable::MemTable;
use crate::storage::record::FieldValue;
use crate::storage::wal::Wal;
use crate::meta::TableMeta;

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

        let wal = Wal::open(path.join("wal.log"))?;
        let memtable = MemTable::new();
        let meta = TableMeta::load(path.join("meta.json"))?;

        let reader = Reader::new(&meta, path.clone());
        let writer = Writer::new();

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
        self.reader.get(&self.memtable, id, snapshot)
    }

    pub fn flush(&mut self) -> Result<()> {
        let pages = self.writer.flush(&mut self.memtable, &self.data_dir)?;
        self.meta.add_pages(pages);
        self.meta.persist(self.data_dir.join("meta.json"))?;
        Ok(())
    }
}
