use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;

use crate::engine::seqno::allocate;
use crate::lsm::level::PageMeta;
use crate::storage::memtable::MemTable;
use crate::storage::page::builder::{PageBuilder, Page};
use crate::storage::page::io::write_page;
use crate::storage::record::{FieldValue, Record};
use crate::storage::wal::{Wal, WalEntry, WalOp};

const MAX_RECORDS_PER_PAGE: usize = 1024;
const MAX_PER_PAGE_SIZE: usize = 32768; // 32 KB for L0

pub struct Writer;

impl Writer {
    pub fn new() -> Self {
        Self
    }

    pub fn put(
        &self,
        memtable: &mut MemTable,
        wal: &mut Wal,
        id: String,
        value: BTreeMap<String, FieldValue>,
    ) -> Result<()> {
        let seqno = allocate();
        let record = Record::new(id, seqno, value);
        let wal_entry = WalEntry::new(WalOp::Insert, String::new(), record.id.clone(), seqno, Some(record.clone()));
        wal.append(&wal_entry)?;
        memtable.put(record);

        Ok(())
    }

    pub fn delete(
        &self,
        memtable: &mut MemTable,
        wal: &mut Wal,
        id: String,
    ) -> Result<()> {
        let seqno = allocate();
        let record = Record::new_tombstone(id, seqno);
        let wal_entry = WalEntry::new(WalOp::Delete, String::new(), record.id.clone(), seqno, Some(record.clone()));
        wal.append(&wal_entry)?;
        memtable.put(record);

        Ok(())
    }

    pub fn flush(
        &self,
        memtable: &mut MemTable,
        dir: &Path,
    ) -> Result<Vec<PageMeta>> {
        let mut builder = PageBuilder::new();
        let mut count = 0;
        let mut pages = Vec::new();

        for (_id, versions) in memtable.iter() {
            for record in versions {
                let estimated_size = builder.estimate_size_with(record);

                if estimated_size > MAX_PER_PAGE_SIZE {
                    let page = builder.build();
                    pages.push(self.flush_one(&page, dir)?);
                    builder = PageBuilder::new();
                    count = 0;
                }

                builder.add(record.clone());
                builder.update_size(&record);
                count += 1;

                if count >= MAX_RECORDS_PER_PAGE {
                    let page = builder.build();
                    pages.push(self.flush_one(&page, dir)?);
                    builder = PageBuilder::new();
                    count = 0;
                }
            }
        }

        if count > 0 {
            let page = builder.build();
            pages.push(self.flush_one(&page, dir)?);
        }

        memtable.clear();
        Ok(pages)
    }

    fn flush_one(
        &self,
        page: &Page,
        dir: &Path,
    ) -> Result<PageMeta> {
        let page_id = page.header.page_seqno;

        let path = dir.join(format!("page_{}.db", page_id));
        let page_size = write_page(&path, &page)?;

        Ok(PageMeta::new(
            page_id,
            page.header.min_id.clone(),
            page.header.max_id.clone(),
            page.header.num_records as usize,
            page_size,
            page.header.page_seqno,
        ))
    }
}
