use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;

use crate::engine::seqno::allocate;
use crate::meta::PageInfo;
use crate::storage::memtable::MemTable;
use crate::storage::page::builder::PageBuilder;
use crate::storage::page::io::write_page;
use crate::storage::record::{FieldValue, Record};
use crate::storage::wal::{Wal, WalEntry, WalOp};

const MAX_RECORDS_PER_PAGE: usize = 128;

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
    ) -> Result<Vec<PageInfo>> {
        let mut builder = PageBuilder::new();
        let mut count = 0;
        let mut pages = Vec::new();

        for (_id, versions) in memtable.iter() {
            for record in versions {
                builder.add(record.clone());
                count += 1;

                if count >= MAX_RECORDS_PER_PAGE {
                    pages.push(self.flush_one(&mut builder, dir)?);
                    builder = PageBuilder::new();
                    count = 0;
                }
            }
        }

        if count > 0 {
            pages.push(self.flush_one(&mut builder, dir)?);
        }

        memtable.clear();
        Ok(pages)
    }

    fn flush_one(
        &self,
        builder: &mut PageBuilder,
        dir: &Path,
    ) -> Result<PageInfo> {
        let page = builder.clone().build();
        let page_id = page.header.page_seqno;

        let path = dir.join(format!("page_{}.db", page_id));
        write_page(&path, &page)?;

        Ok(PageInfo::new(
            page_id,
            page.header.min_id.clone(),
            page.header.max_id.clone(),
            page.header.num_records as usize,
            page.header.page_seqno,
        ))
    }
}
