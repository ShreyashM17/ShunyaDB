use anyhow::{Result, bail};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::storage::memtable::MemTable;
use crate::storage::page::builder::{PageBuilder};
use crate::storage::page::io::write_page;

const MAX_RECORDS_PER_PAGE: usize = 128;
static PAGE_ID_ALLOC: AtomicU64 = AtomicU64::new(0);

fn next_page_id() -> u64 {
    PAGE_ID_ALLOC.fetch_add(1, Ordering::Relaxed)
}

pub fn flush_memtable(memtable: &mut MemTable, page_path: impl AsRef<Path>) -> Result<()> {
  if memtable.is_empty() {
    bail!("Cannot flush empty memtable");
  }

  let page_dir = page_path.as_ref();

  let mut current_builder = PageBuilder::new();
  let mut current_count = 0;

  for (_id, versions) in memtable.iter() {
    for record in versions {
      current_builder.add(record.clone());
      current_count += 1;

      if current_count >= MAX_RECORDS_PER_PAGE {
        let page = current_builder.build();
        let page_id = next_page_id();
        let page_path = page_dir.join(format!("page_{}.db", page_id));
        write_page(page_path, &page)?;

        current_builder = PageBuilder::new();
        current_count = 0;
      }
    }
  }

  if current_count > 0 {
    let page = current_builder.build();
    let page_id = next_page_id();
    let page_path = page_dir.join(format!("page_{}.db", page_id));
    write_page(page_path, &page)?;
  }

  *memtable = MemTable::new();
  Ok(())
}