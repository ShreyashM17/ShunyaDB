use anyhow::{Result, bail};
use std::path::Path;

use crate::storage::memtable::MemTable;
use crate::storage::page::builder::PageBuilder;
use crate::storage::page::io::write_page;

pub fn flush_memtable(memtable: &mut MemTable, page_path: impl AsRef<Path>) -> Result<()> {
  if memtable.is_empty() {
    bail!("Cannot flush empty memtable");
  }

  let mut builder = PageBuilder::new();

  // Move records into page builder
  for record in memtable.iter() {
    builder.add(record.clone());
  }

  let page = builder.build();

  // Write immutable page to disk
  write_page(page_path, &page)?;

  // Clear memtable after successful flush
  *memtable = MemTable::new();

  Ok(())
}