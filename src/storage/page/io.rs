use anyhow::Result;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::storage::page::builder::Page;
use crate::storage::page::reader::read_page;

/// Write page to disk, files must not already exist since pages are immutable.
pub fn write_page(path: impl AsRef<Path>, page: &Page) -> Result<()> {
  let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;

  // Write header
  let header_bytes = bincode::serialize(&page.header)?;
  file.write_all(&header_bytes)?;

  // Write payload
  file.write_all(&page.payload)?;

  file.sync_all()?;
  Ok(())
}


/// Read page from disk and validate
pub fn read_page_from_disk(path: impl AsRef<Path>) -> Result<Page> {
  let mut file = File::open(path)?;
  let mut bytes = Vec::new();
  file.read_to_end(&mut bytes)?;

  read_page(&bytes)
}