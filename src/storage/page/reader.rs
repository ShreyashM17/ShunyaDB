use anyhow::{Result, bail};
use std::io::{Cursor, Read};

use crate::storage::page::header::PageHeader;
use crate::storage::page::builder::Page;
use crate::storage::record::Record;

/// Read a page from raw bytes.
/// Expected layout: [header][payload]
pub fn read_page(bytes: &[u8]) -> Result<Page> {
  let mut cursor = Cursor::new(bytes);

  let header: PageHeader = bincode::deserialize_from(&mut cursor)?;
  header.validate().map_err(|e| anyhow::anyhow!(e))?;

  let mut payload = Vec::new();
  cursor.read_to_end(&mut payload)?;

  let checksum = PageHeader::compute_checksum(&payload);
  if checksum != header.checksum {
    bail!("Page checksum mismatch");
  }

  let records: Vec<Record> = bincode::deserialize(&payload)?;

  if records.len() != header.num_records as usize {
    bail!("Number of records mismatch");
  }

  Ok(Page {
    header,
    records,
    payload,
  })
}