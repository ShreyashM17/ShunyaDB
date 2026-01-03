use anyhow::Result;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use crate::storage::page::builder::Page;
use crate::storage::page::reader::read_page;

/// Write page to disk, files must not already exist since pages are immutable.
pub fn write_page(path: impl AsRef<Path>, page: &Page) -> Result<u64> {
  let path = path.as_ref();

  if path.exists() {
    anyhow::bail!("page already exists: {:?}", path);
  }

  let tmp_path: PathBuf = path.with_extension("temp.new");
  let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp_path)?;

  // Write header
  let header_bytes = bincode::serialize(&page.header)?;
  file.write_all(&header_bytes)?;

  // Write payload
  file.write_all(&page.payload)?;

  file.sync_all()?;
  drop(file);

  std::fs::rename(&tmp_path, path)?;

  // 4️⃣ fsync directory
  #[cfg(unix)] {
    let dir = path.parent().unwrap_or(Path::new("."));
    File::open(dir)?.sync_all()?;
  }

  let page_size_bytes = std::fs::metadata(path)?.len();

  Ok (page_size_bytes)
}


/// Read page from disk and validate
pub fn read_page_from_disk(path: impl AsRef<Path>) -> Result<Page> {
  let mut file = File::open(path)?;
  let mut bytes = Vec::new();
  file.read_to_end(&mut bytes)?;

  read_page(&bytes)
}


#[cfg(test)]
mod tests {
use super::*;

  #[test]
  fn check_duplicate_works() {
    let temp_dir = tempfile::tempdir().unwrap();
    let page_path = temp_dir.path().join("page_1");

    let record = crate::storage::record::Record::new(
      "id1".to_string(),
      1,
      std::collections::BTreeMap::new(),
    );

    let mut builder = crate::storage::page::builder::PageBuilder::new();
    builder.add(record);
    let page = builder.build();

    // First write should succeed
    write_page(&page_path, &page).expect("Failed to write page");

    // Second write should fail due to create_new
    let result = write_page(&page_path, &page);
    assert!(result.is_err());
  }
}