use std::collections::BTreeMap;
use crate::storage::record::FieldValue;
use std::path::Path;
use std::fs;
use std::io::Result;
pub fn page_file(table: &str, page_number: u64) -> String {
  format!("data/{}/page_{}.bin", table, page_number)
}

pub fn from_pairs_to_btree(pairs: Vec<String>) -> BTreeMap<String, FieldValue> {
  let mut fields = BTreeMap::new();
  for pair in pairs {
    if let Some((k,v)) = pair.split_once('=') {
      let value = if let Ok(num) = v.parse::<i64>() {
        FieldValue::Int(num)
      } else if let Ok(f) = v.parse::<f64>()  {
        FieldValue::Float(f)
      } else if v == "true" || v == "false" {
        FieldValue::Bool(v == "true")
      } else {
        FieldValue::Text(v.to_string())
      };
        fields.insert(k.to_string(), value);
      }
    }
  fields
}

pub fn list_tables() -> Result<Vec<String>> {
  let database_path = Path::new("data");
  let mut tables = Vec::new();
  if !database_path.exists() {
    return Ok(tables);
  }
  for table in fs::read_dir(database_path)? {
    let table = table?;
    let metadata = table.metadata()?;
    if metadata.is_dir() {
      if let Some(table_name) = table.file_name().to_str() {
        tables.push(table_name.to_string());
      }
    }
  }

  Ok(tables)
}

pub fn list_pages(table : &str) -> Result<Vec<u64>> {
  let dir = format!("data/{}", table);
  let mut pages: Vec<u64> = Vec::new();
  if Path::new(&dir).exists() {
    for entry in fs::read_dir(dir)? {
      let path = entry?.path();
      if let Some(fname) = path.file_stem() {
        if let Some(s) = fname.to_str() {
          if s.starts_with("page_") {
            if let Ok(id) = s[5..].parse::<u64>() {
              pages.push(id);
            }
          }
        }
      }
    }
  }
  pages.sort();
  Ok(pages)
}