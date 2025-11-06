// src/engine/index.rs
use std::{collections::HashMap, fmt::format, fs::File, io::{BufReader, BufWriter}, iter::FilterMap};
use serde::{Serialize, Deserialize};
use crate::storage::{io, record::{FieldValue, Record}};
use bincode;
use crate::util;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashIndex {
  pub key_map: HashMap<String, HashMap<String, Vec<(u64, u64)>>>,
}

impl HashIndex {
  pub fn new() -> Self {
    Self { key_map: HashMap::new() }
  }

  pub fn add_record(&mut self, record: &Record, page_id: u64) {
    for (k, v) in &record.data {
      let val = match v {
        FieldValue::Text(s) => s.clone(),
        FieldValue::Int(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::Bool(b) => b.to_string(),
      };
      
      self.key_map.entry(k.clone()).or_default().entry(val).or_default().push((page_id, record.id));
    }
  }

  pub fn lookup(&self, field: &str, value: &str) -> Option<&Vec<(u64, u64)>> {
    self.key_map.get(field)?.get(value)
  }

  pub fn save(&self, table: &str) -> std::io::Result<()> {
    let path = format!("data/{}/index.bin", table);
    std::fs::create_dir_all(format!("data/{}", table))?;
    let file = BufWriter::new(File::create(path)?);
    bincode::serialize_into(file, self).unwrap();
    Ok(())
  }

  pub fn load(table: &str) -> std::io::Result<Self> {
    let path = format!("data/{}/index.bin", table);
    if !std::path::Path::new(&path).exists() {
      return Ok(HashIndex::new());
    }
    let file = BufReader::new(File::open(path)?);
    let index: HashIndex = bincode::deserialize_from(file).unwrap();
    Ok(index)
  }

  pub fn rebuild_index(table: &str) -> std::io::Result<Self> {
    let mut new_index = HashIndex::new();
    let table_dir = format!("data/{}", table);

    if !std::path::Path::new(&table_dir).exists() {
      return Ok(new_index);
    }

    for entry in std::fs::read_dir(&table_dir)? {
      let entry = entry?;
      let path = entry.path();
      if path.is_file() && path.file_name().unwrap().to_string_lossy().starts_with("page_") {
        let page = io::load_page_from_disk(path.to_str().unwrap())?;
        for record in &page.records {
          new_index.add_record(record, page.id);
        }
      }
    }
    new_index.save(table)?;
    Ok(new_index)
  }
}