use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use std::fs::{File};
use std::io::{BufReader, BufWriter};
use anyhow::Result;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HashIndex {
  pub map: HashMap<String, u64>, // key -> record_id
}

impl HashIndex {
  pub fn new() -> Self {
    Self { map: HashMap::new() }
  }

  pub fn insert(&mut self, key: &str, record_id: u64) {
    self.map.insert(key.to_string(), record_id);
  }

  pub fn get(&self, key: &str) -> Option<u64> {
    self.map.get(key).copied()
  }

  pub fn save(&self, path: &str) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, &self.map)?;
    Ok(())
  }

  pub fn load(path: &str) -> Result<Self> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let map: HashMap<String, u64> = bincode::deserialize_from(reader)?;
    Ok(Self {map})
  }
}