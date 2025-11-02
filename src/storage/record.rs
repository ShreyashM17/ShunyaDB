use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValue {
  Null,
  Int(i64),
  Float(f64),
  Bool(bool),
  Text(String),
  Binary(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
  pub id: u64,
  pub fields: BTreeMap<String, FieldValue>,
}

impl Record {
  pub fn new(id: u64, fields: BTreeMap<String, FieldValue>) -> Self {
    Self { id, fields }
  }

  // Convert cli inputs from ["name=XYZ", "age=32"] = BTree<name, XYZ>
  pub fn from_pairs(pairs: Vec<String>) -> Self {
    use std::time::{SystemTime, UNIX_EPOCH};
    let id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;

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

    Record::new(id, fields)
  }


  pub fn get(&self, key: &str) -> Option<&FieldValue> {
    self.fields.get(key)
  }
}