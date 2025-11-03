use std::collections::BTreeMap;

use crate::storage::record::FieldValue;

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