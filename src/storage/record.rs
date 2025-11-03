use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use crate::engine::filter::Filter;
use std::cmp::Ordering;
use crate::util;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValue {
  Int(i64),
  Float(f64),
  Bool(bool),
  Text(String),
}

impl FieldValue {
  pub fn from_str_infer(s: &str) -> Self {
    if let Ok(i) = s.parse::<i64>() {
      FieldValue::Int(i)
    } else if let Ok(f) = s.parse::<f64>() {
      FieldValue::Float(f)
    } else if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") {
      FieldValue::Bool(s.eq_ignore_ascii_case("true"))
    } else {
      FieldValue::Text(s.to_string())
    }
  }

  pub fn equals(&self, other: &FieldValue) -> bool {
    match (self, other) {
      (FieldValue::Int(a), FieldValue::Int(b)) => a == b,
      (FieldValue::Float(a), FieldValue::Float(b)) => a == b,
      (FieldValue::Int(a), FieldValue::Float(b)) => (*a as f64) == *b,
      (FieldValue::Float(a), FieldValue::Int(b)) => *a == (*b as f64),
      (FieldValue::Text(a), FieldValue::Text(b)) => a == b,
      (FieldValue::Bool(a), FieldValue::Bool(b)) => a == b,
      _ => false
    }
  }

  /// Numeric comparison when both are numeric (Int/Float). Returns None if not comparable.
  pub fn numeric_cmp(&self, other: &FieldValue) -> Option<Ordering> {
    match (self, other) {
      (FieldValue::Int(a), FieldValue::Int(b)) => Some(a.cmp(b)),
      (FieldValue::Float(a), FieldValue::Float(b)) => a.partial_cmp(b),
      (FieldValue::Int(a), FieldValue::Float(b)) => (*a as f64).partial_cmp(b),
      (FieldValue::Float(a), FieldValue::Int(b)) => a.partial_cmp(&(*b as f64)),
      _ => None,
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
  pub id: u64,
  pub data: BTreeMap<String, FieldValue>,
}

impl Record {
  pub fn new(id: u64, data: BTreeMap<String, FieldValue>) -> Self {
    Self { id, data }
  }

  pub fn get(&self, key: &str) -> Option<&FieldValue> {
    self.data.get(key)
  }

  // Convert cli inputs from ["name=XYZ", "age=32"] = BTree<name, XYZ>
  pub fn from_pairs(pairs: Vec<String>) -> Self {
    use std::time::{SystemTime, UNIX_EPOCH};
    let id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
    let fields = util::from_pairs_to_btree(pairs);
    Record::new(id, fields)
  }
  
  pub fn matches(&self, filter: &Filter) -> bool {
    use Filter::*;
    match filter {
      ById(fid) => self.id == *fid,
      ByKeyValueEq(k, v) => self.matches_eq(k, v),
      ByKeyValueOp(k, op, v) => self.matches_numeric_op(k, v, op),
    }
  }

  pub fn matches_eq(&self, key: &str, val: &FieldValue) -> bool {
    match self.get(key) {
      Some(existing) => existing.equals(val),
      None => false,
    }
  }
    
  /// example for numeric comparison operators: ">" "<" ">=" "<="
  pub fn matches_numeric_op(&self, key: &str, other: &FieldValue, op: &str) -> bool {
    if let Some(existing) = self.data.get(key) {
      if let Some(ord) = existing.numeric_cmp(other) {
        match op {
          ">" => ord == Ordering::Greater,
          "<" => ord == Ordering::Less,
          ">=" => ord == Ordering::Greater || ord == Ordering::Equal,
          "<=" => ord == Ordering::Less || ord == Ordering::Equal,
          _ => false,
        }
      } else { false }
    } else { false }
  }

  pub fn apply_patch(&mut self, patch: &BTreeMap<String, FieldValue>) {
    for (k, v) in patch {
      self.data.insert(k.clone(), v.clone());
    }
  }
}