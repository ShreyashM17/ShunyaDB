use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ordered_float::NotNan;
use std::convert::TryFrom;

#[derive(Debug, Clone)]
pub struct FloatConversionError(pub &'static str);

impl std::fmt::Display for FloatConversionError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "FloatConversionError: {}", self.0)
  }
}

impl std::error::Error for FloatConversionError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldValue {
  Null,
  Bool(bool),
  Int(i64),
  UInt(u64),
  Float(NotNan<f64>),
  Str(String),
}

impl From<&str> for FieldValue {
  fn from(s: &str) -> Self {
    FieldValue::Str(s.to_owned())
  }
}

impl From<String> for FieldValue {
  fn from(s: String) -> Self {
    FieldValue::Str(s)
  }
}

impl From<i64> for FieldValue {
  fn from(i: i64) -> Self {
    FieldValue::Int(i)
  }
}

impl From<u64> for FieldValue {
  fn from(u: u64) -> Self {
    FieldValue::UInt(u)
  }
}

/// Convenience (panics on NaN). Use only in tests or internal helpers.
// impl From<f64> for FieldValue {
//   fn from(v: f64) -> Self {
//     FieldValue::Float(NotNan::new(v).expect("float cannot be NaN"))
//   }
// }

impl TryFrom<f64> for FieldValue {
  type Error = FloatConversionError;

  fn try_from(v: f64) -> Result<Self, Self::Error> {
    NotNan::new(v)
      .map(FieldValue::Float)
      .map_err(|_| FloatConversionError("float value is NaN; FieldValue::Float rquires non-NaN"))
  }
}

/// Core record type for storage layer.
/// `id` is the primary key (string for flexibility), `seqno` is the global sequence number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
  pub id: String,
  pub seqno: u64,
  pub is_tombstone: bool,
  pub data: BTreeMap<String, FieldValue>,
}

impl Record {
  /// Create a new record (nn-tmbstone)
  pub fn new(id: impl Into<String>, seqno: u64, data: BTreeMap<String, FieldValue>) -> Self {
    Self {
      id: id.into(),
      seqno,
      is_tombstone: false,
      data,
    }
  }

  /// Create a tombstone record for deletion.
  pub fn new_tombstone(id: impl Into<String>, seqno: u64) -> Self {
    Self {
      id: id.into(),
      seqno,
      is_tombstone: true,
      data: BTreeMap::new(),
    }
  }

  pub fn from_pairs<I, K, V>(id: impl Into<String>, seqno: u64, pairs: I) -> Self 
  where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<FieldValue>,
  {
    let mut map = BTreeMap::new();
    for (k,v) in pairs {
      map.insert(k.into(), v.into());
    }
    Self::new(id, seqno, map)
  }

  pub fn is_deleted(&self) -> bool {
    self.is_tombstone
  }
}