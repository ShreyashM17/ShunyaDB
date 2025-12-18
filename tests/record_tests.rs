use shunyadb::storage::record::*;
use bincode;

#[test]
fn record_roundtrip_ser_de() {
  let mut m = std::collections::BTreeMap::new();
  m.insert("name".to_string(), FieldValue::Str("alice".into()));
  m.insert("age".to_string(), FieldValue::Int(30));
  let rec = Record::new("user_1", 42, m.clone());

  let bytes = bincode::serialize(&rec).expect("serialize");
  let rec2: Record = bincode::deserialize(&bytes).expect("deserialize");

  assert_eq!(rec, rec2);
  assert_eq!(rec2.id, "user_1");
  assert_eq!(rec2.seqno, 42);
  assert!(!rec2.is_tombstone);
  assert_eq!(rec2.data, m);
}


#[test]
fn tombstone_behaviour() {
  let t = Record::new_tombstone("user_2", 100);
  let bytes = bincode::serialize(&t).unwrap();
  let t2: Record = bincode::deserialize(&bytes).unwrap();

  assert!(t2.is_tombstone);
  assert_eq!(t2.data.len(), 0);
}

#[test]
fn from_pairs_constructor() {
  let pairs = vec![("a", FieldValue::Int(1)), ("b", FieldValue::Str("x".into()))];
  let r = Record::from_pairs("row1", 7, pairs);
  assert_eq!(r.id, "row1");
  assert_eq!(r.seqno, 7);
  assert!(!r.is_tombstone);
  assert_eq!(r.data.get("a").unwrap(), &FieldValue::Int(1));
  assert_eq!(r.data.get("b").unwrap(), &FieldValue::Str("x".into()));
}

#[test]
fn float_tryfrom_ok_roundtrip() {
  let f = 3.14159f64;
  let fv = FieldValue::try_from(f).expect("should accept normal float");
  let mut map = std::collections::BTreeMap::new();
  map.insert("pi".to_string(), fv.clone());
  let r = Record::new("float_row", 10, map);

  let bytes = bincode::serialize(&r).expect("serialize float record");
  let r2: Record = bincode::deserialize(&bytes).expect("deserialize float record");

  assert_eq!(r, r2);
  // check stored float equals (via NotNan inner value)
  match r2.data.get("pi").unwrap() {
    FieldValue::Float(n) => assert_eq!(n.into_inner(), 3.14159f64),
    _ => panic!("expected float"),
  }
}

#[test]
fn float_tryfrom_rejects_nan() {
  let nan = std::f64::NAN;
  let res = FieldValue::try_from(nan);
  assert!(res.is_err(), "NaN must be rejected for FieldValue::Float");
}