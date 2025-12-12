use super::*;
use tempfile::tempdir;
use crate::storage::record::Record;
use crate::engine::seqno;

#[test]
fn wal_append_and_read() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    let mut wal = Wal::open(&wal_path).unwrap();

    let seq1 = seqno::allocate();
    let rec1 = Record::from_pairs("1", seq1, vec![("name", "alice")]);
    let e1 = WalEntry::new(WalOp::Insert, "users", "1", seq1, Some(rec1.clone()));

    wal.append(&e1).unwrap();

    let seq2 = seqno::allocate();
    let rec2 = Record::new_tombstone("1", seq2);
    let e2 = WalEntry::new(WalOp::Delete, "users", "1", seq2, Some(rec2.clone()));

    wal.append(&e2).unwrap();

    let mut wal = Wal::open(&wal_path).unwrap();
    let entries = wal.read_all().unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], e1);
    assert_eq!(entries[1], e2);
}

#[test]
fn wal_handles_truncated_data_gracefully() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    // create truncated WAL
    {
        let mut f = File::create(&wal_path).unwrap();
        f.write_all(&5u64.to_le_bytes()).unwrap();  // fake length
        f.write_all(&[1, 2, 3]).unwrap();           // incomplete payload
    }

    let mut wal = Wal::open(&wal_path).unwrap();
    let entries = wal.read_all().unwrap();

    assert!(entries.is_empty()); // truncated WAL should just stop, not crash
}