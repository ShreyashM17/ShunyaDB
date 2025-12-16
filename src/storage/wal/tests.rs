use super::*;
use tempfile::tempdir;
use crate::storage::record::Record;
use crate::engine::seqno;
use super::replay::ReplayResult;

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

// replay tests in replay.rs

#[test]
fn replay_detects_seqno_ordering() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    let mut wal = Wal::open(&wal_path).unwrap();

    // Create correct order
    let seq1 = seqno::allocate();
    let e1 = WalEntry::new(WalOp::Insert, "tbl", "id1", seq1, None);
    wal.append(&e1).unwrap();

    let seq2 = seqno::allocate();
    let e2 = WalEntry::new(WalOp::Insert, "tbl", "id2", seq2, None);
    wal.append(&e2).unwrap();

    let mut wal = Wal::open(&wal_path).unwrap();
    let replay = ReplayResult::replay_wal(&mut wal).unwrap();

    assert_eq!(replay.entries.len(), 2);
    assert_eq!(replay.max_seqno, seq2);
}

#[test]
fn replay_rejects_out_of_order_seqno() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    // manually create WAL with bad seqno ordering
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&wal_path).unwrap();

        let bad = vec![
            // first entry: seqno 10
            bincode::serialize(&WalEntry {
                seqno: 10, op: WalOp::Insert, table: "x".into(), record_id: "1".into(), record: None
            }).unwrap(),
            // second entry: seqno 9 (invalid!)
            bincode::serialize(&WalEntry {
                seqno: 9, op: WalOp::Insert, table: "x".into(), record_id: "2".into(), record: None
            }).unwrap(),
        ];

        for p in bad {
            let len = p.len() as u64;
            f.write_all(&len.to_le_bytes()).unwrap();
            f.write_all(&p).unwrap();
            f.write_all(&len.to_le_bytes()).unwrap();
        }
    }

    let mut wal = Wal::open(&wal_path).unwrap();
    let replay = ReplayResult::replay_wal(&mut wal);

    assert!(replay.is_err()); // correctly rejects bad WAL order
}