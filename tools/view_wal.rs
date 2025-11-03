use std::fs::File;
use std::io::{BufReader, Read};
use bincode;
use shunyadb::storage::wal::WalEntry;
use shunyadb::storage::record::Record;

fn main() {
    let file = File::open("wal.log").expect("no wal.log found");
    let mut reader = BufReader::new(file);
    loop {
        let mut len_buf = [0u8; 8];
        if reader.read_exact(&mut len_buf).is_err() { break; }
        let len = u64::from_le_bytes(len_buf);
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf).unwrap();

        match bincode::deserialize::<WalEntry>(&buf) {
            Ok(entry) => {
                println!("Operation: {} | Table: {}", entry.operation, entry.table);
                if entry.operation == "INSERT" || entry.operation == "UPDATE" {
                    if let Ok(record) = bincode::deserialize::<Record>(&entry.data) {
                        println!("{:#?}", record);
                    }
                }
                println!("-----------------------------------");
            }
            Err(e) => {
                eprintln!("Failed to parse WAL entry: {:?}", e);
                break;
            }
        }
    }
}
