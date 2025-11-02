mod storage {
  pub mod record;
  pub mod page;
  pub mod io;
  pub mod cache;
  pub mod wal;
}

use std::collections::HashMap;
use storage::record::{Record, Value};
use storage::page::Page;
use storage::io::{save_page_to_disk, load_page_from_disk};
use storage::cache::PageCache;
use std::time::Instant;
use storage::wal::{WalEntry, WriteAheadLog};

fn main() -> std::io::Result<()> {
  let cache = PageCache::new(3);

  for id in 1..=5 {
    let mut data = HashMap::new();
    data.insert("name".to_string(), Value::Text(format!("User afas fa fsa fasf asf asf as fas ff asf asf asf asf saf asf asf asf asf asfas fsa fsa fsaf asf asf asf asf asf asf afas fas fas fasf asf asf asf asf afa a fas fas  {}", id)));
    data.insert("age".to_string(), Value::Int(20 + id as i64));

    let mut page = Page::new(id, 2);
    page.insert(Record::new(id, data)).unwrap();

    save_page_to_disk(&page, &format!("page_{}.bin", id))?;
    println!("Saved Page {}", id);
  }

  for id in [1,2,4,1,2,3,5, 2] {
    let start = Instant::now();

    let page = if let Some(p) = cache.get(id) {
      println!("Cache hit for Page {}", id);
      p
    } else {
      println!("Cache miss for Page {}, loading from disk", id);
      let p = load_page_from_disk(&format!("page_{}.bin", id))?;
      cache.put(p.clone());
      p
    };

    let elapsed = start.elapsed();
    println!("Page {} read time: {:?}\n", page.id, elapsed);
  }


    let mut wal = WriteAheadLog::new("wal.log");

    // Simulate insert
    let entry = WalEntry {
        operation: "INSERT".to_string(),
        table: "users".to_string(),
        record_id: 1,
        data: bincode::serialize(&"Shreyash").unwrap(),
    };

    wal.log(&entry);
    println!("Logged entry to WAL.");

    // Simulate crash recovery
    let recovered = WriteAheadLog::recover("wal.log");
    println!("Recovered entries: {:?}", recovered);
}
