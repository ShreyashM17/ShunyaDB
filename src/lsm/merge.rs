use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;

use crate::storage::record::Record;
use crate::storage::page::io::read_page_from_disk;

pub struct PageIterator {
  records: Vec<Record>,
  index: usize,
}

impl PageIterator {
  pub fn open(path: &Path) -> anyhow::Result<Self> {
    let page = read_page_from_disk(path)?;
    Ok(Self {
      records: page.records,
      index: 0,
    })
  }

  fn peek(&self) -> Option<&Record> {
    self.records.get(self.index)
  }

  fn next(&mut self) -> Option<Record> {
    let rec = self.records.get(self.index).cloned();
    self.index += 1;
    rec
  }
}

#[derive(Clone)]
struct HeapItem {
  key: String,
  seqno: u64,
  level: u32, // 0 = L0, 1 = L1
  iter_id: usize,
}


impl Ord for HeapItem {
  fn cmp(&self, other: &Self) -> Ordering {
    self.key
      .cmp(&other.key)
      .reverse()
      .then_with(|| self.seqno.cmp(&other.seqno))
      .then_with(|| other.level.cmp(&self.level))
  }
}

impl PartialOrd for HeapItem {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl PartialEq for HeapItem {
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key && self.seqno == other.seqno
  }
}

impl Eq for HeapItem {}

pub struct MergeIterator {
  iters: Vec<PageIterator>,
  heap: BinaryHeap<HeapItem>,
}

impl MergeIterator {
  pub fn new(mut sources: Vec<(PageIterator, u32)>) -> Self {
    let mut heap = BinaryHeap::new();
    let mut iters = Vec::new();

    for (i, (iter, level)) in sources.iter_mut().enumerate() {
      if let Some(rec) = iter.peek() {
        heap.push(HeapItem {
          key: rec.id.clone(),
          seqno: rec.seqno,
          level: *level,
          iter_id: i,
        });
      }
      iters.push(std::mem::replace(iter, PageIterator { records: vec![], index: 0}));
    }

    Self { iters, heap }
  }

  pub fn next(&mut self) -> Option<Record> {
    let first = self.heap.pop()?;
    let mut best = self.iters[first.iter_id].next()?;
    let key = best.id.clone();

    if let Some(next) = self.iters[first.iter_id].peek() {
      self.heap.push(HeapItem {
        key: next.id.clone(),
        seqno: next.seqno,
        level: first.level,
        iter_id: first.iter_id,
      });
    }

    while let Some(top) = self.heap.peek() {
      if top.key != key {
        break;
      }

      let dup = self.heap.pop().unwrap();
      let rec = self.iters[dup.iter_id].next().unwrap();

      if rec.seqno > best.seqno {
        best = rec;
      }

      if let Some(next) = self.iters[dup.iter_id].peek() {
        self.heap.push(HeapItem {
          key: next.id.clone(),
          seqno: next.seqno,
          level: dup.level,
          iter_id: dup.iter_id,
        })
      }
    }

    if best.is_tombstone {
      None
    } else {
      Some(best)
    }
  }
}