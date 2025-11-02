use std::num::NonZeroUsize;
use lru::LruCache;
use std::sync::{Arc, Mutex};

use crate::storage::page::Page;

#[derive(Clone)]
pub struct PageCache {
  cache: Arc<Mutex<LruCache<u64, Page>>>,
}

impl PageCache {
  pub fn new(capacity: usize) -> Self {
    Self {
      cache: Arc::new(Mutex::new(
        LruCache::new(NonZeroUsize::new(capacity).unwrap()),
      )),
    }
  }

  pub fn get(&self, page_id: u64) -> Option<Page> {
    let mut cache = self.cache.lock().unwrap();
    cache.get(&page_id).cloned()
  }

  pub fn put(&self, page: Page) {
    let mut cache = self.cache.lock().unwrap();
    cache.put(page.id, page);
  }

  pub fn size(&self) -> usize {
    let cache = self.cache.lock().unwrap();
    cache.len()
  }
}