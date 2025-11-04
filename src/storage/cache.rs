use std::num::NonZeroUsize;
use lru::LruCache;
use std::sync::{Arc, Mutex};
use crate::storage::page::Page;

#[derive(Clone)]
pub struct PageCache {
  cache: Arc<Mutex<LruCache<String, Page>>>,
}

impl PageCache {
  pub fn new(capacity: usize) -> Self {
    Self {
      cache: Arc::new(Mutex::new(
        LruCache::new(NonZeroUsize::new(capacity).unwrap()),
      )),
    }
  }

  pub fn get(&self, key: &str) -> Option<Page> {
    let mut cache = self.cache.lock().unwrap();
    if let Some(page) = cache.get(key) {
        println!("Cache hit: {}", key);
        Some(page.clone())
    } else {
        println!("Cache miss: {}", key);
        None
    }
  }

  pub fn put(&self, key: &str ,page: Page) {
    let mut cache = self.cache.lock().unwrap();
    cache.put(key.to_string(), page);
  }

  pub fn invalidate(&self, key: &str) {
    let mut cache = self.cache.lock().unwrap();
    cache.pop(key);
  }
}