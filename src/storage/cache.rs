use crate::storage::page::Page;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct PageCache {
    cache: Arc<Mutex<LruCache<String, Page>>>,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).unwrap(),
            ))),
        }
    }

    pub fn get(&self, key: &str) -> Option<Page> {
        let mut cache = self.cache.lock().unwrap();
        cache.get(key).cloned()
    }

    pub fn put(&self, key: &str, page: Page) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(key.to_string(), page);
    }

    pub fn invalidate(&self, key: &str) {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(key);
    }

    pub fn clear_cache(&self) {
        self.cache.lock().unwrap().clear();
    }
}
