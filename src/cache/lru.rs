use std::collections::HashMap;
use std::hash::Hash;

use crate::engine::engine::EngineMetrics;

/// Internal doubly-linked list node (key-based, no references)
struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<K>,
    next: Option<K>,
}

pub struct LruCache<K, V>
where
    K: Eq + Hash + Clone,
{
    capacity: usize,
    map: HashMap<K, Node<K, V>>,
    head: Option<K>, // Most recently used
    tail: Option<K>, // Least recently used
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "LRU capacity must be > 0");

        Self {
            capacity,
            map: HashMap::new(),
            head: None,
            tail: None,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if !self.map.contains_key(key) {
            return None;
        }

        let key = key.clone();
        self.move_to_head(&key);
        self.map.get(&key).map(|n| &n.value)
    }

    pub fn put(&mut self, key: K, value: V, metrics: &mut EngineMetrics) {
        if self.map.contains_key(&key) {
            // Update existing
            if let Some(node) = self.map.get_mut(&key) {
                node.value = value;
            }
            self.move_to_head(&key);
            return;
        }

        // Evict if needed
        if self.map.len() == self.capacity {
            self.evict_lru();
            metrics.page_cache_evictions += 1;
        }

        // Insert new node at head
        let node = Node {
            key: key.clone(),
            value,
            prev: None,
            next: self.head.clone(),
        };

        if let Some(old_head) = &self.head {
            if let Some(h) = self.map.get_mut(old_head) {
                h.prev = Some(key.clone());
            }
        }

        if self.tail.is_none() {
            self.tail = Some(key.clone());
        }

        self.head = Some(key.clone());
        self.map.insert(key, node);
    }

    fn move_to_head(&mut self, key: &K) {
        if Some(key.clone()) == self.head {
            return;
        }

        let (prev, next) = {
            let node = self.map.get(key).unwrap();
            (node.prev.clone(), node.next.clone())
        };

        // Detach node
        if let Some(p) = prev.clone() {
            if let Some(pn) = self.map.get_mut(&p) {
                pn.next = next.clone();
            }
        }

        if let Some(n) = next.clone() {
            if let Some(nn) = self.map.get_mut(&n) {
                nn.prev = prev.clone();
            }
        }

        // Update tail if needed
        if Some(key.clone()) == self.tail {
            self.tail = prev.clone();
        }

        // Attach to head
        let old_head = self.head.clone();
        {
            let node = self.map.get_mut(key).unwrap();
            node.prev = None;
            node.next = old_head.clone();
        }

        if let Some(h) = old_head {
            if let Some(hn) = self.map.get_mut(&h) {
                hn.prev = Some(key.clone());
            }
        }

        self.head = Some(key.clone());
        if self.tail.is_none() {
            self.tail = Some(key.clone());
        }
    }

    fn evict_lru(&mut self) {
        if let Some(lru_key) = self.tail.clone() {
            let prev = self.map.get(&lru_key).and_then(|n| n.prev.clone());

            if let Some(p) = prev.clone() {
                if let Some(pn) = self.map.get_mut(&p) {
                    pn.next = None;
                }
            }

            self.map.remove(&lru_key);
            self.tail = prev;

            if self.map.is_empty() {
                self.head = None;
                self.tail = None;
            }
        }
    }
}
