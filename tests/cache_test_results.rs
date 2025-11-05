use shunyadb::storage::page::Page;
use shunyadb::storage::cache::PageCache;

#[test]
fn test_cache_put_get_invalidate() {
  let cache = PageCache::new(2);
  let page = Page::new(2, 4096);
  cache.put("users_page_2", page.clone());
  assert!(cache.get("users_page_2").is_some());
  cache.invalidate("users_page_2");
  assert!(cache.get("users_page_2").is_none());
}
