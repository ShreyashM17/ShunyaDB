use std::time::Instant;
use std::fs;
use shunyadb::storage::{io, page::Page, cache::PageCache};
use shunyadb::util;

#[test]
fn benchmark_cache_vs_uncached_reads() {
    let table = "users";
    let file_path = util::page_file(table, 1);
    fs::create_dir_all(format!("data/{}", table)).expect("Unable to Create directory");

    // Load or create page
    let page = if std::path::Path::new(&file_path).exists() {
        io::load_page_from_disk(&file_path).unwrap()
    } else {
        let mut p = Page::new(1, 4096);
        for i in 0..4094 {
            let record = p.generate_mock_record(i); // optional helper
            p.insert(record).expect("Unable to insert recordS");
        }
        io::save_page_to_disk(&p, &file_path).expect("Unable to save to disk");
        p
    };

    // --- Without cache ---
    let start = Instant::now();
    for _ in 0..4094 {
        let _ = io::load_page_from_disk(&file_path).expect("No Data Found");
    }
    let uncached_duration = start.elapsed().as_millis();

    // --- With cache ---
    let cache = PageCache::new(8);
    cache.put("users_page_1", page.clone());
    let start = Instant::now();
    for _ in 0..4094 {
        let _ = cache.get("users_page_1").unwrap();
    }
    let cached_duration = start.elapsed().as_millis();

    println!(
        "\n📊 Cache Benchmark Results:\nUncached: {} ms\nCached: {} ms\nSpeedup: {:.2}x\n",
        uncached_duration,
        cached_duration,
        uncached_duration as f64 / cached_duration as f64
    );

    assert!(cached_duration < uncached_duration, "Cache should be faster!");
    cache.clear_cache();
}
