use crate::meta::{PageMeta, TableMeta};

#[derive(Debug)]
pub struct CompactionPlan {
  pub source_level: usize, // level 0
  pub target_level: usize, // level 1

  pub input_l0_pages: Vec<PageMeta>,
  pub input_l1_pages: Vec<PageMeta>,

  pub min_key: String,
  pub max_key: String,

  pub target_page_size_bytes: usize,
  pub target_page_id_start: u64,
}

const L0_LEVEL_PAGES_LIMIT: usize = 8;
const L0_LEVEL_SIZE_LIMIT_BYTES: u64 = 256 * 1024; // 256 KB
const L1_PAGE_BYTES: usize = 256 * 1024; // 256 KB

pub fn plan_l0_to_l1(meta: &TableMeta) -> Option<CompactionPlan> {
  let l0 = &meta.level[0];
  if l0.is_empty() {
    return None;
  }

  let l0_bytes: u64 = l0.iter().map(|p| p.size_bytes).sum();
  if l0.len() < L0_LEVEL_PAGES_LIMIT && l0_bytes < L0_LEVEL_SIZE_LIMIT_BYTES {
    return None;
  }

  let min_key = l0.iter().map(|p| p.min_id.clone()).min().unwrap();
  let max_key = l0.iter().map(|p| p.max_id.clone()).max().unwrap();

  let mut input_l1_pages = Vec::new();
  if meta.level.len() > 1 {
    for p in &meta.level[1] {
      let overlaps = !(p.max_id < min_key || p.min_id > max_key);
      if overlaps {
        input_l1_pages.push(p.clone());
      }
    }
  }

  Some(CompactionPlan {
    source_level: 0,
    target_level: 1,
    input_l0_pages: l0.clone(),
    input_l1_pages,
    min_key,
    max_key,
    target_page_size_bytes: L1_PAGE_BYTES,
    target_page_id_start: meta.current_page_id,
  })
}