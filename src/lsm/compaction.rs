use std::path::Path;

use crate::lsm::compaction_plan::CompactionPlan;
use crate::lsm::merge::{MergeIterator, PageIterator};
use crate::storage::page::builder::PageBuilder;
use crate::storage::page::io::write_page;
use crate::meta::PageMeta;

pub fn execute_l0_to_l1(plan: CompactionPlan, data_dir: &Path) -> anyhow::Result<(u64, Vec<PageMeta>)> {
  let mut sources = Vec::new();

  for p in &plan.input_l0_pages {
    let iter = PageIterator::open(&data_dir.join(&p.file_name))?;
    sources.push((iter, 0));
  }

  for p in &plan.input_l1_pages {
    let iter = PageIterator::open(&data_dir.join(&p.file_name))?;
    sources.push((iter, 1));
  }

  let mut merge = MergeIterator::new(sources);

  let mut builder = PageBuilder::new();
  let mut pages = Vec::new();

  while let Some(record) = merge.next() {
    if builder.estimate_size_with(&record) > plan.target_page_size_bytes {
      pages.push(builder.build());
      builder = PageBuilder::new();
    }
    builder.add(record.clone());
    builder.update_size(&record);
  }

  if !builder.is_empty() {
    pages.push(builder.build());
  }

  let mut metas = Vec::new();
  let mut current_page_id = plan.target_page_id_start;
  for page in pages {
    let path = data_dir.join(format!("page_{}.db", current_page_id));
    let size = write_page(&path, &page)?;
    metas.push(PageMeta::new(
      current_page_id,
      page.header.min_id.clone(),
      page.header.max_id.clone(),
      page.header.num_records as usize,
      size,
      page.header.page_seqno,
    ));
    current_page_id = current_page_id + 1;
  }

  Ok((current_page_id, metas))
}