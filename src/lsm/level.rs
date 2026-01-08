use crate::meta::PageMeta;

impl PageMeta {
    pub fn overlaps(&self, other: &PageMeta) -> bool {
        !(self.max_id < other.min_id || self.min_id > other.max_id)
    }
}

#[derive(Debug)]
pub struct Level {
    pub level_id: u32,
    pub max_page_size_bytes: usize,
    pages: Vec<PageMeta>,
}

impl Level {
    pub fn new(level_id: u32, max_page_size_bytes: usize) -> Self {
        Self {
            level_id,
            max_page_size_bytes,
            pages: Vec::new(),
        }
    }

    pub fn insert_page(&mut self, page_meta: PageMeta) {
        if self.level_id > 0 {
            for existing_page in &self.pages {
                if existing_page.overlaps(&page_meta) {
                    panic!("Page key range overlaps with existing page in level {}", self.level_id);
                }
            }
        }

        self.pages.push(page_meta);
        self.pages.sort_by(|a, b| a.min_id.cmp(&b.min_id));
    }

    pub fn get_pages(&self) -> &[PageMeta] {
        &self.pages
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    pub fn clear(&mut self) {
        self.pages.clear();
    }
}