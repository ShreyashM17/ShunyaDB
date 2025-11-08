use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};

use bincode;
use serde::{Deserialize, Serialize};

use crate::storage::page::Page;

// Making Page Serializable
#[derive(Serialize, Deserialize, Debug)]
pub struct SerializablePage {
    pub id: u64,
    pub records: Vec<crate::storage::record::Record>,
    pub capacity: usize,
}

impl From<&Page> for SerializablePage {
    fn from(page: &Page) -> Self {
        Self {
            id: page.id,
            records: page.records.clone(),
            capacity: page.capacity,
        }
    }
}

pub fn save_page_to_disk(page: &Page, file_path: &str) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)?;

    let writer = BufWriter::new(file);
    let s_page = SerializablePage::from(page);

    bincode::serialize_into(writer, &s_page).map_err(std::io::Error::other)?;
    Ok(())
}

pub fn load_page_from_disk(file_path: &str) -> std::io::Result<Page> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let s_page: SerializablePage =
        bincode::deserialize_from(reader).expect("Serializer is failing");

    Ok(Page {
        id: s_page.id,
        records: s_page.records,
        capacity: s_page.capacity,
    })
}
