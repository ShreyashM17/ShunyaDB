use anyhow::Result;

use crate::engine::writer::Writer;
use crate::meta::TableMeta;
use crate::storage::memtable::MemTable;
use crate::storage::wal::Wal;
use crate::storage::wal::replay::ReplayResult;

pub fn recover(
    wal: &mut Wal,
    memtable: &mut MemTable,
    writer: &Writer,
    meta: &mut TableMeta,
    data_dir: &std::path::Path,
) -> Result<()> {
    // Replay WAL
    let replay = ReplayResult::replay_wal(wal)?;

    // Re-apply WAL entries into memtable
    for entry in replay.entries {
        if entry.seqno <= meta.checkpoint_seqno {
            continue;
        }
        match entry.op {
            crate::storage::wal::WalOp::Insert
            | crate::storage::wal::WalOp::Update => {
                if let Some(record) = entry.record {
                    memtable.put(record);
                }
            }

            crate::storage::wal::WalOp::Delete => {
                if let Some(record) = entry.record {
                    // delete is represented as tombstone record
                    memtable.put(record);
                }
            }
        }
    }

    // Flush recovered memtable into immutable pages
    if !memtable.is_empty() {
        let (next_page_id,pages) = writer.flush(memtable, data_dir, &meta.current_page_id)?;
        meta.add_pages(pages);
        meta.current_page_id = next_page_id;
    }

    meta.persist(data_dir.join("meta.json"))?;

    crate::engine::seqno::advance_to(replay.max_seqno + 1);
    Ok(())
}
