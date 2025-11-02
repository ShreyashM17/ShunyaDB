// src/main.rs
mod cli;
mod engine;
mod storage;

use clap::Parser;
use cli::{Cli, Commands};
use engine::Engine;
use std::fs;

fn main() -> anyhow::Result<()> {
    // Ensure data directory exists
    fs::create_dir_all("data")?;

    // Initialize engine (with WAL)
    let mut engine = Engine::new("wal.log");

    // Parse CLI command
    let cli = Cli::parse();

    match cli.command {
        Commands::Insert { table, pairs } => {
            let record = crate::storage::record::Record::from_pairs(pairs);
            engine.insert_record(&table, record)?;

            println!("✅ Inserted record into `{}`", table);
        }

        Commands::Get { table } => {
            // Later we’ll add filtering, multiple pages, etc.
            let file_path = format!("data/{}/page_1.bin", table);
            if std::path::Path::new(&file_path).exists() {
                let page = crate::storage::io::load_page_from_disk(&file_path)?;
                println!("📄 Records in `{}`:\n{:#?}", table, page);
            } else {
                println!("⚠️  No data found for `{}`", table);
            }
        }
    }

    Ok(())
}