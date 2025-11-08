// src/main.rs
mod cli;
mod engine;
mod storage;
mod util;

use clap::Parser;
use cli::{Cli, Commands};
use engine::Engine;
use std::fs;

use crate::engine::filter::Filter;

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

            println!("Inserted record into `{}`", table);
        }

        Commands::Get { table, filter } => {
            let filter_value = Filter::parse(&filter).unwrap();
            let page = engine.get(&table, filter_value);
            println!(
                "Records in table = {} \n Filter = {} \n Records: {:#?}",
                table, filter, page
            );
        }

        Commands::GetAll { table } => {
            let page = engine.get_all(&table)?;
            println!("Records in table = {} \n Records: {:#?}", table, page);
        }

        Commands::Update {
            table,
            filter,
            patch,
        } => {
            let filter_value = Filter::parse(&filter).unwrap();
            let patch = util::from_pairs_to_btree(patch);
            engine.update(&table, filter_value, patch)?;
            println!("Records in {} \n from values {} updated", table, filter);
        }

        Commands::Delete { table, filter } => {
            let filter_value = Filter::parse(&filter).unwrap();
            engine.delete(&table, filter_value)?;
            println!("Records in {} \n with values {} deleted", table, filter);
        }

        Commands::ReplayWal => {
            engine.replay_wal_at_startup()?;
            println!("WAL replay complete");
        }

        Commands::TruncateWal => {
            engine.truncate_wal();
            println!("WAL truncated");
        }

        Commands::IntegrityCheck => {
            engine.integrity_check()?;
            println!("Intergity check completed");
        }
    }

    Ok(())
}
