use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ShunyaDB", version = "0.1")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Insert {
        table: String,
        pairs: Vec<String>,
    },
    Get {
        table: String,
        filter: String,
    },
    GetAll {
        table: String,
    },
    Update {
        table: String,
        filter: String,
        patch: Vec<String>,
    },
    Delete {
        table: String,
        filter: String,
    },
    ReplayWal,
    TruncateWal,
    IntegrityCheck,
}
