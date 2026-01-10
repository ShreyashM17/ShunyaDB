use shunyadb::engine::engine::Engine;
use std::{collections::BTreeMap, fs};
use shunyadb::storage::record::FieldValue;

fn parse_value(input: &str) -> BTreeMap<String, FieldValue> {
    let mut map = BTreeMap::new();
    map.insert("value".to_string(), FieldValue::Str(input.to_string()));
    map
}

fn main() -> anyhow::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let base = std::path::Path::new("./data");
    fs::create_dir_all(base)?;
    let mut engine = Engine::open(base)?;

    match args[1].as_str() {
        "put" => {
            engine.put(args[2].clone(), parse_value(&args[3]))?;
        }
        "get" => {
            let rec = engine.get(&args[2], u64::MAX);
            println!("{:?}", rec);
        }
        "flush" => {
            engine.flush()?;
        }
        "compact" => {
            engine.maybe_compact()?;
        }
        "metrics" => {
            println!("{:#?}", engine.metrics());
        }
        _ => {
            eprintln!("unknown command");
        }
    }

    Ok(())
}
