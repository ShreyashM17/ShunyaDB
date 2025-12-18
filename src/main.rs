fn main() {
    const PAGE_MAGIC: u32 = u32::from_be_bytes(*b"SHDB");
    println!("ShunyaDB starting... {:#?}", PAGE_MAGIC);
}