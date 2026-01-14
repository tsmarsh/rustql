use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lazy_static::lazy_static;

use super::main::Fts5Table;

lazy_static! {
    static ref FTS5_REGISTRY: Mutex<HashMap<String, Arc<Mutex<Fts5Table>>>> =
        Mutex::new(HashMap::new());
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub fn register_table(table: Fts5Table) -> Arc<Mutex<Fts5Table>> {
    let name = normalize_name(&table.name);
    let table = Arc::new(Mutex::new(table));
    let mut registry = FTS5_REGISTRY.lock().expect("fts5 registry lock");
    registry.insert(name, Arc::clone(&table));
    table
}

pub fn get_table(name: &str) -> Option<Arc<Mutex<Fts5Table>>> {
    let registry = FTS5_REGISTRY.lock().expect("fts5 registry lock");
    registry.get(&normalize_name(name)).cloned()
}
