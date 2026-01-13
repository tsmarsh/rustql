use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lazy_static::lazy_static;

use super::fts3::Fts3Table;

lazy_static! {
    static ref FTS3_REGISTRY: Mutex<HashMap<String, Arc<Mutex<Fts3Table>>>> =
        Mutex::new(HashMap::new());
}

fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub fn register_table(table: Fts3Table) -> Arc<Mutex<Fts3Table>> {
    let name = normalize_name(&table.name);
    let table = Arc::new(Mutex::new(table));
    let mut registry = FTS3_REGISTRY.lock().expect("fts3 registry lock");
    registry.insert(name, Arc::clone(&table));
    table
}

pub fn get_table(name: &str) -> Option<Arc<Mutex<Fts3Table>>> {
    let registry = FTS3_REGISTRY.lock().expect("fts3 registry lock");
    registry.get(&normalize_name(name)).cloned()
}
