use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lazy_static::lazy_static;

use super::fts3::Fts3Table;
use super::tokenize::Fts3TokenizeTable;

lazy_static! {
    static ref FTS3_REGISTRY: Mutex<HashMap<String, Arc<Mutex<Fts3Table>>>> =
        Mutex::new(HashMap::new());
    static ref FTS3_TOKENIZE_REGISTRY: Mutex<HashMap<String, Arc<Mutex<Fts3TokenizeTable>>>> =
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

pub fn register_tokenize_table(table: Fts3TokenizeTable) -> Arc<Mutex<Fts3TokenizeTable>> {
    let name = normalize_name(&table.name);
    let table = Arc::new(Mutex::new(table));
    let mut registry = FTS3_TOKENIZE_REGISTRY
        .lock()
        .expect("fts3 tokenize registry lock");
    registry.insert(name, Arc::clone(&table));
    table
}

pub fn get_tokenize_table(name: &str) -> Option<Arc<Mutex<Fts3TokenizeTable>>> {
    let registry = FTS3_TOKENIZE_REGISTRY
        .lock()
        .expect("fts3 tokenize registry lock");
    registry.get(&normalize_name(name)).cloned()
}
