use crate::error::Result;

use super::fts3::{Fts3Token, Fts3Tokenizer};

pub struct Fts3TokenizeTable {
    pub name: String,
    pub tokenizer: Box<dyn Fts3Tokenizer>,
}

impl Fts3TokenizeTable {
    pub fn new(name: impl Into<String>, tokenizer: Box<dyn Fts3Tokenizer>) -> Self {
        Self {
            name: name.into(),
            tokenizer,
        }
    }

    pub fn tokenize(&self, input: &str) -> Result<Vec<Fts3Token>> {
        self.tokenizer.tokenize(input)
    }
}
