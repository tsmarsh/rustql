# Translate fts5_storage.c and fts5_tokenize.c - FTS5 Storage and Tokenization

## Overview
Translate FTS5 content storage and tokenization subsystems.

## Source Reference
- `sqlite3/ext/fts5/fts5_storage.c` - Content storage
- `sqlite3/ext/fts5/fts5_tokenize.c` - Tokenizer framework

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Storage
```rust
/// FTS5 storage layer
pub struct Fts5Storage {
    /// Database connection
    db: *mut Connection,
    /// Configuration
    config: Fts5Config,
    /// Content table name
    content_table: String,
    /// Docsize table name
    docsize_table: String,
    /// Prepared statements
    stmts: StorageStmts,
}

struct StorageStmts {
    /// Insert content
    insert: Option<PreparedStmt>,
    /// Delete content
    delete: Option<PreparedStmt>,
    /// Select content by rowid
    select: Option<PreparedStmt>,
    /// Insert docsize
    insert_docsize: Option<PreparedStmt>,
    /// Select docsize
    select_docsize: Option<PreparedStmt>,
}
```

### Tokenizer
```rust
/// Tokenizer trait
pub trait Fts5Tokenizer: Send + Sync {
    /// Tokenize text and call callback for each token
    fn tokenize(
        &self,
        text: &str,
        reason: TokenizeReason,
        callback: &mut dyn FnMut(&str, i32, i32) -> Result<()>,
    ) -> Result<()>;
}

/// Why tokenization is happening
#[derive(Debug, Clone, Copy)]
pub enum TokenizeReason {
    /// Document being indexed
    Document,
    /// Query being parsed
    Query,
    /// Auxiliary function call
    Aux,
}

/// Token from tokenizer
pub struct Fts5Token {
    /// Token text
    pub text: String,
    /// Start byte offset
    pub start: i32,
    /// End byte offset
    pub end: i32,
    /// Position index
    pub position: i32,
}
```

## Built-in Tokenizers

### Unicode61 Tokenizer
```rust
/// Unicode61 tokenizer - default FTS5 tokenizer
pub struct Unicode61Tokenizer {
    /// Remove diacritics
    remove_diacritics: bool,
    /// Token characters (beyond alphanumeric)
    token_chars: Vec<char>,
    /// Separator characters
    separators: Vec<char>,
}

impl Unicode61Tokenizer {
    pub fn new(args: &[&str]) -> Result<Self> {
        let mut tokenizer = Self {
            remove_diacritics: true,
            token_chars: Vec::new(),
            separators: Vec::new(),
        };

        // Parse options
        let mut i = 0;
        while i < args.len() {
            match args[i] {
                "remove_diacritics" => {
                    i += 1;
                    tokenizer.remove_diacritics = args.get(i).map(|s| *s == "1").unwrap_or(true);
                }
                "tokenchars" => {
                    i += 1;
                    if let Some(chars) = args.get(i) {
                        tokenizer.token_chars.extend(chars.chars());
                    }
                }
                "separators" => {
                    i += 1;
                    if let Some(chars) = args.get(i) {
                        tokenizer.separators.extend(chars.chars());
                    }
                }
                _ => {}
            }
            i += 1;
        }

        Ok(tokenizer)
    }

    fn is_token_char(&self, c: char) -> bool {
        c.is_alphanumeric() ||
        self.token_chars.contains(&c) ||
        (!self.separators.is_empty() && !self.separators.contains(&c))
    }
}

impl Fts5Tokenizer for Unicode61Tokenizer {
    fn tokenize(
        &self,
        text: &str,
        _reason: TokenizeReason,
        callback: &mut dyn FnMut(&str, i32, i32) -> Result<()>,
    ) -> Result<()> {
        let mut token_start = 0;
        let mut in_token = false;
        let mut position = 0;

        for (i, c) in text.char_indices() {
            let is_token = self.is_token_char(c);

            if is_token && !in_token {
                // Start of token
                token_start = i;
                in_token = true;
            } else if !is_token && in_token {
                // End of token
                let token_text = &text[token_start..i];
                let processed = self.process_token(token_text);
                callback(&processed, token_start as i32, i as i32)?;
                position += 1;
                in_token = false;
            }
        }

        // Handle final token
        if in_token {
            let token_text = &text[token_start..];
            let processed = self.process_token(token_text);
            callback(&processed, token_start as i32, text.len() as i32)?;
        }

        Ok(())
    }
}

impl Unicode61Tokenizer {
    fn process_token(&self, token: &str) -> String {
        let mut result = token.to_lowercase();

        if self.remove_diacritics {
            result = self.strip_diacritics(&result);
        }

        result
    }

    fn strip_diacritics(&self, s: &str) -> String {
        // Use unicode normalization to remove diacritics
        use unicode_normalization::UnicodeNormalization;

        s.nfd()
            .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
            .collect()
    }
}
```

### Porter Stemmer Tokenizer
```rust
/// Porter stemmer wrapper tokenizer
pub struct PorterTokenizer {
    /// Wrapped tokenizer
    inner: Box<dyn Fts5Tokenizer>,
}

impl PorterTokenizer {
    pub fn new(inner: Box<dyn Fts5Tokenizer>) -> Self {
        Self { inner }
    }
}

impl Fts5Tokenizer for PorterTokenizer {
    fn tokenize(
        &self,
        text: &str,
        reason: TokenizeReason,
        callback: &mut dyn FnMut(&str, i32, i32) -> Result<()>,
    ) -> Result<()> {
        self.inner.tokenize(text, reason, &mut |token, start, end| {
            let stemmed = porter_stem(token);
            callback(&stemmed, start, end)
        })
    }
}

/// Porter stemming algorithm
fn porter_stem(word: &str) -> String {
    // Simplified Porter stemmer
    let mut s = word.to_string();

    // Step 1a
    if s.ends_with("sses") {
        s.truncate(s.len() - 2);
    } else if s.ends_with("ies") {
        s.truncate(s.len() - 2);
    } else if !s.ends_with("ss") && s.ends_with("s") {
        s.pop();
    }

    // Step 1b
    if s.ends_with("eed") {
        if measure(&s[..s.len()-3]) > 0 {
            s.pop();
        }
    } else if (s.ends_with("ed") && has_vowel(&s[..s.len()-2])) ||
              (s.ends_with("ing") && has_vowel(&s[..s.len()-3])) {
        if s.ends_with("ed") {
            s.truncate(s.len() - 2);
        } else {
            s.truncate(s.len() - 3);
        }

        // Additional rules
        if s.ends_with("at") || s.ends_with("bl") || s.ends_with("iz") {
            s.push('e');
        }
    }

    // Additional steps omitted for brevity

    s
}

fn measure(s: &str) -> i32 {
    // Count VC sequences
    0
}

fn has_vowel(s: &str) -> bool {
    s.chars().any(|c| matches!(c, 'a' | 'e' | 'i' | 'o' | 'u'))
}
```

### Trigram Tokenizer
```rust
/// Trigram tokenizer for substring matching
pub struct TrigramTokenizer {
    case_sensitive: bool,
}

impl TrigramTokenizer {
    pub fn new(args: &[&str]) -> Result<Self> {
        let case_sensitive = args.iter()
            .any(|&s| s == "case_sensitive" || s == "1");

        Ok(Self { case_sensitive })
    }
}

impl Fts5Tokenizer for TrigramTokenizer {
    fn tokenize(
        &self,
        text: &str,
        _reason: TokenizeReason,
        callback: &mut dyn FnMut(&str, i32, i32) -> Result<()>,
    ) -> Result<()> {
        let text = if self.case_sensitive {
            text.to_string()
        } else {
            text.to_lowercase()
        };

        let chars: Vec<char> = text.chars().collect();

        for i in 0..chars.len().saturating_sub(2) {
            let trigram: String = chars[i..i+3].iter().collect();
            callback(&trigram, i as i32, (i + 3) as i32)?;
        }

        Ok(())
    }
}
```

## Tokenizer Registry

```rust
/// Tokenizer factory type
pub type TokenizerFactory = fn(&[&str]) -> Result<Box<dyn Fts5Tokenizer>>;

lazy_static! {
    static ref TOKENIZERS: RwLock<HashMap<String, TokenizerFactory>> = {
        let mut m = HashMap::new();

        m.insert("unicode61".to_string(), |args| {
            Ok(Box::new(Unicode61Tokenizer::new(args)?) as Box<dyn Fts5Tokenizer>)
        });

        m.insert("porter".to_string(), |args| {
            let inner = if args.is_empty() {
                Box::new(Unicode61Tokenizer::new(&[])?)
            } else {
                create_tokenizer(&args[0], &args[1..])?
            };
            Ok(Box::new(PorterTokenizer::new(inner)) as Box<dyn Fts5Tokenizer>)
        });

        m.insert("trigram".to_string(), |args| {
            Ok(Box::new(TrigramTokenizer::new(args)?) as Box<dyn Fts5Tokenizer>)
        });

        RwLock::new(m)
    };
}

pub fn create_tokenizer(name: &str, args: &[&str]) -> Result<Box<dyn Fts5Tokenizer>> {
    let factories = TOKENIZERS.read().unwrap();
    let factory = factories.get(name)
        .ok_or_else(|| Error::with_message(
            ErrorCode::Error,
            format!("unknown tokenizer: {}", name)
        ))?;

    factory(args)
}

pub fn register_tokenizer(name: &str, factory: TokenizerFactory) {
    let mut factories = TOKENIZERS.write().unwrap();
    factories.insert(name.to_string(), factory);
}
```

## Storage Operations

```rust
impl Fts5Storage {
    /// Insert content row
    pub fn insert(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        if self.config.content_mode == ContentMode::Contentless {
            // Don't store content
            return Ok(());
        }

        let stmt = self.stmts.insert.get_or_insert_with(|| {
            let cols: Vec<_> = self.config.columns.iter()
                .map(|c| format!("c{}", c))
                .collect();
            let placeholders = vec!["?"; cols.len() + 1].join(", ");
            let sql = format!(
                "INSERT INTO '{}' (rowid, {}) VALUES ({})",
                self.content_table,
                cols.join(", "),
                placeholders
            );
            self.db.prepare(&sql).unwrap()
        });

        stmt.bind_int64(1, rowid)?;
        for (i, value) in values.iter().enumerate() {
            stmt.bind_text(i as i32 + 2, value)?;
        }

        stmt.step()?;
        stmt.reset()?;

        Ok(())
    }

    /// Get content row
    pub fn get(&mut self, rowid: i64) -> Result<Vec<String>> {
        let stmt = self.stmts.select.get_or_insert_with(|| {
            let sql = format!(
                "SELECT * FROM '{}' WHERE rowid = ?",
                self.content_table
            );
            self.db.prepare(&sql).unwrap()
        });

        stmt.bind_int64(1, rowid)?;

        if stmt.step()? == StepResult::Row {
            let mut values = Vec::new();
            for i in 0..self.config.columns.len() {
                values.push(stmt.column_text(i as i32)?);
            }
            stmt.reset()?;
            Ok(values)
        } else {
            stmt.reset()?;
            Err(Error::with_message(ErrorCode::NotFound, "row not found"))
        }
    }
}
```

## Acceptance Criteria
- [ ] Content storage (normal mode)
- [ ] External content mode
- [ ] Contentless mode
- [ ] Docsize tracking
- [ ] Unicode61 tokenizer
- [ ] Porter stemmer tokenizer
- [ ] Trigram tokenizer
- [ ] Custom tokenizer registration
- [ ] Diacritics removal
- [ ] Case folding
- [ ] Tokenizer options parsing
- [ ] Position tracking
