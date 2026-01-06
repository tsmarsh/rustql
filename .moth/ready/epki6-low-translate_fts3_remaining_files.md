# Translate FTS3 Remaining Files - Tokenizers, Unicode, Porter

## Overview
Translate remaining FTS3 components including tokenizers, unicode support, and Porter stemmer.

## Source Reference
- `sqlite3/ext/fts3/fts3_tokenizer.c` - Tokenizer framework
- `sqlite3/ext/fts3/fts3_tokenize_vtab.c` - Tokenizer virtual table
- `sqlite3/ext/fts3/fts3_unicode.c` - Unicode tokenizer
- `sqlite3/ext/fts3/fts3_unicode2.c` - Unicode tables
- `sqlite3/ext/fts3/fts3_porter.c` - Porter stemmer
- `sqlite3/ext/fts3/fts3_icu.c` - ICU tokenizer
- `sqlite3/ext/fts3/fts3_hash.c` - Hash table

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### FTS3 Tokenizer
```rust
/// FTS3 tokenizer trait
pub trait Fts3Tokenizer: Send + Sync {
    /// Create a cursor for tokenizing text
    fn open(&self, text: &str) -> Result<Box<dyn Fts3TokenCursor>>;

    /// Get tokenizer name
    fn name(&self) -> &str;
}

/// Tokenizer cursor for iteration
pub trait Fts3TokenCursor {
    /// Get next token
    fn next(&mut self) -> Result<Option<Fts3Token>>;

    /// Reset to beginning
    fn reset(&mut self) -> Result<()>;
}

/// Token from FTS3 tokenizer
pub struct Fts3Token {
    /// Token text (normalized)
    pub token: String,
    /// Start byte offset in original
    pub start: i32,
    /// End byte offset in original
    pub end: i32,
    /// Position index (for phrase queries)
    pub position: i32,
}
```

### Simple Tokenizer
```rust
/// Simple ASCII tokenizer
pub struct SimpleTokenizer {
    /// Delimiter characters
    delimiters: Vec<char>,
}

impl SimpleTokenizer {
    pub fn new() -> Self {
        Self {
            delimiters: vec![' ', '\t', '\n', '\r'],
        }
    }
}

impl Fts3Tokenizer for SimpleTokenizer {
    fn open(&self, text: &str) -> Result<Box<dyn Fts3TokenCursor>> {
        Ok(Box::new(SimpleTokenCursor {
            text: text.to_string(),
            pos: 0,
            token_pos: 0,
            delimiters: self.delimiters.clone(),
        }))
    }

    fn name(&self) -> &str {
        "simple"
    }
}

struct SimpleTokenCursor {
    text: String,
    pos: usize,
    token_pos: i32,
    delimiters: Vec<char>,
}

impl Fts3TokenCursor for SimpleTokenCursor {
    fn next(&mut self) -> Result<Option<Fts3Token>> {
        // Skip delimiters
        while self.pos < self.text.len() {
            let c = self.text[self.pos..].chars().next().unwrap();
            if !self.delimiters.contains(&c) {
                break;
            }
            self.pos += c.len_utf8();
        }

        if self.pos >= self.text.len() {
            return Ok(None);
        }

        // Read token
        let start = self.pos;
        while self.pos < self.text.len() {
            let c = self.text[self.pos..].chars().next().unwrap();
            if self.delimiters.contains(&c) {
                break;
            }
            self.pos += c.len_utf8();
        }

        let token = self.text[start..self.pos].to_lowercase();
        let position = self.token_pos;
        self.token_pos += 1;

        Ok(Some(Fts3Token {
            token,
            start: start as i32,
            end: self.pos as i32,
            position,
        }))
    }

    fn reset(&mut self) -> Result<()> {
        self.pos = 0;
        self.token_pos = 0;
        Ok(())
    }
}
```

### Unicode Tokenizer
```rust
/// Unicode-aware tokenizer
pub struct UnicodeTokenizer {
    /// Remove diacritics
    remove_diacritics: bool,
    /// Token characters beyond alphanumeric
    token_chars: Vec<char>,
}

impl UnicodeTokenizer {
    pub fn new(args: &[&str]) -> Result<Self> {
        let mut tokenizer = Self {
            remove_diacritics: true,
            token_chars: Vec::new(),
        };

        // Parse options
        let mut i = 0;
        while i < args.len() {
            match args[i] {
                "remove_diacritics" => {
                    i += 1;
                    tokenizer.remove_diacritics = args.get(i)
                        .map(|s| *s != "0")
                        .unwrap_or(true);
                }
                "tokenchars" => {
                    i += 1;
                    if let Some(chars) = args.get(i) {
                        tokenizer.token_chars.extend(chars.chars());
                    }
                }
                _ => {}
            }
            i += 1;
        }

        Ok(tokenizer)
    }

    fn is_token_char(&self, c: char) -> bool {
        c.is_alphanumeric() || self.token_chars.contains(&c)
    }

    fn normalize(&self, s: &str) -> String {
        let mut result = s.to_lowercase();

        if self.remove_diacritics {
            // Use unicode normalization
            use unicode_normalization::UnicodeNormalization;
            result = result.nfd()
                .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
                .collect();
        }

        result
    }
}

impl Fts3Tokenizer for UnicodeTokenizer {
    fn open(&self, text: &str) -> Result<Box<dyn Fts3TokenCursor>> {
        Ok(Box::new(UnicodeTokenCursor {
            text: text.to_string(),
            pos: 0,
            token_pos: 0,
            remove_diacritics: self.remove_diacritics,
            token_chars: self.token_chars.clone(),
        }))
    }

    fn name(&self) -> &str {
        "unicode61"
    }
}
```

### Porter Stemmer
```rust
/// Porter stemmer tokenizer wrapper
pub struct PorterTokenizer {
    /// Wrapped tokenizer
    inner: Box<dyn Fts3Tokenizer>,
}

impl PorterTokenizer {
    pub fn new(inner: Box<dyn Fts3Tokenizer>) -> Self {
        Self { inner }
    }
}

impl Fts3Tokenizer for PorterTokenizer {
    fn open(&self, text: &str) -> Result<Box<dyn Fts3TokenCursor>> {
        let inner_cursor = self.inner.open(text)?;
        Ok(Box::new(PorterTokenCursor { inner: inner_cursor }))
    }

    fn name(&self) -> &str {
        "porter"
    }
}

struct PorterTokenCursor {
    inner: Box<dyn Fts3TokenCursor>,
}

impl Fts3TokenCursor for PorterTokenCursor {
    fn next(&mut self) -> Result<Option<Fts3Token>> {
        if let Some(mut token) = self.inner.next()? {
            token.token = porter_stem(&token.token);
            Ok(Some(token))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.inner.reset()
    }
}

/// Porter stemming algorithm implementation
pub fn porter_stem(word: &str) -> String {
    let mut s = word.to_string();

    // Step 1a: plurals
    if s.ends_with("sses") {
        s.truncate(s.len() - 2);
    } else if s.ends_with("ies") {
        s.truncate(s.len() - 2);
    } else if !s.ends_with("ss") && s.ends_with('s') {
        s.pop();
    }

    // Step 1b: past tense
    if s.ends_with("eed") {
        if measure(&s[..s.len()-3]) > 0 {
            s.pop();
        }
    } else if s.ends_with("ed") && contains_vowel(&s[..s.len()-2]) {
        s.truncate(s.len() - 2);
        step1b_suffix(&mut s);
    } else if s.ends_with("ing") && contains_vowel(&s[..s.len()-3]) {
        s.truncate(s.len() - 3);
        step1b_suffix(&mut s);
    }

    // Step 1c: y -> i
    if s.ends_with('y') && contains_vowel(&s[..s.len()-1]) {
        s.pop();
        s.push('i');
    }

    // Steps 2-5 omitted for brevity - full implementation needed

    s
}

fn measure(s: &str) -> i32 {
    // Count VC sequences
    let mut m = 0;
    let mut prev_vowel = false;

    for c in s.chars() {
        let is_vowel = matches!(c, 'a' | 'e' | 'i' | 'o' | 'u');
        if !is_vowel && prev_vowel {
            m += 1;
        }
        prev_vowel = is_vowel;
    }

    m
}

fn contains_vowel(s: &str) -> bool {
    s.chars().any(|c| matches!(c, 'a' | 'e' | 'i' | 'o' | 'u'))
}

fn step1b_suffix(s: &mut String) {
    if s.ends_with("at") || s.ends_with("bl") || s.ends_with("iz") {
        s.push('e');
    } else if ends_with_double_consonant(s) && !ends_with_any(s, &['l', 's', 'z']) {
        s.pop();
    } else if measure(s) == 1 && ends_cvc(s) {
        s.push('e');
    }
}
```

### Tokenizer Registry
```rust
/// Global tokenizer registry
lazy_static! {
    static ref FTS3_TOKENIZERS: RwLock<HashMap<String, TokenizerFactory>> = {
        let mut m = HashMap::new();

        m.insert("simple".to_string(), |_args| {
            Ok(Box::new(SimpleTokenizer::new()) as Box<dyn Fts3Tokenizer>)
        });

        m.insert("unicode61".to_string(), |args| {
            Ok(Box::new(UnicodeTokenizer::new(args)?) as Box<dyn Fts3Tokenizer>)
        });

        m.insert("porter".to_string(), |args| {
            let inner = if args.is_empty() {
                Box::new(SimpleTokenizer::new()) as Box<dyn Fts3Tokenizer>
            } else {
                fts3_create_tokenizer(&args[0], &args[1..])?
            };
            Ok(Box::new(PorterTokenizer::new(inner)) as Box<dyn Fts3Tokenizer>)
        });

        RwLock::new(m)
    };
}

pub type TokenizerFactory = fn(&[&str]) -> Result<Box<dyn Fts3Tokenizer>>;

pub fn fts3_create_tokenizer(name: &str, args: &[&str]) -> Result<Box<dyn Fts3Tokenizer>> {
    let factories = FTS3_TOKENIZERS.read().unwrap();
    let factory = factories.get(name)
        .ok_or_else(|| Error::with_message(
            ErrorCode::Error,
            format!("unknown tokenizer: {}", name)
        ))?;

    factory(args)
}

pub fn fts3_register_tokenizer(name: &str, factory: TokenizerFactory) {
    let mut factories = FTS3_TOKENIZERS.write().unwrap();
    factories.insert(name.to_string(), factory);
}
```

### FTS3 Hash Table
```rust
/// FTS3 hash table for term lookups
pub struct Fts3Hash {
    buckets: Vec<Option<Box<HashEntry>>>,
    count: usize,
}

struct HashEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    next: Option<Box<HashEntry>>,
}

impl Fts3Hash {
    pub fn new() -> Self {
        Self {
            buckets: vec![None; 128],
            count: 0,
        }
    }

    pub fn insert(&mut self, key: &[u8], value: Vec<u8>) {
        let hash = self.hash(key);
        let idx = hash % self.buckets.len();

        // Check for existing
        let mut current = &mut self.buckets[idx];
        while let Some(ref mut entry) = current {
            if entry.key == key {
                entry.value = value;
                return;
            }
            current = &mut entry.next;
        }

        // Insert new
        let new_entry = Box::new(HashEntry {
            key: key.to_vec(),
            value,
            next: self.buckets[idx].take(),
        });
        self.buckets[idx] = Some(new_entry);
        self.count += 1;

        // Resize if needed
        if self.count > self.buckets.len() * 2 {
            self.resize();
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let hash = self.hash(key);
        let idx = hash % self.buckets.len();

        let mut current = &self.buckets[idx];
        while let Some(ref entry) = current {
            if entry.key == key {
                return Some(&entry.value);
            }
            current = &entry.next;
        }

        None
    }

    fn hash(&self, key: &[u8]) -> usize {
        let mut h: u32 = 0;
        for &b in key {
            h = h.wrapping_mul(31).wrapping_add(b as u32);
        }
        h as usize
    }

    fn resize(&mut self) {
        let new_size = self.buckets.len() * 2;
        let mut new_buckets = vec![None; new_size];

        for bucket in self.buckets.drain(..) {
            let mut current = bucket;
            while let Some(mut entry) = current {
                let hash = self.hash(&entry.key);
                let idx = hash % new_size;
                current = entry.next.take();
                entry.next = new_buckets[idx].take();
                new_buckets[idx] = Some(entry);
            }
        }

        self.buckets = new_buckets;
    }
}
```

## Acceptance Criteria
- [ ] Simple tokenizer
- [ ] Unicode61 tokenizer
- [ ] Porter stemmer tokenizer
- [ ] Tokenizer registration API
- [ ] Diacritics removal
- [ ] Case folding
- [ ] Custom token characters
- [ ] Tokenizer virtual table
- [ ] Hash table for term storage
- [ ] ICU tokenizer (optional, depends on ICU library)
