# Translate sqlite3_rsync.c - Database Rsync

## Overview
Translate database rsync tool for efficient database synchronization.

## Source Reference
- `sqlite3/tool/sqlite3_rsync.c` - Database rsync implementation (2,397 lines)

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Rsync Configuration
```rust
/// Database rsync configuration
#[derive(Debug, Clone)]
pub struct RsyncConfig {
    /// Source database (local or remote)
    pub source: DbLocation,
    /// Target database (local or remote)
    pub target: DbLocation,
    /// Page size (auto-detect if 0)
    pub page_size: u32,
    /// Verbose output
    pub verbose: bool,
    /// Dry run (don't apply changes)
    pub dry_run: bool,
    /// SSH command for remote
    pub ssh_command: String,
}

#[derive(Debug, Clone)]
pub enum DbLocation {
    /// Local file path
    Local(String),
    /// Remote via SSH (host:path)
    Remote { host: String, path: String },
}

impl DbLocation {
    pub fn parse(s: &str) -> Self {
        if let Some(idx) = s.find(':') {
            // Check if it's a Windows path like C:\...
            if idx == 1 && s.len() > 2 && s.chars().nth(0).map(|c| c.is_ascii_alphabetic()).unwrap_or(false) {
                return DbLocation::Local(s.to_string());
            }

            DbLocation::Remote {
                host: s[..idx].to_string(),
                path: s[idx+1..].to_string(),
            }
        } else {
            DbLocation::Local(s.to_string())
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, DbLocation::Remote { .. })
    }
}

impl Default for RsyncConfig {
    fn default() -> Self {
        Self {
            source: DbLocation::Local(String::new()),
            target: DbLocation::Local(String::new()),
            page_size: 0,
            verbose: false,
            dry_run: false,
            ssh_command: "ssh".to_string(),
        }
    }
}
```

### Page Hash
```rust
/// Page hash for comparison
#[derive(Debug, Clone)]
pub struct PageHash {
    /// Page number
    pub pgno: u32,
    /// Hash value (SHA-1 or similar)
    pub hash: [u8; 20],
}

/// Database hash manifest
#[derive(Debug)]
pub struct DbManifest {
    /// Page size
    pub page_size: u32,
    /// Total pages
    pub n_pages: u32,
    /// Page hashes
    pub pages: Vec<PageHash>,
    /// Change counter from header
    pub change_counter: u32,
    /// Schema cookie
    pub schema_cookie: u32,
}

impl DbManifest {
    /// Build manifest from database file
    pub fn from_file(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;

        // Read header
        let mut header = [0u8; 100];
        file.read_exact(&mut header)?;

        let page_size = u16::from_be_bytes([header[16], header[17]]) as u32;
        let page_size = if page_size == 1 { 65536 } else { page_size };

        let file_size = file.metadata()?.len();
        let n_pages = (file_size / page_size as u64) as u32;

        let change_counter = u32::from_be_bytes([
            header[24], header[25], header[26], header[27]
        ]);
        let schema_cookie = u32::from_be_bytes([
            header[40], header[41], header[42], header[43]
        ]);

        // Hash all pages
        let mut pages = Vec::with_capacity(n_pages as usize);
        file.seek(SeekFrom::Start(0))?;

        let mut page_buf = vec![0u8; page_size as usize];
        for pgno in 1..=n_pages {
            file.read_exact(&mut page_buf)?;

            let mut hasher = Sha1::new();
            hasher.update(&page_buf);
            let hash: [u8; 20] = hasher.finalize().into();

            pages.push(PageHash { pgno, hash });
        }

        Ok(Self {
            page_size,
            n_pages,
            pages,
            change_counter,
            schema_cookie,
        })
    }

    /// Serialize manifest for transmission
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::new();

        // Header
        data.extend_from_slice(&self.page_size.to_le_bytes());
        data.extend_from_slice(&self.n_pages.to_le_bytes());
        data.extend_from_slice(&self.change_counter.to_le_bytes());
        data.extend_from_slice(&self.schema_cookie.to_le_bytes());

        // Page hashes
        for page in &self.pages {
            data.extend_from_slice(&page.pgno.to_le_bytes());
            data.extend_from_slice(&page.hash);
        }

        data
    }

    /// Deserialize manifest
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 16 {
            return Err(Error::with_message(ErrorCode::Error, "invalid manifest"));
        }

        let page_size = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let n_pages = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let change_counter = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let schema_cookie = u32::from_le_bytes(data[12..16].try_into().unwrap());

        let mut pages = Vec::with_capacity(n_pages as usize);
        let mut pos = 16;

        while pos + 24 <= data.len() {
            let pgno = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
            let mut hash = [0u8; 20];
            hash.copy_from_slice(&data[pos+4..pos+24]);
            pages.push(PageHash { pgno, hash });
            pos += 24;
        }

        Ok(Self {
            page_size,
            n_pages,
            pages,
            change_counter,
            schema_cookie,
        })
    }
}
```

### Delta Operations
```rust
/// Page delta for synchronization
#[derive(Debug)]
pub struct PageDelta {
    /// Page number
    pub pgno: u32,
    /// Page data
    pub data: Vec<u8>,
}

/// Compute delta between two manifests
pub fn compute_delta(source: &DbManifest, target: &DbManifest) -> Vec<u32> {
    let mut changed_pages = Vec::new();

    // Build hash map for target
    let target_hashes: HashMap<u32, &[u8; 20]> = target.pages.iter()
        .map(|p| (p.pgno, &p.hash))
        .collect();

    // Find changed pages
    for page in &source.pages {
        match target_hashes.get(&page.pgno) {
            None => {
                // Page doesn't exist in target
                changed_pages.push(page.pgno);
            }
            Some(target_hash) => {
                if &page.hash != *target_hash {
                    // Page differs
                    changed_pages.push(page.pgno);
                }
            }
        }
    }

    // Find pages only in target (need to truncate)
    if source.n_pages < target.n_pages {
        // Target needs truncation
    }

    changed_pages
}
```

## Rsync Protocol

### Local-to-Local Sync
```rust
/// Sync two local databases
pub fn sync_local(source: &str, target: &str, config: &RsyncConfig) -> Result<SyncStats> {
    let mut stats = SyncStats::default();

    // Build manifests
    let source_manifest = DbManifest::from_file(source)?;

    let target_manifest = if std::path::Path::new(target).exists() {
        DbManifest::from_file(target)?
    } else {
        // Create empty target
        std::fs::copy(source, target)?;
        stats.pages_copied = source_manifest.n_pages;
        return Ok(stats);
    };

    // Verify page sizes match
    if source_manifest.page_size != target_manifest.page_size {
        return Err(Error::with_message(
            ErrorCode::Error,
            "page size mismatch",
        ));
    }

    // Compute delta
    let changed_pages = compute_delta(&source_manifest, &target_manifest);

    if config.verbose {
        eprintln!("{} pages changed out of {}",
            changed_pages.len(), source_manifest.n_pages);
    }

    if config.dry_run {
        stats.pages_changed = changed_pages.len() as u32;
        return Ok(stats);
    }

    // Apply delta
    let mut source_file = File::open(source)?;
    let mut target_file = OpenOptions::new()
        .write(true)
        .open(target)?;

    let page_size = source_manifest.page_size as usize;
    let mut page_buf = vec![0u8; page_size];

    for pgno in &changed_pages {
        // Read from source
        let offset = (*pgno as u64 - 1) * page_size as u64;
        source_file.seek(SeekFrom::Start(offset))?;
        source_file.read_exact(&mut page_buf)?;

        // Write to target
        target_file.seek(SeekFrom::Start(offset))?;
        target_file.write_all(&page_buf)?;

        stats.pages_copied += 1;
    }

    // Handle truncation if source is smaller
    if source_manifest.n_pages < target_manifest.n_pages {
        target_file.set_len(source_manifest.n_pages as u64 * page_size as u64)?;
    }

    stats.pages_changed = changed_pages.len() as u32;
    Ok(stats)
}

#[derive(Debug, Default)]
pub struct SyncStats {
    pub pages_changed: u32,
    pub pages_copied: u32,
    pub bytes_transferred: u64,
}
```

### Remote Sync via SSH
```rust
/// Sync with remote database
pub fn sync_remote(config: &RsyncConfig) -> Result<SyncStats> {
    match (&config.source, &config.target) {
        (DbLocation::Local(src), DbLocation::Remote { host, path }) => {
            sync_push(src, host, path, config)
        }
        (DbLocation::Remote { host, path }, DbLocation::Local(tgt)) => {
            sync_pull(host, path, tgt, config)
        }
        (DbLocation::Remote { .. }, DbLocation::Remote { .. }) => {
            Err(Error::with_message(
                ErrorCode::Error,
                "cannot sync between two remote databases directly",
            ))
        }
        (DbLocation::Local(src), DbLocation::Local(tgt)) => {
            sync_local(src, tgt, config)
        }
    }
}

/// Push local database to remote
fn sync_push(source: &str, host: &str, remote_path: &str, config: &RsyncConfig) -> Result<SyncStats> {
    // Build local manifest
    let source_manifest = DbManifest::from_file(source)?;

    // Get remote manifest via SSH
    let output = Command::new(&config.ssh_command)
        .arg(host)
        .arg("sqlite3_rsync")
        .arg("--manifest")
        .arg(remote_path)
        .output()?;

    let target_manifest = DbManifest::from_bytes(&output.stdout)?;

    // Compute delta
    let changed_pages = compute_delta(&source_manifest, &target_manifest);

    if config.dry_run {
        return Ok(SyncStats {
            pages_changed: changed_pages.len() as u32,
            ..Default::default()
        });
    }

    // Send changed pages
    let mut page_data = Vec::new();
    let mut source_file = File::open(source)?;
    let page_size = source_manifest.page_size as usize;
    let mut page_buf = vec![0u8; page_size];

    for pgno in &changed_pages {
        let offset = (*pgno as u64 - 1) * page_size as u64;
        source_file.seek(SeekFrom::Start(offset))?;
        source_file.read_exact(&mut page_buf)?;

        page_data.extend_from_slice(&pgno.to_le_bytes());
        page_data.extend_from_slice(&page_buf);
    }

    // Send to remote
    let mut child = Command::new(&config.ssh_command)
        .arg(host)
        .arg("sqlite3_rsync")
        .arg("--apply")
        .arg(remote_path)
        .stdin(Stdio::piped())
        .spawn()?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(&source_manifest.n_pages.to_le_bytes())?;
        stdin.write_all(&page_data)?;
    }

    child.wait()?;

    Ok(SyncStats {
        pages_changed: changed_pages.len() as u32,
        pages_copied: changed_pages.len() as u32,
        bytes_transferred: page_data.len() as u64,
    })
}

/// Pull remote database to local
fn sync_pull(host: &str, remote_path: &str, target: &str, config: &RsyncConfig) -> Result<SyncStats> {
    // Get remote manifest
    let output = Command::new(&config.ssh_command)
        .arg(host)
        .arg("sqlite3_rsync")
        .arg("--manifest")
        .arg(remote_path)
        .output()?;

    let source_manifest = DbManifest::from_bytes(&output.stdout)?;

    // Build local manifest
    let target_manifest = if std::path::Path::new(target).exists() {
        DbManifest::from_file(target)?
    } else {
        DbManifest {
            page_size: source_manifest.page_size,
            n_pages: 0,
            pages: Vec::new(),
            change_counter: 0,
            schema_cookie: 0,
        }
    };

    // Compute delta
    let changed_pages = compute_delta(&source_manifest, &target_manifest);

    if config.dry_run {
        return Ok(SyncStats {
            pages_changed: changed_pages.len() as u32,
            ..Default::default()
        });
    }

    // Request changed pages from remote
    let pages_str = changed_pages.iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let output = Command::new(&config.ssh_command)
        .arg(host)
        .arg("sqlite3_rsync")
        .arg("--pages")
        .arg(&pages_str)
        .arg(remote_path)
        .output()?;

    // Apply received pages
    let mut target_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(target)?;

    let page_size = source_manifest.page_size as usize;
    let mut pos = 0;

    while pos + 4 + page_size <= output.stdout.len() {
        let pgno = u32::from_le_bytes(output.stdout[pos..pos+4].try_into().unwrap());
        let page_data = &output.stdout[pos+4..pos+4+page_size];

        let offset = (pgno as u64 - 1) * page_size as u64;
        target_file.seek(SeekFrom::Start(offset))?;
        target_file.write_all(page_data)?;

        pos += 4 + page_size;
    }

    // Truncate if needed
    target_file.set_len(source_manifest.n_pages as u64 * page_size as u64)?;

    Ok(SyncStats {
        pages_changed: changed_pages.len() as u32,
        pages_copied: changed_pages.len() as u32,
        bytes_transferred: output.stdout.len() as u64,
    })
}
```

### CLI Entry Point
```rust
/// Run sqlite3_rsync CLI
pub fn rsync_main(args: &[String]) -> Result<i32> {
    let config = parse_args(args)?;

    if config.verbose {
        eprintln!("Source: {:?}", config.source);
        eprintln!("Target: {:?}", config.target);
    }

    let stats = sync_remote(&config)?;

    if config.verbose {
        eprintln!("Pages changed: {}", stats.pages_changed);
        eprintln!("Pages copied: {}", stats.pages_copied);
        eprintln!("Bytes transferred: {}", stats.bytes_transferred);
    }

    Ok(0)
}
```

## Acceptance Criteria
- [ ] Local database manifest generation
- [ ] Page hashing (SHA-1)
- [ ] Delta computation
- [ ] Local-to-local sync
- [ ] Push to remote via SSH
- [ ] Pull from remote via SSH
- [ ] Page size detection
- [ ] Database truncation handling
- [ ] Verbose mode
- [ ] Dry run mode
- [ ] CLI argument parsing
- [ ] Progress reporting

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `rsync.test` - Database rsync tests (if exists)
