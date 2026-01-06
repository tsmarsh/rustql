# Translate random.c - Random Number Generation

## Overview
Translate the random number generator used for RANDOM(), temporary filenames, and other randomness needs.

## Source Reference
- `sqlite3/src/random.c` - ~150 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### PRNG State
```rust
/// Pseudo-random number generator state
pub struct SqlitePrng {
    /// State array
    s: [u8; 256],
    /// Index i
    i: u8,
    /// Index j
    j: u8,
    /// Has been seeded
    is_init: bool,
}

/// Global PRNG instance
lazy_static! {
    static ref GLOBAL_PRNG: Mutex<SqlitePrng> = Mutex::new(SqlitePrng::new());
}
```

## RC4-Based PRNG

```rust
impl SqlitePrng {
    /// Create new uninitialized PRNG
    pub fn new() -> Self {
        Self {
            s: [0; 256],
            i: 0,
            j: 0,
            is_init: false,
        }
    }

    /// Initialize/seed the PRNG
    pub fn seed(&mut self, seed: &[u8]) {
        // Initialize state array
        for i in 0..256 {
            self.s[i] = i as u8;
        }

        // Key scheduling algorithm (KSA)
        let mut j: u8 = 0;
        for i in 0..256 {
            j = j.wrapping_add(self.s[i]).wrapping_add(seed[i % seed.len()]);
            self.s.swap(i, j as usize);
        }

        self.i = 0;
        self.j = 0;
        self.is_init = true;

        // Discard first 256 bytes (improves randomness)
        let mut discard = [0u8; 256];
        self.fill(&mut discard);
    }

    /// Auto-seed from system entropy
    pub fn auto_seed(&mut self) {
        let mut seed = [0u8; 256];

        // Try to get entropy from OS
        #[cfg(unix)]
        {
            if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
                use std::io::Read;
                let _ = file.read_exact(&mut seed);
            }
        }

        #[cfg(windows)]
        {
            // Use Windows crypto API
            unsafe {
                use windows_sys::Win32::Security::Cryptography::*;
                let mut prov: usize = 0;
                if CryptAcquireContextW(
                    &mut prov,
                    std::ptr::null(),
                    std::ptr::null(),
                    PROV_RSA_FULL,
                    CRYPT_VERIFYCONTEXT,
                ) != 0 {
                    CryptGenRandom(prov, seed.len() as u32, seed.as_mut_ptr());
                    CryptReleaseContext(prov, 0);
                }
            }
        }

        // Mix in time and process info
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        let time_bytes = now.as_nanos().to_le_bytes();
        for (i, &b) in time_bytes.iter().enumerate() {
            seed[i] ^= b;
        }

        let pid = std::process::id();
        let pid_bytes = pid.to_le_bytes();
        for (i, &b) in pid_bytes.iter().enumerate() {
            seed[128 + i] ^= b;
        }

        self.seed(&seed);
    }

    /// Generate random bytes
    pub fn fill(&mut self, buf: &mut [u8]) {
        if !self.is_init {
            self.auto_seed();
        }

        // Pseudo-random generation algorithm (PRGA)
        for byte in buf.iter_mut() {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);

            let k = self.s[(self.s[self.i as usize].wrapping_add(self.s[self.j as usize])) as usize];
            *byte = k;
        }
    }

    /// Generate a random u64
    pub fn next_u64(&mut self) -> u64 {
        let mut buf = [0u8; 8];
        self.fill(&mut buf);
        u64::from_le_bytes(buf)
    }

    /// Generate a random i64
    pub fn next_i64(&mut self) -> i64 {
        self.next_u64() as i64
    }

    /// Generate a random f64 in [0, 1)
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}
```

## Global Random Functions

```rust
/// Fill buffer with random bytes
pub fn sqlite3_randomness(buf: &mut [u8]) -> i32 {
    match GLOBAL_PRNG.lock() {
        Ok(mut prng) => {
            prng.fill(buf);
            buf.len() as i32
        }
        Err(_) => 0,
    }
}

/// Generate random blob for SQL RANDOMBLOB function
pub fn sqlite3_random_blob(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    sqlite3_randomness(&mut buf);
    buf
}

/// Generate random integer for SQL RANDOM function
pub fn sqlite3_random_int64() -> i64 {
    match GLOBAL_PRNG.lock() {
        Ok(mut prng) => prng.next_i64(),
        Err(_) => 0,
    }
}

/// Reset/reseed the PRNG
pub fn sqlite3_prng_reset() {
    if let Ok(mut prng) = GLOBAL_PRNG.lock() {
        prng.is_init = false;
    }
}

/// Seed the PRNG with specific value (for testing)
pub fn sqlite3_prng_seed(seed: &[u8]) {
    if let Ok(mut prng) = GLOBAL_PRNG.lock() {
        prng.seed(seed);
    }
}
```

## Temporary Name Generation

```rust
/// Generate random temporary filename
pub fn sqlite3_temp_filename(prefix: &str, suffix: &str) -> String {
    let mut random_part = [0u8; 16];
    sqlite3_randomness(&mut random_part);

    let hex: String = random_part.iter()
        .map(|b| format!("{:02x}", b))
        .collect();

    format!("{}{}{}", prefix, hex, suffix)
}

/// Generate unique temporary file path
pub fn sqlite3_temp_file_path() -> String {
    let temp_dir = std::env::temp_dir();
    let filename = sqlite3_temp_filename("sqlite_", ".tmp");
    temp_dir.join(filename).to_string_lossy().to_string()
}
```

## SQL Functions

```rust
/// RANDOM() SQL function
fn random_func(ctx: &mut Context, _args: &[&Value]) -> Result<()> {
    ctx.result_int(sqlite3_random_int64());
    Ok(())
}

/// RANDOMBLOB(N) SQL function
fn randomblob_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let n = args[0].as_int() as usize;

    if n > 1_000_000_000 {
        return Err(Error::with_message(ErrorCode::TooBig, "blob too big"));
    }

    let blob = sqlite3_random_blob(n);
    ctx.result_blob(&blob);
    Ok(())
}

/// Register random functions
pub fn register_random_functions(conn: &mut Connection) {
    conn.create_function(
        "random",
        0,
        FuncFlags::empty(),  // Not deterministic!
        None,
        random_func,
        None,
    ).ok();

    conn.create_function(
        "randomblob",
        1,
        FuncFlags::empty(),  // Not deterministic!
        None,
        randomblob_func,
        None,
    ).ok();
}
```

## Testing Support

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prng_reproducible() {
        let seed = b"test seed for reproducibility";

        sqlite3_prng_seed(seed);
        let a = sqlite3_random_int64();

        sqlite3_prng_seed(seed);
        let b = sqlite3_random_int64();

        assert_eq!(a, b);
    }

    #[test]
    fn test_prng_different_seeds() {
        sqlite3_prng_seed(b"seed1");
        let a = sqlite3_random_int64();

        sqlite3_prng_seed(b"seed2");
        let b = sqlite3_random_int64();

        assert_ne!(a, b);
    }

    #[test]
    fn test_random_blob() {
        let blob = sqlite3_random_blob(100);
        assert_eq!(blob.len(), 100);

        // Very unlikely to be all zeros
        assert!(blob.iter().any(|&b| b != 0));
    }

    #[test]
    fn test_temp_filename_unique() {
        let name1 = sqlite3_temp_filename("test_", ".tmp");
        let name2 = sqlite3_temp_filename("test_", ".tmp");

        assert_ne!(name1, name2);
    }
}
```

## Acceptance Criteria
- [ ] RC4-based PRNG implementation
- [ ] Auto-seeding from OS entropy
- [ ] /dev/urandom on Unix
- [ ] CryptGenRandom on Windows
- [ ] Time and PID mixing
- [ ] sqlite3_randomness() function
- [ ] RANDOM() SQL function
- [ ] RANDOMBLOB(N) SQL function
- [ ] Temporary filename generation
- [ ] Deterministic seeding for testing
- [ ] Thread-safe global instance
- [ ] Initial bytes discarded for quality
