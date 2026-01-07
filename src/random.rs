//! Random Number Generation
//!
//! RC4-based pseudo-random number generator for SQLite compatibility.
//! Corresponds to SQLite's random.c.

use std::sync::Mutex;

// ============================================================================
// PRNG State
// ============================================================================

/// Pseudo-random number generator state (RC4-based)
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

impl Default for SqlitePrng {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlitePrng {
    /// Create new uninitialized PRNG
    pub const fn new() -> Self {
        Self {
            s: [0; 256],
            i: 0,
            j: 0,
            is_init: false,
        }
    }

    /// Initialize/seed the PRNG using RC4 key scheduling algorithm
    pub fn seed(&mut self, seed: &[u8]) {
        // Handle empty seed
        if seed.is_empty() {
            self.auto_seed();
            return;
        }

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
            // Use getrandom crate for cross-platform entropy
            // For now, fall through to time-based seeding
        }

        // Mix in time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        let time_bytes = now.as_nanos().to_le_bytes();
        for (i, &b) in time_bytes.iter().enumerate() {
            seed[i] ^= b;
        }

        // Mix in subsec nanos for more entropy
        let nanos = now.subsec_nanos().to_le_bytes();
        for (i, &b) in nanos.iter().enumerate() {
            seed[16 + i] ^= b;
        }

        // Mix in process ID
        let pid = std::process::id();
        let pid_bytes = pid.to_le_bytes();
        for (i, &b) in pid_bytes.iter().enumerate() {
            seed[128 + i] ^= b;
        }

        // Mix in thread ID hash
        let thread_id = std::thread::current().id();
        let thread_hash = format!("{:?}", thread_id);
        for (i, b) in thread_hash.bytes().enumerate() {
            if i + 140 < 256 {
                seed[140 + i] ^= b;
            }
        }

        // Now seed with our entropy
        // Initialize state array
        for i in 0..256 {
            self.s[i] = i as u8;
        }

        // Key scheduling algorithm (KSA)
        let mut j: u8 = 0;
        for i in 0..256 {
            j = j.wrapping_add(self.s[i]).wrapping_add(seed[i]);
            self.s.swap(i, j as usize);
        }

        self.i = 0;
        self.j = 0;
        self.is_init = true;

        // Discard first 256 bytes
        let mut discard = [0u8; 256];
        self.fill_internal(&mut discard);
    }

    /// Generate random bytes (internal, doesn't check init)
    fn fill_internal(&mut self, buf: &mut [u8]) {
        // Pseudo-random generation algorithm (PRGA) - RC4
        for byte in buf.iter_mut() {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);

            let k =
                self.s[(self.s[self.i as usize].wrapping_add(self.s[self.j as usize])) as usize];
            *byte = k;
        }
    }

    /// Generate random bytes
    pub fn fill(&mut self, buf: &mut [u8]) {
        if !self.is_init {
            self.auto_seed();
        }
        self.fill_internal(buf);
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

    /// Check if initialized
    pub fn is_initialized(&self) -> bool {
        self.is_init
    }

    /// Reset to uninitialized state
    pub fn reset(&mut self) {
        self.is_init = false;
    }
}

// ============================================================================
// Global PRNG Instance
// ============================================================================

lazy_static::lazy_static! {
    /// Global PRNG instance (thread-safe)
    static ref GLOBAL_PRNG: Mutex<SqlitePrng> = Mutex::new(SqlitePrng::new());
}

// ============================================================================
// Global Functions
// ============================================================================

/// Fill buffer with random bytes (sqlite3_randomness equivalent)
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

/// Reset/reseed the PRNG (will auto-seed on next use)
pub fn sqlite3_prng_reset() {
    if let Ok(mut prng) = GLOBAL_PRNG.lock() {
        prng.reset();
    }
}

/// Seed the PRNG with specific value (for testing/reproducibility)
pub fn sqlite3_prng_seed(seed: &[u8]) {
    if let Ok(mut prng) = GLOBAL_PRNG.lock() {
        prng.seed(seed);
    }
}

// ============================================================================
// Temporary Filename Generation
// ============================================================================

/// Generate random temporary filename
pub fn sqlite3_temp_filename(prefix: &str, suffix: &str) -> String {
    let mut random_part = [0u8; 16];
    sqlite3_randomness(&mut random_part);

    let hex: String = random_part.iter().map(|b| format!("{:02x}", b)).collect();

    format!("{}{}{}", prefix, hex, suffix)
}

/// Generate unique temporary file path
pub fn sqlite3_temp_file_path() -> String {
    let temp_dir = std::env::temp_dir();
    let filename = sqlite3_temp_filename("sqlite_", ".tmp");
    temp_dir.join(filename).to_string_lossy().to_string()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prng_new() {
        let prng = SqlitePrng::new();
        assert!(!prng.is_initialized());
    }

    #[test]
    fn test_prng_seed() {
        let mut prng = SqlitePrng::new();
        prng.seed(b"test seed");
        assert!(prng.is_initialized());
    }

    #[test]
    fn test_prng_auto_seed() {
        let mut prng = SqlitePrng::new();
        prng.auto_seed();
        assert!(prng.is_initialized());
    }

    #[test]
    fn test_prng_fill_auto_seeds() {
        let mut prng = SqlitePrng::new();
        assert!(!prng.is_initialized());

        let mut buf = [0u8; 32];
        prng.fill(&mut buf);

        assert!(prng.is_initialized());
    }

    #[test]
    fn test_prng_reproducible() {
        let seed = b"test seed for reproducibility";

        let mut prng1 = SqlitePrng::new();
        prng1.seed(seed);
        let a = prng1.next_i64();

        let mut prng2 = SqlitePrng::new();
        prng2.seed(seed);
        let b = prng2.next_i64();

        assert_eq!(a, b);
    }

    #[test]
    fn test_prng_different_seeds() {
        let mut prng1 = SqlitePrng::new();
        prng1.seed(b"seed1");
        let a = prng1.next_i64();

        let mut prng2 = SqlitePrng::new();
        prng2.seed(b"seed2");
        let b = prng2.next_i64();

        assert_ne!(a, b);
    }

    #[test]
    fn test_prng_sequence() {
        let mut prng = SqlitePrng::new();
        prng.seed(b"test");

        let a = prng.next_i64();
        let b = prng.next_i64();
        let c = prng.next_i64();

        // Consecutive values should be different
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[test]
    fn test_prng_fill() {
        let mut prng = SqlitePrng::new();
        prng.seed(b"test");

        let mut buf = [0u8; 100];
        prng.fill(&mut buf);

        // Very unlikely to be all zeros
        assert!(buf.iter().any(|&b| b != 0));
    }

    #[test]
    fn test_prng_next_f64() {
        let mut prng = SqlitePrng::new();
        prng.seed(b"test");

        for _ in 0..100 {
            let f = prng.next_f64();
            assert!(f >= 0.0 && f < 1.0);
        }
    }

    #[test]
    fn test_global_random_int64() {
        // Reset to ensure fresh state
        sqlite3_prng_reset();

        let a = sqlite3_random_int64();
        let b = sqlite3_random_int64();

        // Very unlikely to be equal
        assert_ne!(a, b);
    }

    #[test]
    fn test_global_random_blob() {
        let blob = sqlite3_random_blob(100);
        assert_eq!(blob.len(), 100);

        // Very unlikely to be all zeros
        assert!(blob.iter().any(|&b| b != 0));
    }

    #[test]
    fn test_global_randomness() {
        let mut buf = [0u8; 50];
        let n = sqlite3_randomness(&mut buf);

        assert_eq!(n, 50);
        // Very unlikely to be all zeros
        assert!(buf.iter().any(|&b| b != 0));
    }

    #[test]
    fn test_global_prng_seed() {
        // Use a unique seed for this test to avoid interference from other tests
        let seed = b"unique deterministic seed for test_global_prng_seed";

        sqlite3_prng_seed(seed);
        let a = sqlite3_random_int64();
        let a2 = sqlite3_random_int64(); // Get a second value to advance state

        // Reseed and verify we get the same sequence
        sqlite3_prng_seed(seed);
        let b = sqlite3_random_int64();
        let b2 = sqlite3_random_int64();

        assert_eq!(a, b);
        assert_eq!(a2, b2);
    }

    #[test]
    fn test_temp_filename() {
        let name = sqlite3_temp_filename("test_", ".tmp");

        assert!(name.starts_with("test_"));
        assert!(name.ends_with(".tmp"));
        assert_eq!(name.len(), 5 + 32 + 4); // prefix + 32 hex chars + suffix
    }

    #[test]
    fn test_temp_filename_unique() {
        let name1 = sqlite3_temp_filename("test_", ".tmp");
        let name2 = sqlite3_temp_filename("test_", ".tmp");

        assert_ne!(name1, name2);
    }

    #[test]
    fn test_temp_file_path() {
        let path = sqlite3_temp_file_path();

        assert!(path.contains("sqlite_"));
        assert!(path.ends_with(".tmp"));
    }

    #[test]
    fn test_prng_reset() {
        let mut prng = SqlitePrng::new();
        prng.seed(b"test");
        assert!(prng.is_initialized());

        prng.reset();
        assert!(!prng.is_initialized());
    }

    #[test]
    fn test_rc4_consistency() {
        // Test that RC4 produces consistent output for known input
        let mut prng = SqlitePrng::new();
        prng.seed(b"Key");

        // After discarding 256 bytes, get next byte
        let mut buf = [0u8; 1];
        prng.fill_internal(&mut buf);

        // Just verify we get some byte (RC4 test vectors would need exact KSA impl)
        // The important thing is consistency across calls
        let mut prng2 = SqlitePrng::new();
        prng2.seed(b"Key");

        let mut buf2 = [0u8; 1];
        prng2.fill_internal(&mut buf2);

        assert_eq!(buf[0], buf2[0]);
    }

    #[test]
    fn test_empty_seed_auto_seeds() {
        let mut prng = SqlitePrng::new();
        prng.seed(&[]);
        assert!(prng.is_initialized());
    }

    #[test]
    fn test_distribution() {
        // Simple chi-square-like test for uniform distribution
        let mut prng = SqlitePrng::new();
        prng.seed(b"distribution test");

        let mut counts = [0u32; 256];
        let n = 25600; // 100 expected per bucket

        for _ in 0..n {
            let mut buf = [0u8; 1];
            prng.fill(&mut buf);
            counts[buf[0] as usize] += 1;
        }

        // Check that no bucket is too far from expected (simple sanity check)
        let expected = n as f64 / 256.0;
        let mut max_deviation = 0.0;

        for &count in &counts {
            let deviation = ((count as f64) - expected).abs() / expected;
            if deviation > max_deviation {
                max_deviation = deviation;
            }
        }

        // Allow up to 50% deviation (very loose for a quick test)
        assert!(
            max_deviation < 0.5,
            "Distribution too uneven: max deviation {}",
            max_deviation
        );
    }
}
