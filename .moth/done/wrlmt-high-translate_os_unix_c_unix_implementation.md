# Translate os_unix.c - Unix Implementation

## Overview
Translate the Unix/Linux VFS implementation including file locking, memory mapping, and shared memory for WAL.

## Source Reference
- `sqlite3/src/os_unix.c` - 8,588 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Unix VFS
```rust
pub struct UnixVfs {
    /// VFS name
    name: String,
}

impl UnixVfs {
    pub fn new() -> Self {
        Self {
            name: "unix".to_string(),
        }
    }

    pub fn new_with_name(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}
```

### Unix File Handle
```rust
pub struct UnixFile {
    /// File descriptor
    fd: RawFd,
    /// File path
    path: String,
    /// Open flags
    flags: OpenFlags,
    /// Current lock type
    lock_type: LockType,
    /// Lock info (for dotfile and flock locking)
    lock_info: Option<UnixLockInfo>,
    /// Shared memory mapping
    shm: Option<UnixShm>,
    /// Device and inode for lock identification
    dev_ino: (u64, u64),
    /// Sector size
    sector_size: i32,
    /// Device characteristics
    device_chars: DeviceCharacteristics,
}

struct UnixLockInfo {
    /// Lock file path (for dotfile locking)
    lock_path: Option<String>,
    /// Number of shared locks
    shared_count: i32,
    /// Has exclusive lock
    exclusive: bool,
}

struct UnixShm {
    /// Shared memory file descriptor
    fd: RawFd,
    /// Memory mapped regions
    regions: Vec<*mut u8>,
    /// Size of each region
    region_size: usize,
    /// Number of regions
    n_region: i32,
    /// Lock state
    locks: u16,
}
```

## VFS Implementation

```rust
impl Vfs for UnixVfs {
    fn name(&self) -> &str {
        &self.name
    }

    fn max_pathname(&self) -> i32 {
        512  // PATH_MAX on most Unix systems
    }

    fn open(&self, path: Option<&str>, flags: OpenFlags) -> Result<Box<dyn VfsFile>> {
        let mut oflags = 0i32;

        if flags.contains(OpenFlags::READONLY) {
            oflags |= libc::O_RDONLY;
        } else if flags.contains(OpenFlags::READWRITE) {
            oflags |= libc::O_RDWR;
        }

        if flags.contains(OpenFlags::CREATE) {
            oflags |= libc::O_CREAT;
        }

        if flags.contains(OpenFlags::EXCLUSIVE) {
            oflags |= libc::O_EXCL;
        }

        let path_str = match path {
            Some(p) => p.to_string(),
            None => {
                // Create temp file
                self.create_temp_file()?
            }
        };

        let c_path = CString::new(path_str.as_str())?;
        let fd = unsafe {
            libc::open(c_path.as_ptr(), oflags, 0o644)
        };

        if fd < 0 {
            return Err(Error::from_errno());
        }

        // Get device/inode for lock identification
        let mut stat = std::mem::MaybeUninit::uninit();
        if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
            unsafe { libc::close(fd) };
            return Err(Error::from_errno());
        }
        let stat = unsafe { stat.assume_init() };

        Ok(Box::new(UnixFile {
            fd,
            path: path_str,
            flags,
            lock_type: LockType::None,
            lock_info: None,
            shm: None,
            dev_ino: (stat.st_dev as u64, stat.st_ino as u64),
            sector_size: 4096,
            device_chars: self.detect_device_characteristics(fd),
        }))
    }

    fn delete(&self, path: &str, sync_dir: bool) -> Result<()> {
        let c_path = CString::new(path)?;
        let rc = unsafe { libc::unlink(c_path.as_ptr()) };

        if rc != 0 && unsafe { *libc::__errno_location() } != libc::ENOENT {
            return Err(Error::from_errno());
        }

        if sync_dir {
            // Sync the directory
            if let Some(dir_path) = std::path::Path::new(path).parent() {
                let dir_str = dir_path.to_str().unwrap_or(".");
                let c_dir = CString::new(dir_str)?;
                let dir_fd = unsafe { libc::open(c_dir.as_ptr(), libc::O_RDONLY) };
                if dir_fd >= 0 {
                    unsafe {
                        libc::fsync(dir_fd);
                        libc::close(dir_fd);
                    }
                }
            }
        }

        Ok(())
    }

    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool> {
        let c_path = CString::new(path)?;
        let mode = match flags {
            AccessFlags::EXISTS => libc::F_OK,
            AccessFlags::READWRITE => libc::R_OK | libc::W_OK,
            AccessFlags::READ => libc::R_OK,
            _ => libc::F_OK,
        };

        let rc = unsafe { libc::access(c_path.as_ptr(), mode) };
        Ok(rc == 0)
    }

    fn full_pathname(&self, path: &str) -> Result<String> {
        let c_path = CString::new(path)?;
        let mut buf = vec![0u8; self.max_pathname() as usize];

        let result = unsafe {
            libc::realpath(c_path.as_ptr(), buf.as_mut_ptr() as *mut i8)
        };

        if result.is_null() {
            // realpath failed, return as-is if absolute
            if path.starts_with('/') {
                return Ok(path.to_string());
            }
            // Make relative path absolute
            let cwd = std::env::current_dir()?;
            return Ok(cwd.join(path).to_string_lossy().to_string());
        }

        let len = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
        Ok(String::from_utf8_lossy(&buf[..len]).to_string())
    }

    fn randomness(&self, buf: &mut [u8]) -> i32 {
        // Try /dev/urandom
        if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
            use std::io::Read;
            if file.read_exact(buf).is_ok() {
                return buf.len() as i32;
            }
        }

        // Fallback to time-based randomness
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let seed = now.as_nanos() as u64;

        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = ((seed >> ((i % 8) * 8)) ^ (seed.wrapping_mul(i as u64 + 1))) as u8;
        }

        buf.len() as i32
    }

    fn sleep(&self, microseconds: i32) -> i32 {
        std::thread::sleep(std::time::Duration::from_micros(microseconds as u64));
        microseconds
    }

    fn current_time(&self) -> f64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        // Unix epoch to Julian day offset
        const UNIX_EPOCH_JD: f64 = 2440587.5;

        UNIX_EPOCH_JD + (now.as_secs_f64() / 86400.0)
    }

    fn current_time_i64(&self) -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        // Milliseconds since Julian epoch
        const UNIX_EPOCH_JD_MS: i64 = 210866760000000;

        UNIX_EPOCH_JD_MS + (now.as_millis() as i64)
    }

    fn get_last_error(&self) -> (i32, String) {
        let errno = unsafe { *libc::__errno_location() };
        let msg = std::io::Error::from_raw_os_error(errno).to_string();
        (errno, msg)
    }
}
```

## File Operations

```rust
impl VfsFile for UnixFile {
    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        let n = unsafe {
            libc::pread(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), offset)
        };

        if n < 0 {
            return Err(Error::from_errno());
        }

        // Zero-fill short reads (SQLite convention)
        if (n as usize) < buf.len() {
            buf[n as usize..].fill(0);
        }

        Ok(n as usize)
    }

    fn write(&self, buf: &[u8], offset: i64) -> Result<usize> {
        let n = unsafe {
            libc::pwrite(self.fd, buf.as_ptr() as *const libc::c_void, buf.len(), offset)
        };

        if n < 0 {
            return Err(Error::from_errno());
        }

        if (n as usize) != buf.len() {
            return Err(Error::with_code(ErrorCode::Full));
        }

        Ok(n as usize)
    }

    fn truncate(&self, size: i64) -> Result<()> {
        let rc = unsafe { libc::ftruncate(self.fd, size) };
        if rc != 0 {
            return Err(Error::from_errno());
        }
        Ok(())
    }

    fn sync(&self, flags: SyncFlags) -> Result<()> {
        let rc = if flags.contains(SyncFlags::DATAONLY) {
            unsafe { libc::fdatasync(self.fd) }
        } else {
            unsafe { libc::fsync(self.fd) }
        };

        if rc != 0 {
            return Err(Error::from_errno());
        }
        Ok(())
    }

    fn file_size(&self) -> Result<i64> {
        let mut stat = std::mem::MaybeUninit::uninit();
        let rc = unsafe { libc::fstat(self.fd, stat.as_mut_ptr()) };
        if rc != 0 {
            return Err(Error::from_errno());
        }
        let stat = unsafe { stat.assume_init() };
        Ok(stat.st_size as i64)
    }
}
```

## File Locking

```rust
impl UnixFile {
    fn lock_posix(&mut self, lock_type: LockType) -> Result<()> {
        let mut flock = libc::flock {
            l_type: match lock_type {
                LockType::Shared => libc::F_RDLCK,
                LockType::Exclusive | LockType::Reserved | LockType::Pending => libc::F_WRLCK,
                LockType::None => libc::F_UNLCK,
            },
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 0,  // 0 = entire file
            l_pid: 0,
        };

        loop {
            let rc = unsafe { libc::fcntl(self.fd, libc::F_SETLK, &flock) };
            if rc == 0 {
                self.lock_type = lock_type;
                return Ok(());
            }

            let errno = unsafe { *libc::__errno_location() };
            if errno == libc::EINTR {
                continue;  // Retry on interrupt
            }

            if errno == libc::EAGAIN || errno == libc::EACCES {
                return Err(Error::with_code(ErrorCode::Busy));
            }

            return Err(Error::from_errno());
        }
    }
}

impl VfsFile for UnixFile {
    fn lock(&self, lock_type: LockType) -> Result<()> {
        // Implement lock escalation
        if lock_type <= self.lock_type {
            return Ok(());  // Already have this lock or better
        }

        // Can't jump from NONE to anything other than SHARED
        if self.lock_type == LockType::None && lock_type != LockType::Shared {
            return Err(Error::with_code(ErrorCode::Misuse));
        }

        unsafe {
            let self_mut = &mut *(self as *const Self as *mut Self);
            self_mut.lock_posix(lock_type)
        }
    }

    fn unlock(&self, lock_type: LockType) -> Result<()> {
        if lock_type >= self.lock_type {
            return Ok(());  // Nothing to do
        }

        unsafe {
            let self_mut = &mut *(self as *const Self as *mut Self);
            self_mut.lock_posix(lock_type)
        }
    }

    fn check_reserved_lock(&self) -> Result<bool> {
        if self.lock_type >= LockType::Reserved {
            return Ok(true);
        }

        // Try to acquire reserved lock
        let mut flock = libc::flock {
            l_type: libc::F_WRLCK,
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 1,
            l_pid: 0,
        };

        let rc = unsafe { libc::fcntl(self.fd, libc::F_GETLK, &flock) };
        if rc != 0 {
            return Err(Error::from_errno());
        }

        Ok(flock.l_type != libc::F_UNLCK)
    }
}
```

## Shared Memory (for WAL)

```rust
impl VfsFile for UnixFile {
    fn shm_map(&self, region: i32, size: i32, extend: bool) -> Result<*mut u8> {
        let self_mut = unsafe { &mut *(self as *const Self as *mut Self) };

        // Create or open shared memory file
        if self_mut.shm.is_none() {
            let shm_path = format!("{}-shm", self.path);
            let c_path = CString::new(shm_path.as_str())?;

            let flags = libc::O_RDWR | libc::O_CREAT;
            let fd = unsafe { libc::open(c_path.as_ptr(), flags, 0o644) };
            if fd < 0 {
                return Err(Error::from_errno());
            }

            self_mut.shm = Some(UnixShm {
                fd,
                regions: Vec::new(),
                region_size: size as usize,
                n_region: 0,
                locks: 0,
            });
        }

        let shm = self_mut.shm.as_mut().unwrap();

        // Extend file if needed
        if extend {
            let required = ((region + 1) as i64) * (size as i64);
            let rc = unsafe { libc::ftruncate(shm.fd, required) };
            if rc != 0 {
                return Err(Error::from_errno());
            }
        }

        // Ensure we have enough region slots
        while shm.regions.len() <= region as usize {
            shm.regions.push(std::ptr::null_mut());
        }

        // Map the region if not already mapped
        if shm.regions[region as usize].is_null() {
            let offset = (region as i64) * (size as i64);
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    size as usize,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    shm.fd,
                    offset,
                )
            };

            if ptr == libc::MAP_FAILED {
                return Err(Error::from_errno());
            }

            shm.regions[region as usize] = ptr as *mut u8;
            shm.n_region = std::cmp::max(shm.n_region, region + 1);
        }

        Ok(shm.regions[region as usize])
    }

    fn shm_lock(&self, offset: i32, n: i32, flags: ShmLockFlags) -> Result<()> {
        let self_mut = unsafe { &mut *(self as *const Self as *mut Self) };
        let shm = self_mut.shm.as_mut()
            .ok_or_else(|| Error::with_code(ErrorCode::Misuse))?;

        let lock_type = if flags.contains(ShmLockFlags::UNLOCK) {
            libc::F_UNLCK
        } else if flags.contains(ShmLockFlags::SHARED) {
            libc::F_RDLCK
        } else {
            libc::F_WRLCK
        };

        let mut flock = libc::flock {
            l_type: lock_type,
            l_whence: libc::SEEK_SET as i16,
            l_start: offset as i64,
            l_len: n as i64,
            l_pid: 0,
        };

        let rc = unsafe { libc::fcntl(shm.fd, libc::F_SETLK, &flock) };
        if rc != 0 {
            let errno = unsafe { *libc::__errno_location() };
            if errno == libc::EAGAIN || errno == libc::EACCES {
                return Err(Error::with_code(ErrorCode::Busy));
            }
            return Err(Error::from_errno());
        }

        Ok(())
    }

    fn shm_barrier(&self) {
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    }

    fn shm_unmap(&self, delete: bool) -> Result<()> {
        let self_mut = unsafe { &mut *(self as *const Self as *mut Self) };

        if let Some(mut shm) = self_mut.shm.take() {
            // Unmap all regions
            for ptr in &shm.regions {
                if !ptr.is_null() {
                    unsafe {
                        libc::munmap(*ptr as *mut libc::c_void, shm.region_size);
                    }
                }
            }

            // Close file
            unsafe { libc::close(shm.fd) };

            // Delete file if requested
            if delete {
                let shm_path = format!("{}-shm", self.path);
                let c_path = CString::new(shm_path.as_str()).unwrap();
                unsafe { libc::unlink(c_path.as_ptr()) };
            }
        }

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Unix VFS implementation
- [ ] File open/read/write/close operations
- [ ] POSIX file locking (fcntl)
- [ ] Lock escalation (shared -> reserved -> exclusive)
- [ ] File truncation
- [ ] fsync/fdatasync
- [ ] File size queries
- [ ] Shared memory mapping for WAL
- [ ] Shared memory locking
- [ ] /dev/urandom for randomness
- [ ] realpath for full pathnames
- [ ] Proper error handling (errno)
- [ ] Memory-mapped I/O support
