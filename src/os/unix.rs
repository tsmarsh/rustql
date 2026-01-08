//! Unix VFS implementation
//!
//! This module provides the Unix-specific VFS implementation including
//! file locking, memory mapping, and shared memory for WAL.

use crate::error::{Error, ErrorCode, Result};
use crate::os::vfs::{
    AccessFlags, DeviceCharacteristics, FileControlOp, LockType, OpenFlags, ShmLockFlags,
    SyncFlags, Vfs, VfsFile,
};
use std::cell::UnsafeCell;
use std::ffi::CString;
use std::os::unix::io::RawFd;
use std::sync::Arc;

// ============================================================================
// Platform-specific helpers
// ============================================================================

/// Get errno in a cross-platform way (Linux vs macOS/BSD)
#[cfg(target_os = "linux")]
fn get_errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

#[cfg(not(target_os = "linux"))]
fn get_errno() -> i32 {
    unsafe { *libc::__error() }
}

/// fdatasync - use fsync on platforms without fdatasync (macOS)
#[cfg(target_os = "linux")]
unsafe fn platform_fdatasync(fd: RawFd) -> i32 {
    libc::fdatasync(fd)
}

#[cfg(not(target_os = "linux"))]
unsafe fn platform_fdatasync(fd: RawFd) -> i32 {
    libc::fsync(fd)
}

// ============================================================================
// Unix VFS
// ============================================================================

/// Unix VFS implementation
pub struct UnixVfs {
    /// VFS name
    name: String,
}

impl UnixVfs {
    /// Create a new Unix VFS with the default name "unix"
    pub fn new() -> Self {
        Self {
            name: "unix".to_string(),
        }
    }

    /// Create a new Unix VFS with a custom name
    pub fn new_with_name(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    /// Detect device characteristics for a file
    fn detect_device_characteristics(&self, _fd: RawFd) -> DeviceCharacteristics {
        // Most Unix systems support powersafe overwrite
        DeviceCharacteristics::POWERSAFE_OVERWRITE
    }

    /// Create a temporary file and return its path
    fn create_temp_file(&self) -> Result<(String, RawFd)> {
        let template = "/tmp/rustql_XXXXXX";
        let c_template = CString::new(template).map_err(|_| Error::new(ErrorCode::CantOpen))?;

        // Create mutable buffer for mkstemp
        let mut template_bytes: Vec<i8> = c_template
            .as_bytes_with_nul()
            .iter()
            .map(|&b| b as i8)
            .collect();

        let fd = unsafe { libc::mkstemp(template_bytes.as_mut_ptr()) };

        if fd < 0 {
            return Err(Self::error_from_errno());
        }

        // Convert back to string
        let path = String::from_utf8_lossy(
            &template_bytes
                .iter()
                .take_while(|&&c| c != 0)
                .map(|&c| c as u8)
                .collect::<Vec<u8>>(),
        )
        .to_string();

        Ok((path, fd))
    }

    /// Create an error from the current errno
    fn error_from_errno() -> Error {
        let errno = get_errno();
        let msg = std::io::Error::from_raw_os_error(errno).to_string();

        let code = match errno {
            libc::ENOENT => ErrorCode::CantOpen,
            libc::EACCES | libc::EPERM => ErrorCode::Perm,
            libc::ENOSPC | libc::EDQUOT => ErrorCode::Full,
            libc::EBUSY | libc::EAGAIN => ErrorCode::Busy,
            libc::EINTR => ErrorCode::Interrupt,
            libc::ENOMEM => ErrorCode::NoMem,
            libc::EROFS => ErrorCode::ReadOnly,
            _ => ErrorCode::IoErr,
        };

        Error::with_message(code, msg)
    }
}

impl Default for UnixVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for UnixVfs {
    fn name(&self) -> &str {
        &self.name
    }

    fn max_pathname(&self) -> i32 {
        512 // Conservative limit for portability
    }

    fn open(&self, path: Option<&str>, flags: OpenFlags) -> Result<Box<dyn VfsFile>> {
        let mut oflags: libc::c_int = 0;

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

        // Handle path or temp file
        let (path_str, fd) = match path {
            Some(p) => {
                let c_path = CString::new(p).map_err(|_| Error::new(ErrorCode::CantOpen))?;
                let fd = unsafe { libc::open(c_path.as_ptr(), oflags, 0o644) };
                if fd < 0 {
                    return Err(Self::error_from_errno());
                }
                (p.to_string(), fd)
            }
            None => self.create_temp_file()?,
        };

        // Get device/inode for lock identification
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut stat) } != 0 {
            unsafe { libc::close(fd) };
            return Err(Self::error_from_errno());
        }

        let delete_on_close = flags.contains(OpenFlags::DELETEONCLOSE);

        Ok(Box::new(UnixFile {
            fd,
            path: path_str,
            flags,
            lock_type: UnsafeCell::new(LockType::None),
            shm: UnsafeCell::new(None),
            dev_ino: (stat.st_dev as u64, stat.st_ino as u64),
            sector_size: 4096,
            device_chars: self.detect_device_characteristics(fd),
            delete_on_close,
        }))
    }

    fn delete(&self, path: &str, sync_dir: bool) -> Result<()> {
        let c_path = CString::new(path).map_err(|_| Error::new(ErrorCode::CantOpen))?;
        let rc = unsafe { libc::unlink(c_path.as_ptr()) };

        if rc != 0 {
            let errno = get_errno();
            if errno != libc::ENOENT {
                return Err(Self::error_from_errno());
            }
        }

        if sync_dir {
            // Sync the directory
            if let Some(dir_path) = std::path::Path::new(path).parent() {
                let dir_str = dir_path.to_str().unwrap_or(".");
                if let Ok(c_dir) = CString::new(dir_str) {
                    let dir_fd = unsafe { libc::open(c_dir.as_ptr(), libc::O_RDONLY) };
                    if dir_fd >= 0 {
                        unsafe {
                            libc::fsync(dir_fd);
                            libc::close(dir_fd);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool> {
        let c_path = CString::new(path).map_err(|_| Error::new(ErrorCode::CantOpen))?;

        let mode = if flags.contains(AccessFlags::READWRITE) {
            libc::R_OK | libc::W_OK
        } else if flags.contains(AccessFlags::READ) {
            libc::R_OK
        } else {
            libc::F_OK
        };

        let rc = unsafe { libc::access(c_path.as_ptr(), mode) };
        Ok(rc == 0)
    }

    fn full_pathname(&self, path: &str) -> Result<String> {
        let c_path = CString::new(path).map_err(|_| Error::new(ErrorCode::CantOpen))?;
        let mut buf = vec![0i8; self.max_pathname() as usize];

        let result = unsafe { libc::realpath(c_path.as_ptr(), buf.as_mut_ptr()) };

        if result.is_null() {
            // realpath failed, return as-is if absolute
            if path.starts_with('/') {
                return Ok(path.to_string());
            }
            // Make relative path absolute
            let cwd = std::env::current_dir().map_err(|_| Error::new(ErrorCode::CantOpen))?;
            return Ok(cwd.join(path).to_string_lossy().to_string());
        }

        let len = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
        Ok(
            String::from_utf8_lossy(&buf[..len].iter().map(|&c| c as u8).collect::<Vec<u8>>())
                .to_string(),
        )
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
        let errno = get_errno();
        let msg = std::io::Error::from_raw_os_error(errno).to_string();
        (errno, msg)
    }
}

// ============================================================================
// Unix File Handle
// ============================================================================

/// Shared memory state for WAL
struct UnixShm {
    /// Shared memory file descriptor
    fd: RawFd,
    /// Memory mapped regions
    regions: Vec<*mut u8>,
    /// Size of each region
    region_size: usize,
    /// Number of regions
    n_region: i32,
}

/// Unix file handle
pub struct UnixFile {
    /// File descriptor
    fd: RawFd,
    /// File path
    path: String,
    /// Open flags
    flags: OpenFlags,
    /// Current lock type (interior mutable for lock operations)
    lock_type: UnsafeCell<LockType>,
    /// Shared memory mapping (interior mutable for shm operations)
    shm: UnsafeCell<Option<UnixShm>>,
    /// Device and inode for lock identification
    dev_ino: (u64, u64),
    /// Sector size
    sector_size: i32,
    /// Device characteristics
    device_chars: DeviceCharacteristics,
    /// Delete on close
    delete_on_close: bool,
}

// Safety: UnixFile can be sent between threads (file descriptors are thread-safe)
unsafe impl Send for UnixFile {}
unsafe impl Sync for UnixFile {}

impl Drop for UnixFile {
    fn drop(&mut self) {
        // Unmap shared memory
        if let Some(shm) = self.shm.get_mut().take() {
            for ptr in &shm.regions {
                if !ptr.is_null() {
                    unsafe {
                        libc::munmap(*ptr as *mut libc::c_void, shm.region_size);
                    }
                }
            }
            unsafe { libc::close(shm.fd) };
        }

        // Close file
        unsafe { libc::close(self.fd) };

        // Delete if requested
        if self.delete_on_close {
            if let Ok(c_path) = CString::new(self.path.as_str()) {
                unsafe { libc::unlink(c_path.as_ptr()) };
            }
        }
    }
}

impl UnixFile {
    /// Apply POSIX lock
    fn lock_posix(&self, new_lock: LockType) -> Result<()> {
        let l_type: libc::c_short = match new_lock {
            LockType::Shared => libc::F_RDLCK as libc::c_short,
            LockType::Exclusive | LockType::Reserved | LockType::Pending => {
                libc::F_WRLCK as libc::c_short
            }
            LockType::None => libc::F_UNLCK as libc::c_short,
        };

        let flock = libc::flock {
            l_type,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: 0,
            l_len: 0, // 0 = entire file
            l_pid: 0,
        };

        loop {
            let rc = unsafe { libc::fcntl(self.fd, libc::F_SETLK, &flock) };
            if rc == 0 {
                // SAFETY: We have exclusive logical access through the VfsFile trait
                unsafe { *self.lock_type.get() = new_lock };
                return Ok(());
            }

            let errno = get_errno();
            if errno == libc::EINTR {
                continue; // Retry on interrupt
            }

            if errno == libc::EAGAIN || errno == libc::EACCES {
                return Err(Error::new(ErrorCode::Busy));
            }

            return Err(UnixVfs::error_from_errno());
        }
    }

    /// Get current lock type
    fn get_lock_type(&self) -> LockType {
        // SAFETY: Reading lock_type is safe as it's only modified through lock operations
        unsafe { *self.lock_type.get() }
    }
}

impl VfsFile for UnixFile {
    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        let n = unsafe {
            libc::pread(
                self.fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                offset as libc::off_t,
            )
        };

        if n < 0 {
            return Err(UnixVfs::error_from_errno());
        }

        // Zero-fill short reads (SQLite convention)
        let n = n as usize;
        if n < buf.len() {
            buf[n..].fill(0);
        }

        Ok(n)
    }

    fn write(&self, buf: &[u8], offset: i64) -> Result<usize> {
        let n = unsafe {
            libc::pwrite(
                self.fd,
                buf.as_ptr() as *const libc::c_void,
                buf.len(),
                offset as libc::off_t,
            )
        };

        if n < 0 {
            return Err(UnixVfs::error_from_errno());
        }

        let n = n as usize;
        if n != buf.len() {
            return Err(Error::new(ErrorCode::Full));
        }

        Ok(n)
    }

    fn truncate(&self, size: i64) -> Result<()> {
        let rc = unsafe { libc::ftruncate(self.fd, size as libc::off_t) };
        if rc != 0 {
            return Err(UnixVfs::error_from_errno());
        }
        Ok(())
    }

    fn sync(&self, flags: SyncFlags) -> Result<()> {
        let rc = if flags.contains(SyncFlags::DATAONLY) {
            unsafe { platform_fdatasync(self.fd) }
        } else {
            unsafe { libc::fsync(self.fd) }
        };

        if rc != 0 {
            return Err(UnixVfs::error_from_errno());
        }
        Ok(())
    }

    fn file_size(&self) -> Result<i64> {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::fstat(self.fd, &mut stat) };
        if rc != 0 {
            return Err(UnixVfs::error_from_errno());
        }
        Ok(stat.st_size as i64)
    }

    fn lock(&self, lock_type: LockType) -> Result<()> {
        let current = self.get_lock_type();

        // Implement lock escalation
        if lock_type <= current {
            return Ok(()); // Already have this lock or better
        }

        // Can't jump from NONE to anything other than SHARED
        if current == LockType::None && lock_type != LockType::Shared {
            return Err(Error::new(ErrorCode::Misuse));
        }

        self.lock_posix(lock_type)
    }

    fn unlock(&self, lock_type: LockType) -> Result<()> {
        if lock_type >= self.get_lock_type() {
            return Ok(()); // Nothing to do
        }

        self.lock_posix(lock_type)
    }

    fn check_reserved_lock(&self) -> Result<bool> {
        if self.get_lock_type() >= LockType::Reserved {
            return Ok(true);
        }

        // Try to acquire reserved lock (non-blocking check)
        let mut flock = libc::flock {
            l_type: libc::F_WRLCK as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: 0,
            l_len: 1,
            l_pid: 0,
        };

        let rc = unsafe { libc::fcntl(self.fd, libc::F_GETLK, &mut flock) };
        if rc != 0 {
            return Err(UnixVfs::error_from_errno());
        }

        Ok(flock.l_type != libc::F_UNLCK as libc::c_short)
    }

    fn file_control(&mut self, _op: FileControlOp) -> Result<()> {
        // Most file control operations are no-ops or return NotFound
        Err(Error::new(ErrorCode::NotFound))
    }

    fn sector_size(&self) -> i32 {
        self.sector_size
    }

    fn device_characteristics(&self) -> DeviceCharacteristics {
        self.device_chars
    }

    fn shm_map(&self, region: i32, size: i32, extend: bool) -> Result<*mut u8> {
        // SAFETY: We have logical exclusive access through the VfsFile trait
        let shm_ptr = self.shm.get();

        // Create or open shared memory file
        if unsafe { (*shm_ptr).is_none() } {
            let shm_path = format!("{}-shm", self.path);
            let c_path =
                CString::new(shm_path.as_str()).map_err(|_| Error::new(ErrorCode::IoErr))?;

            let flags = libc::O_RDWR | libc::O_CREAT;
            let fd = unsafe { libc::open(c_path.as_ptr(), flags, 0o644) };
            if fd < 0 {
                return Err(UnixVfs::error_from_errno());
            }

            unsafe {
                *shm_ptr = Some(UnixShm {
                    fd,
                    regions: Vec::new(),
                    region_size: size as usize,
                    n_region: 0,
                });
            }
        }

        let shm = unsafe { (*shm_ptr).as_mut().unwrap() };

        // Extend file if needed
        if extend {
            let required = ((region + 1) as i64) * (size as i64);
            let rc = unsafe { libc::ftruncate(shm.fd, required as libc::off_t) };
            if rc != 0 {
                return Err(UnixVfs::error_from_errno());
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
                    offset as libc::off_t,
                )
            };

            if ptr == libc::MAP_FAILED {
                return Err(UnixVfs::error_from_errno());
            }

            shm.regions[region as usize] = ptr as *mut u8;
            shm.n_region = std::cmp::max(shm.n_region, region + 1);
        }

        Ok(shm.regions[region as usize])
    }

    fn shm_lock(&self, offset: i32, n: i32, flags: ShmLockFlags) -> Result<()> {
        // SAFETY: We have logical exclusive access through the VfsFile trait
        let shm_ptr = self.shm.get();
        let shm = unsafe { (*shm_ptr).as_ref() }.ok_or_else(|| Error::new(ErrorCode::Misuse))?;

        let lock_type: libc::c_short = if flags.contains(ShmLockFlags::UNLOCK) {
            libc::F_UNLCK as libc::c_short
        } else if flags.contains(ShmLockFlags::SHARED) {
            libc::F_RDLCK as libc::c_short
        } else {
            libc::F_WRLCK as libc::c_short
        };

        let flock = libc::flock {
            l_type: lock_type,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: offset as libc::off_t,
            l_len: n as libc::off_t,
            l_pid: 0,
        };

        let rc = unsafe { libc::fcntl(shm.fd, libc::F_SETLK, &flock) };
        if rc != 0 {
            let errno = get_errno();
            if errno == libc::EAGAIN || errno == libc::EACCES {
                return Err(Error::new(ErrorCode::Busy));
            }
            return Err(UnixVfs::error_from_errno());
        }

        Ok(())
    }

    fn shm_barrier(&self) {
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    }

    fn shm_unmap(&self, delete: bool) -> Result<()> {
        // SAFETY: We have logical exclusive access through the VfsFile trait
        let shm_ptr = self.shm.get();

        if let Some(shm) = unsafe { (*shm_ptr).take() } {
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
                if let Ok(c_path) = CString::new(shm_path.as_str()) {
                    unsafe { libc::unlink(c_path.as_ptr()) };
                }
            }
        }

        Ok(())
    }
}

// ============================================================================
// Registration
// ============================================================================

/// Register the Unix VFS with the global registry
pub fn register_unix_vfs() -> Result<()> {
    let unix_vfs = Arc::new(UnixVfs::new());
    crate::os::vfs::vfs_register(unix_vfs, true)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unix_vfs_name() {
        let vfs = UnixVfs::new();
        assert_eq!(vfs.name(), "unix");
    }

    #[test]
    fn test_unix_vfs_custom_name() {
        let vfs = UnixVfs::new_with_name("custom-unix");
        assert_eq!(vfs.name(), "custom-unix");
    }

    #[test]
    fn test_unix_vfs_max_pathname() {
        let vfs = UnixVfs::new();
        assert!(vfs.max_pathname() > 0);
    }

    #[test]
    fn test_unix_vfs_randomness() {
        let vfs = UnixVfs::new();
        let mut buf1 = [0u8; 16];
        let mut buf2 = [0u8; 16];

        let n1 = vfs.randomness(&mut buf1);
        let n2 = vfs.randomness(&mut buf2);

        assert_eq!(n1, 16);
        assert_eq!(n2, 16);
        // Buffers should be different (with very high probability)
        assert_ne!(buf1, buf2);
    }

    #[test]
    fn test_unix_vfs_current_time() {
        let vfs = UnixVfs::new();
        let jd = vfs.current_time();

        // Julian day should be around 2460000 in modern times
        assert!(jd > 2400000.0);
        assert!(jd < 2500000.0);
    }

    #[test]
    fn test_unix_vfs_sleep() {
        let vfs = UnixVfs::new();
        let start = std::time::Instant::now();
        let result = vfs.sleep(10000); // 10ms
        let elapsed = start.elapsed();

        assert_eq!(result, 10000);
        assert!(elapsed >= std::time::Duration::from_micros(10000));
    }

    #[test]
    fn test_unix_vfs_access_nonexistent() {
        let vfs = UnixVfs::new();
        let result = vfs.access("/nonexistent/path/to/file", AccessFlags::EXISTS);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_unix_vfs_full_pathname_absolute() {
        let vfs = UnixVfs::new();
        let result = vfs.full_pathname("/tmp");
        assert!(result.is_ok());
        let path = result.unwrap();
        assert!(path.starts_with('/'));
    }

    #[test]
    fn test_unix_file_open_close() {
        let vfs = UnixVfs::new();
        let flags = OpenFlags::READWRITE | OpenFlags::CREATE | OpenFlags::DELETEONCLOSE;

        let file = vfs.open(Some("/tmp/rustql_test_file.db"), flags);
        assert!(file.is_ok());
        // File will be deleted on drop
    }

    #[test]
    fn test_unix_file_read_write() {
        let vfs = UnixVfs::new();
        let flags = OpenFlags::READWRITE | OpenFlags::CREATE | OpenFlags::DELETEONCLOSE;

        let file = vfs.open(Some("/tmp/rustql_test_rw.db"), flags).unwrap();

        // Write some data
        let data = b"Hello, RustQL!";
        let written = file.write(data, 0).unwrap();
        assert_eq!(written, data.len());

        // Read it back
        let mut buf = [0u8; 14];
        let read = file.read(&mut buf, 0).unwrap();
        assert_eq!(read, 14);
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_unix_file_truncate() {
        let vfs = UnixVfs::new();
        let flags = OpenFlags::READWRITE | OpenFlags::CREATE | OpenFlags::DELETEONCLOSE;

        let file = vfs.open(Some("/tmp/rustql_test_trunc.db"), flags).unwrap();

        // Write some data
        let data = b"Hello, RustQL!";
        file.write(data, 0).unwrap();

        // Truncate
        file.truncate(5).unwrap();

        // Check size
        let size = file.file_size().unwrap();
        assert_eq!(size, 5);
    }

    #[test]
    fn test_unix_file_locking() {
        let vfs = UnixVfs::new();
        let flags = OpenFlags::READWRITE | OpenFlags::CREATE | OpenFlags::DELETEONCLOSE;

        let file = vfs.open(Some("/tmp/rustql_test_lock.db"), flags).unwrap();

        // Acquire shared lock
        assert!(file.lock(LockType::Shared).is_ok());

        // Upgrade to reserved
        assert!(file.lock(LockType::Reserved).is_ok());

        // Upgrade to exclusive
        assert!(file.lock(LockType::Exclusive).is_ok());

        // Downgrade to shared
        assert!(file.unlock(LockType::Shared).is_ok());

        // Release all locks
        assert!(file.unlock(LockType::None).is_ok());
    }
}
