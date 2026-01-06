# Translate os_win.c - Windows Implementation

## Overview
Translate the Windows VFS implementation including file locking, memory mapping, and Windows-specific APIs.

## Source Reference
- `sqlite3/src/os_win.c` - 6,801 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Windows VFS
```rust
pub struct WinVfs {
    /// VFS name
    name: String,
}

impl WinVfs {
    pub fn new() -> Self {
        Self {
            name: "win32".to_string(),
        }
    }
}
```

### Windows File Handle
```rust
#[cfg(windows)]
pub struct WinFile {
    /// Windows file handle
    handle: HANDLE,
    /// File path
    path: String,
    /// Open flags
    flags: OpenFlags,
    /// Current lock type
    lock_type: LockType,
    /// Shared lock count
    shared_lock_byte: u8,
    /// Last error code
    last_errno: u32,
    /// Sector size
    sector_size: u32,
    /// Device characteristics
    device_chars: DeviceCharacteristics,
    /// Shared memory handle
    shm: Option<WinShm>,
}

#[cfg(windows)]
struct WinShm {
    /// Shared memory file handle
    file_handle: HANDLE,
    /// Memory mapping handle
    map_handle: HANDLE,
    /// Mapped regions
    regions: Vec<*mut u8>,
    /// Size of each region
    region_size: usize,
    /// Number of regions
    n_region: i32,
    /// Lock masks
    shared_mask: u16,
    exclusive_mask: u16,
}
```

### Windows Constants
```rust
#[cfg(windows)]
mod win_const {
    use super::*;

    // Lock byte offsets (same as SQLite)
    pub const SHARED_FIRST: u32 = 510;
    pub const SHARED_SIZE: u32 = 1;
    pub const RESERVED_BYTE: u32 = 512;
    pub const PENDING_BYTE: u32 = 0x40000000;

    // File sharing
    pub const FILE_SHARE_FLAGS: u32 =
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
}
```

## VFS Implementation

```rust
#[cfg(windows)]
impl Vfs for WinVfs {
    fn name(&self) -> &str {
        &self.name
    }

    fn max_pathname(&self) -> i32 {
        260  // MAX_PATH
    }

    fn open(&self, path: Option<&str>, flags: OpenFlags) -> Result<Box<dyn VfsFile>> {
        use windows_sys::Win32::Storage::FileSystem::*;

        let desired_access = if flags.contains(OpenFlags::READONLY) {
            GENERIC_READ
        } else {
            GENERIC_READ | GENERIC_WRITE
        };

        let share_mode = win_const::FILE_SHARE_FLAGS;

        let creation = if flags.contains(OpenFlags::CREATE) {
            if flags.contains(OpenFlags::EXCLUSIVE) {
                CREATE_NEW
            } else {
                OPEN_ALWAYS
            }
        } else {
            OPEN_EXISTING
        };

        let attributes = FILE_ATTRIBUTE_NORMAL;

        let path_str = match path {
            Some(p) => p.to_string(),
            None => self.create_temp_file()?,
        };

        let wide_path: Vec<u16> = path_str.encode_utf16()
            .chain(std::iter::once(0))
            .collect();

        let handle = unsafe {
            CreateFileW(
                wide_path.as_ptr(),
                desired_access,
                share_mode,
                std::ptr::null(),
                creation,
                attributes,
                std::ptr::null_mut(),
            )
        };

        if handle == INVALID_HANDLE_VALUE {
            return Err(Error::from_win32());
        }

        Ok(Box::new(WinFile {
            handle,
            path: path_str,
            flags,
            lock_type: LockType::None,
            shared_lock_byte: 0,
            last_errno: 0,
            sector_size: self.detect_sector_size(handle),
            device_chars: self.detect_device_characteristics(handle),
            shm: None,
        }))
    }

    fn delete(&self, path: &str, sync_dir: bool) -> Result<()> {
        let wide_path: Vec<u16> = path.encode_utf16()
            .chain(std::iter::once(0))
            .collect();

        // Try to delete with retry for sharing violations
        for _ in 0..3 {
            let rc = unsafe { DeleteFileW(wide_path.as_ptr()) };
            if rc != 0 {
                return Ok(());
            }

            let err = unsafe { GetLastError() };
            if err == ERROR_FILE_NOT_FOUND {
                return Ok(());
            }
            if err != ERROR_SHARING_VIOLATION {
                return Err(Error::from_win32_code(err));
            }

            // Wait and retry
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Err(Error::from_win32())
    }

    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool> {
        let wide_path: Vec<u16> = path.encode_utf16()
            .chain(std::iter::once(0))
            .collect();

        let attrs = unsafe { GetFileAttributesW(wide_path.as_ptr()) };

        if attrs == INVALID_FILE_ATTRIBUTES {
            let err = unsafe { GetLastError() };
            if err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND {
                return Ok(false);
            }
            return Err(Error::from_win32_code(err));
        }

        match flags {
            AccessFlags::EXISTS => Ok(true),
            AccessFlags::READ => Ok(true),
            AccessFlags::READWRITE => {
                Ok((attrs & FILE_ATTRIBUTE_READONLY) == 0)
            }
            _ => Ok(true),
        }
    }

    fn full_pathname(&self, path: &str) -> Result<String> {
        let wide_path: Vec<u16> = path.encode_utf16()
            .chain(std::iter::once(0))
            .collect();

        let mut buf = vec![0u16; self.max_pathname() as usize];

        let len = unsafe {
            GetFullPathNameW(
                wide_path.as_ptr(),
                buf.len() as u32,
                buf.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        };

        if len == 0 {
            return Err(Error::from_win32());
        }

        Ok(String::from_utf16_lossy(&buf[..len as usize]))
    }

    fn randomness(&self, buf: &mut [u8]) -> i32 {
        // Use Windows crypto API
        use windows_sys::Win32::Security::Cryptography::*;

        unsafe {
            let mut prov: usize = 0;
            if CryptAcquireContextW(
                &mut prov as *mut _,
                std::ptr::null(),
                std::ptr::null(),
                PROV_RSA_FULL,
                CRYPT_VERIFYCONTEXT,
            ) != 0 {
                let result = CryptGenRandom(
                    prov,
                    buf.len() as u32,
                    buf.as_mut_ptr(),
                );
                CryptReleaseContext(prov, 0);
                if result != 0 {
                    return buf.len() as i32;
                }
            }
        }

        // Fallback
        let tick = unsafe { GetTickCount64() };
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = ((tick >> ((i % 8) * 8)) ^ (i as u64)) as u8;
        }
        buf.len() as i32
    }

    fn sleep(&self, microseconds: i32) -> i32 {
        let milliseconds = (microseconds + 999) / 1000;
        unsafe { Sleep(milliseconds as u32) };
        milliseconds * 1000
    }

    fn current_time(&self) -> f64 {
        let mut ft = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
        unsafe { GetSystemTimeAsFileTime(&mut ft) };

        // FILETIME is 100-nanosecond intervals since Jan 1, 1601
        let ft64 = ((ft.dwHighDateTime as u64) << 32) | (ft.dwLowDateTime as u64);

        // Convert to Julian day
        // Days between 1601-01-01 and 2000-01-01
        const JD_2000: f64 = 2451545.0;
        const FILETIME_2000: u64 = 125911584000000000;

        JD_2000 + ((ft64 as f64 - FILETIME_2000 as f64) / (864000000000.0))
    }

    fn current_time_i64(&self) -> i64 {
        let mut ft = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
        unsafe { GetSystemTimeAsFileTime(&mut ft) };

        let ft64 = ((ft.dwHighDateTime as u64) << 32) | (ft.dwLowDateTime as u64);

        // Convert to milliseconds since Julian epoch
        const FILETIME_JD_OFFSET: i64 = 210866760000000;
        const FILETIME_MS_FACTOR: u64 = 10000;

        FILETIME_JD_OFFSET + ((ft64 / FILETIME_MS_FACTOR) as i64)
    }

    fn get_last_error(&self) -> (i32, String) {
        let err = unsafe { GetLastError() };
        let msg = format_win32_error(err);
        (err as i32, msg)
    }
}
```

## File Operations

```rust
#[cfg(windows)]
impl VfsFile for WinFile {
    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        let mut overlapped = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: (offset & 0xFFFFFFFF) as u32,
                    OffsetHigh: (offset >> 32) as u32,
                },
            },
            hEvent: std::ptr::null_mut(),
        };

        let mut bytes_read: u32 = 0;
        let rc = unsafe {
            ReadFile(
                self.handle,
                buf.as_mut_ptr() as *mut _,
                buf.len() as u32,
                &mut bytes_read,
                &mut overlapped,
            )
        };

        if rc == 0 {
            let err = unsafe { GetLastError() };
            if err != ERROR_HANDLE_EOF {
                return Err(Error::from_win32_code(err));
            }
        }

        // Zero-fill short reads
        if (bytes_read as usize) < buf.len() {
            buf[bytes_read as usize..].fill(0);
        }

        Ok(bytes_read as usize)
    }

    fn write(&self, buf: &[u8], offset: i64) -> Result<usize> {
        let mut overlapped = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: (offset & 0xFFFFFFFF) as u32,
                    OffsetHigh: (offset >> 32) as u32,
                },
            },
            hEvent: std::ptr::null_mut(),
        };

        let mut bytes_written: u32 = 0;
        let rc = unsafe {
            WriteFile(
                self.handle,
                buf.as_ptr() as *const _,
                buf.len() as u32,
                &mut bytes_written,
                &mut overlapped,
            )
        };

        if rc == 0 {
            return Err(Error::from_win32());
        }

        if (bytes_written as usize) != buf.len() {
            return Err(Error::with_code(ErrorCode::Full));
        }

        Ok(bytes_written as usize)
    }

    fn truncate(&self, size: i64) -> Result<()> {
        let mut li = LARGE_INTEGER { QuadPart: size };
        let rc = unsafe { SetFilePointerEx(self.handle, li, std::ptr::null_mut(), FILE_BEGIN) };
        if rc == 0 {
            return Err(Error::from_win32());
        }

        let rc = unsafe { SetEndOfFile(self.handle) };
        if rc == 0 {
            return Err(Error::from_win32());
        }

        Ok(())
    }

    fn sync(&self, flags: SyncFlags) -> Result<()> {
        let rc = unsafe { FlushFileBuffers(self.handle) };
        if rc == 0 {
            return Err(Error::from_win32());
        }
        Ok(())
    }

    fn file_size(&self) -> Result<i64> {
        let mut size: i64 = 0;
        let rc = unsafe { GetFileSizeEx(self.handle, &mut size as *mut _ as *mut _) };
        if rc == 0 {
            return Err(Error::from_win32());
        }
        Ok(size)
    }
}
```

## Windows File Locking

```rust
#[cfg(windows)]
impl WinFile {
    fn lock_region(&self, offset: u32, length: u32, exclusive: bool) -> Result<()> {
        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
        } else {
            LOCKFILE_FAIL_IMMEDIATELY
        };

        let mut overlapped = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: offset,
                    OffsetHigh: 0,
                },
            },
            hEvent: std::ptr::null_mut(),
        };

        let rc = unsafe {
            LockFileEx(
                self.handle,
                flags,
                0,
                length,
                0,
                &mut overlapped,
            )
        };

        if rc == 0 {
            let err = unsafe { GetLastError() };
            if err == ERROR_LOCK_VIOLATION {
                return Err(Error::with_code(ErrorCode::Busy));
            }
            return Err(Error::from_win32_code(err));
        }

        Ok(())
    }

    fn unlock_region(&self, offset: u32, length: u32) -> Result<()> {
        let mut overlapped = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: offset,
                    OffsetHigh: 0,
                },
            },
            hEvent: std::ptr::null_mut(),
        };

        let rc = unsafe {
            UnlockFileEx(
                self.handle,
                0,
                length,
                0,
                &mut overlapped,
            )
        };

        if rc == 0 {
            return Err(Error::from_win32());
        }

        Ok(())
    }
}

#[cfg(windows)]
impl VfsFile for WinFile {
    fn lock(&self, lock_type: LockType) -> Result<()> {
        use win_const::*;

        let self_mut = unsafe { &mut *(self as *const Self as *mut Self) };

        match lock_type {
            LockType::Shared => {
                self_mut.lock_region(SHARED_FIRST, SHARED_SIZE, false)?;
            }
            LockType::Reserved => {
                self_mut.lock_region(RESERVED_BYTE, 1, true)?;
            }
            LockType::Pending => {
                self_mut.lock_region(PENDING_BYTE, 1, true)?;
            }
            LockType::Exclusive => {
                // Must release shared lock, get exclusive
                self_mut.unlock_region(SHARED_FIRST, SHARED_SIZE)?;
                self_mut.lock_region(SHARED_FIRST, SHARED_SIZE, true)?;
            }
            _ => {}
        }

        self_mut.lock_type = lock_type;
        Ok(())
    }

    fn unlock(&self, lock_type: LockType) -> Result<()> {
        use win_const::*;

        let self_mut = unsafe { &mut *(self as *const Self as *mut Self) };

        if self.lock_type >= LockType::Exclusive && lock_type < LockType::Exclusive {
            self_mut.unlock_region(SHARED_FIRST, SHARED_SIZE)?;
            self_mut.lock_region(SHARED_FIRST, SHARED_SIZE, false)?;
        }

        if self.lock_type >= LockType::Pending && lock_type < LockType::Pending {
            self_mut.unlock_region(PENDING_BYTE, 1)?;
        }

        if self.lock_type >= LockType::Reserved && lock_type < LockType::Reserved {
            self_mut.unlock_region(RESERVED_BYTE, 1)?;
        }

        if self.lock_type >= LockType::Shared && lock_type < LockType::Shared {
            self_mut.unlock_region(SHARED_FIRST, SHARED_SIZE)?;
        }

        self_mut.lock_type = lock_type;
        Ok(())
    }

    fn check_reserved_lock(&self) -> Result<bool> {
        if self.lock_type >= LockType::Reserved {
            return Ok(true);
        }

        // Try to acquire reserved lock briefly
        match self.lock_region(win_const::RESERVED_BYTE, 1, true) {
            Ok(()) => {
                self.unlock_region(win_const::RESERVED_BYTE, 1)?;
                Ok(false)
            }
            Err(e) if e.code() == ErrorCode::Busy => Ok(true),
            Err(e) => Err(e),
        }
    }
}
```

## Acceptance Criteria
- [ ] Windows VFS implementation
- [ ] CreateFileW for file open
- [ ] ReadFile/WriteFile with overlapped I/O
- [ ] SetFilePointerEx and SetEndOfFile for truncate
- [ ] FlushFileBuffers for sync
- [ ] LockFileEx/UnlockFileEx for locking
- [ ] Windows byte-range locking
- [ ] CryptGenRandom for randomness
- [ ] GetFullPathNameW for full pathnames
- [ ] GetFileAttributesW for access checks
- [ ] FILETIME handling for timestamps
- [ ] Proper Win32 error handling
- [ ] Memory-mapped file support (CreateFileMapping)
- [ ] Shared memory for WAL on Windows
