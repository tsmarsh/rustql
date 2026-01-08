//! Windows VFS implementation
//!
//! This module provides the Windows-specific VFS implementation including
//! file locking, memory mapping, and shared memory for WAL.

use crate::error::{Error, ErrorCode, Result};
use crate::os::vfs::{
    AccessFlags, DeviceCharacteristics, FileControlOp, LockType, OpenFlags, ShmLockFlags,
    SyncFlags, Vfs, VfsFile,
};
use std::cell::UnsafeCell;
use std::ffi::OsStr;
use std::os::windows::ffi::OsStrExt;
use std::sync::Arc;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Foundation::{
    ERROR_ACCESS_DENIED, ERROR_DISK_FULL, ERROR_FILE_NOT_FOUND, ERROR_HANDLE_EOF,
    ERROR_INVALID_PARAMETER, ERROR_IO_PENDING, ERROR_LOCK_VIOLATION, ERROR_PATH_NOT_FOUND,
    ERROR_SHARING_VIOLATION, ERROR_WRITE_PROTECT,
};
use windows_sys::Win32::Security::Cryptography::{
    CryptAcquireContextW, CryptGenRandom, CryptReleaseContext, CRYPT_VERIFYCONTEXT, PROV_RSA_FULL,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, DeleteFileW, FlushFileBuffers, GetFileAttributesW, GetFileSizeEx,
    GetFullPathNameW, GetTempFileNameW, GetTempPathW, LockFileEx, ReadFile, SetEndOfFile,
    SetFilePointerEx, UnlockFileEx, WriteFile, CREATE_ALWAYS, CREATE_NEW, FILE_ATTRIBUTE_NORMAL,
    FILE_BEGIN, FILE_FLAG_DELETE_ON_CLOSE, FILE_FLAG_OVERLAPPED, FILE_FLAG_RANDOM_ACCESS,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_ALWAYS, OPEN_EXISTING,
};
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, MapViewOfFile, UnmapViewOfFile, FILE_MAP_ALL_ACCESS,
    MEMORY_MAPPED_VIEW_ADDRESS, PAGE_READWRITE,
};
use windows_sys::Win32::System::SystemInformation::{GetSystemTimeAsFileTime, GetTickCount64};
use windows_sys::Win32::System::Threading::{GetCurrentThreadId, Sleep};
use windows_sys::Win32::System::IO::{
    GetOverlappedResult, OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0,
};

// ============================================================================
// Windows constants
// ============================================================================

const MAX_PATH_LEN: usize = 260;
const FILE_SHARE_FLAGS: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

const PENDING_BYTE: u32 = 0x4000_0000;
const RESERVED_BYTE: u32 = PENDING_BYTE + 1;
const SHARED_FIRST: u32 = PENDING_BYTE + 2;
const SHARED_SIZE: u32 = 510;
const NO_SHARED_LOCK: u8 = 0xFF;

// ============================================================================
// Windows VFS
// ============================================================================

/// Windows VFS implementation
pub struct WinVfs {
    /// VFS name
    name: String,
}

impl WinVfs {
    /// Create a new Windows VFS with the default name "win32"
    pub fn new() -> Self {
        Self {
            name: "win32".to_string(),
        }
    }

    fn error_from_win32_code(code: u32) -> Error {
        let msg = std::io::Error::from_raw_os_error(code as i32).to_string();
        let mapped = match code {
            ERROR_ACCESS_DENIED => ErrorCode::Perm,
            ERROR_FILE_NOT_FOUND | ERROR_PATH_NOT_FOUND => ErrorCode::CantOpen,
            ERROR_DISK_FULL => ErrorCode::Full,
            ERROR_WRITE_PROTECT => ErrorCode::ReadOnly,
            ERROR_LOCK_VIOLATION | ERROR_SHARING_VIOLATION => ErrorCode::Busy,
            ERROR_INVALID_PARAMETER => ErrorCode::Misuse,
            _ => ErrorCode::IoErr,
        };

        Error::with_message(mapped, msg)
    }

    fn error_from_win32() -> Error {
        let code = unsafe { GetLastError() };
        Self::error_from_win32_code(code)
    }

    fn to_utf16(path: &str) -> Vec<u16> {
        OsStr::new(path)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    fn create_temp_file(&self) -> Result<String> {
        let mut temp_path = vec![0u16; MAX_PATH_LEN + 1];
        let len = unsafe { GetTempPathW(temp_path.len() as u32, temp_path.as_mut_ptr()) };
        if len == 0 || len as usize >= temp_path.len() {
            return Err(Self::error_from_win32());
        }

        let mut temp_file = vec![0u16; MAX_PATH_LEN + 1];
        let prefix = ['r' as u16, 'q' as u16, 'l' as u16, 0];
        let rc = unsafe {
            GetTempFileNameW(
                temp_path.as_ptr(),
                prefix.as_ptr(),
                0,
                temp_file.as_mut_ptr(),
            )
        };
        if rc == 0 {
            return Err(Self::error_from_win32());
        }

        let end = temp_file.iter().position(|&c| c == 0).unwrap_or(0);
        Ok(String::from_utf16_lossy(&temp_file[..end]))
    }
}

impl Default for WinVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for WinVfs {
    fn name(&self) -> &str {
        &self.name
    }

    fn max_pathname(&self) -> i32 {
        MAX_PATH_LEN as i32
    }

    fn open(&self, path: Option<&str>, flags: OpenFlags) -> Result<Box<dyn VfsFile>> {
        let desired_access = if flags.contains(OpenFlags::READONLY) {
            GENERIC_READ
        } else {
            GENERIC_READ | GENERIC_WRITE
        };

        let mut attributes = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_OVERLAPPED;

        if flags.contains(OpenFlags::DELETEONCLOSE) {
            attributes |= FILE_FLAG_DELETE_ON_CLOSE;
        }

        let creation = if flags.contains(OpenFlags::CREATE) {
            if flags.contains(OpenFlags::EXCLUSIVE) {
                CREATE_NEW
            } else {
                OPEN_ALWAYS
            }
        } else if flags.contains(OpenFlags::EXCLUSIVE) {
            CREATE_ALWAYS
        } else {
            OPEN_EXISTING
        };

        let path_str = match path {
            Some(p) => p.to_string(),
            None => self.create_temp_file()?,
        };

        let wide_path = Self::to_utf16(&path_str);
        let handle = unsafe {
            CreateFileW(
                wide_path.as_ptr(),
                desired_access,
                FILE_SHARE_FLAGS,
                std::ptr::null(),
                creation,
                attributes,
                0,
            )
        };

        if handle == INVALID_HANDLE_VALUE {
            return Err(Self::error_from_win32());
        }

        Ok(Box::new(WinFile {
            handle,
            path: path_str,
            flags,
            lock_type: UnsafeCell::new(LockType::None),
            shared_lock_byte: UnsafeCell::new(NO_SHARED_LOCK),
            last_errno: UnsafeCell::new(0),
            sector_size: 4096,
            device_chars: DeviceCharacteristics::POWERSAFE_OVERWRITE,
            shm: UnsafeCell::new(None),
            delete_on_close: flags.contains(OpenFlags::DELETEONCLOSE),
        }))
    }

    fn delete(&self, path: &str, _sync_dir: bool) -> Result<()> {
        let wide_path = Self::to_utf16(path);

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
                return Err(Self::error_from_win32_code(err));
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Err(Self::error_from_win32())
    }

    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool> {
        let wide_path = Self::to_utf16(path);
        let attrs = unsafe { GetFileAttributesW(wide_path.as_ptr()) };

        if attrs == windows_sys::Win32::Storage::FileSystem::INVALID_FILE_ATTRIBUTES {
            let err = unsafe { GetLastError() };
            if err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND {
                return Ok(false);
            }
            return Err(Self::error_from_win32_code(err));
        }

        if flags.contains(AccessFlags::READWRITE) {
            let readonly =
                (attrs & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_READONLY) != 0;
            return Ok(!readonly);
        }

        Ok(true)
    }

    fn full_pathname(&self, path: &str) -> Result<String> {
        let wide_path = Self::to_utf16(path);
        let mut buf = vec![0u16; self.max_pathname() as usize];

        let mut len = unsafe {
            GetFullPathNameW(
                wide_path.as_ptr(),
                buf.len() as u32,
                buf.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        };

        if len == 0 {
            return Err(Self::error_from_win32());
        }

        if len as usize >= buf.len() {
            buf.resize(len as usize + 1, 0);
            len = unsafe {
                GetFullPathNameW(
                    wide_path.as_ptr(),
                    buf.len() as u32,
                    buf.as_mut_ptr(),
                    std::ptr::null_mut(),
                )
            };
            if len == 0 {
                return Err(Self::error_from_win32());
            }
        }

        Ok(String::from_utf16_lossy(&buf[..len as usize]))
    }

    fn randomness(&self, buf: &mut [u8]) -> i32 {
        unsafe {
            let mut prov: usize = 0;
            if CryptAcquireContextW(
                &mut prov as *mut _,
                std::ptr::null(),
                std::ptr::null(),
                PROV_RSA_FULL,
                CRYPT_VERIFYCONTEXT,
            ) != 0
            {
                let rc = CryptGenRandom(prov, buf.len() as u32, buf.as_mut_ptr());
                CryptReleaseContext(prov, 0);
                if rc != 0 {
                    return buf.len() as i32;
                }
            }
        }

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
        let mut ft = windows_sys::Win32::Foundation::FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        unsafe { GetSystemTimeAsFileTime(&mut ft) };

        let ft64 = ((ft.dwHighDateTime as u64) << 32) | (ft.dwLowDateTime as u64);

        const JD_2000: f64 = 2451545.0;
        const FILETIME_2000: u64 = 125911584000000000;
        JD_2000 + ((ft64 as f64 - FILETIME_2000 as f64) / 864000000000.0)
    }

    fn current_time_i64(&self) -> i64 {
        let mut ft = windows_sys::Win32::Foundation::FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        unsafe { GetSystemTimeAsFileTime(&mut ft) };

        let ft64 = ((ft.dwHighDateTime as u64) << 32) | (ft.dwLowDateTime as u64);

        const FILETIME_JD_OFFSET: i64 = 210866760000000;
        const FILETIME_MS_FACTOR: u64 = 10000;

        FILETIME_JD_OFFSET + (ft64 / FILETIME_MS_FACTOR) as i64
    }

    fn get_last_error(&self) -> (i32, String) {
        let err = unsafe { GetLastError() };
        let msg = std::io::Error::from_raw_os_error(err as i32).to_string();
        (err as i32, msg)
    }
}

// ============================================================================
// Windows File Handle
// ============================================================================

struct WinShm {
    file_handle: HANDLE,
    map_handle: HANDLE,
    regions: Vec<*mut u8>,
    region_size: usize,
    n_region: i32,
    shared_mask: u16,
    exclusive_mask: u16,
}

/// Windows file handle
pub struct WinFile {
    handle: HANDLE,
    path: String,
    flags: OpenFlags,
    lock_type: UnsafeCell<LockType>,
    shared_lock_byte: UnsafeCell<u8>,
    last_errno: UnsafeCell<u32>,
    sector_size: i32,
    device_chars: DeviceCharacteristics,
    shm: UnsafeCell<Option<WinShm>>,
    delete_on_close: bool,
}

unsafe impl Send for WinFile {}
unsafe impl Sync for WinFile {}

impl Drop for WinFile {
    fn drop(&mut self) {
        if let Some(shm) = self.shm.get_mut().take() {
            for ptr in &shm.regions {
                if !ptr.is_null() {
                    unsafe {
                        let addr = MEMORY_MAPPED_VIEW_ADDRESS {
                            Value: *ptr as *mut _,
                        };
                        UnmapViewOfFile(addr);
                    }
                }
            }
            unsafe {
                if shm.map_handle != 0 {
                    CloseHandle(shm.map_handle);
                }
                if shm.file_handle != 0 {
                    CloseHandle(shm.file_handle);
                }
            }
        }

        unsafe {
            if self.handle != 0 {
                CloseHandle(self.handle);
            }
        }

        if self.delete_on_close {
            let wide_path = WinVfs::to_utf16(&self.path);
            unsafe {
                DeleteFileW(wide_path.as_ptr());
            }
        }
    }
}

impl WinFile {
    fn set_last_errno(&self, err: u32) {
        unsafe {
            *self.last_errno.get() = err;
        }
    }

    fn get_lock_type(&self) -> LockType {
        unsafe { *self.lock_type.get() }
    }

    fn lock_region(&self, offset: u32, length: u32, exclusive: bool) -> Result<()> {
        let mut overlapped = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: offset,
                    OffsetHigh: 0,
                },
            },
            hEvent: 0,
        };

        let flags = if exclusive {
            windows_sys::Win32::Storage::FileSystem::LOCKFILE_EXCLUSIVE_LOCK
                | windows_sys::Win32::Storage::FileSystem::LOCKFILE_FAIL_IMMEDIATELY
        } else {
            windows_sys::Win32::Storage::FileSystem::LOCKFILE_FAIL_IMMEDIATELY
        };

        let rc = unsafe { LockFileEx(self.handle, flags, 0, length, 0, &mut overlapped) };
        if rc == 0 {
            let err = unsafe { GetLastError() };
            self.set_last_errno(err);
            if err == ERROR_LOCK_VIOLATION {
                return Err(Error::new(ErrorCode::Busy));
            }
            return Err(WinVfs::error_from_win32_code(err));
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
            hEvent: 0,
        };

        let rc = unsafe { UnlockFileEx(self.handle, 0, length, 0, &mut overlapped) };
        if rc == 0 {
            let err = unsafe { GetLastError() };
            self.set_last_errno(err);
            return Err(WinVfs::error_from_win32_code(err));
        }

        Ok(())
    }

    fn lock_shared_byte(&self) -> Result<()> {
        let current = unsafe { *self.shared_lock_byte.get() };
        let mut start = if current == NO_SHARED_LOCK {
            (unsafe { GetCurrentThreadId() } % SHARED_SIZE) as u32
        } else {
            (current as u32) % SHARED_SIZE
        };
        if start >= SHARED_SIZE {
            start = 0;
        }

        for i in 0..SHARED_SIZE {
            let idx = (start + i) % SHARED_SIZE;
            let offset = SHARED_FIRST + idx;
            match self.lock_region(offset, 1, false) {
                Ok(()) => {
                    unsafe {
                        *self.shared_lock_byte.get() = idx as u8;
                    }
                    return Ok(());
                }
                Err(err) if err.code == ErrorCode::Busy => continue,
                Err(err) => return Err(err),
            }
        }

        Err(Error::new(ErrorCode::Busy))
    }

    fn unlock_shared_byte(&self) -> Result<()> {
        let current = unsafe { *self.shared_lock_byte.get() };
        if current == NO_SHARED_LOCK {
            return Ok(());
        }
        let idx = (current as u32) % SHARED_SIZE;
        if idx < SHARED_SIZE {
            self.unlock_region(SHARED_FIRST + idx, 1)?;
        }
        unsafe {
            *self.shared_lock_byte.get() = NO_SHARED_LOCK;
        }
        Ok(())
    }

    fn shm_open(&self, region_size: usize) -> Result<()> {
        let shm_ptr = self.shm.get();
        if unsafe { (*shm_ptr).is_some() } {
            return Ok(());
        }

        let shm_path = format!("{}-shm", self.path);
        let wide_path = WinVfs::to_utf16(&shm_path);
        let handle = unsafe {
            CreateFileW(
                wide_path.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_FLAGS,
                std::ptr::null(),
                OPEN_ALWAYS,
                FILE_ATTRIBUTE_NORMAL,
                0,
            )
        };

        if handle == INVALID_HANDLE_VALUE {
            return Err(WinVfs::error_from_win32());
        }

        let map_handle = unsafe {
            CreateFileMappingW(
                handle,
                std::ptr::null(),
                PAGE_READWRITE,
                0,
                0,
                std::ptr::null(),
            )
        };
        if map_handle == 0 {
            unsafe { CloseHandle(handle) };
            return Err(WinVfs::error_from_win32());
        }

        unsafe {
            *shm_ptr = Some(WinShm {
                file_handle: handle,
                map_handle,
                regions: Vec::new(),
                region_size,
                n_region: 0,
                shared_mask: 0,
                exclusive_mask: 0,
            });
        }

        Ok(())
    }

    fn ensure_shm_mapping(&self, required_size: u64) -> Result<()> {
        let shm_ptr = self.shm.get();
        let shm = unsafe { (*shm_ptr).as_mut().unwrap() };

        if required_size == 0 {
            return Ok(());
        }

        if shm.map_handle != 0 {
            unsafe { CloseHandle(shm.map_handle) };
        }

        let size_high = (required_size >> 32) as u32;
        let size_low = (required_size & 0xFFFF_FFFF) as u32;
        let map_handle = unsafe {
            CreateFileMappingW(
                shm.file_handle,
                std::ptr::null(),
                PAGE_READWRITE,
                size_high,
                size_low,
                std::ptr::null(),
            )
        };

        if map_handle == 0 {
            return Err(WinVfs::error_from_win32());
        }

        shm.map_handle = map_handle;
        Ok(())
    }
}

impl VfsFile for WinFile {
    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        let mut overlapped = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: (offset & 0xFFFF_FFFF) as u32,
                    OffsetHigh: ((offset >> 32) & 0xFFFF_FFFF) as u32,
                },
            },
            hEvent: 0,
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
            let mut err = unsafe { GetLastError() };
            if err == ERROR_IO_PENDING {
                let rc2 = unsafe {
                    GetOverlappedResult(self.handle, &mut overlapped, &mut bytes_read, 1)
                };
                if rc2 == 0 {
                    err = unsafe { GetLastError() };
                } else {
                    err = 0;
                }
            }
            if err != 0 && err != ERROR_HANDLE_EOF {
                self.set_last_errno(err);
                return Err(WinVfs::error_from_win32_code(err));
            }
        }

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
                    Offset: (offset & 0xFFFF_FFFF) as u32,
                    OffsetHigh: ((offset >> 32) & 0xFFFF_FFFF) as u32,
                },
            },
            hEvent: 0,
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
            let mut err = unsafe { GetLastError() };
            if err == ERROR_IO_PENDING {
                let rc2 = unsafe {
                    GetOverlappedResult(self.handle, &mut overlapped, &mut bytes_written, 1)
                };
                if rc2 == 0 {
                    err = unsafe { GetLastError() };
                } else {
                    err = 0;
                }
            }
            if err != 0 {
                self.set_last_errno(err);
                return Err(WinVfs::error_from_win32_code(err));
            }
        }

        if (bytes_written as usize) != buf.len() {
            return Err(Error::new(ErrorCode::Full));
        }

        Ok(bytes_written as usize)
    }

    fn truncate(&self, size: i64) -> Result<()> {
        let rc = unsafe { SetFilePointerEx(self.handle, size, std::ptr::null_mut(), FILE_BEGIN) };
        if rc == 0 {
            return Err(WinVfs::error_from_win32());
        }

        let rc = unsafe { SetEndOfFile(self.handle) };
        if rc == 0 {
            return Err(WinVfs::error_from_win32());
        }

        Ok(())
    }

    fn sync(&self, _flags: SyncFlags) -> Result<()> {
        let rc = unsafe { FlushFileBuffers(self.handle) };
        if rc == 0 {
            return Err(WinVfs::error_from_win32());
        }
        Ok(())
    }

    fn file_size(&self) -> Result<i64> {
        let mut size: i64 = 0;
        let rc = unsafe { GetFileSizeEx(self.handle, &mut size as *mut _ as *mut _) };
        if rc == 0 {
            return Err(WinVfs::error_from_win32());
        }
        Ok(size)
    }

    fn lock(&self, lock_type: LockType) -> Result<()> {
        let current = self.get_lock_type();

        if lock_type <= current {
            return Ok(());
        }

        if current == LockType::None && lock_type != LockType::Shared {
            return Err(Error::new(ErrorCode::Misuse));
        }

        if current < LockType::Shared && lock_type >= LockType::Shared {
            self.lock_shared_byte()?;
        }

        if current < LockType::Reserved && lock_type >= LockType::Reserved {
            self.lock_region(RESERVED_BYTE, 1, true)?;
        }

        if current < LockType::Pending && lock_type >= LockType::Pending {
            self.lock_region(PENDING_BYTE, 1, true)?;
        }

        if lock_type == LockType::Exclusive {
            self.unlock_shared_byte()?;
            self.lock_region(SHARED_FIRST, SHARED_SIZE, true)?;
        }

        unsafe {
            *self.lock_type.get() = lock_type;
        }

        Ok(())
    }

    fn unlock(&self, lock_type: LockType) -> Result<()> {
        let current = self.get_lock_type();

        if lock_type >= current {
            return Ok(());
        }

        if current == LockType::Exclusive && lock_type < LockType::Exclusive {
            self.unlock_region(SHARED_FIRST, SHARED_SIZE)?;
            if lock_type == LockType::Shared {
                self.lock_shared_byte()?;
            }
        }

        if current >= LockType::Pending && lock_type < LockType::Pending {
            self.unlock_region(PENDING_BYTE, 1)?;
        }

        if current >= LockType::Reserved && lock_type < LockType::Reserved {
            self.unlock_region(RESERVED_BYTE, 1)?;
        }

        if current >= LockType::Shared && lock_type < LockType::Shared {
            self.unlock_shared_byte()?;
        }

        unsafe {
            *self.lock_type.get() = lock_type;
        }

        Ok(())
    }

    fn check_reserved_lock(&self) -> Result<bool> {
        if self.get_lock_type() >= LockType::Reserved {
            return Ok(true);
        }

        match self.lock_region(RESERVED_BYTE, 1, true) {
            Ok(()) => {
                self.unlock_region(RESERVED_BYTE, 1)?;
                Ok(false)
            }
            Err(err) if err.code == ErrorCode::Busy => Ok(true),
            Err(err) => Err(err),
        }
    }

    fn file_control(&mut self, _op: FileControlOp) -> Result<()> {
        Err(Error::new(ErrorCode::NotFound))
    }

    fn sector_size(&self) -> i32 {
        self.sector_size
    }

    fn device_characteristics(&self) -> DeviceCharacteristics {
        self.device_chars
    }

    fn shm_map(&self, region: i32, size: i32, extend: bool) -> Result<*mut u8> {
        let region_size = size as usize;
        self.shm_open(region_size)?;

        let shm_ptr = self.shm.get();
        let shm = unsafe { (*shm_ptr).as_mut().unwrap() };

        if extend {
            let required = ((region + 1) as u64) * (size as u64);
            let rc = unsafe {
                SetFilePointerEx(
                    shm.file_handle,
                    required as i64,
                    std::ptr::null_mut(),
                    FILE_BEGIN,
                )
            };
            if rc == 0 {
                return Err(WinVfs::error_from_win32());
            }
            let rc = unsafe { SetEndOfFile(shm.file_handle) };
            if rc == 0 {
                return Err(WinVfs::error_from_win32());
            }

            self.ensure_shm_mapping(required)?;
        }

        while shm.regions.len() <= region as usize {
            shm.regions.push(std::ptr::null_mut());
        }

        if shm.regions[region as usize].is_null() {
            let offset = (region as u64) * (size as u64);
            let offset_low = (offset & 0xFFFF_FFFF) as u32;
            let offset_high = (offset >> 32) as u32;
            let mapped = unsafe {
                MapViewOfFile(
                    shm.map_handle,
                    FILE_MAP_ALL_ACCESS,
                    offset_high,
                    offset_low,
                    size as usize,
                )
            };

            if mapped.Value.is_null() {
                return Err(WinVfs::error_from_win32());
            }

            shm.regions[region as usize] = mapped.Value as *mut u8;
            shm.n_region = std::cmp::max(shm.n_region, region + 1);
        }

        Ok(shm.regions[region as usize])
    }

    fn shm_lock(&self, offset: i32, n: i32, flags: ShmLockFlags) -> Result<()> {
        let shm_ptr = self.shm.get();
        let shm = unsafe { (*shm_ptr).as_mut() }.ok_or_else(|| Error::new(ErrorCode::Misuse))?;

        let exclusive = flags.contains(ShmLockFlags::EXCLUSIVE);
        let unlock = flags.contains(ShmLockFlags::UNLOCK);

        for i in 0..n {
            let byte = (offset + i) as u32;
            let mut overlapped = OVERLAPPED {
                Internal: 0,
                InternalHigh: 0,
                Anonymous: OVERLAPPED_0 {
                    Anonymous: OVERLAPPED_0_0 {
                        Offset: byte,
                        OffsetHigh: 0,
                    },
                },
                hEvent: 0,
            };

            if unlock {
                let rc = unsafe { UnlockFileEx(shm.file_handle, 0, 1, 0, &mut overlapped) };
                if rc == 0 {
                    return Err(WinVfs::error_from_win32());
                }
                continue;
            }

            let mut lock_flags = windows_sys::Win32::Storage::FileSystem::LOCKFILE_FAIL_IMMEDIATELY;
            if exclusive {
                lock_flags |= windows_sys::Win32::Storage::FileSystem::LOCKFILE_EXCLUSIVE_LOCK;
            }

            let rc = unsafe { LockFileEx(shm.file_handle, lock_flags, 0, 1, 0, &mut overlapped) };
            if rc == 0 {
                let err = unsafe { GetLastError() };
                if err == ERROR_LOCK_VIOLATION {
                    return Err(Error::new(ErrorCode::Busy));
                }
                return Err(WinVfs::error_from_win32_code(err));
            }
        }

        if offset >= 0 && n > 0 && (offset + n) <= 16 {
            let offset_u = offset as u32;
            let n_u = n as u32;
            let base = if n_u == 16 { 0xFFFF } else { (1u16 << n_u) - 1 };
            let mask = base << offset_u;
            if unlock {
                shm.shared_mask &= !mask;
                shm.exclusive_mask &= !mask;
            } else if exclusive {
                shm.exclusive_mask |= mask;
            } else {
                shm.shared_mask |= mask;
            }
        }

        Ok(())
    }

    fn shm_barrier(&self) {
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    }

    fn shm_unmap(&self, delete: bool) -> Result<()> {
        let shm_ptr = self.shm.get();
        if unsafe { (*shm_ptr).is_none() } {
            return Ok(());
        }

        let shm = unsafe { (*shm_ptr).take().unwrap() };
        for ptr in &shm.regions {
            if !ptr.is_null() {
                unsafe {
                    UnmapViewOfFile(*ptr as *const _);
                }
            }
        }
        unsafe {
            if shm.map_handle != 0 {
                CloseHandle(shm.map_handle);
            }
            if shm.file_handle != 0 {
                CloseHandle(shm.file_handle);
            }
        }

        if delete {
            let shm_path = format!("{}-shm", self.path);
            let wide_path = WinVfs::to_utf16(&shm_path);
            unsafe {
                DeleteFileW(wide_path.as_ptr());
            }
        }

        Ok(())
    }
}

// ============================================================================
// Registration
// ============================================================================

/// Register the Windows VFS with the global registry
pub fn register_windows_vfs() -> Result<()> {
    let win_vfs = Arc::new(WinVfs::new());
    crate::os::vfs::vfs_register(win_vfs, true)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_win_vfs_name() {
        let vfs = WinVfs::new();
        assert_eq!(vfs.name(), "win32");
    }
}
