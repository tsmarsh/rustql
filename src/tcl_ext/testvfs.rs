//! Test VFS (Virtual File System) TCL command implementation
//!
//! This module implements the `testvfs` TCL command used by SQLite's test suite.
//! The testvfs creates a named VFS that wraps the default VFS and allows test
//! scripts to intercept and fault-inject file operations. It also provides
//! the `atomic_batch_write`, `file_control_reservebytes`, and
//! `sqlite3_simulate_device` TCL commands.
//!
//! Usage in TCL:
//!   testvfs tvfs ?-default BOOL? ?-noshm BOOL? ?-fullshm BOOL?
//!               ?-szosfile N? ?-mxpathname N? ?-iversion N?
//!
//! Instance commands:
//!   tvfs filter {xOpen xRead xWrite ...}
//!   tvfs script {tcl_script}
//!   tvfs ioerr N PERSIST
//!   tvfs fullerr N PERSIST
//!   tvfs cantopenerr N PERSIST
//!   tvfs delete
//!   tvfs devchar ?{flags}?
//!   tvfs sectorsize ?VALUE?
//!   tvfs shm FILE ?VALUE?

use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_int;
use std::sync::{Arc, Mutex};

use super::ffi::{
    Tcl_BackgroundError, Tcl_CreateObjCommand, Tcl_DecrRefCount, Tcl_DeleteCommand,
    Tcl_DuplicateObj, Tcl_EvalObjEx, Tcl_GetBooleanFromObj, Tcl_GetIntFromObj,
    Tcl_GetStringResult, Tcl_IncrRefCount, Tcl_Interp, Tcl_ListObjAppendElement,
    Tcl_ListObjGetElements, Tcl_NewListObj, Tcl_NewStringObj, Tcl_Obj,
    Tcl_ResetResult, Tcl_SetObjResult, TCL_ERROR, TCL_EVAL_GLOBAL, TCL_OK,
};
use super::helpers::{obj_to_string, set_result_int, set_result_string};

use crate::os::vfs::{
    self, AccessFlags, DeviceCharacteristics, FileControlOp, LockType, OpenFlags, ShmLockFlags,
    SyncFlags, Vfs, VfsFile,
};

// ============================================================================
// Mask constants for VFS method filtering
// ============================================================================

const TESTVFS_SHMOPEN_MASK: u32 = 0x00000001;
const TESTVFS_SHMLOCK_MASK: u32 = 0x00000010;
const TESTVFS_SHMMAP_MASK: u32 = 0x00000020;
const TESTVFS_SHMBARRIER_MASK: u32 = 0x00000040;
const TESTVFS_SHMCLOSE_MASK: u32 = 0x00000080;

const TESTVFS_OPEN_MASK: u32 = 0x00000100;
const TESTVFS_SYNC_MASK: u32 = 0x00000200;
const TESTVFS_DELETE_MASK: u32 = 0x00000400;
const TESTVFS_CLOSE_MASK: u32 = 0x00000800;
const TESTVFS_WRITE_MASK: u32 = 0x00001000;
const TESTVFS_TRUNCATE_MASK: u32 = 0x00002000;
const TESTVFS_ACCESS_MASK: u32 = 0x00004000;
const TESTVFS_FULLPATHNAME_MASK: u32 = 0x00008000;
const TESTVFS_READ_MASK: u32 = 0x00010000;
const TESTVFS_UNLOCK_MASK: u32 = 0x00020000;
const TESTVFS_LOCK_MASK: u32 = 0x00040000;
const TESTVFS_CKLOCK_MASK: u32 = 0x00080000;
const TESTVFS_FCNTL_MASK: u32 = 0x00100000;
const TESTVFS_SLEEP_MASK: u32 = 0x00200000;

const TESTVFS_ALL_MASK: u32 = 0x003FFFFF;

// ============================================================================
// Fault injection constants
// ============================================================================

const FAULT_INJECT_NONE: i32 = 0;
const FAULT_INJECT_TRANSIENT: i32 = 1;
const FAULT_INJECT_PERSISTENT: i32 = 2;

// ============================================================================
// VFS method name to mask mapping
// ============================================================================

fn method_name_to_mask(name: &str) -> Option<u32> {
    match name {
        "xShmOpen" => Some(TESTVFS_SHMOPEN_MASK),
        "xShmLock" => Some(TESTVFS_SHMLOCK_MASK),
        "xShmBarrier" => Some(TESTVFS_SHMBARRIER_MASK),
        "xShmUnmap" => Some(TESTVFS_SHMCLOSE_MASK),
        "xShmMap" => Some(TESTVFS_SHMMAP_MASK),
        "xSync" => Some(TESTVFS_SYNC_MASK),
        "xDelete" => Some(TESTVFS_DELETE_MASK),
        "xWrite" => Some(TESTVFS_WRITE_MASK),
        "xRead" => Some(TESTVFS_READ_MASK),
        "xTruncate" => Some(TESTVFS_TRUNCATE_MASK),
        "xOpen" => Some(TESTVFS_OPEN_MASK),
        "xClose" => Some(TESTVFS_CLOSE_MASK),
        "xAccess" => Some(TESTVFS_ACCESS_MASK),
        "xFullPathname" => Some(TESTVFS_FULLPATHNAME_MASK),
        "xUnlock" => Some(TESTVFS_UNLOCK_MASK),
        "xLock" => Some(TESTVFS_LOCK_MASK),
        "xCheckReservedLock" => Some(TESTVFS_CKLOCK_MASK),
        "xFileControl" => Some(TESTVFS_FCNTL_MASK),
        "xSleep" => Some(TESTVFS_SLEEP_MASK),
        _ => None,
    }
}

// ============================================================================
// Fault injection state
// ============================================================================

#[derive(Debug)]
struct FaultInject {
    /// Remaining calls before fault injection
    cnt: i32,
    /// Fault type (NONE, TRANSIENT, PERSISTENT)
    fault: i32,
    /// Number of faults injected so far
    n_fail: i32,
}

impl FaultInject {
    fn new() -> Self {
        FaultInject {
            cnt: 0,
            fault: FAULT_INJECT_NONE,
            n_fail: 0,
        }
    }

    /// Check if fault should be injected. Decrements counter and returns true when
    /// the counter reaches zero (transient) or is at/below zero (persistent).
    fn inject(&mut self) -> bool {
        if self.fault != FAULT_INJECT_NONE {
            self.cnt -= 1;
            if self.cnt == 0 || (self.cnt < 0 && self.fault == FAULT_INJECT_PERSISTENT) {
                self.n_fail += 1;
                return true;
            }
        }
        false
    }

    /// Reset and return the fail count
    fn reset(&mut self) -> i32 {
        let ret = self.n_fail;
        self.n_fail = 0;
        self.fault = FAULT_INJECT_NONE;
        self.cnt = 0;
        ret
    }
}

// ============================================================================
// Shared memory buffer for WAL testing
// ============================================================================

const TESTVFS_MAX_PAGES: usize = 1024;

#[derive(Debug)]
struct ShmBuffer {
    /// Associated file name
    file: String,
    /// Page size
    pgsz: usize,
    /// Pages of shared memory
    pages: Vec<Option<Vec<u8>>>,
}

impl ShmBuffer {
    fn new(file: &str) -> Self {
        ShmBuffer {
            file: file.to_string(),
            pgsz: 0,
            pages: Vec::new(),
        }
    }
}

// ============================================================================
// Open file descriptor state for testvfs
// ============================================================================

struct TestvfsFd {
    /// Filename
    filename: String,
    /// The real underlying file descriptor
    real_file: Box<dyn VfsFile>,
    /// Exclusive lock mask for shared memory
    excl_lock: u32,
    /// Shared lock mask for shared memory
    shared_lock: u32,
    /// Shared memory buffer name (for looking up in the testvfs buffer list)
    shm_name: Option<String>,
}

// ============================================================================
// Core Testvfs state
// ============================================================================

/// The main testvfs state, shared between the VFS trait implementation
/// and the TCL instance command.
struct TestvfsState {
    /// Name of this VFS
    name: String,
    /// TCL interpreter for callbacks
    interp: *mut Tcl_Interp,
    /// TCL script to run on intercepted methods (reference-counted TCL object)
    script: Option<*mut Tcl_Obj>,
    /// Mask controlling which methods are intercepted
    mask: u32,
    /// IO error fault injection
    ioerr: FaultInject,
    /// SQLITE_FULL fault injection
    fullerr: FaultInject,
    /// SQLITE_CANTOPEN fault injection
    cantopenerr: FaultInject,
    /// Device characteristics (from devchar command), -1 means default
    devchar: i32,
    /// Sector size (from sectorsize command), -1 means default
    sectorsize: i32,
    /// Whether shared memory methods are omitted
    is_noshm: bool,
    /// Whether to use full shared memory
    #[allow(dead_code)]
    is_fullshm: bool,
    /// Shared memory buffers
    shm_buffers: Vec<ShmBuffer>,
}

// Safety: The interp and script pointers are only accessed from the TCL thread
// (which is single-threaded). The VFS callbacks are also called from the same thread.
unsafe impl Send for TestvfsState {}
unsafe impl Sync for TestvfsState {}

impl TestvfsState {
    fn new(name: &str, interp: *mut Tcl_Interp) -> Self {
        TestvfsState {
            name: name.to_string(),
            interp,
            script: None,
            mask: TESTVFS_ALL_MASK,
            ioerr: FaultInject::new(),
            fullerr: FaultInject::new(),
            cantopenerr: FaultInject::new(),
            devchar: -1,
            sectorsize: -1,
            is_noshm: false,
            is_fullshm: false,
            shm_buffers: Vec::new(),
        }
    }

    /// Execute a TCL callback for a VFS method
    unsafe fn exec_tcl(
        &self,
        method: &str,
        arg1: Option<&str>,
        arg2: Option<&str>,
        arg3: Option<&str>,
        arg4: Option<&str>,
    ) {
        let script = match self.script {
            Some(s) => s,
            None => return,
        };

        let eval_obj = Tcl_DuplicateObj(script);
        Tcl_IncrRefCount(eval_obj);

        let method_c = CString::new(method).unwrap();
        Tcl_ListObjAppendElement(
            self.interp,
            eval_obj,
            Tcl_NewStringObj(method_c.as_ptr(), method.len() as c_int),
        );

        if let Some(a1) = arg1 {
            let c = CString::new(a1).unwrap();
            Tcl_ListObjAppendElement(
                self.interp,
                eval_obj,
                Tcl_NewStringObj(c.as_ptr(), a1.len() as c_int),
            );
        }
        if let Some(a2) = arg2 {
            let c = CString::new(a2).unwrap();
            Tcl_ListObjAppendElement(
                self.interp,
                eval_obj,
                Tcl_NewStringObj(c.as_ptr(), a2.len() as c_int),
            );
        }
        if let Some(a3) = arg3 {
            let c = CString::new(a3).unwrap();
            Tcl_ListObjAppendElement(
                self.interp,
                eval_obj,
                Tcl_NewStringObj(c.as_ptr(), a3.len() as c_int),
            );
        }
        if let Some(a4) = arg4 {
            let c = CString::new(a4).unwrap();
            Tcl_ListObjAppendElement(
                self.interp,
                eval_obj,
                Tcl_NewStringObj(c.as_ptr(), a4.len() as c_int),
            );
        }

        let rc = Tcl_EvalObjEx(self.interp, eval_obj, TCL_EVAL_GLOBAL);
        if rc != TCL_OK {
            Tcl_BackgroundError(self.interp);
            Tcl_ResetResult(self.interp);
        }

        Tcl_DecrRefCount(eval_obj);
    }

    /// Parse the TCL result as an SQLite result code name.
    /// Returns Some(error_code) if the result string matches a known code,
    /// None otherwise.
    unsafe fn result_code(&self) -> Option<crate::error::ErrorCode> {
        let result_ptr = Tcl_GetStringResult(self.interp);
        if result_ptr.is_null() {
            return None;
        }
        let result = CStr::from_ptr(result_ptr).to_str().unwrap_or("");
        match result {
            "SQLITE_OK" => Some(crate::error::ErrorCode::Ok),
            "SQLITE_ERROR" => Some(crate::error::ErrorCode::Error),
            "SQLITE_IOERR" => Some(crate::error::ErrorCode::IoErr),
            "SQLITE_LOCKED" => Some(crate::error::ErrorCode::Locked),
            "SQLITE_BUSY" => Some(crate::error::ErrorCode::Busy),
            "SQLITE_READONLY" => Some(crate::error::ErrorCode::ReadOnly),
            "SQLITE_READONLY_CANTINIT" => Some(crate::error::ErrorCode::ReadOnly),
            "SQLITE_NOTFOUND" => Some(crate::error::ErrorCode::NotFound),
            "SQLITE_OMIT" => None, // Special: skip this operation
            _ => None,
        }
    }

    /// Find or create a shared memory buffer for a given filename
    fn find_or_create_shm_buffer(&mut self, filename: &str) -> usize {
        for (i, buf) in self.shm_buffers.iter().enumerate() {
            if buf.file == filename {
                return i;
            }
        }
        self.shm_buffers.push(ShmBuffer::new(filename));
        self.shm_buffers.len() - 1
    }
}

// ============================================================================
// Testvfs VFS wrapper file
// ============================================================================

/// A file handle that wraps a real file and intercepts operations
struct TestvfsFile {
    /// The underlying real file
    real_file: Box<dyn VfsFile>,
    /// Filename
    filename: String,
    /// Reference to the shared testvfs state
    state: Arc<Mutex<TestvfsState>>,
}

impl VfsFile for TestvfsFile {
    fn read(&self, buf: &mut [u8], offset: i64) -> crate::error::Result<usize> {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_READ_MASK) != 0 {
            unsafe {
                state.exec_tcl("xRead", Some(&self.filename), None, None, None);
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        if (state.mask & TESTVFS_READ_MASK) != 0 {
            // We need mutable access for fault injection - drop and re-acquire
            drop(state);
            let mut state = self.state.lock().unwrap();
            if state.ioerr.inject() {
                return Err(crate::error::Error::new(crate::error::ErrorCode::IoErr));
            }
            drop(state);
        } else {
            drop(state);
        }
        self.real_file.read(buf, offset)
    }

    fn write(&self, buf: &[u8], offset: i64) -> crate::error::Result<usize> {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_WRITE_MASK) != 0 {
            let offset_str = offset.to_string();
            let amt_str = buf.len().to_string();
            unsafe {
                state.exec_tcl(
                    "xWrite",
                    Some(&self.filename),
                    None,
                    Some(&offset_str),
                    Some(&amt_str),
                );
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        drop(state);

        // Check fullerr
        {
            let mut state = self.state.lock().unwrap();
            if state.fullerr.inject() {
                return Err(crate::error::Error::new(crate::error::ErrorCode::Full));
            }
            if (state.mask & TESTVFS_WRITE_MASK) != 0 && state.ioerr.inject() {
                return Err(crate::error::Error::new(crate::error::ErrorCode::IoErr));
            }
        }

        self.real_file.write(buf, offset)
    }

    fn truncate(&self, size: i64) -> crate::error::Result<()> {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_TRUNCATE_MASK) != 0 {
            unsafe {
                state.exec_tcl("xTruncate", Some(&self.filename), None, None, None);
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        drop(state);
        self.real_file.truncate(size)
    }

    fn sync(&self, flags: SyncFlags) -> crate::error::Result<()> {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_SYNC_MASK) != 0 {
            let flags_str = if flags.contains(SyncFlags::FULL) && flags.contains(SyncFlags::DATAONLY)
            {
                "full|dataonly"
            } else if flags.contains(SyncFlags::NORMAL) && flags.contains(SyncFlags::DATAONLY) {
                "normal|dataonly"
            } else if flags.contains(SyncFlags::FULL) {
                "full"
            } else {
                "normal"
            };
            unsafe {
                state.exec_tcl(
                    "xSync",
                    Some(&self.filename),
                    Some(flags_str),
                    None,
                    None,
                );
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        if (state.mask & TESTVFS_SYNC_MASK) != 0 {
            drop(state);
            let mut state = self.state.lock().unwrap();
            if state.ioerr.inject() {
                return Err(crate::error::Error::new(crate::error::ErrorCode::IoErr));
            }
            drop(state);
        } else {
            drop(state);
        }
        self.real_file.sync(flags)
    }

    fn file_size(&self) -> crate::error::Result<i64> {
        self.real_file.file_size()
    }

    fn lock(&self, lock_type: LockType) -> crate::error::Result<()> {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_LOCK_MASK) != 0 {
            let lock_str = match lock_type {
                LockType::None => "none",
                LockType::Shared => "shared",
                LockType::Reserved => "reserved",
                LockType::Pending => "pending",
                LockType::Exclusive => "exclusive",
            };
            unsafe {
                state.exec_tcl("xLock", Some(&self.filename), Some(lock_str), None, None);
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        drop(state);
        self.real_file.lock(lock_type)
    }

    fn unlock(&self, lock_type: LockType) -> crate::error::Result<()> {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_UNLOCK_MASK) != 0 {
            let lock_str = match lock_type {
                LockType::None => "none",
                LockType::Shared => "shared",
                LockType::Reserved => "reserved",
                LockType::Pending => "pending",
                LockType::Exclusive => "exclusive",
            };
            unsafe {
                state.exec_tcl(
                    "xUnlock",
                    Some(&self.filename),
                    Some(lock_str),
                    None,
                    None,
                );
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        drop(state);
        self.real_file.unlock(lock_type)
    }

    fn check_reserved_lock(&self) -> crate::error::Result<bool> {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_CKLOCK_MASK) != 0 {
            unsafe {
                state.exec_tcl(
                    "xCheckReservedLock",
                    Some(&self.filename),
                    None,
                    None,
                    None,
                );
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        drop(state);
        self.real_file.check_reserved_lock()
    }

    fn file_control(&mut self, op: FileControlOp) -> crate::error::Result<()> {
        self.real_file.file_control(op)
    }

    fn sector_size(&self) -> i32 {
        let state = self.state.lock().unwrap();
        if state.sectorsize >= 0 {
            state.sectorsize
        } else {
            self.real_file.sector_size()
        }
    }

    fn device_characteristics(&self) -> DeviceCharacteristics {
        let state = self.state.lock().unwrap();
        if state.devchar >= 0 {
            // Strip the marker bit 0x10000000 that SQLite's C code uses
            DeviceCharacteristics::from_bits_truncate((state.devchar & 0x0FFFFFFF) as u32)
        } else {
            self.real_file.device_characteristics()
        }
    }

    fn shm_map(&self, region: i32, size: i32, extend: bool) -> crate::error::Result<*mut u8> {
        let state = self.state.lock().unwrap();
        if state.is_noshm {
            return Err(crate::error::Error::new(crate::error::ErrorCode::IoErr));
        }
        if state.script.is_some() && (state.mask & TESTVFS_SHMMAP_MASK) != 0 {
            let region_str = region.to_string();
            unsafe {
                state.exec_tcl(
                    "xShmMap",
                    Some(&self.filename),
                    Some(&region_str),
                    None,
                    None,
                );
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        drop(state);
        self.real_file.shm_map(region, size, extend)
    }

    fn shm_lock(&self, offset: i32, n: i32, flags: ShmLockFlags) -> crate::error::Result<()> {
        let state = self.state.lock().unwrap();
        if state.is_noshm {
            return Err(crate::error::Error::new(crate::error::ErrorCode::IoErr));
        }
        if state.script.is_some() && (state.mask & TESTVFS_SHMLOCK_MASK) != 0 {
            let lock_type = if flags.contains(ShmLockFlags::EXCLUSIVE) {
                "exclusive"
            } else {
                "shared"
            };
            let lock_action = if flags.contains(ShmLockFlags::LOCK) {
                "lock"
            } else {
                "unlock"
            };
            let lock_str = format!("{} {} {} {}", offset, n, lock_action, lock_type);
            unsafe {
                state.exec_tcl(
                    "xShmLock",
                    Some(&self.filename),
                    Some(&lock_str),
                    None,
                    None,
                );
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        drop(state);
        self.real_file.shm_lock(offset, n, flags)
    }

    fn shm_barrier(&self) {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_SHMBARRIER_MASK) != 0 {
            unsafe {
                state.exec_tcl("xShmBarrier", Some(&self.filename), None, None, None);
            }
        }
        drop(state);
        self.real_file.shm_barrier();
    }

    fn shm_unmap(&self, delete: bool) -> crate::error::Result<()> {
        let state = self.state.lock().unwrap();
        if state.script.is_some() && (state.mask & TESTVFS_SHMCLOSE_MASK) != 0 {
            unsafe {
                state.exec_tcl("xShmUnmap", Some(&self.filename), None, None, None);
            }
            if let Some(code) = unsafe { state.result_code() } {
                if code != crate::error::ErrorCode::Ok {
                    return Err(crate::error::Error::new(code));
                }
            }
        }
        drop(state);
        self.real_file.shm_unmap(delete)
    }
}

// ============================================================================
// Testvfs VFS implementation
// ============================================================================

/// The VFS that wraps the real VFS and intercepts operations
struct TestvfsVfs {
    name: String,
    state: Arc<Mutex<TestvfsState>>,
}

impl Vfs for TestvfsVfs {
    fn name(&self) -> &str {
        &self.name
    }

    fn max_pathname(&self) -> i32 {
        // Delegate to default VFS
        if let Some(parent) = vfs::vfs_find(None) {
            parent.max_pathname()
        } else {
            1024
        }
    }

    fn open(
        &self,
        path: Option<&str>,
        flags: OpenFlags,
    ) -> crate::error::Result<Box<dyn VfsFile>> {
        let filename = path.unwrap_or("").to_string();

        // Execute callback if filtered
        {
            let state = self.state.lock().unwrap();
            if state.script.is_some() && (state.mask & TESTVFS_OPEN_MASK) != 0 {
                unsafe {
                    state.exec_tcl("xOpen", Some(&filename), None, None, None);
                }
                if let Some(code) = unsafe { state.result_code() } {
                    if code != crate::error::ErrorCode::Ok {
                        return Err(crate::error::Error::new(code));
                    }
                }
            }

            // Check cantopen fault injection
            if (state.mask & TESTVFS_OPEN_MASK) != 0 {
                drop(state);
                let mut state = self.state.lock().unwrap();
                if state.cantopenerr.inject() {
                    return Err(crate::error::Error::new(crate::error::ErrorCode::CantOpen));
                }
            }
        }

        // Get the default VFS and open the real file
        // We need to find a VFS that is NOT us (the parent)
        let parent = get_parent_vfs(&self.name)
            .ok_or_else(|| crate::error::Error::new(crate::error::ErrorCode::Error))?;
        let real_file = parent.open(path, flags)?;

        Ok(Box::new(TestvfsFile {
            real_file,
            filename,
            state: Arc::clone(&self.state),
        }))
    }

    fn delete(&self, path: &str, sync_dir: bool) -> crate::error::Result<()> {
        {
            let state = self.state.lock().unwrap();
            if state.script.is_some() && (state.mask & TESTVFS_DELETE_MASK) != 0 {
                let sync_str = if sync_dir { "1" } else { "0" };
                unsafe {
                    state.exec_tcl("xDelete", Some(path), Some(sync_str), None, None);
                }
                if let Some(code) = unsafe { state.result_code() } {
                    if code != crate::error::ErrorCode::Ok {
                        return Err(crate::error::Error::new(code));
                    }
                }
            }
            if (state.mask & TESTVFS_DELETE_MASK) != 0 {
                drop(state);
                let mut state = self.state.lock().unwrap();
                if state.ioerr.inject() {
                    return Err(crate::error::Error::new(crate::error::ErrorCode::IoErr));
                }
            }
        }

        let parent = get_parent_vfs(&self.name)
            .ok_or_else(|| crate::error::Error::new(crate::error::ErrorCode::Error))?;
        parent.delete(path, sync_dir)
    }

    fn access(&self, path: &str, flags: AccessFlags) -> crate::error::Result<bool> {
        {
            let state = self.state.lock().unwrap();
            if state.script.is_some() && (state.mask & TESTVFS_ACCESS_MASK) != 0 {
                unsafe {
                    state.exec_tcl("xAccess", Some(path), None, None, None);
                }
                if let Some(code) = unsafe { state.result_code() } {
                    if code != crate::error::ErrorCode::Ok {
                        return Err(crate::error::Error::new(code));
                    }
                }
            }
        }

        let parent = get_parent_vfs(&self.name)
            .ok_or_else(|| crate::error::Error::new(crate::error::ErrorCode::Error))?;
        parent.access(path, flags)
    }

    fn full_pathname(&self, path: &str) -> crate::error::Result<String> {
        {
            let state = self.state.lock().unwrap();
            if state.script.is_some() && (state.mask & TESTVFS_FULLPATHNAME_MASK) != 0 {
                unsafe {
                    state.exec_tcl("xFullPathname", Some(path), None, None, None);
                }
                if let Some(code) = unsafe { state.result_code() } {
                    if code != crate::error::ErrorCode::Ok {
                        return Err(crate::error::Error::new(code));
                    }
                }
            }
        }

        let parent = get_parent_vfs(&self.name)
            .ok_or_else(|| crate::error::Error::new(crate::error::ErrorCode::Error))?;
        parent.full_pathname(path)
    }

    fn randomness(&self, buf: &mut [u8]) -> i32 {
        if let Some(parent) = get_parent_vfs(&self.name) {
            parent.randomness(buf)
        } else {
            0
        }
    }

    fn sleep(&self, microseconds: i32) -> i32 {
        {
            let state = self.state.lock().unwrap();
            if state.script.is_some() && (state.mask & TESTVFS_SLEEP_MASK) != 0 {
                let us_str = microseconds.to_string();
                unsafe {
                    state.exec_tcl("xSleep", Some(&us_str), None, None, None);
                }
            }
        }

        if let Some(parent) = get_parent_vfs(&self.name) {
            parent.sleep(microseconds)
        } else {
            microseconds
        }
    }

    fn current_time(&self) -> f64 {
        if let Some(parent) = get_parent_vfs(&self.name) {
            parent.current_time()
        } else {
            0.0
        }
    }

    fn current_time_i64(&self) -> i64 {
        if let Some(parent) = get_parent_vfs(&self.name) {
            parent.current_time_i64()
        } else {
            0
        }
    }

    fn get_last_error(&self) -> (i32, String) {
        if let Some(parent) = get_parent_vfs(&self.name) {
            parent.get_last_error()
        } else {
            (0, String::new())
        }
    }
}

/// Helper: find the parent VFS (the first VFS that is not our testvfs name)
fn get_parent_vfs(exclude_name: &str) -> Option<Arc<dyn Vfs>> {
    crate::os::vfs::vfs_find_parent(exclude_name)
}

// ============================================================================
// TCL command: testvfs
// ============================================================================

/// The `testvfs` TCL command handler
///
/// Usage: testvfs VFSNAME ?-default BOOL? ?-noshm BOOL? ?-fullshm BOOL?
///                        ?-szosfile N? ?-mxpathname N? ?-iversion N?
pub unsafe extern "C" fn testvfs_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 || (objc % 2) != 0 {
        set_result_string(
            interp,
            "wrong # args: should be \"testvfs VFSNAME ?-noshm BOOL? ?-fullshm BOOL? ?-default BOOL? ?-mxpathname INT? ?-szosfile INT? ?-iversion INT?\"",
        );
        return TCL_ERROR;
    }

    let vfs_name = obj_to_string(*objv.offset(1));

    let mut is_noshm = false;
    let mut is_fullshm = false;
    let mut is_default = false;
    let mut _sz_os_file: i32 = 0;
    let mut _mx_pathname: i32 = -1;
    let mut _i_version: i32 = 3;

    // Parse options
    let mut i = 2;
    while i < objc as isize {
        let switch = obj_to_string(*objv.offset(i));
        if i + 1 >= objc as isize {
            set_result_string(interp, "missing argument for option");
            return TCL_ERROR;
        }

        match switch.as_str() {
            s if s.len() > 2 && "-noshm".starts_with(s) => {
                let mut val: c_int = 0;
                if Tcl_GetBooleanFromObj(interp, *objv.offset(i + 1), &mut val) != TCL_OK {
                    return TCL_ERROR;
                }
                is_noshm = val != 0;
                if is_noshm {
                    is_fullshm = false;
                }
            }
            s if s.len() > 2 && "-fullshm".starts_with(s) => {
                let mut val: c_int = 0;
                if Tcl_GetBooleanFromObj(interp, *objv.offset(i + 1), &mut val) != TCL_OK {
                    return TCL_ERROR;
                }
                is_fullshm = val != 0;
                if is_fullshm {
                    is_noshm = false;
                }
            }
            s if s.len() > 2 && "-default".starts_with(s) => {
                let mut val: c_int = 0;
                if Tcl_GetBooleanFromObj(interp, *objv.offset(i + 1), &mut val) != TCL_OK {
                    return TCL_ERROR;
                }
                is_default = val != 0;
            }
            s if s.len() > 2 && "-szosfile".starts_with(s) => {
                let mut val: c_int = 0;
                if Tcl_GetIntFromObj(interp, *objv.offset(i + 1), &mut val) != TCL_OK {
                    return TCL_ERROR;
                }
                _sz_os_file = val;
            }
            s if s.len() > 2 && "-mxpathname".starts_with(s) => {
                let mut val: c_int = 0;
                if Tcl_GetIntFromObj(interp, *objv.offset(i + 1), &mut val) != TCL_OK {
                    return TCL_ERROR;
                }
                _mx_pathname = val;
            }
            s if s.len() > 2 && "-iversion".starts_with(s) => {
                let mut val: c_int = 0;
                if Tcl_GetIntFromObj(interp, *objv.offset(i + 1), &mut val) != TCL_OK {
                    return TCL_ERROR;
                }
                _i_version = val;
            }
            _ => {
                set_result_string(
                    interp,
                    &format!("bad option \"{}\": must be -noshm, -fullshm, -default, -szosfile, -mxpathname, or -iversion", switch),
                );
                return TCL_ERROR;
            }
        }
        i += 2;
    }

    // Create shared state
    let mut state = TestvfsState::new(&vfs_name, interp);
    state.is_noshm = is_noshm;
    state.is_fullshm = is_fullshm;
    let state = Arc::new(Mutex::new(state));

    // Create the VFS
    let tvfs = TestvfsVfs {
        name: vfs_name.clone(),
        state: Arc::clone(&state),
    };

    // Register with the VFS registry
    if let Err(e) = vfs::vfs_register(Arc::new(tvfs), is_default) {
        set_result_string(interp, &format!("failed to register VFS: {}", e));
        return TCL_ERROR;
    }

    // Create the instance TCL command with the same name as the VFS
    let state_ptr = Box::into_raw(Box::new(TestvfsInstanceData {
        state: Arc::clone(&state),
        name: vfs_name.clone(),
    })) as *mut c_void;

    let cmd_cstr = CString::new(vfs_name).unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_cstr.as_ptr(),
        Some(testvfs_instance_cmd),
        state_ptr,
        Some(testvfs_instance_delete),
    );

    TCL_OK
}

/// Data attached to the testvfs instance TCL command
struct TestvfsInstanceData {
    state: Arc<Mutex<TestvfsState>>,
    name: String,
}

// Safety: see TestvfsState safety note
unsafe impl Send for TestvfsInstanceData {}
unsafe impl Sync for TestvfsInstanceData {}

/// Cleanup when the testvfs TCL command is deleted
unsafe extern "C" fn testvfs_instance_delete(client_data: *mut c_void) {
    if client_data.is_null() {
        return;
    }
    let data = Box::from_raw(client_data as *mut TestvfsInstanceData);

    // Clean up script reference
    {
        let mut state = data.state.lock().unwrap();
        if let Some(script) = state.script.take() {
            Tcl_DecrRefCount(script);
        }
    }

    // Unregister the VFS
    let _ = vfs::vfs_unregister(&data.name);

    // data is dropped here
}

/// The testvfs instance command handler
///
/// Dispatches to subcommands: shm, delete, filter, ioerr, fullerr,
/// cantopenerr, script, devchar, sectorsize
unsafe extern "C" fn testvfs_instance_cmd(
    client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if client_data.is_null() {
        set_result_string(interp, "testvfs: null client data");
        return TCL_ERROR;
    }
    let data = &*(client_data as *const TestvfsInstanceData);

    if objc < 2 {
        set_result_string(interp, "wrong # args: should be \"VFSNAME SUBCOMMAND ...\"");
        return TCL_ERROR;
    }

    let subcmd = obj_to_string(*objv.offset(1));

    match subcmd.as_str() {
        "shm" => testvfs_cmd_shm(data, interp, objc, objv),
        "delete" => testvfs_cmd_delete(data, interp, objc, objv),
        "filter" => testvfs_cmd_filter(data, interp, objc, objv),
        "ioerr" => testvfs_cmd_fault(data, interp, objc, objv, FaultType::IoErr),
        "fullerr" => testvfs_cmd_fault(data, interp, objc, objv, FaultType::FullErr),
        "cantopenerr" => testvfs_cmd_fault(data, interp, objc, objv, FaultType::CantOpenErr),
        "script" => testvfs_cmd_script(data, interp, objc, objv),
        "devchar" => testvfs_cmd_devchar(data, interp, objc, objv),
        "sectorsize" => testvfs_cmd_sectorsize(data, interp, objc, objv),
        _ => {
            set_result_string(
                interp,
                &format!(
                    "bad subcommand \"{}\": must be shm, delete, filter, ioerr, fullerr, cantopenerr, script, devchar, or sectorsize",
                    subcmd
                ),
            );
            TCL_ERROR
        }
    }
}

// ============================================================================
// Subcommand implementations
// ============================================================================

/// TESTVFS filter METHOD-LIST
unsafe fn testvfs_cmd_filter(
    data: &TestvfsInstanceData,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 3 {
        set_result_string(interp, "wrong # args: should be \"VFSNAME filter LIST\"");
        return TCL_ERROR;
    }

    let mut elem_count: c_int = 0;
    let mut elem_ptr: *const *mut Tcl_Obj = std::ptr::null();

    if Tcl_ListObjGetElements(interp, *objv.offset(2), &mut elem_count, &mut elem_ptr) != TCL_OK {
        return TCL_ERROR;
    }

    let mut mask: u32 = 0;
    for i in 0..elem_count as isize {
        let elem = obj_to_string(*elem_ptr.offset(i));
        match method_name_to_mask(&elem) {
            Some(m) => mask |= m,
            None => {
                set_result_string(interp, &format!("unknown method: {}", elem));
                return TCL_ERROR;
            }
        }
    }

    let mut state = data.state.lock().unwrap();
    state.mask = mask;
    Tcl_ResetResult(interp);

    TCL_OK
}

/// TESTVFS script ?SCRIPT?
unsafe fn testvfs_cmd_script(
    data: &TestvfsInstanceData,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    let mut state = data.state.lock().unwrap();

    if objc == 3 {
        // Set new script
        if let Some(old_script) = state.script.take() {
            Tcl_DecrRefCount(old_script);
        }

        let script_str = obj_to_string(*objv.offset(2));
        if !script_str.is_empty() {
            let new_script = Tcl_DuplicateObj(*objv.offset(2));
            Tcl_IncrRefCount(new_script);
            state.script = Some(new_script);
        }
    } else if objc != 2 {
        set_result_string(interp, "wrong # args: should be \"VFSNAME script ?SCRIPT?\"");
        return TCL_ERROR;
    }

    // Return current script
    Tcl_ResetResult(interp);
    if let Some(script) = state.script {
        Tcl_SetObjResult(interp, script);
    }

    TCL_OK
}

/// Fault type discriminator
enum FaultType {
    IoErr,
    FullErr,
    CantOpenErr,
}

/// TESTVFS ioerr|fullerr|cantopenerr ?CNT PERSIST?
unsafe fn testvfs_cmd_fault(
    data: &TestvfsInstanceData,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
    fault_type: FaultType,
) -> c_int {
    let mut state = data.state.lock().unwrap();

    let fault = match fault_type {
        FaultType::IoErr => &mut state.ioerr,
        FaultType::FullErr => &mut state.fullerr,
        FaultType::CantOpenErr => &mut state.cantopenerr,
    };

    // Get old fail count and reset
    let old_nfail = fault.reset();

    if objc == 4 {
        let mut cnt: c_int = 0;
        let mut persist: c_int = 0;

        if Tcl_GetIntFromObj(interp, *objv.offset(2), &mut cnt) != TCL_OK {
            return TCL_ERROR;
        }
        if Tcl_GetBooleanFromObj(interp, *objv.offset(3), &mut persist) != TCL_OK {
            return TCL_ERROR;
        }

        fault.fault = if persist != 0 {
            FAULT_INJECT_PERSISTENT
        } else {
            FAULT_INJECT_TRANSIENT
        };
        fault.cnt = cnt;
    } else if objc != 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"VFSNAME ioerr|fullerr|cantopenerr ?CNT PERSIST?\"",
        );
        return TCL_ERROR;
    }

    set_result_int(interp, old_nfail);
    TCL_OK
}

/// TESTVFS delete
unsafe fn testvfs_cmd_delete(
    _data: &TestvfsInstanceData,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    // Get command name from objv[0]
    let cmd_name = obj_to_string(*objv.offset(0));
    let cmd_cstr = CString::new(cmd_name).unwrap();
    Tcl_DeleteCommand(interp, cmd_cstr.as_ptr());
    TCL_OK
}

/// Device characteristic flag names
static DEVCHAR_FLAGS: &[(&str, u32)] = &[
    ("default", 0xFFFFFFFF), // Special sentinel
    ("atomic", 0x00000001),
    ("atomic512", 0x00000002),
    ("atomic1k", 0x00000004),
    ("atomic2k", 0x00000008),
    ("atomic4k", 0x00000010),
    ("atomic8k", 0x00000020),
    ("atomic16k", 0x00000040),
    ("atomic32k", 0x00000080),
    ("atomic64k", 0x00000100),
    ("sequential", 0x00000400),
    ("safe_append", 0x00000200),
    ("undeletable_when_open", 0x00000800),
    ("powersafe_overwrite", 0x00001000),
    ("immutable", 0x00002000),
    ("batch_atomic", 0x00004000),
];

/// TESTVFS devchar ?ATTR-LIST?
unsafe fn testvfs_cmd_devchar(
    data: &TestvfsInstanceData,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc > 3 {
        set_result_string(
            interp,
            "wrong # args: should be \"VFSNAME devchar ?ATTR-LIST?\"",
        );
        return TCL_ERROR;
    }

    let mut state = data.state.lock().unwrap();

    if objc == 3 {
        let mut elem_count: c_int = 0;
        let mut elem_ptr: *const *mut Tcl_Obj = std::ptr::null();

        if Tcl_ListObjGetElements(interp, *objv.offset(2), &mut elem_count, &mut elem_ptr)
            != TCL_OK
        {
            return TCL_ERROR;
        }

        let mut new_devchar: u32 = 0;
        for i in 0..elem_count as isize {
            let flag_name = obj_to_string(*elem_ptr.offset(i));
            let mut found = false;
            for (name, value) in DEVCHAR_FLAGS {
                if *name == flag_name {
                    if *value == 0xFFFFFFFF {
                        // "default" flag
                        if elem_count > 1 {
                            set_result_string(
                                interp,
                                &format!("bad flags: {}", obj_to_string(*objv.offset(2))),
                            );
                            return TCL_ERROR;
                        }
                        state.devchar = -1;
                        found = true;
                        break;
                    }
                    new_devchar |= value;
                    found = true;
                    break;
                }
            }
            if !found {
                set_result_string(interp, &format!("unknown flag: {}", flag_name));
                return TCL_ERROR;
            }
        }

        if state.devchar != -1 || elem_count > 0 {
            // Set the marker bit 0x10000000 like SQLite does
            state.devchar = (new_devchar | 0x10000000) as i32;
        }
    }

    // Build result list of active flags
    let ret_list = Tcl_NewListObj(0, std::ptr::null());
    if state.devchar >= 0 {
        let dc = state.devchar as u32;
        for (name, value) in DEVCHAR_FLAGS {
            if *value != 0xFFFFFFFF && (dc & value) != 0 {
                let name_c = CString::new(*name).unwrap();
                Tcl_ListObjAppendElement(
                    interp,
                    ret_list,
                    Tcl_NewStringObj(name_c.as_ptr(), name.len() as c_int),
                );
            }
        }
    }
    Tcl_SetObjResult(interp, ret_list);

    TCL_OK
}

/// TESTVFS sectorsize ?VALUE?
unsafe fn testvfs_cmd_sectorsize(
    data: &TestvfsInstanceData,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc > 3 {
        set_result_string(
            interp,
            "wrong # args: should be \"VFSNAME sectorsize ?VALUE?\"",
        );
        return TCL_ERROR;
    }

    let mut state = data.state.lock().unwrap();

    if objc == 3 {
        let mut val: c_int = 0;
        if Tcl_GetIntFromObj(interp, *objv.offset(2), &mut val) != TCL_OK {
            return TCL_ERROR;
        }
        state.sectorsize = val;
    }

    set_result_int(interp, state.sectorsize);
    TCL_OK
}

/// TESTVFS shm FILE ?VALUE?
unsafe fn testvfs_cmd_shm(
    _data: &TestvfsInstanceData,
    interp: *mut Tcl_Interp,
    objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 3 && objc != 4 {
        set_result_string(
            interp,
            "wrong # args: should be \"VFSNAME shm FILE ?VALUE?\"",
        );
        return TCL_ERROR;
    }

    // SHM operations require full WAL support which we don't have yet.
    // Return empty result for now - this allows tests to at least proceed
    // past the shm command.
    set_result_string(interp, "");
    TCL_OK
}

// ============================================================================
// TCL command: atomic_batch_write
// ============================================================================

/// The `atomic_batch_write` TCL command handler
///
/// Usage: atomic_batch_write FILENAME
/// Returns 1 if the VFS supports atomic batch writes, 0 otherwise.
/// We always return 0 since RustQL doesn't support this yet.
pub unsafe extern "C" fn atomic_batch_write_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    set_result_int(interp, 0);
    TCL_OK
}

// ============================================================================
// TCL command: file_control_reservebytes
// ============================================================================

/// The `file_control_reservebytes` TCL command handler
///
/// Usage: file_control_reservebytes DB N
/// Sets the number of reserve bytes per page. This is a no-op stub
/// that allows tests to proceed.
pub unsafe extern "C" fn file_control_reservebytes_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 3 {
        set_result_string(
            interp,
            "wrong # args: should be \"file_control_reservebytes DB N\"",
        );
        return TCL_ERROR;
    }
    // Stub: just return OK
    set_result_string(interp, "");
    TCL_OK
}

// ============================================================================
// TCL command: sqlite3_simulate_device
// ============================================================================

/// The `sqlite3_simulate_device` TCL command handler
///
/// Usage: sqlite3_simulate_device ?-sectorsize N? ?-characteristics FLAGS?
/// Configures the simulated device for testing. This is a no-op stub.
pub unsafe extern "C" fn sqlite3_simulate_device_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    set_result_string(interp, "");
    TCL_OK
}

