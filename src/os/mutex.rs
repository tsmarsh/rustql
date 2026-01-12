//! Mutex implementation (mutex.c translation).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;

use crate::api::{global_config, ThreadingMode};
use crate::error::ErrorCode;

pub const SQLITE_MUTEX_FAST: i32 = 0;
pub const SQLITE_MUTEX_RECURSIVE: i32 = 1;
pub const SQLITE_MUTEX_STATIC_MASTER: i32 = 2;
pub const SQLITE_MUTEX_STATIC_MEM: i32 = 3;
pub const SQLITE_MUTEX_STATIC_OPEN: i32 = 4;
pub const SQLITE_MUTEX_STATIC_PRNG: i32 = 5;
pub const SQLITE_MUTEX_STATIC_LRU: i32 = 6;
pub const SQLITE_MUTEX_STATIC_PMEM: i32 = 7;
pub const SQLITE_MUTEX_STATIC_APP1: i32 = 8;
pub const SQLITE_MUTEX_STATIC_APP2: i32 = 9;
pub const SQLITE_MUTEX_STATIC_APP3: i32 = 10;
pub const SQLITE_MUTEX_STATIC_VFS1: i32 = 11;
pub const SQLITE_MUTEX_STATIC_VFS2: i32 = 12;
pub const SQLITE_MUTEX_STATIC_VFS3: i32 = 13;

const SQLITE_MUTEX_STATIC_COUNT: usize = 12;

static MUTEX_INIT: AtomicBool = AtomicBool::new(false);
static STATIC_MUTEXES: std::sync::OnceLock<Vec<Arc<SqliteMutex>>> = std::sync::OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutexKind {
    Fast,
    Recursive,
    Static,
}

#[derive(Debug)]
struct MutexState {
    owner: Option<ThreadId>,
    count: u32,
}

/// SQLite mutex handle.
#[derive(Debug)]
pub struct SqliteMutex {
    kind: MutexKind,
    state: Mutex<MutexState>,
    condvar: Condvar,
}

impl SqliteMutex {
    fn new(kind: MutexKind) -> Self {
        Self {
            kind,
            state: Mutex::new(MutexState {
                owner: None,
                count: 0,
            }),
            condvar: Condvar::new(),
        }
    }

    fn enter(&self) {
        let tid = std::thread::current().id();
        let mut guard = self.state.lock().unwrap();
        loop {
            match guard.owner {
                None => {
                    guard.owner = Some(tid);
                    guard.count = 1;
                    return;
                }
                Some(owner) if owner == tid && self.kind == MutexKind::Recursive => {
                    guard.count += 1;
                    return;
                }
                _ => {
                    guard = self.condvar.wait(guard).unwrap();
                }
            }
        }
    }

    fn try_enter(&self) -> bool {
        let tid = std::thread::current().id();
        let mut guard = self.state.lock().unwrap();
        match guard.owner {
            None => {
                guard.owner = Some(tid);
                guard.count = 1;
                true
            }
            Some(owner) if owner == tid && self.kind == MutexKind::Recursive => {
                guard.count += 1;
                true
            }
            _ => false,
        }
    }

    fn leave(&self) {
        let tid = std::thread::current().id();
        let mut guard = self.state.lock().unwrap();
        if guard.owner == Some(tid) {
            guard.count = guard.count.saturating_sub(1);
            if guard.count == 0 {
                guard.owner = None;
                self.condvar.notify_one();
            }
        }
    }

    fn held(&self) -> bool {
        self.state.lock().unwrap().owner == Some(std::thread::current().id())
    }

    fn not_held(&self) -> bool {
        !self.held()
    }
}

fn is_single_threaded() -> bool {
    matches!(
        *global_config().threading_mode.read().unwrap(),
        ThreadingMode::SingleThread
    )
}

pub fn mutex_init() -> ErrorCode {
    if MUTEX_INIT.swap(true, Ordering::SeqCst) {
        return ErrorCode::Ok;
    }
    let mut statics = Vec::with_capacity(SQLITE_MUTEX_STATIC_COUNT);
    for _ in 0..SQLITE_MUTEX_STATIC_COUNT {
        statics.push(Arc::new(SqliteMutex::new(MutexKind::Static)));
    }
    let _ = STATIC_MUTEXES.set(statics);
    ErrorCode::Ok
}

pub fn mutex_end() -> ErrorCode {
    MUTEX_INIT.store(false, Ordering::SeqCst);
    ErrorCode::Ok
}

pub fn mutex_alloc(id: i32) -> Option<Arc<SqliteMutex>> {
    if is_single_threaded() {
        return None;
    }

    if id == SQLITE_MUTEX_FAST {
        return Some(Arc::new(SqliteMutex::new(MutexKind::Fast)));
    }
    if id == SQLITE_MUTEX_RECURSIVE {
        return Some(Arc::new(SqliteMutex::new(MutexKind::Recursive)));
    }
    if id >= SQLITE_MUTEX_STATIC_MASTER {
        let index = (id - SQLITE_MUTEX_STATIC_MASTER) as usize;
        if let Some(statics) = STATIC_MUTEXES.get() {
            return statics.get(index).cloned();
        }
    }
    None
}

pub fn mutex_free(_mutex: Option<Arc<SqliteMutex>>) {}

pub fn mutex_enter(mutex: Option<&Arc<SqliteMutex>>) {
    if let Some(m) = mutex {
        m.enter();
    }
}

pub fn mutex_try(mutex: Option<&Arc<SqliteMutex>>) -> ErrorCode {
    if let Some(m) = mutex {
        if m.try_enter() {
            ErrorCode::Ok
        } else {
            ErrorCode::Busy
        }
    } else {
        ErrorCode::Ok
    }
}

pub fn mutex_leave(mutex: Option<&Arc<SqliteMutex>>) {
    if let Some(m) = mutex {
        m.leave();
    }
}

pub fn mutex_held(mutex: Option<&Arc<SqliteMutex>>) -> bool {
    mutex.is_none_or(|m| m.held())
}

pub fn mutex_notheld(mutex: Option<&Arc<SqliteMutex>>) -> bool {
    mutex.is_none_or(|m| m.not_held())
}
