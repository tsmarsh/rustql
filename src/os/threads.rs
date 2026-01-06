//! Threading support (threads.c translation).

use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use crate::api::{global_config, ThreadingMode};
use crate::error::ErrorCode;

pub type ThreadTask = fn(*mut c_void) -> *mut c_void;

#[derive(Debug)]
enum ThreadState {
    Running(JoinHandle<usize>),
    InlinePending {
        task: ThreadTask,
        input: *mut c_void,
    },
    InlineDone {
        result: *mut c_void,
    },
}

/// SQLite thread handle.
#[derive(Debug)]
pub struct SqliteThread {
    state: ThreadState,
}

static RUN_INLINE_TOGGLE: AtomicBool = AtomicBool::new(false);

fn is_single_threaded() -> bool {
    matches!(
        *global_config().threading_mode.read().unwrap(),
        ThreadingMode::SingleThread
    )
}

fn run_inline_now() -> bool {
    RUN_INLINE_TOGGLE.fetch_xor(true, Ordering::SeqCst)
}

pub fn sqlite3_thread_create(
    out: &mut Option<SqliteThread>,
    task: ThreadTask,
    input: *mut c_void,
) -> ErrorCode {
    *out = None;

    if is_single_threaded() {
        let state = if run_inline_now() {
            ThreadState::InlineDone {
                result: task(input),
            }
        } else {
            ThreadState::InlinePending { task, input }
        };
        *out = Some(SqliteThread { state });
        return ErrorCode::Ok;
    }

    let input_ptr = input as usize;
    let spawn_result = thread::Builder::new().spawn(move || {
        let input = input_ptr as *mut c_void;
        task(input) as usize
    });
    let state = match spawn_result {
        Ok(handle) => ThreadState::Running(handle),
        Err(_) => ThreadState::InlineDone {
            result: task(input),
        },
    };
    *out = Some(SqliteThread { state });
    ErrorCode::Ok
}

pub fn sqlite3_thread_join(thread: Option<SqliteThread>, out: &mut *mut c_void) -> ErrorCode {
    let thread = match thread {
        Some(thread) => thread,
        None => return ErrorCode::NoMem,
    };

    match thread.state {
        ThreadState::Running(handle) => match handle.join() {
            Ok(result) => {
                *out = result as *mut c_void;
                ErrorCode::Ok
            }
            Err(_) => ErrorCode::Error,
        },
        ThreadState::InlinePending { task, input } => {
            *out = task(input);
            ErrorCode::Ok
        }
        ThreadState::InlineDone { result } => {
            *out = result;
            ErrorCode::Ok
        }
    }
}
