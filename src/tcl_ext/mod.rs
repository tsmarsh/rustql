//! TCL Extension for RustQL
//!
//! This module provides a TCL extension that implements the `sqlite3` command,
//! allowing RustQL to be used with SQLite's TCL test suite.
//!
//! Build with: cargo build --release --features tcl
//! Load in TCL: load ./target/release/librustql.so
//!
//! Usage in TCL:
//!   sqlite3 db :memory:
//!   db eval {CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT * FROM t}

// Allow raw pointer args in extern "C" functions (required for TCL FFI)
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_int;

use crate::{sqlite3_initialize, SqliteConnection};

// Submodules
pub mod db;
pub mod echo;
pub mod ffi;
pub mod helpers;
pub mod md5;
pub mod printf;
pub mod stubs;
pub mod tcl_module;
pub mod testvfs;
pub mod user_func;

// Re-exports for public API
pub use ffi::{Tcl_Interp, Tcl_Obj};
pub use user_func::{
    call_tcl_collation, call_tcl_user_function, has_tcl_collation, has_tcl_user_function,
    has_tcl_user_function_with_args,
};

use ffi::{Tcl_CreateObjCommand, TCL_ERROR, TCL_OK};

// Thread-local storage for database connections (TCL is single-threaded)
thread_local! {
    pub(crate) static CONNECTIONS: RefCell<HashMap<String, Box<SqliteConnection>>> = RefCell::new(HashMap::new());
    // Per-connection null value representation (default is empty string)
    pub(crate) static NULL_VALUES: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    // Current TCL interpreter for user function callbacks
    pub(crate) static CURRENT_INTERP: RefCell<Option<*mut Tcl_Interp>> = const { RefCell::new(None) };
    // User-defined TCL functions per connection: (db_name, func_name, argcount) -> tcl_proc_name
    // argcount of -1 means "any number of args"
    pub(crate) static USER_FUNCTIONS: RefCell<HashMap<(String, String, i32), String>> = RefCell::new(HashMap::new());
    // User-defined TCL collations per connection: (db_name, collation_name) -> tcl_proc_name
    pub(crate) static USER_COLLATIONS: RefCell<HashMap<(String, String), String>> = RefCell::new(HashMap::new());
    // Function destructors: (db_name, func_name, encoding) -> destructor_proc_name
    // encoding is the text encoding: "any", "utf8", "utf16le", "utf16be"
    pub(crate) static FUNCTION_DESTRUCTORS: RefCell<HashMap<(String, String, String), String>> = RefCell::new(HashMap::new());
}

// Force the linker to keep the init functions - they're called by TCL, not Rust code
// Without this, dead code elimination would remove them from the shared library
#[used]
static KEEP_TCL_INIT_FUNCTIONS: [extern "C" fn(*mut Tcl_Interp) -> c_int; 3] =
    [Rustql_Init, Tclsqlite3_Init, Sqlite3_Init];

/// Initialize the extension - called by TCL when loading
#[no_mangle]
pub extern "C" fn Rustql_Init(interp: *mut Tcl_Interp) -> c_int {
    // Initialize RustQL
    if let Err(e) = sqlite3_initialize() {
        eprintln!("Failed to initialize RustQL: {}", e);
        return TCL_ERROR;
    }

    unsafe {
        // Register the sqlite3 command
        let cmd_name = CString::new("sqlite3").unwrap();
        Tcl_CreateObjCommand(
            interp,
            cmd_name.as_ptr(),
            Some(db::sqlite3_cmd),
            std::ptr::null_mut(),
            None,
        );

        // Register test infrastructure stubs required by tester.tcl
        stubs::register_test_stubs(interp);
    }

    TCL_OK
}

/// Also provide Tclsqlite3_Init for compatibility with SQLite test harness
#[no_mangle]
pub extern "C" fn Tclsqlite3_Init(interp: *mut Tcl_Interp) -> c_int {
    Rustql_Init(interp)
}

/// Also provide Sqlite3_Init
#[no_mangle]
pub extern "C" fn Sqlite3_Init(interp: *mut Tcl_Interp) -> c_int {
    Rustql_Init(interp)
}
