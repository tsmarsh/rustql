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

use crate::types::{ColumnType, StepResult};
use crate::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_column_type,
    sqlite3_finalize, sqlite3_initialize, sqlite3_open, sqlite3_prepare_v2, sqlite3_step,
    SqliteConnection,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};

// Thread-local storage for database connections (TCL is single-threaded)
thread_local! {
    static CONNECTIONS: RefCell<HashMap<String, Box<SqliteConnection>>> = RefCell::new(HashMap::new());
}

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
            Some(sqlite3_cmd),
            std::ptr::null_mut(),
            None,
        );
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

/// The sqlite3 command - creates a database handle
/// Usage: sqlite3 DBNAME FILENAME ?-options?
unsafe extern "C" fn sqlite3_cmd(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"sqlite3 DBNAME ?FILENAME? ?-options?\"",
        );
        return TCL_ERROR;
    }

    // Get database handle name
    let db_name = obj_to_string(*objv.offset(1));

    // Get filename (default to :memory:)
    let filename = if objc >= 3 {
        obj_to_string(*objv.offset(2))
    } else {
        ":memory:".to_string()
    };

    // Open the database
    let conn = match sqlite3_open(&filename) {
        Ok(c) => c,
        Err(e) => {
            set_result_string(interp, &format!("unable to open database: {}", e));
            return TCL_ERROR;
        }
    };

    // Store the connection
    CONNECTIONS.with(|connections| {
        connections.borrow_mut().insert(db_name.clone(), conn);
    });

    // Create the database command
    let cmd_name = CString::new(db_name.clone()).unwrap();
    let db_name_ptr = Box::into_raw(Box::new(db_name)) as *mut std::ffi::c_void;

    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(db_cmd),
        db_name_ptr,
        Some(db_delete_cmd),
    );

    TCL_OK
}

/// Database instance command - handles db eval, db close, etc.
unsafe extern "C" fn db_cmd(
    client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(interp, "wrong # args: should be \"db method ?args?\"");
        return TCL_ERROR;
    }

    let db_name = &*(client_data as *const String);
    let method = obj_to_string(*objv.offset(1));

    match method.as_str() {
        "eval" => db_eval(db_name, interp, objc, objv),
        "close" => db_close(db_name, interp),
        "errorcode" => {
            set_result_int(interp, 0);
            TCL_OK
        }
        "changes" => {
            set_result_int(interp, 0);
            TCL_OK
        }
        "total_changes" => {
            set_result_int(interp, 0);
            TCL_OK
        }
        "last_insert_rowid" => {
            set_result_int(interp, 0);
            TCL_OK
        }
        "exists" => {
            if objc < 3 {
                set_result_string(interp, "wrong # args: should be \"db exists SQL\"");
                return TCL_ERROR;
            }
            db_exists(db_name, interp, objv)
        }
        "onecolumn" => {
            if objc < 3 {
                set_result_string(interp, "wrong # args: should be \"db onecolumn SQL\"");
                return TCL_ERROR;
            }
            db_onecolumn(db_name, interp, objv)
        }
        "status" => {
            if objc >= 3 {
                // All status queries return 0 for now
                set_result_int(interp, 0);
            } else {
                set_result_string(interp, "");
            }
            TCL_OK
        }
        "version" => {
            set_result_string(interp, "3.0.0");
            TCL_OK
        }
        "function"
        | "collate"
        | "trace"
        | "profile"
        | "nullvalue"
        | "progress"
        | "busy"
        | "timeout"
        | "cache"
        | "enable_load_extension"
        | "authorizer"
        | "update_hook"
        | "commit_hook"
        | "rollback_hook"
        | "wal_hook"
        | "preupdate" => {
            // Stub these methods - accept but ignore
            TCL_OK
        }
        "transaction" => {
            // Simple transaction support - just execute the script
            if objc >= 3 {
                // The script is the last argument
                let script = obj_to_string(*objv.offset(objc as isize - 1));
                // Execute the script by evaluating it
                let script_c = CString::new(script).unwrap();
                Tcl_Eval(interp, script_c.as_ptr())
            } else {
                TCL_OK
            }
        }
        _ => {
            set_result_string(interp, &format!("unknown method: {}", method));
            TCL_ERROR
        }
    }
}

/// Execute SQL and return results as a TCL list
unsafe fn db_eval(
    db_name: &str,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 3 {
        set_result_string(
            interp,
            "wrong # args: should be \"db eval SQL ?array-name? ?script?\"",
        );
        return TCL_ERROR;
    }

    let sql = obj_to_string(*objv.offset(2));

    // Execute SQL and collect results
    let result_list = Tcl_NewListObj(0, std::ptr::null());

    CONNECTIONS.with(|connections| {
        let mut conns = connections.borrow_mut();
        let conn = match conns.get_mut(db_name) {
            Some(c) => c.as_mut(),
            None => {
                set_result_string(interp, &format!("no such database: {}", db_name));
                return TCL_ERROR;
            }
        };

        let mut remaining = sql.as_str();

        while !remaining.trim().is_empty() {
            let trimmed = remaining.trim_start();
            if trimmed.starts_with("--") {
                if let Some(pos) = trimmed.find('\n') {
                    remaining = &trimmed[pos + 1..];
                    continue;
                } else {
                    break;
                }
            }

            let (mut stmt, tail) = match sqlite3_prepare_v2(conn, remaining) {
                Ok(r) => r,
                Err(e) => {
                    // result_list is unmanaged, but TCL handles cleanup
                    set_result_string(interp, &e.sqlite_errmsg());
                    return TCL_ERROR;
                }
            };

            if stmt.sql().is_empty() {
                remaining = tail;
                continue;
            }

            loop {
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        let col_count = sqlite3_column_count(&stmt);
                        for i in 0..col_count {
                            let col_type = sqlite3_column_type(&stmt, i);
                            let value = match col_type {
                                ColumnType::Null => "".to_string(),
                                _ => sqlite3_column_text(&stmt, i),
                            };
                            let obj = string_to_obj(&value);
                            Tcl_ListObjAppendElement(interp, result_list, obj);
                        }
                    }
                    Ok(StepResult::Done) => break,
                    Err(e) => {
                        let _ = sqlite3_finalize(stmt);
                        set_result_string(interp, &e.sqlite_errmsg());
                        return TCL_ERROR;
                    }
                }
            }

            let _ = sqlite3_finalize(stmt);
            remaining = tail;
        }

        Tcl_SetObjResult(interp, result_list);
        TCL_OK
    })
}

/// Check if query returns any rows
unsafe fn db_exists(db_name: &str, interp: *mut Tcl_Interp, objv: *const *mut Tcl_Obj) -> c_int {
    let sql = obj_to_string(*objv.offset(2));

    CONNECTIONS.with(|connections| {
        let mut conns = connections.borrow_mut();
        let conn = match conns.get_mut(db_name) {
            Some(c) => c.as_mut(),
            None => {
                set_result_string(interp, &format!("no such database: {}", db_name));
                return TCL_ERROR;
            }
        };

        let (mut stmt, _) = match sqlite3_prepare_v2(conn, &sql) {
            Ok(r) => r,
            Err(e) => {
                set_result_string(interp, &e.sqlite_errmsg());
                return TCL_ERROR;
            }
        };

        let exists = match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => 1,
            _ => 0,
        };

        let _ = sqlite3_finalize(stmt);
        set_result_int(interp, exists);
        TCL_OK
    })
}

/// Return first column of first row
unsafe fn db_onecolumn(db_name: &str, interp: *mut Tcl_Interp, objv: *const *mut Tcl_Obj) -> c_int {
    let sql = obj_to_string(*objv.offset(2));

    CONNECTIONS.with(|connections| {
        let mut conns = connections.borrow_mut();
        let conn = match conns.get_mut(db_name) {
            Some(c) => c.as_mut(),
            None => {
                set_result_string(interp, &format!("no such database: {}", db_name));
                return TCL_ERROR;
            }
        };

        let (mut stmt, _) = match sqlite3_prepare_v2(conn, &sql) {
            Ok(r) => r,
            Err(e) => {
                set_result_string(interp, &e.sqlite_errmsg());
                return TCL_ERROR;
            }
        };

        let result = match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                let col_type = sqlite3_column_type(&stmt, 0);
                match col_type {
                    ColumnType::Null => "".to_string(),
                    _ => sqlite3_column_text(&stmt, 0),
                }
            }
            _ => "".to_string(),
        };

        let _ = sqlite3_finalize(stmt);
        set_result_string(interp, &result);
        TCL_OK
    })
}

/// Close database connection
unsafe fn db_close(db_name: &str, interp: *mut Tcl_Interp) -> c_int {
    CONNECTIONS.with(|connections| {
        if let Some(conn) = connections.borrow_mut().remove(db_name) {
            let _ = sqlite3_close(conn);
        }
    });

    // Delete the command
    let cmd_name = CString::new(db_name).unwrap();
    Tcl_DeleteCommand(interp, cmd_name.as_ptr());

    TCL_OK
}

/// Cleanup callback when db command is deleted
unsafe extern "C" fn db_delete_cmd(client_data: *mut std::ffi::c_void) {
    if !client_data.is_null() {
        let db_name = Box::from_raw(client_data as *mut String);
        CONNECTIONS.with(|connections| {
            if let Some(conn) = connections.borrow_mut().remove(&*db_name) {
                let _ = sqlite3_close(conn);
            }
        });
    }
}

// Helper functions

unsafe fn obj_to_string(obj: *mut Tcl_Obj) -> String {
    let mut len: c_int = 0;
    let ptr = Tcl_GetStringFromObj(obj, &mut len);
    if ptr.is_null() {
        return String::new();
    }
    let slice = std::slice::from_raw_parts(ptr as *const u8, len as usize);
    String::from_utf8_lossy(slice).to_string()
}

unsafe fn string_to_obj(s: &str) -> *mut Tcl_Obj {
    let c_str = CString::new(s).unwrap_or_else(|_| CString::new("").unwrap());
    Tcl_NewStringObj(c_str.as_ptr(), s.len() as c_int)
}

unsafe fn set_result_string(interp: *mut Tcl_Interp, s: &str) {
    let obj = string_to_obj(s);
    Tcl_SetObjResult(interp, obj);
}

unsafe fn set_result_int(interp: *mut Tcl_Interp, i: i32) {
    let obj = Tcl_NewIntObj(i);
    Tcl_SetObjResult(interp, obj);
}

// TCL C API bindings
const TCL_OK: c_int = 0;
const TCL_ERROR: c_int = 1;

#[repr(C)]
pub struct Tcl_Interp {
    _private: [u8; 0],
}

#[repr(C)]
pub struct Tcl_Obj {
    _private: [u8; 0],
}

type Tcl_ObjCmdProc = unsafe extern "C" fn(
    *mut std::ffi::c_void,
    *mut Tcl_Interp,
    c_int,
    *const *mut Tcl_Obj,
) -> c_int;

type Tcl_CmdDeleteProc = unsafe extern "C" fn(*mut std::ffi::c_void);

extern "C" {
    fn Tcl_CreateObjCommand(
        interp: *mut Tcl_Interp,
        cmdName: *const c_char,
        proc: Option<Tcl_ObjCmdProc>,
        clientData: *mut std::ffi::c_void,
        deleteProc: Option<Tcl_CmdDeleteProc>,
    ) -> *mut std::ffi::c_void;

    fn Tcl_DeleteCommand(interp: *mut Tcl_Interp, cmdName: *const c_char) -> c_int;

    fn Tcl_SetObjResult(interp: *mut Tcl_Interp, objPtr: *mut Tcl_Obj);

    fn Tcl_GetStringFromObj(objPtr: *mut Tcl_Obj, lengthPtr: *mut c_int) -> *const c_char;

    fn Tcl_NewStringObj(bytes: *const c_char, length: c_int) -> *mut Tcl_Obj;

    fn Tcl_NewIntObj(intValue: c_int) -> *mut Tcl_Obj;

    fn Tcl_NewListObj(objc: c_int, objv: *const *mut Tcl_Obj) -> *mut Tcl_Obj;

    fn Tcl_ListObjAppendElement(
        interp: *mut Tcl_Interp,
        listPtr: *mut Tcl_Obj,
        objPtr: *mut Tcl_Obj,
    ) -> c_int;

    fn Tcl_Eval(interp: *mut Tcl_Interp, script: *const c_char) -> c_int;
}
