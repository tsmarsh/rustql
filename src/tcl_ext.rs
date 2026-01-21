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
    sqlite3_changes, sqlite3_close, sqlite3_column_count, sqlite3_column_name, sqlite3_column_text,
    sqlite3_column_type, sqlite3_finalize, sqlite3_initialize, sqlite3_last_insert_rowid,
    sqlite3_open, sqlite3_prepare_v2, sqlite3_step, sqlite3_total_changes, SqliteConnection,
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

        // Register test infrastructure stubs required by tester.tcl
        register_test_stubs(interp);
    }

    TCL_OK
}

/// Register all test infrastructure stub commands
unsafe fn register_test_stubs(interp: *mut Tcl_Interp) {
    // List of test commands that return 0/empty
    let stub_commands = [
        "sqlite3_test_control_pending_byte",
        "sqlite3_soft_heap_limit64",
        "sqlite3_soft_heap_limit",
        "sqlite3_hard_heap_limit64",
        "sqlite3_config_memstatus",
        "sqlite3_config_pagecache",
        "sqlite3_config",
        "sqlite3_initialize",
        "sqlite3_shutdown",
        "sqlite3_enable_shared_cache",
        "sqlite3_extended_result_codes",
        "sqlite3_reset_auto_extension",
        "sqlite3_memdebug_settitle",
        "sqlite3_memdebug_dump",
        "sqlite3_memdebug_log",
        "sqlite3_memdebug_backtrace",
        "sqlite3_clear_tsd_memdebug",
        "sqlite3_crash_enable",
        "sqlite3_crash_on_write",
        "sqlite3_crashparams",
        "sqlite3_connection_pointer",
        "sqlite3_db_config",
        "sqlite3_db_filename",
        "sqlite3_db_status",
        "sqlite3_exec_nr",
        "sqlite3_next_stmt",
        "sqlite3_stmt_status",
        "sqlite3_unlock_notify",
        "sqlite3_wal_autocheckpoint",
        "autoinstall_test_functions",
        "install_malloc_faultsim",
        "sqlite3_memdebug_fail",
        "sqlite3_memdebug_pending",
        "database_never_corrupt",
        "database_may_be_corrupt",
        "optimization_control",
        "load_static_extension",
        "sqlite3_limit",
        "sqlite3_tcl_to_ptr",
        "sqlite3_register_cksumvfs",
        "sqlite3_register_tclcmd_cksumvfs",
        "extra_schema_checks",
        "sqlite3_test_control",
        "test_control_pending_byte",
        "sqlite3_create_function_v2",
        "sqlite3_create_function",
        "sqlite3_create_aggregate",
        "sqlite3_create_collation",
        "sqlite3_sleep",
        "sqlite3_busy_timeout",
        "sqlite3_interrupt",
        "sqlite3_bind_int",
        "sqlite3_bind_text",
        "sqlite3_bind_blob",
        "sqlite3_bind_null",
        "sqlite3_bind_double",
        "sqlite3_column_name",
        "sqlite3_column_type",
        "sqlite3_column_int",
        "sqlite3_column_int64",
        "sqlite3_column_double",
        "sqlite3_column_blob",
        "sqlite3_column_bytes",
        "sqlite3_reset",
        "sqlite3_clear_bindings",
        "sqlite3_errcode",
        "sqlite3_errmsg",
        "sqlite3_errmsg16",
        "sqlite3_extended_errcode",
        "sqlite3_result_int",
        "sqlite3_result_text",
        "sqlite3_result_blob",
        "sqlite3_result_null",
        "sqlite3_result_double",
        "sqlite3_result_error",
        "sqlite3_result_zeroblob",
        "sqlite3_value_int",
        "sqlite3_value_text",
        "sqlite3_value_blob",
        "sqlite3_value_type",
        "sqlite3_value_bytes",
        "sqlite3_aggregate_context",
        "sqlite3_get_auxdata",
        "sqlite3_set_auxdata",
        "sqlite3_complete",
        "sqlite3_complete16",
        "sqlite3_open",
        "sqlite3_open16",
        "sqlite3_open_v2",
        "sqlite3_close",
        "sqlite3_close_v2",
        "sqlite3_prepare_v2",
        "sqlite3_prepare",
        "sqlite3_step",
        "sqlite3_finalize",
        "sqlite3_db_handle",
        "sqlite3_changes",
        "sqlite3_total_changes",
        "sqlite3_last_insert_rowid",
        "sqlite3_get_autocommit",
        "sqlite3_data_count",
        "sqlite3_column_count",
        "sqlite3_column_text",
        "sqlite3_column_text16",
        "sqlite3_sql",
        "sqlite3_expanded_sql",
        "sqlite3_normalized_sql",
        "register_echo_module",
        "register_tclvar_module",
        "register_fs_module",
        "register_wholenumber_module",
        "register_regexp_module",
        "register_fuzzer_module",
        "register_unionvtab_module",
    ];

    for cmd in stub_commands {
        let cmd_name = CString::new(cmd).unwrap();
        Tcl_CreateObjCommand(
            interp,
            cmd_name.as_ptr(),
            Some(test_stub_return_zero),
            std::ptr::null_mut(),
            None,
        );
    }

    // Commands that return specific values
    let cmd_name = CString::new("sqlite3_memory_used").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("sqlite3_memory_highwater").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("sqlite3_status").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_status),
        std::ptr::null_mut(),
        None,
    );

    // Initialize sqlite_options array with capability flags
    // These flags tell the test harness which features are supported
    let sqlite_options = [
        // Core features we support
        ("cast", "1"),
        ("altertable", "1"),
        ("schema_pragmas", "1"),
        ("pragma", "1"),
        ("subquery", "1"),
        ("compound", "1"),
        ("view", "1"),
        ("trigger", "0"), // Triggers not fully supported yet
        ("foreignkey", "0"),
        ("vtab", "0"), // Virtual tables not supported
        ("auth", "0"), // Authorization not supported
        ("like_opt", "1"),
        ("cursorhints", "0"),
        ("stat4", "0"),
        ("lookaside", "0"),
        ("uri", "0"),
        ("wal", "0"),
        ("pager_pragmas", "1"),
        ("attach", "1"),
        ("vacuum", "0"), // Vacuum not fully supported yet
        ("tempdb", "1"),
        ("memorydb", "1"),
        ("explain", "1"),
        ("bloblit", "1"),
        ("integrityck", "0"),
        ("autoindex", "0"),
        ("analyze", "0"),
        ("datetime", "1"),
        ("long_double", "0"),
        ("encoding", "1"),
        ("incrblob", "0"),
        ("progress", "0"),
        ("windowfunc", "0"),
        ("cte", "0"),
        ("conflict", "1"),
        ("or_opt", "1"),
        ("update_delete_limit", "0"),
        ("between_opt", "1"),
    ];

    let arr_name = CString::new("::sqlite_options").unwrap();
    for (key, value) in &sqlite_options {
        let key_c = CString::new(*key).unwrap();
        let val_obj = Tcl_NewIntObj(value.parse::<c_int>().unwrap_or(0));
        Tcl_SetVar2Ex(
            interp,
            arr_name.as_ptr(),
            key_c.as_ptr(),
            val_obj,
            TCL_GLOBAL_ONLY,
        );
    }

    // Also set bitmask_size variable used by join3.test
    let bitmask_size_name = CString::new("::bitmask_size").unwrap();
    let bitmask_size_val = CString::new("64").unwrap();
    Tcl_SetVar(
        interp,
        bitmask_size_name.as_ptr(),
        bitmask_size_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );
}

/// Stub that returns 0
unsafe extern "C" fn test_stub_return_zero(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    set_result_int(interp, 0);
    TCL_OK
}

/// Stub for sqlite3_status - returns {0 0 0}
unsafe extern "C" fn test_stub_status(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    set_result_string(interp, "0 0 0");
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
/// Also handles: sqlite3 -has-codec (returns 0)
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

    // Get first argument
    let first_arg = obj_to_string(*objv.offset(1));

    // Handle special options
    if first_arg == "-has-codec" {
        // RustQL doesn't support encryption
        set_result_int(interp, 0);
        return TCL_OK;
    }
    if first_arg == "-version" {
        set_result_string(interp, "3.46.0");
        return TCL_OK;
    }

    // Get database handle name
    let db_name = first_arg;

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
            CONNECTIONS.with(|connections| {
                let conns = connections.borrow();
                if let Some(conn) = conns.get(db_name) {
                    set_result_int(interp, sqlite3_changes(conn) as c_int);
                } else {
                    set_result_int(interp, 0);
                }
            });
            TCL_OK
        }
        "total_changes" => {
            CONNECTIONS.with(|connections| {
                let conns = connections.borrow();
                if let Some(conn) = conns.get(db_name) {
                    set_result_int(interp, sqlite3_total_changes(conn) as c_int);
                } else {
                    set_result_int(interp, 0);
                }
            });
            TCL_OK
        }
        "last_insert_rowid" => {
            CONNECTIONS.with(|connections| {
                let conns = connections.borrow();
                if let Some(conn) = conns.get(db_name) {
                    set_result_int(interp, sqlite3_last_insert_rowid(conn) as c_int);
                } else {
                    set_result_int(interp, 0);
                }
            });
            TCL_OK
        }
        "exists" => {
            if objc < 3 {
                set_result_string(interp, "wrong # args: should be \"db exists SQL\"");
                return TCL_ERROR;
            }
            db_exists(db_name, interp, objv)
        }
        "onecolumn" | "one" => {
            if objc < 3 {
                set_result_string(interp, "wrong # args: should be \"db onecolumn SQL\"");
                return TCL_ERROR;
            }
            db_onecolumn(db_name, interp, objv)
        }
        "status" => {
            if objc >= 3 {
                let what = obj_to_string(*objv.offset(2));
                if what == "sort" {
                    // Return whether the most recent query performed a sort
                    let did_sort = crate::vdbe::get_sort_flag();
                    set_result_int(interp, if did_sort { 1 } else { 0 });
                } else {
                    // Other status queries return 0
                    set_result_int(interp, 0);
                }
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
/// Supports three forms:
/// - db eval SQL                    - returns results as a flat list
/// - db eval SQL array-name script  - sets array elements and runs script for each row
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

    // Check if we have array-name and script arguments
    let (array_name, script) = if objc >= 5 {
        let arr = obj_to_string(*objv.offset(3));
        let scr = obj_to_string(*objv.offset(4));
        (Some(arr), Some(scr))
    } else {
        (None, None)
    };

    // For array-script form, we need to collect rows first, then release the
    // connection borrow before calling Tcl_Eval (which may re-enter db_eval)
    if let (Some(ref arr_name), Some(ref scr)) = (&array_name, &script) {
        // Collect all rows with their column names and values
        let collected_rows: Result<Vec<(Vec<String>, Vec<String>)>, String> =
            CONNECTIONS.with(|connections| {
                let mut conns = connections.borrow_mut();
                let conn = match conns.get_mut(db_name) {
                    Some(c) => c.as_mut(),
                    None => {
                        return Err(format!("no such database: {}", db_name));
                    }
                };

                let mut rows = Vec::new();
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
                        Err(e) => return Err(e.sqlite_errmsg()),
                    };

                    if stmt.sql().is_empty() {
                        remaining = tail;
                        continue;
                    }

                    loop {
                        match sqlite3_step(&mut stmt) {
                            Ok(StepResult::Row) => {
                                let col_count = sqlite3_column_count(&stmt);
                                let mut col_names = Vec::new();
                                let mut col_values = Vec::new();

                                for i in 0..col_count {
                                    let col_name =
                                        sqlite3_column_name(&stmt, i).unwrap_or("").to_string();
                                    col_names.push(col_name);

                                    let col_type = sqlite3_column_type(&stmt, i);
                                    let value = match col_type {
                                        ColumnType::Null => "".to_string(),
                                        _ => sqlite3_column_text(&stmt, i),
                                    };
                                    col_values.push(value);
                                }

                                rows.push((col_names, col_values));
                            }
                            Ok(StepResult::Done) => break,
                            Err(e) => {
                                let _ = sqlite3_finalize(stmt);
                                return Err(e.sqlite_errmsg());
                            }
                        }
                    }

                    let _ = sqlite3_finalize(stmt);
                    remaining = tail;
                }

                Ok(rows)
            });

        // Now process the collected rows outside the borrow
        match collected_rows {
            Ok(rows) => {
                let arr_c =
                    CString::new(arr_name.as_str()).unwrap_or_else(|_| CString::new("").unwrap());
                let script_c =
                    CString::new(scr.as_str()).unwrap_or_else(|_| CString::new("").unwrap());

                const TCL_BREAK: c_int = 3;
                const TCL_CONTINUE: c_int = 4;

                for (col_names, col_values) in rows {
                    // Set array elements
                    for (name, value) in col_names.iter().zip(col_values.iter()) {
                        let col_c = CString::new(name.as_str())
                            .unwrap_or_else(|_| CString::new("").unwrap());
                        let val_obj = string_to_obj(value);
                        Tcl_SetVar2Ex(interp, arr_c.as_ptr(), col_c.as_ptr(), val_obj, 0);
                    }

                    // Set array(*) = list of column names
                    let star = CString::new("*").unwrap();
                    let names_list = Tcl_NewListObj(0, std::ptr::null());
                    for name in &col_names {
                        let name_obj = string_to_obj(name);
                        Tcl_ListObjAppendElement(interp, names_list, name_obj);
                    }
                    Tcl_SetVar2Ex(interp, arr_c.as_ptr(), star.as_ptr(), names_list, 0);

                    // Evaluate the script (now safe to call Tcl_Eval)
                    let eval_result = Tcl_Eval(interp, script_c.as_ptr());

                    if eval_result == TCL_BREAK {
                        return TCL_OK;
                    } else if eval_result == TCL_ERROR {
                        return TCL_ERROR;
                    }
                    // TCL_CONTINUE and TCL_OK: continue to next row
                }

                TCL_OK
            }
            Err(msg) => {
                set_result_string(interp, &msg);
                TCL_ERROR
            }
        }
    } else {
        // Simple form: execute SQL and collect results as a flat list
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
const TCL_GLOBAL_ONLY: c_int = 1;

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

    fn Tcl_SetVar2Ex(
        interp: *mut Tcl_Interp,
        part1: *const c_char,
        part2: *const c_char,
        newValuePtr: *mut Tcl_Obj,
        flags: c_int,
    ) -> *mut Tcl_Obj;

    fn Tcl_UnsetVar2(
        interp: *mut Tcl_Interp,
        part1: *const c_char,
        part2: *const c_char,
        flags: c_int,
    ) -> c_int;

    fn Tcl_SetVar(
        interp: *mut Tcl_Interp,
        varName: *const c_char,
        newValue: *const c_char,
        flags: c_int,
    ) -> *const c_char;
}
