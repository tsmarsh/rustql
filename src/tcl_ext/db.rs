//! Database connection and query commands
//!
//! This module handles all database connection management and SQL execution commands
//! for the RustQL TCL extension. It includes:
//!
//! - `sqlite3` command: Creates database connections
//! - `sqlite3_txn_state`: Returns transaction state
//! - `sqlite3_exec`: Executes SQL and returns results
//! - `sqlite3_exec_hex`: Executes SQL with hex-encoded bytes
//! - Database instance methods: eval, close, changes, exists, onecolumn, etc.

use std::ffi::CString;
use std::os::raw::c_int;

use super::ffi::{
    Tcl_CreateObjCommand, Tcl_DeleteCommand, Tcl_Eval, Tcl_GetVar, Tcl_Interp,
    Tcl_ListObjAppendElement, Tcl_NewListObj, Tcl_Obj, Tcl_SetObjResult, Tcl_SetVar, Tcl_SetVar2Ex,
    TCL_ERROR, TCL_GLOBAL_ONLY, TCL_OK,
};
use super::helpers::{obj_to_string, set_result_int, set_result_string, string_to_obj};
use super::user_func::{clear_current_interp, set_current_interp};
use super::{CONNECTIONS, NULL_VALUES, USER_FUNCTIONS};

use crate::api::{
    sqlite3_bind_double, sqlite3_bind_int64, sqlite3_bind_null, sqlite3_bind_parameter_count,
    sqlite3_bind_parameter_name, sqlite3_bind_text, PreparedStmt,
};
use crate::types::{ColumnType, StepResult};
use crate::vdbe::{
    get_like_count, get_search_count, get_sort_count, reset_like_count, reset_search_count,
    reset_sort_count, reset_step_count,
};
use crate::{
    sqlite3_changes, sqlite3_close, sqlite3_column_count, sqlite3_column_name, sqlite3_column_text,
    sqlite3_column_type, sqlite3_finalize, sqlite3_last_insert_rowid, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_step, sqlite3_total_changes, SqliteConnection,
};

/// Usage: sqlite3_txn_state DB ?SCHEMA?
/// Returns: -1 (error), 0 (none), 1 (read), 2 (write)
pub unsafe extern "C" fn sqlite3_txn_state_cmd(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"sqlite3_txn_state DB ?SCHEMA?\"",
        );
        return TCL_ERROR;
    }

    let db_name = obj_to_string(*objv.offset(1));

    // Get transaction state
    let state = CONNECTIONS.with(|connections| {
        let conns = connections.borrow();
        if let Some(conn) = conns.get(&db_name) {
            // Check if in autocommit mode
            use crate::api::TransactionState;
            match conn.transaction_state {
                TransactionState::None => 0,  // No transaction
                TransactionState::Read => 1,  // Read transaction
                TransactionState::Write => 2, // Write transaction
            }
        } else {
            -1 // Error - no such connection
        }
    });

    set_result_int(interp, state);
    TCL_OK
}

/// sqlite3_get_autocommit - returns 1 if autocommit is on, else 0
/// Usage: sqlite3_get_autocommit DB
pub unsafe extern "C" fn sqlite3_get_autocommit_cmd(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"sqlite3_get_autocommit DB\"",
        );
        return TCL_ERROR;
    }

    let db_name = obj_to_string(*objv.offset(1));
    let autocommit = CONNECTIONS.with(|connections| {
        let conns = connections.borrow();
        conns
            .get(&db_name)
            .map(|conn| conn.get_autocommit())
            .unwrap_or(false)
    });

    set_result_int(interp, if autocommit { 1 } else { 0 });
    TCL_OK
}

/// sqlite3_exec - execute SQL and return result code and results
/// Usage: sqlite3_exec DB SQL
/// Returns: {RESULT_CODE {results}}
pub unsafe extern "C" fn sqlite3_exec_cmd(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 3 {
        set_result_string(interp, "wrong # args: should be \"sqlite3_exec DB SQL\"");
        return TCL_ERROR;
    }

    let db_name = obj_to_string(*objv.offset(1));
    let sql = obj_to_string(*objv.offset(2));

    // Execute the SQL and collect results
    let (result_code, results) = CONNECTIONS.with(|connections| {
        let mut conns = connections.borrow_mut();
        if let Some(conn) = conns.get_mut(&db_name) {
            let mut all_results: Vec<String> = Vec::new();

            // Execute the SQL
            match execute_sql_collecting_results(conn, &sql) {
                Ok(rows) => {
                    for row in rows {
                        all_results.extend(row);
                    }
                    (0, all_results)
                }
                Err(_e) => (1, vec![]), // Error
            }
        } else {
            (1, vec![]) // No such connection
        }
    });

    // Create result list: {code {results}}
    let result_str = if results.is_empty() {
        format!("{} {{}}", result_code)
    } else {
        format!("{} {{{}}}", result_code, results.join(" "))
    };
    set_result_string(interp, &result_str);
    TCL_OK
}

/// sqlite3_exec_hex - execute SQL with hex-encoded bytes
/// Usage: sqlite3_exec_hex DB SQL
/// Decodes %XX sequences in SQL before executing
/// Returns: {RESULT_CODE {column_name value ...}}
pub unsafe extern "C" fn sqlite3_exec_hex_cmd(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 3 {
        set_result_string(
            interp,
            "wrong # args: should be \"sqlite3_exec_hex DB SQL\"",
        );
        return TCL_ERROR;
    }

    let db_name = obj_to_string(*objv.offset(1));
    let sql_hex = obj_to_string(*objv.offset(2));

    // Decode hex escapes in SQL: %XX -> byte
    let sql = decode_hex_escapes(&sql_hex);

    // Execute the SQL and collect results
    let (result_code, results) = CONNECTIONS.with(|connections| {
        let mut conns = connections.borrow_mut();
        if let Some(conn) = conns.get_mut(&db_name) {
            // Execute the SQL
            match execute_sql_collecting_results(conn, &sql) {
                Ok(rows) => {
                    let mut all_results: Vec<String> = Vec::new();
                    for row in rows {
                        all_results.extend(row);
                    }
                    (0, all_results)
                }
                Err(_e) => (1, vec![]), // Error
            }
        } else {
            (1, vec![]) // No such connection
        }
    });

    // Create result list: {code {column value ...}}
    let result_str = if results.is_empty() {
        format!("{} {{}}", result_code)
    } else {
        format!("{} {{{}}}", result_code, results.join(" "))
    };
    set_result_string(interp, &result_str);
    TCL_OK
}

/// Decode hex escapes (%XX) in a string
fn decode_hex_escapes(s: &str) -> String {
    let mut result = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            // Try to parse the next two chars as hex
            let hex_str = &s[i + 1..i + 3];
            if let Ok(byte) = u8::from_str_radix(hex_str, 16) {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }

    String::from_utf8_lossy(&result).into_owned()
}

/// Execute SQL and collect all result rows
fn execute_sql_collecting_results(
    conn: &mut SqliteConnection,
    sql: &str,
) -> Result<Vec<Vec<String>>, crate::error::Error> {
    let mut results = Vec::new();
    let mut remaining = sql;

    while !remaining.trim().is_empty() {
        let trimmed = remaining.trim_start();

        // Skip comments
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
            Err(e) => return Err(e),
        };

        if stmt.sql().is_empty() {
            remaining = tail;
            continue;
        }

        // Step through results
        loop {
            match sqlite3_step(&mut stmt) {
                Ok(StepResult::Row) => {
                    let col_count = sqlite3_column_count(&stmt);
                    let mut row = Vec::new();

                    // Get column names and values
                    for i in 0..col_count {
                        let col_name = sqlite3_column_name(&stmt, i).unwrap_or("").to_string();
                        let col_value = sqlite3_column_text(&stmt, i);
                        row.push(col_name);
                        row.push(col_value);
                    }
                    results.push(row);
                }
                Ok(StepResult::Done) => break,
                Err(e) => {
                    let _ = sqlite3_finalize(stmt);
                    return Err(e);
                }
            }
        }

        let _ = sqlite3_finalize(stmt);
        remaining = tail;
    }

    Ok(results)
}

/// working_64bit_int - returns 1 if platform supports 64-bit integers
pub unsafe extern "C" fn working_64bit_int_cmd(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    // Rust always supports 64-bit integers
    set_result_int(interp, 1);
    TCL_OK
}

/// tcl_variable_type - returns the type of a TCL variable
pub unsafe extern "C" fn tcl_variable_type_cmd(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"tcl_variable_type VARNAME\"",
        );
        return TCL_ERROR;
    }
    // For compatibility, just return empty string (unknown type)
    set_result_string(interp, "");
    TCL_OK
}

/// clang_sanitize_address - returns 1 if running with address sanitizer
pub unsafe extern "C" fn clang_sanitize_address_cmd(
    _client_data: *mut std::ffi::c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    // Not running with sanitizer
    set_result_int(interp, 0);
    TCL_OK
}

/// The sqlite3 command - creates a database handle
/// Usage: sqlite3 DBNAME FILENAME ?-options?
/// Also handles: sqlite3 -has-codec (returns 0)
pub unsafe extern "C" fn sqlite3_cmd(
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
        Some(db_method_cmd),
        db_name_ptr,
        Some(db_delete_cmd),
    );

    TCL_OK
}

/// Database instance command - handles db eval, db close, etc.
pub unsafe extern "C" fn db_method_cmd(
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
                match what.as_str() {
                    "sort" => {
                        // Return whether the most recent query performed a sort
                        let did_sort = crate::vdbe::get_sort_flag();
                        set_result_int(interp, if did_sort { 1 } else { 0 });
                    }
                    "step" => {
                        // Return the number of VDBE steps executed
                        let step_count = crate::vdbe::get_step_count();
                        set_result_int(interp, step_count as i32);
                    }
                    "vmstep" => {
                        // Alias for step - some tests use vmstep
                        let step_count = crate::vdbe::get_step_count();
                        set_result_int(interp, step_count as i32);
                    }
                    "search_count" => {
                        // Return the search count (B-tree seeks)
                        let search_count = crate::vdbe::get_search_count();
                        set_result_int(interp, search_count as i32);
                    }
                    "reset" => {
                        // Reset all counters
                        crate::vdbe::reset_search_count();
                        crate::vdbe::reset_sort_count();
                        crate::vdbe::reset_step_count();
                        crate::vdbe::reset_like_count();
                        set_result_string(interp, "");
                    }
                    _ => {
                        // Other status queries return 0
                        set_result_int(interp, 0);
                    }
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
        "function" | "func" => {
            // Register a custom SQL function
            // Usage: db func name ?-argcount N? proc
            if objc >= 4 {
                let func_name = obj_to_string(*objv.offset(2));
                // Parse options including -argcount
                let mut proc_idx = 3;
                let mut argcount: i32 = -1; // -1 means any number of args
                while proc_idx < objc as isize {
                    let arg = obj_to_string(*objv.offset(proc_idx));
                    if arg == "-argcount" && proc_idx + 1 < objc as isize {
                        // Parse argcount value
                        let count_str = obj_to_string(*objv.offset(proc_idx + 1));
                        argcount = count_str.parse().unwrap_or(-1);
                        proc_idx += 2;
                    } else if arg.starts_with('-') {
                        // Skip other options and their values
                        proc_idx += 2;
                    } else {
                        break;
                    }
                }
                if proc_idx < objc as isize {
                    let proc_name = obj_to_string(*objv.offset(proc_idx));
                    USER_FUNCTIONS.with(|funcs| {
                        funcs.borrow_mut().insert(
                            (db_name.to_string(), func_name.to_lowercase(), argcount),
                            proc_name,
                        );
                    });
                }
            }
            TCL_OK
        }
        "collate"
        | "trace"
        | "profile"
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
        "collation_needed" => {
            // Register callback for when unknown collation is needed
            // Usage: db collation_needed callback_proc
            // For now, just accept and ignore (no-op)
            TCL_OK
        }
        "config" => {
            // Database configuration options
            // Usage: db config option ?value?
            if objc < 3 {
                // No args or just option name - return empty/current value
                set_result_string(interp, "");
                return TCL_OK;
            }
            let option = obj_to_string(*objv.offset(2));
            match option.as_str() {
                "enable_view" => {
                    CONNECTIONS.with(|connections| {
                        let mut conns = connections.borrow_mut();
                        if let Some(conn) = conns.get_mut(db_name) {
                            if objc >= 4 {
                                // Setting enable_view
                                let value_str = obj_to_string(*objv.offset(3));
                                let value =
                                    value_str == "on" || value_str == "1" || value_str == "true";
                                conn.db_config.enable_view = value;
                            }
                            // Return current value
                            set_result_int(interp, conn.db_config.enable_view as i32);
                        }
                    });
                }
                _ => {
                    // Other config options - silently accept
                }
            }
            TCL_OK
        }
        "nullvalue" | "null" => {
            // Set/get the null representation string
            // Usage: db nullvalue ?string?
            if objc >= 3 {
                // Setting null value
                let null_str = obj_to_string(*objv.offset(2));
                NULL_VALUES.with(|nv| {
                    nv.borrow_mut().insert(db_name.to_string(), null_str);
                });
                TCL_OK
            } else {
                // Getting null value
                let null_str =
                    NULL_VALUES.with(|nv| nv.borrow().get(db_name).cloned().unwrap_or_default());
                set_result_string(interp, &null_str);
                TCL_OK
            }
        }
        "incrblob" => {
            // Incremental BLOB I/O - open a blob for reading/writing
            // Usage: db incrblob ?-readonly? table column rowid
            // This is complex - requires TCL channel creation
            // For now, return error explaining it's not implemented
            set_result_string(interp, "incrblob not implemented");
            TCL_ERROR
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
pub unsafe fn db_eval(
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
    let sql_trimmed = sql.trim().to_uppercase();
    let is_explain = sql_trimmed.starts_with("EXPLAIN");

    // Reset search, sort, step, and like counts at the start of each eval
    // But don't reset like_count for EXPLAIN queries (preserves count from actual query)
    reset_search_count();
    reset_sort_count();
    reset_step_count();
    if !is_explain {
        reset_like_count();
    }

    // Set the current interpreter for user function callbacks
    set_current_interp(interp);

    // Check if we have script arguments
    // objc == 4: db eval SQL script (column names as variables)
    // objc >= 5: db eval SQL array-name script (array elements)
    let (array_name, script) = if objc >= 5 {
        let arr = obj_to_string(*objv.offset(3));
        let scr = obj_to_string(*objv.offset(4));
        (Some(arr), Some(scr))
    } else if objc == 4 {
        // 4-argument form: no array, column names become direct variables
        let scr = obj_to_string(*objv.offset(3));
        (None, Some(scr))
    } else {
        (None, None)
    };

    // For script form, use streaming execution so scripts can modify the database
    // while iteration is in progress (cursor stability).
    // Handles both: "db eval SQL script" and "db eval SQL array script"
    if let Some(ref scr) = script {
        let arr_c = array_name.as_ref().map(|arr_name| {
            CString::new(arr_name.as_str()).unwrap_or_else(|_| CString::new("").unwrap())
        });
        let script_c = CString::new(scr.as_str()).unwrap_or_else(|_| CString::new("").unwrap());

        const TCL_BREAK: c_int = 3;
        const TCL_CONTINUE: c_int = 4;

        let db_name_owned = db_name.to_string();
        let mut remaining = sql.to_string();

        while !remaining.trim().is_empty() {
            let trimmed = remaining.trim_start();
            if trimmed.starts_with("--") {
                if let Some(pos) = trimmed.find('\n') {
                    remaining = trimmed[pos + 1..].to_string();
                    continue;
                } else {
                    break;
                }
            }

            // Prepare statement inside the borrow, then move it out
            let prepare_result: Result<(Box<PreparedStmt>, String), String> =
                CONNECTIONS.with(|connections| {
                    let mut conns = connections.borrow_mut();
                    let conn = match conns.get_mut(&db_name_owned) {
                        Some(c) => c.as_mut(),
                        None => {
                            return Err(format!("no such database: {}", db_name_owned));
                        }
                    };

                    let (mut stmt, tail) = match sqlite3_prepare_v2(conn, &remaining) {
                        Ok(r) => r,
                        Err(e) => return Err(e.sqlite_errmsg()),
                    };

                    // Bind TCL variables to SQL parameters
                    bind_tcl_variables(interp, &mut stmt);

                    Ok((stmt, tail.to_string()))
                });

            let (mut stmt, tail) = match prepare_result {
                Ok(r) => r,
                Err(msg) => {
                    set_result_string(interp, &msg);
                    update_search_count_var(interp);
                    return TCL_ERROR;
                }
            };

            if stmt.sql().is_empty() {
                let _ = sqlite3_finalize(stmt);
                remaining = tail;
                continue;
            }

            // Step through results with streaming execution - connection is NOT borrowed here
            loop {
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        let col_count = sqlite3_column_count(&stmt);
                        let mut col_names = Vec::new();
                        let mut col_values = Vec::new();

                        for i in 0..col_count {
                            let col_name = sqlite3_column_name(&stmt, i).unwrap_or("").to_string();
                            col_names.push(col_name.clone());

                            let col_type = sqlite3_column_type(&stmt, i);
                            let value = match col_type {
                                ColumnType::Null => NULL_VALUES.with(|nv| {
                                    nv.borrow().get(&db_name_owned).cloned().unwrap_or_default()
                                }),
                                _ => sqlite3_column_text(&stmt, i),
                            };
                            col_values.push(value);

                            // Set variable: either array(col) or just $col
                            let col_c = CString::new(col_name.as_str())
                                .unwrap_or_else(|_| CString::new("").unwrap());
                            let val_obj = string_to_obj(&col_values.last().unwrap());
                            if let Some(ref arr) = arr_c {
                                // Array form: array(column)
                                Tcl_SetVar2Ex(interp, arr.as_ptr(), col_c.as_ptr(), val_obj, 0);
                            } else {
                                // Direct variable form: set $column directly
                                Tcl_SetVar2Ex(interp, col_c.as_ptr(), std::ptr::null(), val_obj, 0);
                            }
                        }

                        // Set array(*) = list of column names (only for array form)
                        if let Some(ref arr) = arr_c {
                            let star = CString::new("*").unwrap();
                            let names_list = Tcl_NewListObj(0, std::ptr::null());
                            for name in &col_names {
                                let name_obj = string_to_obj(name);
                                Tcl_ListObjAppendElement(interp, names_list, name_obj);
                            }
                            Tcl_SetVar2Ex(interp, arr.as_ptr(), star.as_ptr(), names_list, 0);
                        }

                        // Evaluate the script - connection is NOT borrowed, so nested db_eval works
                        let eval_result = Tcl_Eval(interp, script_c.as_ptr());

                        if eval_result == TCL_BREAK {
                            let _ = sqlite3_finalize(stmt);
                            update_search_count_var(interp);
                            return TCL_OK;
                        } else if eval_result == TCL_ERROR {
                            let _ = sqlite3_finalize(stmt);
                            update_search_count_var(interp);
                            return TCL_ERROR;
                        }
                        // TCL_CONTINUE and TCL_OK: continue to next row
                    }
                    Ok(StepResult::Done) => break,
                    Err(e) => {
                        let msg = e.sqlite_errmsg();
                        let _ = sqlite3_finalize(stmt);
                        set_result_string(interp, &msg);
                        update_search_count_var(interp);
                        return TCL_ERROR;
                    }
                }
            }

            let _ = sqlite3_finalize(stmt);
            remaining = tail;
        }

        update_search_count_var(interp);
        TCL_OK
    } else {
        // Simple form: execute SQL and collect results as a flat list
        let result_list = Tcl_NewListObj(0, std::ptr::null());

        let result = CONNECTIONS.with(|connections| {
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

                // Bind TCL variables to SQL parameters
                bind_tcl_variables(interp, &mut stmt);

                loop {
                    match sqlite3_step(&mut stmt) {
                        Ok(StepResult::Row) => {
                            let col_count = sqlite3_column_count(&stmt);
                            for i in 0..col_count {
                                let col_type = sqlite3_column_type(&stmt, i);
                                let value = match col_type {
                                    ColumnType::Null => NULL_VALUES.with(|nv| {
                                        nv.borrow().get(db_name).cloned().unwrap_or_default()
                                    }),
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
        });

        // Update ::sqlite_search_count, ::sqlite_sort_count, and ::sqlite_like_count variables
        // Don't update like_count for EXPLAIN queries (preserves count from actual query)
        update_search_count_var(interp);
        update_sort_count_var(interp);
        if !is_explain {
            update_like_count_var(interp);
        }

        // Clear the interpreter context
        clear_current_interp();
        result
    }
}

/// Update the ::sqlite_search_count TCL variable with current search count
pub unsafe fn update_search_count_var(interp: *mut Tcl_Interp) {
    let var_name = CString::new("::sqlite_search_count").unwrap();
    let count_str = CString::new(get_search_count().to_string()).unwrap();
    Tcl_SetVar(
        interp,
        var_name.as_ptr(),
        count_str.as_ptr(),
        TCL_GLOBAL_ONLY,
    );
}

/// Update the ::sqlite_sort_count TCL variable with current sort count
pub unsafe fn update_sort_count_var(interp: *mut Tcl_Interp) {
    let var_name = CString::new("::sqlite_sort_count").unwrap();
    let count_str = CString::new(get_sort_count().to_string()).unwrap();
    Tcl_SetVar(
        interp,
        var_name.as_ptr(),
        count_str.as_ptr(),
        TCL_GLOBAL_ONLY,
    );
}

/// Update the ::sqlite_like_count TCL variable with current like count
pub unsafe fn update_like_count_var(interp: *mut Tcl_Interp) {
    let count = get_like_count();
    let var_name = CString::new("::sqlite_like_count").unwrap();
    let count_str = CString::new(count.to_string()).unwrap();
    Tcl_SetVar(
        interp,
        var_name.as_ptr(),
        count_str.as_ptr(),
        TCL_GLOBAL_ONLY,
    );
}

/// Bind TCL variables to SQL parameters
/// SQLite's TCL extension automatically binds $varname and :varname in SQL
/// to corresponding TCL variables
pub unsafe fn bind_tcl_variables(interp: *mut Tcl_Interp, stmt: &mut PreparedStmt) {
    let param_count = sqlite3_bind_parameter_count(stmt);
    for i in 1..=param_count {
        if let Some(param_name) = sqlite3_bind_parameter_name(stmt, i) {
            // Parameter names start with $, :, @, or ?
            // For $ and :, look up the TCL variable
            let var_name = if param_name.starts_with('$') || param_name.starts_with(':') {
                &param_name[1..] // Strip the prefix
            } else if param_name.starts_with('@') {
                &param_name[1..]
            } else {
                continue; // Unnamed or ? parameters, skip
            };

            // Look up the TCL variable - first try current scope, then global
            let var_cstr = CString::new(var_name).unwrap_or_else(|_| CString::new("").unwrap());
            let mut value_ptr = Tcl_GetVar(interp, var_cstr.as_ptr(), 0);
            if value_ptr.is_null() {
                // Try global scope
                value_ptr = Tcl_GetVar(interp, var_cstr.as_ptr(), TCL_GLOBAL_ONLY);
            }

            if value_ptr.is_null() {
                // Variable not found, bind NULL
                let _ = sqlite3_bind_null(stmt, i);
            } else {
                let value_str = std::ffi::CStr::from_ptr(value_ptr).to_str().unwrap_or("");

                // Try to parse as number, otherwise bind as text
                if let Ok(int_val) = value_str.parse::<i64>() {
                    let _ = sqlite3_bind_int64(stmt, i, int_val);
                } else if let Ok(float_val) = value_str.parse::<f64>() {
                    let _ = sqlite3_bind_double(stmt, i, float_val);
                } else {
                    let _ = sqlite3_bind_text(stmt, i, value_str);
                }
            }
        }
    }
}

/// Check if query returns any rows
pub unsafe fn db_exists(
    db_name: &str,
    interp: *mut Tcl_Interp,
    objv: *const *mut Tcl_Obj,
) -> c_int {
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

        // Bind TCL variables to SQL parameters
        bind_tcl_variables(interp, &mut stmt);

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
pub unsafe fn db_onecolumn(
    db_name: &str,
    interp: *mut Tcl_Interp,
    objv: *const *mut Tcl_Obj,
) -> c_int {
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

        // Bind TCL variables to SQL parameters
        bind_tcl_variables(interp, &mut stmt);

        let result = match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                let col_type = sqlite3_column_type(&stmt, 0);
                match col_type {
                    ColumnType::Null => {
                        NULL_VALUES.with(|nv| nv.borrow().get(db_name).cloned().unwrap_or_default())
                    }
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
pub unsafe fn db_close(db_name: &str, interp: *mut Tcl_Interp) -> c_int {
    CONNECTIONS.with(|connections| {
        if let Some(conn) = connections.borrow_mut().remove(db_name) {
            let _ = sqlite3_close(conn);
        }
    });
    // Clean up null value setting
    NULL_VALUES.with(|nv| {
        nv.borrow_mut().remove(db_name);
    });
    // Clean up user-defined functions for this connection
    USER_FUNCTIONS.with(|uf| {
        uf.borrow_mut().retain(|(name, _, _), _| name != db_name);
    });

    // Delete the command
    let cmd_name = CString::new(db_name).unwrap();
    Tcl_DeleteCommand(interp, cmd_name.as_ptr());

    TCL_OK
}

/// Cleanup callback when db command is deleted
pub unsafe extern "C" fn db_delete_cmd(client_data: *mut std::ffi::c_void) {
    if !client_data.is_null() {
        let db_name = Box::from_raw(client_data as *mut String);
        CONNECTIONS.with(|connections| {
            if let Some(conn) = connections.borrow_mut().remove(&*db_name) {
                let _ = sqlite3_close(conn);
            }
        });
        // Clean up null value setting
        NULL_VALUES.with(|nv| {
            nv.borrow_mut().remove(&*db_name);
        });
        // Clean up user-defined functions for this connection
        USER_FUNCTIONS.with(|uf| {
            uf.borrow_mut().retain(|(name, _, _), _| name != &*db_name);
        });
    }
}
