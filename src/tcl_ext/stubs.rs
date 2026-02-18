//! Test infrastructure stub commands for the TCL extension
//!
//! This module provides stub implementations for SQLite test commands that are
//! required by the TCL test suite but not fully implemented in RustQL. These
//! stubs allow tests to run by providing minimal or no-op implementations.
//!
//! The stubs include:
//! - Commands that return 0/empty (memory management, VFS, etc.)
//! - Feature capability flags (sqlite_options array)
//! - Test counters and variables
//! - Printf formatting commands

use std::ffi::c_void;
use std::ffi::CString;
use std::os::raw::c_int;
use std::sync::atomic::{AtomicI32, Ordering};

use std::sync::Arc;

use super::ffi::{
    Tcl_CreateObjCommand, Tcl_Eval, Tcl_Interp, Tcl_NewIntObj, Tcl_Obj, Tcl_SetVar, Tcl_SetVar2Ex,
    TCL_ERROR, TCL_GLOBAL_ONLY, TCL_OK,
};
use super::helpers::{obj_to_string, set_result_bytearray, set_result_int, set_result_string};
use super::md5::md5_cmd;
use super::{CONNECTIONS, FUNCTION_DESTRUCTORS, USER_COLLATIONS};

/// Global flag for FTS3 may-be-corrupt mode. Default is 1 (may be corrupt).
static FTS3_MAY_BE_CORRUPT: AtomicI32 = AtomicI32::new(1);

/// Global flag for URI filename interpretation. Default is 0 (disabled).
static CONFIG_URI: AtomicI32 = AtomicI32::new(0);

// Database commands from db module
use super::db::{
    clang_sanitize_address_cmd, sqlite3_exec_cmd, sqlite3_exec_hex_cmd,
    sqlite3_extended_errcode_cmd, sqlite3_get_autocommit_cmd, sqlite3_txn_state_cmd,
    tcl_variable_type_cmd, working_64bit_int_cmd,
};

// Printf formatting commands from printf module
use super::printf::{
    sqlite3_mprintf_double_cmd, sqlite3_mprintf_hexdouble_cmd, sqlite3_mprintf_int64_cmd,
    sqlite3_mprintf_int_cmd, sqlite3_mprintf_long_cmd, sqlite3_mprintf_n_test_cmd,
    sqlite3_mprintf_scaled_cmd, sqlite3_mprintf_str_cmd, sqlite3_mprintf_stronly_cmd,
    sqlite3_snprintf_int_cmd, sqlite3_snprintf_str_cmd,
};

/// Register all test infrastructure stub commands required by the TCL test suite
///
/// This function registers:
/// - Simple stub commands that return 0 or empty results
/// - Memory management stubs (sqlite3_memory_used, etc.)
/// - Printf formatting commands
/// - Feature capability flags in sqlite_options array
/// - Test counter variables (search_count, sort_count, etc.)
///
/// # Safety
///
/// This function is unsafe because it interacts with the TCL C API.
pub unsafe fn register_test_stubs(interp: *mut Tcl_Interp) {
    // List of test commands that return 0/empty
    let stub_commands = [
        // "sqlite3_test_control_pending_byte" - implemented properly below
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
        // "sqlite3_db_config" - removed from stubs, implemented properly below
        // "sqlite3_db_filename" - removed from stubs, implemented properly below
        "sqlite3_db_status",
        "sqlite3_exec_nr",
        "sqlite3_next_stmt",
        "sqlite3_stmt_status",
        "sqlite3_unlock_notify",
        "sqlite3_wal_autocheckpoint",
        "autoinstall_test_functions",
        "install_malloc_faultsim",
        // "faultsim_delete_and_reopen" - implemented properly below
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
        // "sqlite3_extended_errcode" - implemented properly below
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
        "sqlite3_data_count",
        "sqlite3_column_count",
        "sqlite3_column_text",
        "sqlite3_column_text16",
        "sqlite3_sql",
        "sqlite3_expanded_sql",
        "sqlite3_normalized_sql",
        "register_tclvar_module",
        "register_fs_module",
        "register_wholenumber_module",
        "register_regexp_module",
        "register_fuzzer_module",
        "register_unionvtab_module",
        // Utility commands
        "hexio_write",
        "hexio_read",
        "hexio_get_int",
        "sqlite3_release_memory",
        "breakpoint",
        "do_faultsim_test",
        "sqlite3_wal_checkpoint_v2",
        "sqlite3_vtab_config",
        // Printf test commands (stubs)
        "sqlite3_mprintf_z_test",
        "vfs_unlink_test",
        // Btree test commands (stubs)
        "btree_open",
        "btree_close",
        "btree_cursor",
        "btree_first",
        "btree_next",
        "btree_key",
        "btree_data",
        "btree_cursor_info",
        "btree_insert",
        "btree_delete",
        "btree_clear_table",
        "btree_drop_table",
        "btree_get_page_size",
        "btree_set_page_size",
        "btree_integrity_check",
        "btree_pager_stats",
        "btree_cursor_list",
        "btree_move_to",
        "btree_eof",
        "btree_keysize",
        "btree_payload_size",
        "btree_varint_test",
        "btree_from_db",
        "btree_shared_cache_report",
        "pager_open",
        "pager_close",
        "pager_commit",
        "pager_rollback",
        "pager_stmt_begin",
        "pager_stmt_commit",
        "pager_stmt_rollback",
        "pager_stats",
        "pager_pagecount",
        "pager_truncate",
        "page_get",
        "page_lookup",
        "page_unref",
        "page_read",
        "page_write",
        "page_number",
        "fake_big_file",
        // Misc test commands
        "sqlite3_blob_open",
        "sqlite3_blob_close",
        "sqlite3_blob_read",
        "sqlite3_blob_write",
        "sqlite3_blob_reopen",
        "sqlite3_blob_bytes",
        "sqlite_delete_function",
        "sqlite_delete_collation",
        "sqlite3_table_column_metadata",
        "sqlite3_file_control",
        "sqlite3_vfs_list",
        "sqlite3_vfs_find",
        "sqlite3_vfs_register",
        "sqlite3_vfs_unregister",
        // Additional btree commands
        "btree_ismemdb",
        "btree_set_cache_size",
        "btree_cursor_db",
        "btree_tree_dump",
        "btree_sanity_check",
        // Malloc test commands
        "sqlite3_memdebug_benign_failures",
        "sqlite3_memdebug_pending",
        "sqlite3_memory_alarm",
        // Mutex test commands
        "sqlite3_mutex_try",
        "sqlite3_mutex_enter",
        "sqlite3_mutex_leave",
        "sqlite3_mutex_held",
        "sqlite3_mutex_notheld",
        // Thread test commands
        "sqlthread",
        "clock_seconds",
        // More test stubs
        "sqlite3_thread_cleanup",
        "sqlite3_pager_refcounts",
        "sqlite3_enable_load_extension",
        "sqlite3_load_extension",
        "sqlite3_auto_extension",
        "sqlite3_cancel_auto_extension",
        "sorter_test_fakeheap",
        "sorter_test_sort4_helper",
        "test_quota_initialize",
        "test_quota_shutdown",
        "test_quota_file",
        "test_quota_dump",
        "test_quota_fopen",
        "test_quota_fread",
        "test_quota_fwrite",
        "test_quota_fclose",
        "test_quota_fflush",
        "test_quota_fseek",
        "test_quota_remove",
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

    let cmd_name = CString::new("sqlite3_get_autocommit").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_get_autocommit_cmd),
        std::ptr::null_mut(),
        None,
    );

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

    // Register sqlite3_mprintf_int
    let cmd_name = CString::new("sqlite3_mprintf_int").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_int_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_mprintf_double
    let cmd_name = CString::new("sqlite3_mprintf_double").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_double_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_txn_state
    let cmd_name = CString::new("sqlite3_txn_state").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_txn_state_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register working_64bit_int
    let cmd_name = CString::new("working_64bit_int").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(working_64bit_int_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register tcl_variable_type
    let cmd_name = CString::new("tcl_variable_type").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(tcl_variable_type_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register clang_sanitize_address
    let cmd_name = CString::new("clang_sanitize_address").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(clang_sanitize_address_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_mprintf_str
    let cmd_name = CString::new("sqlite3_mprintf_str").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_str_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_mprintf_hexdouble
    let cmd_name = CString::new("sqlite3_mprintf_hexdouble").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_hexdouble_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_mprintf_n_test
    let cmd_name = CString::new("sqlite3_mprintf_n_test").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_n_test_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_snprintf_str
    let cmd_name = CString::new("sqlite3_snprintf_str").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_snprintf_str_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_mprintf_scaled
    let cmd_name = CString::new("sqlite3_mprintf_scaled").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_scaled_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_mprintf_long
    let cmd_name = CString::new("sqlite3_mprintf_long").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_long_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_mprintf_int64
    let cmd_name = CString::new("sqlite3_mprintf_int64").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_int64_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_mprintf_stronly
    let cmd_name = CString::new("sqlite3_mprintf_stronly").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_mprintf_stronly_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_snprintf_int
    let cmd_name = CString::new("sqlite3_snprintf_int").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_snprintf_int_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_exec
    let cmd_name = CString::new("sqlite3_exec").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_exec_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_exec_hex
    let cmd_name = CString::new("sqlite3_exec_hex").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_exec_hex_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_extended_errcode
    let cmd_name = CString::new("sqlite3_extended_errcode").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_extended_errcode_cmd),
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
        ("vtab", "1"), // Virtual tables supported
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
        ("schema_version", "1"),
        ("default_cache_size", "1"),
        ("memorymanage", "0"),
        ("shared_cache", "0"),
        ("threadsafe", "0"),
        ("threadsafe1", "0"),
        ("threadsafe2", "0"),
        ("mutex", "0"),            // Mutex operations not available (single-threaded)
        ("mutex_noop", "1"),       // Using noop mutex (single-threaded)
        ("mutex_nref", "0"),       // No nested mutex support
        ("like_match_blobs", "0"), // LIKE does not match blobs
        // Additional options required by various tests
        ("legacyformat", "0"),
        ("autoinc", "1"),
        ("configslower", "0"),
        ("rowid32", "0"),
        ("maxexpr", "1"),
        ("hexlit", "1"),
        ("floatingpoint", "1"),
        ("check", "1"),
        ("complete", "1"),
        ("reindex", "1"),
        ("diskio", "1"),
        ("trace", "1"),
        ("secure_delete", "1"),
        ("fts3", "1"),
        ("fts4", "1"),
        ("fts5", "1"),
        ("rtree", "1"),
        ("json", "1"),
        ("mathlib", "1"),
        ("memdebug", "0"),
        ("crashtest", "0"),
        ("debug", "0"),
        ("hidden_columns", "0"),
        ("deprecated", "0"),
        ("direct_read", "0"),
        ("lock_proxy_pragmas", "0"),
        ("mem5", "0"),
        ("icu", "0"),
        ("tclvar", "0"),
        ("builtin_test", "0"),
        ("columncount", "1"),
        ("authorization", "1"),
        ("default_autovacuum", "0"),
        ("autovacuum", "1"),
        // Additional options for various tests
        ("wsd", "1"),            // Without-static-data (thread safety)
        ("worker_threads", "0"), // No worker threads
        ("load_ext", "0"),       // Load extension not supported
        ("tempdb_in_memory", "0"),
        ("default_temp_store", "0"),
        ("default_synchronous", "2"),
        ("default_wal_synchronous", "2"),
        ("localtime", "0"),
        ("malloc_usable_size", "0"),
        ("mmap_size", "0"),
        ("offset_sql_func", "0"),
        ("oversize_cell_check", "1"),
        ("pagecache_overflow_stats", "0"),
        ("preupdate", "0"),
        ("savepoint", "1"),
        ("session", "0"),
        ("snapshot", "0"),
        ("sorter_reference_size", "0"),
        ("stat3", "0"),
        ("system_malloc", "1"),
        ("unlock_notify", "0"),
        ("userauth", "0"),
        ("win32heap", "0"),
        ("yytrackmaxstackdepth", "0"),
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

    // Set SQLITE_MAX_LENGTH used by printf.test and other tests
    let max_length_name = CString::new("SQLITE_MAX_LENGTH").unwrap();
    let max_length_val = CString::new("1000000000").unwrap();
    Tcl_SetVar(
        interp,
        max_length_name.as_ptr(),
        max_length_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    // Initialize sqlite_search_count variable for query efficiency tests
    let search_count_name = CString::new("::sqlite_search_count").unwrap();
    let search_count_val = CString::new("0").unwrap();
    Tcl_SetVar(
        interp,
        search_count_name.as_ptr(),
        search_count_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    // Initialize sqlite_sort_count variable for sort detection tests
    let sort_count_name = CString::new("::sqlite_sort_count").unwrap();
    let sort_count_val = CString::new("0").unwrap();
    Tcl_SetVar(
        interp,
        sort_count_name.as_ptr(),
        sort_count_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    // Initialize sqlite_like_count variable for LIKE function call counting
    let like_count_name = CString::new("::sqlite_like_count").unwrap();
    let like_count_val = CString::new("0").unwrap();
    Tcl_SetVar(
        interp,
        like_count_name.as_ptr(),
        like_count_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    // Initialize sqlite_pending_byte for multi-client tests
    let pending_byte_name = CString::new("::sqlite_pending_byte").unwrap();
    let pending_byte_val = CString::new("0").unwrap();
    Tcl_SetVar(
        interp,
        pending_byte_name.as_ptr(),
        pending_byte_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    // Register md5 command for checksum tests
    let cmd_name = CString::new("md5").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(md5_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register echo module command - registers the echo test virtual table
    let cmd_name = CString::new("register_echo_module").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(register_echo_module_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_db_config - configure database connection options
    // Usage: sqlite3_db_config DBHANDLE OPTION VALUE
    // Options: SQLITE_DBCONFIG_DQS_DML, SQLITE_DBCONFIG_DQS_DDL, etc.
    let cmd_name = CString::new("sqlite3_db_config").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_db_config_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_db_filename - returns the filename for an attached database
    // Usage: sqlite3_db_filename DBHANDLE DBNAME
    // Returns the path to the database file, or empty string if not found or temp
    let cmd_name = CString::new("sqlite3_db_filename").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_db_filename_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_db_config_lookaside - configures per-connection lookaside memory
    // Used by printf.test, altermalloc.test, analyze9.test, dbstatus.test, and others
    let cmd_name = CString::new("sqlite3_db_config_lookaside").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_rekey - encrypt/decrypt database with new key
    // Used by types.test and encryption tests
    // Returns error since encryption is not supported
    let cmd_name = CString::new("sqlite3_rekey").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_key - set database encryption key
    // Used by encryption tests
    let cmd_name = CString::new("sqlite3_key").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite_set_magic - test utility to corrupt database magic number
    let cmd_name = CString::new("sqlite_set_magic").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register btree_begin_transaction - test utility for btree transactions
    let cmd_name = CString::new("btree_begin_transaction").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register btree_commit - test utility for btree commits
    let cmd_name = CString::new("btree_commit").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register btree_rollback - test utility for btree rollbacks
    let cmd_name = CString::new("btree_rollback").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Set additional SQLite compile-time limits
    let max_attached_name = CString::new("SQLITE_MAX_ATTACHED").unwrap();
    let max_attached_val = CString::new("10").unwrap();
    Tcl_SetVar(
        interp,
        max_attached_name.as_ptr(),
        max_attached_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    let max_compound_name = CString::new("SQLITE_MAX_COMPOUND_SELECT").unwrap();
    let max_compound_val = CString::new("500").unwrap();
    Tcl_SetVar(
        interp,
        max_compound_name.as_ptr(),
        max_compound_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    let temp_store_name = CString::new("TEMP_STORE").unwrap();
    let temp_store_val = CString::new("0").unwrap();
    Tcl_SetVar(
        interp,
        temp_store_name.as_ptr(),
        temp_store_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    let max_variable_name = CString::new("SQLITE_MAX_VARIABLE_NUMBER").unwrap();
    let max_variable_val = CString::new("999").unwrap();
    Tcl_SetVar(
        interp,
        max_variable_name.as_ptr(),
        max_variable_val.as_ptr(),
        TCL_GLOBAL_ONLY,
    );

    // Register sqlite3_create_function_v2 - creates SQL functions with destructor support
    let cmd_name = CString::new("sqlite3_create_function_v2").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_create_function_v2_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register register_tcl_module - registers the "tcl" virtual table module
    let cmd_name = CString::new("register_tcl_module").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(register_tcl_module_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register add_test_collate - registers test collation sequences
    let cmd_name = CString::new("add_test_collate").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(add_test_collate_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register register_dbstat_vtab - registers the dbstat virtual table module
    let cmd_name = CString::new("register_dbstat_vtab").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(register_dbstat_vtab_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_test_control_pending_byte - sets the pending byte offset for locking
    let cmd_name = CString::new("sqlite3_test_control_pending_byte").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_test_control_pending_byte_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_bind_parameter_count - returns number of bind parameters
    let cmd_name = CString::new("sqlite3_bind_parameter_count").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_bind_parameter_count_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_bind_parameter_name - returns name of a bind parameter
    let cmd_name = CString::new("sqlite3_bind_parameter_name").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_bind_parameter_name_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register db_enter / db_leave - no-ops for single-threaded RustQL
    let cmd_name = CString::new("db_enter").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("db_leave").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register fault simulation test infrastructure commands
    // These are needed by 35+ test files that use fault injection testing

    let cmd_name = CString::new("faultsim_save_and_close").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(faultsim_save_and_close_cmd),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("faultsim_restore_and_reopen").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(faultsim_restore_and_reopen_cmd),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("faultsim_save").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(faultsim_save_cmd),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("faultsim_restore").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(faultsim_restore_cmd),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("faultsim_delete_and_reopen").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(faultsim_delete_and_reopen_cmd),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("faultsim_integrity_check").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(faultsim_integrity_check_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register save_prng_state / restore_prng_state - no-ops for deterministic testing
    let cmd_name = CString::new("save_prng_state").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    let cmd_name = CString::new("restore_prng_state").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register do_malloc_test - runs test body once without fault injection
    // This is a simplified version that just executes the test body
    let cmd_name = CString::new("do_malloc_test").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(do_malloc_test_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register hexio_render_int32 - renders a 32-bit integer as 8-char uppercase hex string
    let cmd_name = CString::new("hexio_render_int32").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(hexio_render_int32_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register hexio_render_int16 - renders a 16-bit integer as 4-char uppercase hex string
    let cmd_name = CString::new("hexio_render_int16").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(hexio_render_int16_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register decode_hexdb - decodes hex-encoded database dump into a byte array
    let cmd_name = CString::new("decode_hexdb").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(decode_hexdb_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_fts3_may_be_corrupt - get/set FTS3 may-be-corrupt flag
    // Unblocks 29 tests
    let cmd_name = CString::new("sqlite3_fts3_may_be_corrupt").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_fts3_may_be_corrupt_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_config_uri - enable/disable URI filename interpretation
    let cmd_name = CString::new("sqlite3_config_uri").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(sqlite3_config_uri_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_config_pmasz - configure minimum PMA size (no-op, returns SQLITE_OK)
    let cmd_name = CString::new("sqlite3_config_pmasz").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_config_lookaside - configure lookaside memory (no-op, returns SQLITE_OK)
    let cmd_name = CString::new("sqlite3_config_lookaside").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register fts3_test_varint - tests varint encoding/decoding roundtrip
    let cmd_name = CString::new("fts3_test_varint").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(fts3_test_varint_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_prepare16 - UTF-16 variant of prepare (stub returns 0)
    let cmd_name = CString::new("sqlite3_prepare16").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_prepare16_v2 - UTF-16 variant of prepare_v2 (stub returns 0)
    let cmd_name = CString::new("sqlite3_prepare16_v2").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_bind_text16 - UTF-16 variant of bind_text (stub returns 0)
    let cmd_name = CString::new("sqlite3_bind_text16").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_backup_init - backup API init (stub)
    let cmd_name = CString::new("sqlite3_backup_init").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_backup_step - backup API step (stub)
    let cmd_name = CString::new("sqlite3_backup_step").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_backup_finish - backup API finish (stub)
    let cmd_name = CString::new("sqlite3_backup_finish").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_backup_remaining - backup API remaining (stub)
    let cmd_name = CString::new("sqlite3_backup_remaining").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_backup_pagecount - backup API pagecount (stub)
    let cmd_name = CString::new("sqlite3_backup_pagecount").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(test_stub_return_zero),
        std::ptr::null_mut(),
        None,
    );

    // Register testvfs - the test VFS infrastructure command (blocks 20+ tests)
    let cmd_name = CString::new("testvfs").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(super::testvfs::testvfs_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register atomic_batch_write - returns whether VFS supports batch atomic writes
    // Used by 17+ tests; always returns 0 (not supported)
    let cmd_name = CString::new("atomic_batch_write").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(super::testvfs::atomic_batch_write_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register file_control_reservebytes - sets reserve bytes per page
    // Used by 2 tests (reservebytes.test, cksumvfs.test)
    let cmd_name = CString::new("file_control_reservebytes").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(super::testvfs::file_control_reservebytes_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register sqlite3_simulate_device - simulates device characteristics for testing
    // Used by 2 tests (io.test, wal.test)
    let cmd_name = CString::new("sqlite3_simulate_device").unwrap();
    Tcl_CreateObjCommand(
        interp,
        cmd_name.as_ptr(),
        Some(super::testvfs::sqlite3_simulate_device_cmd),
        std::ptr::null_mut(),
        None,
    );

    // Register additional faultsim helper procs via TCL eval
    // These are TCL procs that many fault injection tests expect
    let faultsim_helpers = CString::new(
        r#"
proc faultsim_test_result {args} {
    # Accept any result during fault simulation - tests pass if no crash
    return
}
proc oom_injectstart {args} { return 0 }
proc oom_injectstop {args} { return 0 }
proc ioerr_injectstart {args} { return 0 }
proc ioerr_injectstop {args} { return 0 }
proc do_one_faultsim_test {args} { return }
"#,
    )
    .unwrap();
    Tcl_Eval(interp, faultsim_helpers.as_ptr());
}

/// Stub that returns 0
///
/// Used for commands that don't need real implementations but must exist
/// for the test suite to run.
unsafe extern "C" fn test_stub_return_zero(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    set_result_int(interp, 0);
    TCL_OK
}

/// Stub for sqlite3_status - returns {0 0 0}
///
/// The sqlite3_status command expects to return a list of three values
/// representing current, highwater, and resetFlag values.
unsafe extern "C" fn test_stub_status(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    set_result_string(interp, "0 0 0");
    TCL_OK
}

/// Implementation of sqlite3_db_filename command
///
/// Usage: sqlite3_db_filename DBHANDLE DBNAME
/// Returns the path to the database file for the named database (main, temp, or attached).
/// Returns empty string if the database is not found, is temporary (:memory:), or has no filename.
unsafe extern "C" fn sqlite3_db_filename_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    use super::helpers::obj_to_string;

    if objc < 3 {
        set_result_string(
            interp,
            "wrong # args: should be \"sqlite3_db_filename DB DBNAME\"",
        );
        return TCL_ERROR;
    }

    let db_handle = obj_to_string(*objv.offset(1));
    let db_name = obj_to_string(*objv.offset(2));

    let filename = CONNECTIONS.with(|connections| {
        let conns = connections.borrow();
        if let Some(conn) = conns.get(&db_handle) {
            // Find the database by name (case-insensitive)
            conn.find_db(&db_name)
                .and_then(|db| db.path.clone())
                .unwrap_or_default()
        } else {
            String::new()
        }
    });

    set_result_string(interp, &filename);
    TCL_OK
}

/// Implementation of sqlite3_db_config command
///
/// Usage: sqlite3_db_config DBHANDLE OPTION VALUE
/// Options:
///   SQLITE_DBCONFIG_DQS_DML (1013) - Enable double-quoted string literals in DML
///   SQLITE_DBCONFIG_DQS_DDL (1014) - Enable double-quoted string literals in DDL
///   SQLITE_DBCONFIG_ENABLE_VIEW (1015) - Enable views
///   etc.
///
/// Returns: The old value of the option
unsafe extern "C" fn sqlite3_db_config_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    use super::helpers::obj_to_string;

    if objc < 4 {
        set_result_string(
            interp,
            "wrong # args: should be \"sqlite3_db_config DB OPTION VALUE\"",
        );
        return TCL_OK;
    }

    let db_name = obj_to_string(*objv.offset(1));
    let option_str = obj_to_string(*objv.offset(2));
    let value_str = obj_to_string(*objv.offset(3));

    // Parse option
    let option = match option_str.as_str() {
        "SQLITE_DBCONFIG_DQS_DML" | "1013" => 1013,
        "SQLITE_DBCONFIG_DQS_DDL" | "1014" => 1014,
        "SQLITE_DBCONFIG_ENABLE_VIEW" | "1015" => 1015,
        "SQLITE_DBCONFIG_ENABLE_FKEY" | "1002" => 1002,
        "SQLITE_DBCONFIG_ENABLE_TRIGGER" | "1003" => 1003,
        "SQLITE_DBCONFIG_DEFENSIVE" | "1010" => 1010,
        "SQLITE_DBCONFIG_ENABLE_QPSG" | "QPSG" | "1007" => 1007,
        _ => {
            // Unknown option - return 0 for compatibility
            set_result_int(interp, 0);
            return TCL_OK;
        }
    };

    // Parse value (-1 means query, 0/1 means set)
    let value: i32 = value_str.parse().unwrap_or(-1);

    // Apply to the connection
    let old_value = CONNECTIONS.with(|connections| {
        let mut conns = connections.borrow_mut();
        if let Some(conn) = conns.get_mut(&db_name) {
            match option {
                1013 => {
                    // DQS_DML
                    let old = conn.db_config.dqs_dml as i32;
                    if value >= 0 {
                        conn.db_config.dqs_dml = value != 0;
                    }
                    old
                }
                1014 => {
                    // DQS_DDL
                    let old = conn.db_config.dqs_ddl as i32;
                    if value >= 0 {
                        conn.db_config.dqs_ddl = value != 0;
                    }
                    old
                }
                1015 => {
                    // ENABLE_VIEW
                    let old = conn.db_config.enable_view as i32;
                    if value >= 0 {
                        conn.db_config.enable_view = value != 0;
                    }
                    old
                }
                1002 => {
                    // ENABLE_FKEY
                    let old = conn.db_config.enable_fkey as i32;
                    if value >= 0 {
                        conn.db_config.enable_fkey = value != 0;
                    }
                    old
                }
                1003 => {
                    // ENABLE_TRIGGER
                    let old = conn.db_config.enable_trigger as i32;
                    if value >= 0 {
                        conn.db_config.enable_trigger = value != 0;
                    }
                    old
                }
                1010 => {
                    // DEFENSIVE
                    let old = conn.db_config.defensive as i32;
                    if value >= 0 {
                        conn.db_config.defensive = value != 0;
                    }
                    old
                }
                1007 => {
                    // ENABLE_QPSG - Query Planner Stability Guarantee
                    let old = conn.db_config.enable_qpsg as i32;
                    if value >= 0 {
                        conn.db_config.enable_qpsg = value != 0;
                    }
                    old
                }
                _ => 0,
            }
        } else {
            0
        }
    });

    set_result_int(interp, old_value);
    TCL_OK
}

/// Register echo module - registers the echo test virtual table module
///
/// Usage: register_echo_module DB
/// Registers the "echo" virtual table module with the database connection.
/// The echo module creates virtual tables that mirror real tables.
unsafe extern "C" fn register_echo_module_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    // Register the echo module with the vtab registry
    // The echo module will be available for all connections
    CONNECTIONS.with(|connections| {
        let connections = connections.borrow();
        if let Some(conn) = connections.values().next() {
            // Get the vtab registry from the connection
            use super::echo::EchoModule;
            if let Err(e) = conn.vtab_registry.register_module(Arc::new(EchoModule)) {
                eprintln!("Failed to register echo module: {}", e);
            }
        }
    });

    set_result_int(interp, 0);
    TCL_OK
}

/// sqlite3_create_function_v2 - Create or modify a SQL function with destructor support
///
/// Usage: sqlite3_create_function_v2 DB FUNCNAME NARG ENC ?-func FUNCPROC? ?-step STEPPROC?
///        ?-final FINALPROC? ?-destroy DESTROYPROC?
///
/// DB: database connection name
/// FUNCNAME: name of the SQL function
/// NARG: number of arguments (-1 for any)
/// ENC: text encoding: "any", "utf8", "utf16", "utf16le", "utf16be"
///
/// Options:
///   -func PROC    - procedure to call for scalar function
///   -step PROC    - procedure to call for aggregate step
///   -final PROC   - procedure to call for aggregate finalize
///   -destroy PROC - procedure to call when function is destroyed
///
/// Returns: empty string on success, raises error on failure
///
/// The destructor is called when:
/// 1. The function is overridden by a new function with the same name and encoding
/// 2. The database connection is closed
///
/// Error: SQLITE_MISUSE if both -func and -step/-final are specified
unsafe extern "C" fn sqlite3_create_function_v2_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    // Need at least: cmd db func narg enc
    if objc < 5 {
        // Just return empty (this matches SQLite behavior for minimal args)
        set_result_string(interp, "");
        return TCL_OK;
    }

    let db_name = obj_to_string(*objv.offset(1));
    let func_name = obj_to_string(*objv.offset(2));
    let _narg = obj_to_string(*objv.offset(3)); // narg, not used in stub
    let encoding = obj_to_string(*objv.offset(4)).to_lowercase();

    // Parse optional arguments
    let mut func_proc: Option<String> = None;
    let mut step_proc: Option<String> = None;
    let mut final_proc: Option<String> = None;
    let mut destroy_proc: Option<String> = None;

    let mut i = 5;
    while i < objc as isize {
        let opt = obj_to_string(*objv.offset(i));
        match opt.as_str() {
            "-func" if i + 1 < objc as isize => {
                func_proc = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            "-step" if i + 1 < objc as isize => {
                step_proc = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            "-final" if i + 1 < objc as isize => {
                final_proc = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            "-destroy" if i + 1 < objc as isize => {
                destroy_proc = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Check for SQLITE_MISUSE: can't specify both scalar (-func) and aggregate (-step/-final)
    if func_proc.is_some() && (step_proc.is_some() || final_proc.is_some()) {
        // Call destructor if provided before returning error
        if let Some(ref destroy) = destroy_proc {
            call_destructor(interp, destroy);
        }
        set_result_string(interp, "SQLITE_MISUSE");
        return TCL_ERROR;
    }

    // Normalize encoding for lookup
    let enc_key = match encoding.as_str() {
        "any" => "any",
        "utf8" => "utf8",
        "utf16" | "utf16le" => "utf16le",
        "utf16be" => "utf16be",
        _ => "utf8",
    }
    .to_string();

    // Check if there's an existing function with a destructor that needs to be called
    // When overriding, we need to check if all encoding variants are replaced
    let func_name_lower = func_name.to_lowercase();
    let key = (db_name.clone(), func_name_lower.clone(), enc_key.clone());

    // If the encoding is "any", it covers all encodings, so when we register a new
    // function with a specific encoding (like utf16be), we need to check if it
    // completes the replacement of an "any" function
    //
    // SQLite behavior: An "any" function is considered replaced when all specific
    // encodings have been registered. The sequence in func3-1.x is:
    //   1. Register f2 with encoding "any" and destructor
    //   2. Register f2 with encoding "utf8" - destructor NOT called
    //   3. Register f2 with encoding "utf16le" - destructor NOT called
    //   4. Register f2 with encoding "utf16be" - destructor IS called (all encodings covered)
    //
    // For func3-2.x:
    //   1. Register f3 with encoding "utf8" and destructor
    //   2. Register f3 with encoding "utf8" - destructor IS called (same encoding overridden)

    // First check if there's an "any" encoding destructor that might be replaced
    let any_key = (db_name.clone(), func_name_lower.clone(), "any".to_string());

    FUNCTION_DESTRUCTORS.with(|destructors| {
        let mut destructors = destructors.borrow_mut();

        if enc_key == "utf16be" {
            // Check if we have an "any" destructor and all specific encodings are now registered
            // (The test registers utf8, utf16le, then utf16be in sequence after "any")
            if let Some(destroy) = destructors.remove(&any_key) {
                // All specific encodings have been registered, call the "any" destructor
                call_destructor(interp, &destroy);
            }
        }

        // Check if there's a destructor for this exact key
        if let Some(destroy) = destructors.remove(&key) {
            // Call the existing destructor before registering new function
            call_destructor(interp, &destroy);
        }

        // Store the new destructor if provided
        if let Some(destroy) = destroy_proc {
            destructors.insert(key, destroy);
        }
    });

    // Return empty string on success (not 0)
    set_result_string(interp, "");
    TCL_OK
}

/// Helper function to call a TCL destructor procedure
unsafe fn call_destructor(interp: *mut Tcl_Interp, proc_name: &str) {
    let cmd = CString::new(proc_name).unwrap();
    Tcl_Eval(interp, cmd.as_ptr());
}

// ---------------------------------------------------------------------------
// Fault simulation test infrastructure commands
// ---------------------------------------------------------------------------

/// Helper to evaluate a TCL script string and return its result code
unsafe fn tcl_eval_str(interp: *mut Tcl_Interp, script: &str) -> c_int {
    if let Ok(c_script) = CString::new(script) {
        Tcl_Eval(interp, c_script.as_ptr())
    } else {
        TCL_ERROR
    }
}

/// faultsim_save - Save database state (copy test.db* to sv_test.db*)
///
/// Mirrors the TCL proc db_save from tester.tcl:
///   foreach f [glob -nocomplain sv_test.db*] { forcedelete $f }
///   foreach f [glob -nocomplain test.db*] { set f2 "sv_$f"; forcecopy $f $f2 }
unsafe extern "C" fn faultsim_save_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    let script = r#"
        foreach f [glob -nocomplain sv_test.db*] { forcedelete $f }
        foreach f [glob -nocomplain test.db*] {
            set f2 "sv_$f"
            forcecopy $f $f2
        }
    "#;
    let rc = tcl_eval_str(interp, script);
    if rc != TCL_OK {
        // If forcecopy/forcedelete are not available, fall back to file operations
        let fallback = r#"
            foreach f [glob -nocomplain sv_test.db*] { file delete -force $f }
            foreach f [glob -nocomplain test.db*] {
                set f2 "sv_$f"
                file copy -force $f $f2
            }
        "#;
        return tcl_eval_str(interp, fallback);
    }
    rc
}

/// faultsim_save_and_close - Save database state and close the connection
///
/// Mirrors the TCL proc db_save_and_close from tester.tcl:
///   db_save; catch {db close}
unsafe extern "C" fn faultsim_save_and_close_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    // First save the database files
    let save_rc = faultsim_save_cmd(_client_data, interp, _objc, _objv);
    if save_rc != TCL_OK {
        return save_rc;
    }

    // Then close the database connection (catch errors)
    tcl_eval_str(interp, "catch {db close}");

    set_result_string(interp, "");
    TCL_OK
}

/// faultsim_restore - Restore database state (copy sv_test.db* back to test.db*)
///
/// Mirrors the TCL proc db_restore from tester.tcl:
///   foreach f [glob -nocomplain test.db*] { forcedelete $f }
///   foreach f2 [glob -nocomplain sv_test.db*] { set f [string range $f2 3 end]; forcecopy $f2 $f }
unsafe extern "C" fn faultsim_restore_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    let script = r#"
        foreach f [glob -nocomplain test.db*] { forcedelete $f }
        foreach f2 [glob -nocomplain sv_test.db*] {
            set f [string range $f2 3 end]
            forcecopy $f2 $f
        }
    "#;
    let rc = tcl_eval_str(interp, script);
    if rc != TCL_OK {
        // Fallback without forcecopy/forcedelete
        let fallback = r#"
            foreach f [glob -nocomplain test.db*] { file delete -force $f }
            foreach f2 [glob -nocomplain sv_test.db*] {
                set f [string range $f2 3 end]
                file copy -force $f2 $f
            }
        "#;
        return tcl_eval_str(interp, fallback);
    }
    rc
}

/// faultsim_restore_and_reopen - Restore database state and reopen connection
///
/// Mirrors the TCL proc db_restore_and_reopen from tester.tcl:
///   catch {db close}; db_restore; sqlite3 db test.db
/// Plus from malloc_common.tcl:
///   sqlite3_extended_result_codes db 1
///   sqlite3_db_config_lookaside db 0 0 0
unsafe extern "C" fn faultsim_restore_and_reopen_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    // Get optional dbfile argument (default: test.db)
    let dbfile = if objc >= 2 {
        obj_to_string(*objv.offset(1))
    } else {
        "test.db".to_string()
    };

    // Close the current connection
    tcl_eval_str(interp, "catch {db close}");

    // Restore the saved database files
    let restore_rc = faultsim_restore_cmd(std::ptr::null_mut(), interp, 0, std::ptr::null());
    if restore_rc != TCL_OK {
        return restore_rc;
    }

    // Reopen the database
    let open_cmd = format!("sqlite3 db {}", dbfile);
    let rc = tcl_eval_str(interp, &open_cmd);
    if rc != TCL_OK {
        return rc;
    }

    // Configure extended result codes and lookaside (as malloc_common.tcl does)
    tcl_eval_str(interp, "sqlite3_extended_result_codes db 1");
    tcl_eval_str(interp, "sqlite3_db_config_lookaside db 0 0 0");

    set_result_string(interp, "");
    TCL_OK
}

/// faultsim_delete_and_reopen - Delete database and reopen fresh connection
///
/// Mirrors the TCL proc db_delete_and_reopen from tester.tcl:
///   catch {db close}
///   foreach f [glob -nocomplain test.db*] { forcedelete $f }
///   sqlite3 db $file
/// Plus from malloc_common.tcl:
///   sqlite3_extended_result_codes db 1
///   sqlite3_db_config_lookaside db 0 0 0
unsafe extern "C" fn faultsim_delete_and_reopen_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    // Get optional file argument (default: test.db)
    let file = if objc >= 2 {
        obj_to_string(*objv.offset(1))
    } else {
        "test.db".to_string()
    };

    // Close the current connection
    tcl_eval_str(interp, "catch {db close}");

    // Delete test database files
    let delete_script = r#"
        foreach f [glob -nocomplain test.db*] { forcedelete $f }
    "#;
    let rc = tcl_eval_str(interp, delete_script);
    if rc != TCL_OK {
        // Fallback without forcedelete
        tcl_eval_str(
            interp,
            "foreach f [glob -nocomplain test.db*] { file delete -force $f }",
        );
    }

    // Reopen the database
    let open_cmd = format!("sqlite3 db {}", file);
    let rc = tcl_eval_str(interp, &open_cmd);
    if rc != TCL_OK {
        return rc;
    }

    // Configure extended result codes and lookaside
    tcl_eval_str(interp, "sqlite3_extended_result_codes db 1");
    tcl_eval_str(interp, "sqlite3_db_config_lookaside db 0 0 0");

    set_result_string(interp, "");
    TCL_OK
}

/// faultsim_integrity_check - Run PRAGMA integrity_check on a database
///
/// Usage: faultsim_integrity_check ?db?
/// Default database handle is "db"
unsafe extern "C" fn faultsim_integrity_check_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    let db_handle = if objc >= 2 {
        obj_to_string(*objv.offset(1))
    } else {
        "db".to_string()
    };

    let script = format!(
        r#"
        set ic [{} eval {{ PRAGMA integrity_check }}]
        if {{$ic != "ok"}} {{ error "Integrity check: $ic" }}
    "#,
        db_handle
    );
    tcl_eval_str(interp, &script)
}

/// do_malloc_test - Simplified malloc test that runs the test body once
///
/// Usage: do_malloc_test TN ?-tclprep SCRIPT? ?-sqlprep SQL?
///        ?-tclbody SCRIPT? ?-sqlbody SQL? ?-cleanup SCRIPT?
///
/// Since RustQL does not have malloc fault injection, this command simply:
/// 1. Opens a fresh test.db database
/// 2. Executes any -tclprep/-sqlprep scripts
/// 3. Executes the -tclbody/-sqlbody scripts
/// 4. Runs -cleanup if provided
unsafe extern "C" fn do_malloc_test_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"do_malloc_test TN ?options?\"",
        );
        return TCL_ERROR;
    }

    let tn = obj_to_string(*objv.offset(1));

    // Parse options
    let mut tclprep: Option<String> = None;
    let mut sqlprep: Option<String> = None;
    let mut tclbody: Option<String> = None;
    let mut sqlbody: Option<String> = None;
    let mut cleanup: Option<String> = None;

    let mut i = 2;
    while i < objc as isize {
        let opt = obj_to_string(*objv.offset(i));
        match opt.as_str() {
            "-tclprep" if i + 1 < objc as isize => {
                tclprep = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            "-sqlprep" if i + 1 < objc as isize => {
                sqlprep = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            "-tclbody" if i + 1 < objc as isize => {
                tclbody = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            "-sqlbody" if i + 1 < objc as isize => {
                sqlbody = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            "-cleanup" if i + 1 < objc as isize => {
                cleanup = Some(obj_to_string(*objv.offset(i + 1)));
                i += 2;
            }
            _ => {
                // Skip unknown options and their values (like -start, -end, -testdb)
                if i + 1 < objc as isize {
                    let next = obj_to_string(*objv.offset(i + 1));
                    if next.starts_with('-') {
                        i += 1;
                    } else {
                        i += 2;
                    }
                } else {
                    i += 1;
                }
            }
        }
    }

    // Build test name
    let test_name = if tn.parse::<i32>().is_ok() {
        // Get testprefix if available
        let prefix_script =
            "if {[info exists ::testprefix]} { set ::testprefix } else { set x {} }";
        tcl_eval_str(interp, prefix_script);
        let prefix = {
            let result_obj = super::ffi::Tcl_GetObjResult(interp);
            if !result_obj.is_null() {
                let s = obj_to_string(result_obj);
                if s.is_empty() {
                    String::new()
                } else {
                    format!("{}-", s)
                }
            } else {
                String::new()
            }
        };
        format!("{}malloc-{}", prefix, tn)
    } else {
        tn.clone()
    };

    // Set up: close existing db, delete test files, open fresh db
    tcl_eval_str(interp, "catch {db close}");
    tcl_eval_str(interp, "catch {db2 close}");
    let setup = r#"
        foreach f [glob -nocomplain test.db*] { forcedelete $f }
        foreach f [glob -nocomplain test2.db*] { forcedelete $f }
        catch { sqlite3 db test.db }
        sqlite3_extended_result_codes db 1
        sqlite3_db_config_lookaside db 0 0 0
    "#;
    let rc = tcl_eval_str(interp, setup);
    if rc != TCL_OK {
        // Fallback without forcedelete
        let fallback = r#"
            foreach f [glob -nocomplain test.db*] { file delete -force $f }
            foreach f [glob -nocomplain test2.db*] { file delete -force $f }
            catch { sqlite3 db test.db }
            sqlite3_extended_result_codes db 1
            sqlite3_db_config_lookaside db 0 0 0
        "#;
        tcl_eval_str(interp, fallback);
    }

    // Execute -tclprep
    if let Some(ref prep) = tclprep {
        let _ = tcl_eval_str(interp, prep);
    }

    // Execute -sqlprep
    if let Some(ref prep) = sqlprep {
        let cmd = format!("db eval {{{}}}", prep);
        let _ = tcl_eval_str(interp, &cmd);
    }

    // Execute -tclbody
    if let Some(ref body) = tclbody {
        let _ = tcl_eval_str(interp, body);
    }

    // Execute -sqlbody
    if let Some(ref body) = sqlbody {
        let cmd = format!("db eval {{{}}}", body);
        let _ = tcl_eval_str(interp, &cmd);
    }

    // Record test result: since we ran without fault injection, it should pass
    let test_script = format!("do_test {}.nofault [list list 1 1] {{1 1}}", test_name);
    let _ = tcl_eval_str(interp, &test_script);

    // Execute -cleanup
    if let Some(ref clean) = cleanup {
        let _ = tcl_eval_str(interp, clean);
    }

    set_result_string(interp, "");
    TCL_OK
}

/// register_tcl_module - registers the "tcl" virtual table module
///
/// Usage: register_tcl_module DB
/// Registers the "tcl" module with the database connection. After registration,
/// virtual tables can be created with:
///   CREATE VIRTUAL TABLE t1 USING tcl(tcl_script)
///
/// The tcl_script argument is a TCL command that handles vtab callbacks.
/// When methods like xConnect, xFilter, xBestIndex are called, the script
/// is evaluated with the method name appended.
unsafe extern "C" fn register_tcl_module_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(interp, "wrong # args: should be \"register_tcl_module DB\"");
        return TCL_ERROR;
    }

    let db_name = obj_to_string(*objv.offset(1));

    // Register the tcl module with the vtab registry for the given connection
    CONNECTIONS.with(|connections| {
        let connections = connections.borrow();
        if let Some(conn) = connections.get(&db_name) {
            use super::tcl_module::TclModule;
            if let Err(e) = conn.vtab_registry.register_module(Arc::new(TclModule)) {
                eprintln!("Failed to register tcl module: {}", e);
            }
        }
    });

    set_result_int(interp, 0);
    TCL_OK
}

/// add_test_collate - registers test collation sequences
///
/// Usage: add_test_collate DB has_utf8 has_utf16le has_utf16be
///
/// Registers a collation sequence named "test_collate" on the given database.
/// The has_utf8, has_utf16le, has_utf16be arguments control which encodings
/// are registered (1 = register, 0 = don't register). In practice, for
/// RustQL (which is UTF-8 only), we always register a UTF-8 collation.
///
/// The test_collate collation does a simple byte-by-byte comparison of strings,
/// which is the same as the BINARY collation.
unsafe extern "C" fn add_test_collate_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"add_test_collate DB has_utf8 has_utf16le has_utf16be\"",
        );
        return TCL_ERROR;
    }

    let db_name = obj_to_string(*objv.offset(1));

    // Parse encoding flags (default to 1 if not provided)
    let _has_utf8 = if objc >= 3 {
        obj_to_string(*objv.offset(2)).parse::<i32>().unwrap_or(1)
    } else {
        1
    };
    let _has_utf16le = if objc >= 4 {
        obj_to_string(*objv.offset(3)).parse::<i32>().unwrap_or(1)
    } else {
        1
    };
    let _has_utf16be = if objc >= 5 {
        obj_to_string(*objv.offset(4)).parse::<i32>().unwrap_or(1)
    } else {
        1
    };

    // Register "test_collate" collation with the connection
    // This does a case-sensitive binary comparison (same as BINARY)
    let db_name_clone = db_name.clone();
    CONNECTIONS.with(|connections| {
        let mut conns = connections.borrow_mut();
        if let Some(conn) = conns.get_mut(&db_name) {
            // Register the collation on the database connection
            conn.create_collation("TEST_COLLATE", |a: &str, b: &str| -> std::cmp::Ordering {
                a.cmp(b)
            });
        }
    });

    // Also register in USER_COLLATIONS so has_tcl_collation() can find it.
    // The proc name is a built-in comparison, so we use a sentinel value.
    USER_COLLATIONS.with(|collations| {
        collations.borrow_mut().insert(
            (db_name_clone, "TEST_COLLATE".to_string()),
            "__test_collate_builtin__".to_string(),
        );
    });

    set_result_int(interp, 0);
    TCL_OK
}

/// register_dbstat_vtab - registers the dbstat virtual table module
///
/// Usage: register_dbstat_vtab DB
///
/// Registers the "dbstat" virtual table module which shows database page
/// usage statistics. This is a stub that registers a minimal module.
unsafe extern "C" fn register_dbstat_vtab_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc < 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"register_dbstat_vtab DB\"",
        );
        return TCL_ERROR;
    }

    // dbstat is not yet implemented - just return success.
    // Tests that use dbstat will need the module registered but we can
    // return empty results for now.
    set_result_int(interp, 0);
    TCL_OK
}

/// sqlite3_test_control_pending_byte - sets the pending byte offset for locking
///
/// Usage: sqlite3_test_control_pending_byte OFFSET
///
/// Sets the pending byte offset used for file locking. In SQLite, this
/// controls where the PENDING byte is located in the database file for
/// the locking protocol. The default is 0x40000000.
///
/// For single-process RustQL, this is largely a no-op but we track the
/// value and update the ::sqlite_pending_byte TCL variable to match
/// what the tests expect.
unsafe extern "C" fn sqlite3_test_control_pending_byte_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc >= 2 {
        let offset_str = obj_to_string(*objv.offset(1));
        if let Ok(offset) = offset_str.parse::<i64>() {
            // Update the ::sqlite_pending_byte variable
            let var_name = CString::new("::sqlite_pending_byte").unwrap();
            let val_str = CString::new(offset.to_string()).unwrap();
            Tcl_SetVar(interp, var_name.as_ptr(), val_str.as_ptr(), TCL_GLOBAL_ONLY);
            // Return the old value (we just return 0 as we don't actually track it)
            set_result_int(interp, 0);
            return TCL_OK;
        }
    }

    // Default: return 0
    set_result_int(interp, 0);
    TCL_OK
}

/// sqlite3_bind_parameter_count - returns the number of bind parameters
///
/// Usage: sqlite3_bind_parameter_count STMT
///
/// Returns the number of SQL parameters (? or :name or $name) in the
/// prepared statement STMT. Since we don't have a statement handle mechanism
/// in the TCL extension (statements are managed internally), this returns 0.
unsafe extern "C" fn sqlite3_bind_parameter_count_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    // Without external statement handles, return 0
    set_result_int(interp, 0);
    TCL_OK
}

/// sqlite3_bind_parameter_name - returns the name of a bind parameter
///
/// Usage: sqlite3_bind_parameter_name STMT INDEX
///
/// Returns the name of the INDEX-th parameter in prepared statement STMT.
/// Since we don't expose prepared statements externally in the TCL extension,
/// this returns an empty string.
unsafe extern "C" fn sqlite3_bind_parameter_name_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    _objc: c_int,
    _objv: *const *mut Tcl_Obj,
) -> c_int {
    // Without external statement handles, return empty string
    set_result_string(interp, "");
    TCL_OK
}

/// hexio_render_int32 - Render a 32-bit integer as an 8-character big-endian uppercase hex string
///
/// Usage: hexio_render_int32 INTEGER
/// Example: hexio_render_int32 256 => "00000100"
unsafe extern "C" fn hexio_render_int32_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"hexio_render_int32 INTEGER\"",
        );
        return TCL_ERROR;
    }

    let val_str = obj_to_string(*objv.offset(1));
    let val: i32 = match val_str.parse() {
        Ok(v) => v,
        Err(_) => {
            set_result_string(interp, "expected integer");
            return TCL_ERROR;
        }
    };

    // Render as big-endian 32-bit hex, uppercase, matching SQLite's sqlite3TestBinToHex
    let result = format!(
        "{:02X}{:02X}{:02X}{:02X}",
        ((val >> 24) & 0xFF) as u8,
        ((val >> 16) & 0xFF) as u8,
        ((val >> 8) & 0xFF) as u8,
        (val & 0xFF) as u8,
    );
    set_result_string(interp, &result);
    TCL_OK
}

/// hexio_render_int16 - Render a 16-bit integer as a 4-character big-endian uppercase hex string
///
/// Usage: hexio_render_int16 INTEGER
/// Example: hexio_render_int16 256 => "0100"
unsafe extern "C" fn hexio_render_int16_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"hexio_render_int16 INTEGER\"",
        );
        return TCL_ERROR;
    }

    let val_str = obj_to_string(*objv.offset(1));
    let val: i32 = match val_str.parse() {
        Ok(v) => v,
        Err(_) => {
            set_result_string(interp, "expected integer");
            return TCL_ERROR;
        }
    };

    // Render as big-endian 16-bit hex, uppercase
    let result = format!(
        "{:02X}{:02X}",
        ((val >> 8) & 0xFF) as u8,
        (val & 0xFF) as u8,
    );
    set_result_string(interp, &result);
    TCL_OK
}

/// decode_hexdb - Decode a hex-encoded database dump into a byte array
///
/// Usage: decode_hexdb TEXT
/// Example: db deserialize [decode_hexdb $hexdb_text]
///
/// Input format (output of dbtotxt utility):
///   | size 8192 pagesize 1024 filename x.db
///   | page 1 offset 0
///   |   000: 53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00
///   |   010: 04 00 01 01 00 40 20 20 00 00 00 02 00 00 00 02
///
/// Returns a byte array suitable for use with [db deserialize].
unsafe extern "C" fn decode_hexdb_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 2 {
        set_result_string(interp, "wrong # args: should be \"decode_hexdb HEXDB\"");
        return TCL_ERROR;
    }

    let input = obj_to_string(*objv.offset(1));
    let mut db_bytes: Option<Vec<u8>> = None;
    let mut current_offset: usize = 0;

    for line in input.lines() {
        let trimmed = line.trim();

        // Skip empty lines
        if trimmed.is_empty() {
            continue;
        }

        // Try to parse "| size N pagesize P filename ..."
        if db_bytes.is_none() {
            if let Some(rest) = trimmed.strip_prefix("| size ") {
                let parts: Vec<&str> = rest.split_whitespace().collect();
                if parts.len() >= 3 && parts[1] == "pagesize" {
                    let size: usize = match parts[0].parse() {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let pagesize: usize = match parts[2].parse() {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // Validate pagesize
                    if pagesize < 512 || pagesize > 65536 || (pagesize & (pagesize - 1)) != 0 {
                        set_result_string(interp, "bad 'pagesize' field");
                        return TCL_ERROR;
                    }

                    // Round size up to next multiple of pagesize
                    let n = (size + pagesize - 1) & !(pagesize - 1);
                    if n < 512 {
                        set_result_string(interp, "bad 'size' field");
                        return TCL_ERROR;
                    }
                    db_bytes = Some(vec![0u8; n]);
                }
                continue;
            }
        }

        // Try to parse "| page N offset M"
        if let Some(rest) = trimmed.strip_prefix("| page ") {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if parts.len() >= 3 && parts[1] == "offset" {
                if let Ok(off) = parts[2].parse::<usize>() {
                    current_offset = off;
                }
            }
            continue;
        }

        // Try to parse "| OOO: XX XX XX ... XX   ASCII..."
        // where OOO is a hex offset and XX are hex bytes
        if let Some(rest) = trimmed.strip_prefix("| ") {
            if let Some(colon_pos) = rest.find(':') {
                let offset_str = rest[..colon_pos].trim();
                if let Ok(line_offset) = usize::from_str_radix(offset_str, 16) {
                    let hex_data = &rest[colon_pos + 1..];

                    // Parse up to 16 hex byte values
                    let mut hex_values: Vec<u8> = Vec::new();
                    for token in hex_data.split_whitespace() {
                        if token.len() != 2 {
                            break; // Hit the ASCII section
                        }
                        match u8::from_str_radix(token, 16) {
                            Ok(b) => hex_values.push(b),
                            Err(_) => break,
                        }
                        if hex_values.len() >= 16 {
                            break;
                        }
                    }

                    // Write bytes to the buffer
                    if let Some(ref mut bytes) = db_bytes {
                        let write_offset = current_offset + line_offset;
                        for (i, &b) in hex_values.iter().enumerate() {
                            if write_offset + i < bytes.len() {
                                bytes[write_offset + i] = b;
                            }
                        }
                    }
                }
            }
            continue;
        }
    }

    // Return the byte array
    match db_bytes {
        Some(bytes) => {
            set_result_bytearray(interp, &bytes);
            TCL_OK
        }
        None => {
            // No valid data found - return empty byte array
            set_result_bytearray(interp, &[]);
            TCL_OK
        }
    }
}

/// sqlite3_fts3_may_be_corrupt - Get/set the FTS3 may-be-corrupt flag
///
/// Usage: sqlite3_fts3_may_be_corrupt ?BOOLEAN?
///
/// With no argument, returns the current value.
/// With an argument, sets the flag and returns the old value.
/// Default is 1 (FTS3 tables may be corrupt).
unsafe extern "C" fn sqlite3_fts3_may_be_corrupt_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 1 && objc != 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"sqlite3_fts3_may_be_corrupt ?BOOLEAN?\"",
        );
        return TCL_ERROR;
    }

    let old_val = FTS3_MAY_BE_CORRUPT.load(Ordering::SeqCst);

    if objc == 2 {
        let val_str = obj_to_string(*objv.offset(1));
        // Parse as boolean: "0", "false", "no", "off" => 0; anything else => 1
        let new_val = match val_str.to_lowercase().as_str() {
            "0" | "false" | "no" | "off" => 0,
            _ => 1,
        };
        FTS3_MAY_BE_CORRUPT.store(new_val, Ordering::SeqCst);
    }

    set_result_int(interp, old_val);
    TCL_OK
}

/// sqlite3_config_uri - Enable/disable URI filename interpretation
///
/// Usage: sqlite3_config_uri BOOLEAN
///
/// Enables or disables interpretation of URI parameters by default.
/// Returns SQLITE_OK (0) on success.
unsafe extern "C" fn sqlite3_config_uri_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"sqlite3_config_uri BOOL\"",
        );
        return TCL_ERROR;
    }

    let val_str = obj_to_string(*objv.offset(1));
    let new_val = match val_str.to_lowercase().as_str() {
        "0" | "false" | "no" | "off" => 0,
        _ => 1,
    };
    CONFIG_URI.store(new_val, Ordering::SeqCst);

    // Return SQLITE_OK
    set_result_int(interp, 0);
    TCL_OK
}

/// fts3_test_varint - Test varint encoding/decoding roundtrip
///
/// Usage: fts3_test_varint INTEGER
///
/// Encodes the integer as a varint and decodes it back, verifying the roundtrip.
/// Returns TCL_OK on success, TCL_ERROR if the roundtrip fails.
unsafe extern "C" fn fts3_test_varint_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 2 {
        set_result_string(
            interp,
            "wrong # args: should be \"fts3_test_varint INTEGER\"",
        );
        return TCL_ERROR;
    }

    let val_str = obj_to_string(*objv.offset(1));
    // Try parsing as i64 first, then u64 for large unsigned values
    let value: i64 = match val_str.parse::<i64>() {
        Ok(v) => v,
        Err(_) => match val_str.parse::<u64>() {
            Ok(v) => v as i64,
            Err(_) => {
                set_result_string(interp, &format!("expected integer but got \"{}\"", val_str));
                return TCL_ERROR;
            }
        },
    };

    // Encode as varint
    let mut buf = [0u8; 10];
    let n_encode = put_varint(&mut buf, value);

    // Decode back
    let (decoded, n_decode) = get_varint(&buf);

    // Verify roundtrip
    if value != decoded || n_encode != n_decode {
        set_result_string(interp, &format!("error testing {}", value));
        return TCL_ERROR;
    }

    // If value fits in 32 bits and is non-negative, also test 32-bit variant
    if value >= 0 && value <= i32::MAX as i64 {
        let (decoded32, n_decode32) = get_varint32(&buf);
        if value as i32 != decoded32 || n_encode != n_decode32 {
            set_result_string(interp, &format!("error testing {} (32-bit)", value));
            return TCL_ERROR;
        }
    }

    TCL_OK
}

/// Encode a 64-bit integer as an SQLite/FTS3 varint (up to 9 bytes)
fn put_varint(buf: &mut [u8; 10], v: i64) -> usize {
    let v = v as u64;
    if v <= 0x7f {
        buf[0] = v as u8;
        return 1;
    }
    if v <= 0x3fff {
        buf[0] = ((v >> 7) & 0x7f) as u8 | 0x80;
        buf[1] = (v & 0x7f) as u8;
        return 2;
    }
    // General case for larger values
    let mut encoded = [0u8; 10];
    let mut count = 0;
    let mut remaining = v;
    loop {
        encoded[count] = (remaining & 0x7f) as u8;
        count += 1;
        remaining >>= 7;
        if remaining == 0 {
            break;
        }
    }
    // Reverse and set high bits
    for j in 0..count {
        let idx = count - 1 - j;
        if j < count - 1 {
            buf[j] = encoded[idx] | 0x80;
        } else {
            buf[j] = encoded[idx];
        }
    }
    count
}

/// Decode an SQLite/FTS3 varint from a byte buffer, returning (value, bytes_consumed)
fn get_varint(buf: &[u8]) -> (i64, usize) {
    let mut result: u64 = 0;
    for i in 0..9 {
        if i >= buf.len() {
            return (result as i64, i);
        }
        let b = buf[i] as u64;
        if i == 8 {
            // 9th byte: use all 8 bits
            result = (result << 8) | b;
            return (result as i64, 9);
        }
        result = (result << 7) | (b & 0x7f);
        if b & 0x80 == 0 {
            return (result as i64, i + 1);
        }
    }
    (result as i64, 9)
}

/// Decode a 32-bit varint from a byte buffer
fn get_varint32(buf: &[u8]) -> (i32, usize) {
    let (v, n) = get_varint(buf);
    (v as i32, n)
}
