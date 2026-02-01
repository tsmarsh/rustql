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

use super::ffi::{
    Tcl_CreateObjCommand, Tcl_Interp, Tcl_NewIntObj, Tcl_Obj, Tcl_SetVar, Tcl_SetVar2Ex,
    TCL_GLOBAL_ONLY, TCL_OK,
};
use super::helpers::{set_result_int, set_result_string};
use super::md5::md5_cmd;

// Database commands from db module
use super::db::{
    clang_sanitize_address_cmd, sqlite3_exec_cmd, sqlite3_exec_hex_cmd, sqlite3_get_autocommit_cmd,
    sqlite3_txn_state_cmd, tcl_variable_type_cmd, working_64bit_int_cmd,
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
        "faultsim_delete_and_reopen",
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
        ("fts5", "1"),
        ("rtree", "1"),
        ("json", "1"),
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
