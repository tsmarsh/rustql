//! User-defined TCL function support
//!
//! This module handles registering and calling user-defined TCL functions
//! from SQL queries.

use std::ffi::CString;
use std::os::raw::c_int;

use super::ffi::{Tcl_Eval, Tcl_GetObjResult, Tcl_GetStringFromObj, Tcl_Interp, TCL_OK};
use super::{CURRENT_INTERP, USER_FUNCTIONS};

/// Set the current TCL interpreter for user function callbacks
pub fn set_current_interp(interp: *mut Tcl_Interp) {
    CURRENT_INTERP.with(|cell| {
        *cell.borrow_mut() = Some(interp);
    });
}

/// Clear the current TCL interpreter
pub fn clear_current_interp() {
    CURRENT_INTERP.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Call a user-defined TCL function and return its result
/// Returns None if no such function is registered or interp is not set
pub fn call_tcl_user_function(func_name: &str, args: &[String]) -> Option<String> {
    // Get the current interpreter
    let interp = CURRENT_INTERP.with(|cell| *cell.borrow())?;
    if interp.is_null() {
        return None;
    }

    // Find the function - check all connections for now
    let proc_name = USER_FUNCTIONS.with(|funcs| {
        let funcs = funcs.borrow();
        // Find any registered function with this name (ignoring db_name for simplicity)
        for ((_, fname), proc) in funcs.iter() {
            if fname.eq_ignore_ascii_case(func_name) {
                return Some(proc.clone());
            }
        }
        None
    })?;

    // Build the TCL command: proc_name arg1 arg2 ...
    let mut cmd = proc_name;
    for arg in args {
        cmd.push(' ');
        // Quote the argument
        cmd.push('{');
        cmd.push_str(arg);
        cmd.push('}');
    }

    // Execute the TCL command
    unsafe {
        let cmd_cstr = CString::new(cmd).ok()?;
        let result = Tcl_Eval(interp, cmd_cstr.as_ptr());
        if result == TCL_OK {
            // Get the result
            let result_obj = Tcl_GetObjResult(interp);
            if !result_obj.is_null() {
                let mut len: c_int = 0;
                let ptr = Tcl_GetStringFromObj(result_obj, &mut len);
                if !ptr.is_null() {
                    let slice = std::slice::from_raw_parts(ptr as *const u8, len as usize);
                    return Some(String::from_utf8_lossy(slice).to_string());
                }
            }
        }
    }
    None
}

/// Check if a user-defined TCL function exists
pub fn has_tcl_user_function(func_name: &str) -> bool {
    USER_FUNCTIONS.with(|funcs| {
        let funcs = funcs.borrow();
        for ((_, fname), _) in funcs.iter() {
            if fname.eq_ignore_ascii_case(func_name) {
                return true;
            }
        }
        false
    })
}
