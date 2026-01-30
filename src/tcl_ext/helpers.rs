//! TCL helper functions for value conversion
//!
//! This module provides utility functions for converting between Rust and TCL values.

use std::ffi::CString;
use std::os::raw::c_int;

use super::ffi::{
    Tcl_GetStringFromObj, Tcl_Interp, Tcl_NewIntObj, Tcl_NewStringObj, Tcl_Obj, Tcl_SetObjResult,
};

/// Convert a TCL object to a Rust String
pub unsafe fn obj_to_string(obj: *mut Tcl_Obj) -> String {
    let mut len: c_int = 0;
    let ptr = Tcl_GetStringFromObj(obj, &mut len);
    if ptr.is_null() {
        return String::new();
    }
    let slice = std::slice::from_raw_parts(ptr as *const u8, len as usize);
    String::from_utf8_lossy(slice).to_string()
}

/// Convert a Rust string to a TCL object
pub unsafe fn string_to_obj(s: &str) -> *mut Tcl_Obj {
    let c_str = CString::new(s).unwrap_or_else(|_| CString::new("").unwrap());
    Tcl_NewStringObj(c_str.as_ptr(), s.len() as c_int)
}

/// Set the TCL interpreter result to a string
pub unsafe fn set_result_string(interp: *mut Tcl_Interp, s: &str) {
    let obj = string_to_obj(s);
    Tcl_SetObjResult(interp, obj);
}

/// Set the TCL interpreter result to an integer
pub unsafe fn set_result_int(interp: *mut Tcl_Interp, i: i32) {
    let obj = Tcl_NewIntObj(i);
    Tcl_SetObjResult(interp, obj);
}
