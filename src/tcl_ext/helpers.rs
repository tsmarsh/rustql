//! TCL helper functions for value conversion
//!
//! This module provides utility functions for converting between Rust and TCL values.

use std::ffi::CString;
use std::os::raw::c_int;

use super::ffi::{
    Tcl_GetByteArrayFromObj, Tcl_GetStringFromObj, Tcl_Interp, Tcl_NewByteArrayObj,
    Tcl_NewDoubleObj, Tcl_NewIntObj, Tcl_NewStringObj, Tcl_NewWideIntObj, Tcl_Obj,
    Tcl_SetObjResult,
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

/// Set the TCL interpreter result to a 64-bit integer (wide int)
pub unsafe fn set_result_wide_int(interp: *mut Tcl_Interp, i: i64) {
    let obj = Tcl_NewWideIntObj(i);
    Tcl_SetObjResult(interp, obj);
}

/// Set the TCL interpreter result to a double
pub unsafe fn set_result_double(interp: *mut Tcl_Interp, d: f64) {
    let obj = Tcl_NewDoubleObj(d);
    Tcl_SetObjResult(interp, obj);
}

/// Set the TCL interpreter result to a bytearray (blob)
pub unsafe fn set_result_bytearray(interp: *mut Tcl_Interp, bytes: &[u8]) {
    let obj = Tcl_NewByteArrayObj(bytes.as_ptr(), bytes.len() as c_int);
    Tcl_SetObjResult(interp, obj);
}

/// Get byte array from a TCL object
pub unsafe fn obj_to_bytes(obj: *mut Tcl_Obj) -> Vec<u8> {
    let mut len: c_int = 0;
    let ptr = Tcl_GetByteArrayFromObj(obj, &mut len);
    if ptr.is_null() || len <= 0 {
        return Vec::new();
    }
    std::slice::from_raw_parts(ptr, len as usize).to_vec()
}

/// Get the type name of a TCL object
///
/// Returns the internal type name: "int", "wideInt", "double", "bytearray", etc.
/// Returns empty string if no type is set (pure string).
pub unsafe fn get_tcl_obj_type_name(obj: *mut Tcl_Obj) -> String {
    if obj.is_null() {
        return String::new();
    }

    // The Tcl_Obj structure has a typePtr field at a known offset.
    // In TCL 8.x, the structure is:
    //   - refCount: c_int
    //   - bytes: *mut c_char
    //   - length: c_int
    //   - typePtr: *const Tcl_ObjType
    //   - internalRep: union
    //
    // We need to read the typePtr field. The offset depends on architecture.
    // For 64-bit: refCount(4) + padding(4) + bytes(8) + length(4) + padding(4) = 24 bytes
    // For 32-bit: refCount(4) + bytes(4) + length(4) = 12 bytes
    //
    // Actually, let's use a safer approach: parse the object to get its type.
    // TCL provides the type in the internal Tcl_Obj structure.

    // We'll read the typePtr directly from the Tcl_Obj structure.
    // This is fragile but necessary since TCL doesn't expose a clean API for this.

    #[cfg(target_pointer_width = "64")]
    let type_ptr_offset = 24usize; // offset on 64-bit systems

    #[cfg(target_pointer_width = "32")]
    let type_ptr_offset = 12usize; // offset on 32-bit systems

    let type_ptr_addr =
        (obj as *const u8).add(type_ptr_offset) as *const *const super::ffi::Tcl_ObjType;
    let type_ptr = *type_ptr_addr;

    if type_ptr.is_null() {
        return String::new();
    }

    let name_ptr = (*type_ptr).name;
    if name_ptr.is_null() {
        return String::new();
    }

    std::ffi::CStr::from_ptr(name_ptr)
        .to_str()
        .unwrap_or("")
        .to_string()
}

/// Check if a TCL object has a string representation
///
/// Returns true if the object has a cached string representation (bytes != NULL)
pub unsafe fn tcl_obj_has_string_rep(obj: *mut Tcl_Obj) -> bool {
    if obj.is_null() {
        return false;
    }

    // The bytes field is at offset 8 on 64-bit (after refCount + padding)
    // or offset 4 on 32-bit (after refCount)
    #[cfg(target_pointer_width = "64")]
    let bytes_offset = 8usize;

    #[cfg(target_pointer_width = "32")]
    let bytes_offset = 4usize;

    let bytes_ptr_addr = (obj as *const u8).add(bytes_offset) as *const *const std::os::raw::c_char;
    let bytes_ptr = *bytes_ptr_addr;

    !bytes_ptr.is_null()
}
