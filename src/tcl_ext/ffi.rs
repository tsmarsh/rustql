//! TCL FFI bindings
//!
//! This module contains the raw FFI bindings to the TCL C library.

use std::ffi::c_void;
use std::os::raw::{c_char, c_int};

// TCL result codes
pub const TCL_OK: c_int = 0;
pub const TCL_ERROR: c_int = 1;

// TCL variable flags
pub const TCL_GLOBAL_ONLY: c_int = 1;
pub const TCL_APPEND_VALUE: c_int = 4;
pub const TCL_LIST_ELEMENT: c_int = 8;

// TCL eval flags
pub const TCL_EVAL_GLOBAL: c_int = 0x00020000;

/// Opaque TCL interpreter handle
#[repr(C)]
pub struct Tcl_Interp {
    _private: [u8; 0],
}

/// Opaque TCL object handle
#[repr(C)]
pub struct Tcl_Obj {
    _private: [u8; 0],
}

/// TCL command procedure type
pub type Tcl_ObjCmdProc =
    unsafe extern "C" fn(*mut c_void, *mut Tcl_Interp, c_int, *const *mut Tcl_Obj) -> c_int;

/// TCL command deletion callback type
pub type Tcl_CmdDeleteProc = unsafe extern "C" fn(*mut c_void);

/// TCL object type structure (partial, we only need the name)
#[repr(C)]
pub struct Tcl_ObjType {
    pub name: *const c_char,
    // ... other fields we don't need
}

extern "C" {
    pub fn Tcl_CreateObjCommand(
        interp: *mut Tcl_Interp,
        cmdName: *const c_char,
        proc: Option<Tcl_ObjCmdProc>,
        clientData: *mut c_void,
        deleteProc: Option<Tcl_CmdDeleteProc>,
    ) -> *mut c_void;

    pub fn Tcl_DeleteCommand(interp: *mut Tcl_Interp, cmdName: *const c_char) -> c_int;

    pub fn Tcl_SetObjResult(interp: *mut Tcl_Interp, objPtr: *mut Tcl_Obj);

    pub fn Tcl_GetObjResult(interp: *mut Tcl_Interp) -> *mut Tcl_Obj;

    pub fn Tcl_GetStringFromObj(objPtr: *mut Tcl_Obj, lengthPtr: *mut c_int) -> *const c_char;

    pub fn Tcl_NewStringObj(bytes: *const c_char, length: c_int) -> *mut Tcl_Obj;

    pub fn Tcl_NewIntObj(intValue: c_int) -> *mut Tcl_Obj;

    pub fn Tcl_NewWideIntObj(wideValue: i64) -> *mut Tcl_Obj;

    pub fn Tcl_NewDoubleObj(doubleValue: f64) -> *mut Tcl_Obj;

    pub fn Tcl_NewByteArrayObj(bytes: *const u8, length: c_int) -> *mut Tcl_Obj;

    pub fn Tcl_GetByteArrayFromObj(objPtr: *mut Tcl_Obj, lengthPtr: *mut c_int) -> *const u8;

    pub fn Tcl_NewListObj(objc: c_int, objv: *const *mut Tcl_Obj) -> *mut Tcl_Obj;

    pub fn Tcl_ListObjAppendElement(
        interp: *mut Tcl_Interp,
        listPtr: *mut Tcl_Obj,
        objPtr: *mut Tcl_Obj,
    ) -> c_int;

    pub fn Tcl_Eval(interp: *mut Tcl_Interp, script: *const c_char) -> c_int;

    pub fn Tcl_SetVar2Ex(
        interp: *mut Tcl_Interp,
        part1: *const c_char,
        part2: *const c_char,
        newValuePtr: *mut Tcl_Obj,
        flags: c_int,
    ) -> *mut Tcl_Obj;

    pub fn Tcl_UnsetVar2(
        interp: *mut Tcl_Interp,
        part1: *const c_char,
        part2: *const c_char,
        flags: c_int,
    ) -> c_int;

    pub fn Tcl_SetVar(
        interp: *mut Tcl_Interp,
        varName: *const c_char,
        newValue: *const c_char,
        flags: c_int,
    ) -> *const c_char;

    pub fn Tcl_GetVar(
        interp: *mut Tcl_Interp,
        varName: *const c_char,
        flags: c_int,
    ) -> *const c_char;

    pub fn Tcl_GetVar2Ex(
        interp: *mut Tcl_Interp,
        part1: *const c_char,
        part2: *const c_char,
        flags: c_int,
    ) -> *mut Tcl_Obj;

    pub fn Tcl_ObjGetVar2(
        interp: *mut Tcl_Interp,
        part1Ptr: *mut Tcl_Obj,
        part2Ptr: *mut Tcl_Obj,
        flags: c_int,
    ) -> *mut Tcl_Obj;

    pub fn Tcl_ListObjGetElements(
        interp: *mut Tcl_Interp,
        listPtr: *mut Tcl_Obj,
        objcPtr: *mut c_int,
        objvPtr: *mut *const *mut Tcl_Obj,
    ) -> c_int;

    pub fn Tcl_GetIntFromObj(
        interp: *mut Tcl_Interp,
        objPtr: *mut Tcl_Obj,
        intPtr: *mut c_int,
    ) -> c_int;

    pub fn Tcl_GetBooleanFromObj(
        interp: *mut Tcl_Interp,
        objPtr: *mut Tcl_Obj,
        boolPtr: *mut c_int,
    ) -> c_int;

    pub fn Tcl_EvalObjEx(interp: *mut Tcl_Interp, objPtr: *mut Tcl_Obj, flags: c_int) -> c_int;

    pub fn Tcl_DuplicateObj(objPtr: *mut Tcl_Obj) -> *mut Tcl_Obj;

    pub fn Tcl_ResetResult(interp: *mut Tcl_Interp);

    pub fn Tcl_GetStringResult(interp: *mut Tcl_Interp) -> *const c_char;

    pub fn Tcl_BackgroundError(interp: *mut Tcl_Interp);
}

/// Tcl_IncrRefCount is a macro in TCL, not a function.
/// It increments the reference count of a Tcl_Obj.
/// The refCount field is at offset 0 in the Tcl_Obj struct (it's the first field).
#[no_mangle]
pub unsafe extern "C" fn Tcl_IncrRefCount(obj_ptr: *mut Tcl_Obj) {
    if !obj_ptr.is_null() {
        // refCount is the first field (c_int) in Tcl_Obj
        let ref_count_ptr = obj_ptr as *mut c_int;
        *ref_count_ptr += 1;
    }
}

/// Tcl_DecrRefCount is a macro in TCL, not a function.
/// It decrements the reference count and frees the object if it reaches 0.
/// Since we can't call TclFreeObj from Rust (it's internal to TCL),
/// we just decrement and trust the TCL runtime to handle cleanup.
#[no_mangle]
pub unsafe extern "C" fn Tcl_DecrRefCount(obj_ptr: *mut Tcl_Obj) {
    if !obj_ptr.is_null() {
        // refCount is the first field (c_int) in Tcl_Obj
        let ref_count_ptr = obj_ptr as *mut c_int;
        *ref_count_ptr -= 1;
        // Note: In real TCL, if refCount reaches 0, TclFreeObj is called.
        // We can't do that here since TclFreeObj is an internal TCL function.
        // The TCL runtime will handle cleanup when it sees refCount == 0.
    }
}
