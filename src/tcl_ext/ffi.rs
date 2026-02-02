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
}
