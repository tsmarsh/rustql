/*
 * vdbe_ref.c — Direct VDBE access wrapper for execution comparison tests
 *
 * This file #includes the SQLite3 amalgamation to get access to internal types
 * (Vdbe, Parse, Mem, VdbeOp) and exposes a flat C API for constructing and
 * executing VDBE programs from Rust test code via FFI.
 *
 * Compiled by build.rs when the sqlite3-ffi feature is enabled.
 */

/* Include the full amalgamation so we have access to all internal types.
 * build.rs sets -I sqlite3/ so "sqlite3.c" resolves to sqlite3/sqlite3.c */
#include "sqlite3.c"

#include <string.h>
#include <stdlib.h>

/* ========================================================================
 * Opaque handle wrapping a directly-constructed Vdbe
 * ======================================================================== */

typedef struct VdbeRef {
    sqlite3 *db;
    Vdbe    *v;
    Parse    parse;
    int      ready;     /* Has sqlite3VdbeMakeReady been called? */
    int      n_col;     /* Number of result columns */
} VdbeRef;

/* ========================================================================
 * Opcode name -> number lookup using sqlite3OpcodeName()
 * ======================================================================== */

static int opcode_from_name(const char *name) {
    int i;
    for (i = 0; i < 256; i++) {
        const char *n = sqlite3OpcodeName(i);
        if (n && strcmp(n, name) == 0) return i;
    }
    return -1;
}

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

VdbeRef* vdbe_ref_new(int n_mem, int n_cursor, int n_col) {
    VdbeRef *ref = (VdbeRef*)sqlite3_malloc(sizeof(VdbeRef));
    if (!ref) return NULL;
    memset(ref, 0, sizeof(VdbeRef));

    /* Open an in-memory database */
    int rc = sqlite3_open(":memory:", &ref->db);
    if (rc != SQLITE_OK) {
        sqlite3_free(ref);
        return NULL;
    }

    /* Set up minimal Parse context */
    memset(&ref->parse, 0, sizeof(Parse));
    ref->parse.db = ref->db;
    ref->parse.nMem = n_mem;
    ref->parse.nTab = n_cursor;
    ref->parse.nVar = 0;
    ref->parse.nMaxArg = 0;

    /* Create the Vdbe */
    ref->v = sqlite3VdbeCreate(&ref->parse);
    if (!ref->v) {
        sqlite3_close(ref->db);
        sqlite3_free(ref);
        return NULL;
    }

    /* Set result column count */
    ref->n_col = n_col;
    if (n_col > 0) {
        sqlite3VdbeSetNumCols(ref->v, n_col);
    }

    ref->ready = 0;
    return ref;
}

void vdbe_ref_destroy(VdbeRef *ref) {
    if (!ref) return;
    if (ref->v) {
        sqlite3_finalize((sqlite3_stmt*)ref->v);
    }
    if (ref->db) {
        sqlite3_close(ref->db);
    }
    sqlite3_free(ref);
}

/* ========================================================================
 * Program construction
 * ======================================================================== */

int vdbe_ref_add_op(VdbeRef *ref, const char *opname, int p1, int p2, int p3) {
    int op = opcode_from_name(opname);
    if (op < 0) return -1;
    return sqlite3VdbeAddOp3(ref->v, op, p1, p2, p3);
}

void vdbe_ref_change_p5(VdbeRef *ref, int p5) {
    if (ref->v && ref->v->nOp > 0) {
        ref->v->aOp[ref->v->nOp - 1].p5 = (u16)p5;
    }
}

void vdbe_ref_change_p4_int(VdbeRef *ref, int addr, int val) {
    sqlite3VdbeChangeP4(ref->v, addr, SQLITE_INT_TO_PTR(val), P4_INT32);
}

void vdbe_ref_change_p4_int64(VdbeRef *ref, int addr, long long val) {
    sqlite3_int64 *pVal = (sqlite3_int64*)sqlite3DbMallocRaw(ref->db, sizeof(sqlite3_int64));
    if (pVal) {
        *pVal = val;
        sqlite3VdbeChangeP4(ref->v, addr, (const char*)pVal, P4_INT64);
    }
}

void vdbe_ref_change_p4_real(VdbeRef *ref, int addr, double val) {
    double *pVal = (double*)sqlite3DbMallocRaw(ref->db, sizeof(double));
    if (pVal) {
        *pVal = val;
        sqlite3VdbeChangeP4(ref->v, addr, (const char*)pVal, P4_REAL);
    }
}

void vdbe_ref_change_p4_str(VdbeRef *ref, int addr, const char *str) {
    /* Pass 0 for n: sqlite3VdbeChangeP4 will strlen and copy */
    sqlite3VdbeChangeP4(ref->v, addr, str, 0);
}

void vdbe_ref_set_col_name(VdbeRef *ref, int idx, const char *name) {
    if (ref->v && idx >= 0 && idx < ref->n_col) {
        sqlite3VdbeSetColName(ref->v, idx, COLNAME_NAME, name, SQLITE_TRANSIENT);
    }
}

/* ========================================================================
 * Execution
 * ======================================================================== */

int vdbe_ref_step(VdbeRef *ref) {
    if (!ref || !ref->v) return SQLITE_ERROR;

    /* Finalize the program on first step */
    if (!ref->ready) {
        sqlite3VdbeMakeReady(ref->v, &ref->parse);
        ref->ready = 1;
    }

    return sqlite3_step((sqlite3_stmt*)ref->v);
}

/* ========================================================================
 * Result access (after SQLITE_ROW)
 * ======================================================================== */

int vdbe_ref_column_count(VdbeRef *ref) {
    if (!ref || !ref->v) return 0;
    return sqlite3_column_count((sqlite3_stmt*)ref->v);
}

int vdbe_ref_column_type(VdbeRef *ref, int col) {
    if (!ref || !ref->v) return SQLITE_NULL;
    return sqlite3_column_type((sqlite3_stmt*)ref->v, col);
}

long long vdbe_ref_column_int64(VdbeRef *ref, int col) {
    if (!ref || !ref->v) return 0;
    return sqlite3_column_int64((sqlite3_stmt*)ref->v, col);
}

double vdbe_ref_column_double(VdbeRef *ref, int col) {
    if (!ref || !ref->v) return 0.0;
    return sqlite3_column_double((sqlite3_stmt*)ref->v, col);
}

const char* vdbe_ref_column_text(VdbeRef *ref, int col) {
    if (!ref || !ref->v) return NULL;
    return (const char*)sqlite3_column_text((sqlite3_stmt*)ref->v, col);
}

const void* vdbe_ref_column_blob(VdbeRef *ref, int col) {
    if (!ref || !ref->v) return NULL;
    return sqlite3_column_blob((sqlite3_stmt*)ref->v, col);
}

int vdbe_ref_column_bytes(VdbeRef *ref, int col) {
    if (!ref || !ref->v) return 0;
    return sqlite3_column_bytes((sqlite3_stmt*)ref->v, col);
}

/* ========================================================================
 * Setup SQL (for table tests — execute DDL/DML on the internal db)
 * ======================================================================== */

int vdbe_ref_exec_sql(VdbeRef *ref, const char *sql) {
    if (!ref || !ref->db) return SQLITE_ERROR;
    return sqlite3_exec(ref->db, sql, NULL, NULL, NULL);
}

/* ========================================================================
 * Error message access
 * ======================================================================== */

const char* vdbe_ref_errmsg(VdbeRef *ref) {
    if (!ref || !ref->db) return "no database";
    return sqlite3_errmsg(ref->db);
}

/* ========================================================================
 * Debug: inspect the stored program
 * ======================================================================== */

/* ========================================================================
 * Adjust jump targets to account for the Init op prepended by VdbeCreate
 * ======================================================================== */

void vdbe_ref_adjust_jumps(VdbeRef *ref, int offset) {
    int i;
    if (!ref || !ref->v) return;
    /* Skip address 0 (the auto-inserted Init) — only adjust our ops */
    for (i = 1; i < ref->v->nOp; i++) {
        VdbeOp *op = &ref->v->aOp[i];
        if (sqlite3OpcodeProperty[op->opcode] & OPFLG_JUMP) {
            if (op->p2 > 0) {
                op->p2 += offset;
            }
        }
    }
}

int vdbe_ref_get_op_count(VdbeRef *ref) {
    if (!ref || !ref->v) return 0;
    return ref->v->nOp;
}

/* Returns opcode number, p1, p2, p3, and the C opcode name */
const char* vdbe_ref_get_op_name(VdbeRef *ref, int addr) {
    if (!ref || !ref->v || addr < 0 || addr >= ref->v->nOp) return "INVALID";
    return sqlite3OpcodeName(ref->v->aOp[addr].opcode);
}

int vdbe_ref_get_op_p1(VdbeRef *ref, int addr) {
    if (!ref || !ref->v || addr < 0 || addr >= ref->v->nOp) return -999;
    return ref->v->aOp[addr].p1;
}

int vdbe_ref_get_op_p2(VdbeRef *ref, int addr) {
    if (!ref || !ref->v || addr < 0 || addr >= ref->v->nOp) return -999;
    return ref->v->aOp[addr].p2;
}

int vdbe_ref_get_op_p3(VdbeRef *ref, int addr) {
    if (!ref || !ref->v || addr < 0 || addr >= ref->v->nOp) return -999;
    return ref->v->aOp[addr].p3;
}
