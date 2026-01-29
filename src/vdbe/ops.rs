//! VDBE Opcodes and Instructions
//!
//! This module defines the virtual machine opcodes that form the intermediate
//! representation for SQL execution. Based on SQLite's vdbe.c opcodes.

use std::fmt;
use std::sync::Arc;

// ============================================================================
// Opcode Definitions
// ============================================================================

/// VDBE opcode (operation code)
///
/// Each opcode performs a specific operation in the virtual machine.
/// The naming follows SQLite conventions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Opcode {
    // ========================================================================
    // Control Flow
    // ========================================================================
    /// Do nothing (placeholder)
    Noop = 0,

    /// Initialize program state
    Init,

    /// End of program
    Halt,

    /// Unconditional jump to P2
    Goto,

    /// Jump to P2 if P1 is true (non-zero)
    If,

    /// Jump to P2 if P1 is false (zero or null)
    IfNot,

    /// Jump to P2 if P1 != P3
    Ne,

    /// Jump to P2 if P1 == P3
    Eq,

    /// Jump to P2 if P1 < P3
    Lt,

    /// Jump to P2 if P1 <= P3
    Le,

    /// Jump to P2 if P1 > P3
    Gt,

    /// Jump to P2 if P1 >= P3
    Ge,

    /// Jump to P2 if P1 is null
    IsNull,

    /// Jump to P2 if P1 is not null
    NotNull,

    /// Jump to P2 if P1 is true/false/null (3-valued logic)
    IfNullRow,

    /// Subroutine call - save return address and jump
    Gosub,

    /// Return from subroutine
    Return,

    /// End subroutine and set flag
    EndCoroutine,

    /// Yield coroutine and jump if done
    Yield,

    /// If r[P1] > 0 then r[P1] -= P3, jump to P2
    IfPos,

    /// Decrement r[P1], jump to P2 if result <= 0
    DecrJumpZero,

    /// Compute remaining LIMIT after OFFSET
    OffsetLimit,

    // ========================================================================
    // Register Operations
    // ========================================================================
    /// Copy P1 to P2
    Copy,

    /// Move P1 to P2 (P1 becomes null)
    Move,

    /// Swap registers P1 and P2
    SCopy,

    /// Store NULL in register P2
    Null,

    /// Set NULL counter
    NullRow,

    /// Store integer P1 in register P2
    Integer,

    /// Store 64-bit integer in register P2
    Int64,

    /// Store real P4 in register P2
    Real,

    /// Store string P4 in register P2
    String8,

    /// Store blob P4 in register P2
    Blob,

    /// Store P4 variable value in register P2
    Variable,

    /// Increment register P1 by P2
    Add,

    /// Add immediate P2 to register P1
    AddImm,

    /// P3 = P2 - P1
    Subtract,

    /// P3 = P2 * P1
    Multiply,

    /// P3 = P2 / P1
    Divide,

    /// P3 = P2 % P1
    Remainder,

    /// P2 = P1 || P3 (string concatenation)
    Concat,

    /// P2 = NOT P1 (boolean not)
    Not,

    /// P2 = ~P1 (bitwise not)
    BitNot,

    /// P3 = P1 & P2 (bitwise and)
    BitAnd,

    /// P3 = P1 | P2 (bitwise or)
    BitOr,

    /// P3 = P2 << P1 (shift left)
    ShiftLeft,

    /// P3 = P2 >> P1 (shift right)
    ShiftRight,

    /// Short-circuit AND
    And,

    /// Short-circuit OR
    Or,

    /// P2 = CAST(P1 AS P4)
    Cast,

    /// Soft cast (affinity)
    Affinity,

    // ========================================================================
    // Comparison Operations
    // ========================================================================
    /// Compare P1 and P3, store result in P2
    Compare,

    /// Jump based on previous Compare result
    Jump,

    /// Boolean result of P1 == P3
    Once,

    /// Check if P1 matches pattern in P3
    Like,

    /// Check if P1 matches glob pattern in P3
    Glob,

    /// Check if P1 matches regexp in P3
    Regexp,

    /// Check if P1 matches pattern in P3 (user-defined match function)
    Match,

    // ========================================================================
    // B-tree/Table Operations
    // ========================================================================
    /// Open cursor P1 on table P2, root page P3
    OpenRead,

    /// Open cursor P1 on table P2 for writing
    OpenWrite,

    /// Apply filter to virtual table cursor P1 (P4 = query)
    VFilter,

    /// Open an ephemeral table
    OpenEphemeral,

    /// Open a pseudo-table that reads from registers
    OpenPseudo,

    /// Open autoindex cursor
    OpenAutoindex,

    /// Close cursor P1
    Close,

    /// Move cursor P1 to first entry
    Rewind,

    /// Move cursor P1 to last entry
    Last,

    /// Move cursor P1 to next entry, jump to P2 if done
    Next,

    /// Move cursor P1 to previous entry, jump to P2 if done
    Prev,

    /// Seek cursor P1 to rowid P3
    SeekRowid,

    /// Seek cursor P1 to key >= P3
    SeekGE,

    /// Seek cursor P1 to key > P3
    SeekGT,

    /// Seek cursor P1 to key <= P3
    SeekLE,

    /// Seek cursor P1 to key < P3
    SeekLT,

    /// Jump if cursor P1 not pointing to valid row
    NotExists,

    /// Check if record P3 exists in index P1, jump to P2 if found
    Found,

    /// Check if record P3 does not exist in index P1, jump to P2 if not found
    NotFound,

    /// Delete row at cursor P1
    Delete,

    /// Reset a sorter cursor
    ResetSorter,

    // ========================================================================
    // Column/Row Access
    // ========================================================================
    /// Read column P2 from cursor P1 into register P3
    Column,

    /// Get rowid from cursor P1 into register P2
    Rowid,

    /// Make a record from P1..P1+P2-1 registers, store in P3
    MakeRecord,

    /// Decode record in P1, store columns starting at P2, P3 columns total
    DecodeRecord,

    /// Output result row from P1..P1+P2-1 registers
    ResultRow,

    /// Insert record into cursor P1
    Insert,

    /// Insert record with int key into cursor P1
    InsertInt,

    /// Start a new row for insert
    NewRowid,

    // ========================================================================
    // Index Operations
    // ========================================================================
    /// Seek index cursor P1 using key in P3
    IdxGE,

    /// Seek index cursor P1 using key in P3
    IdxGT,

    /// Seek index cursor P1 using key in P3
    IdxLE,

    /// Seek index cursor P1 using key in P3
    IdxLT,

    /// Read rowid from index cursor into register
    IdxRowid,

    /// Insert into index cursor
    IdxInsert,

    /// Delete from index cursor
    IdxDelete,

    // ========================================================================
    // Aggregation Operations
    // ========================================================================
    /// Begin aggregate function
    AggStep,

    /// Get aggregate final value
    AggFinal,

    /// Accumulator operations
    AggStep0,
    AggValue,

    // ========================================================================
    // Sorting Operations
    // ========================================================================
    /// Insert into sorter
    SorterInsert,

    /// Sort accumulated rows
    SorterSort,

    /// Get next from sorter
    SorterNext,

    /// Extract data from sorter
    SorterData,

    /// Compare sorter keys
    SorterCompare,

    /// Configure sorter sort order - P1=cursor, P4=blob of sort directions (0=ASC, 1=DESC)
    SorterConfig,

    // ========================================================================
    // Schema Operations
    // ========================================================================
    /// Create a new btree table, store root page in P2
    CreateBtree,

    /// Parse a CREATE statement and add to schema
    ParseSchema,

    /// Parse a CREATE INDEX statement and add to schema
    ParseSchemaIndex,

    /// Remove table/index from schema (DROP TABLE/INDEX)
    DropSchema,

    // ========================================================================
    // Transaction Operations
    // ========================================================================
    /// Begin transaction
    Transaction,

    /// Commit transaction
    AutoCommit,

    /// Start a savepoint
    Savepoint,

    /// Read lock a table
    ReadCookie,

    /// Write lock a table
    SetCookie,

    // ========================================================================
    // Function Operations
    // ========================================================================
    /// Call scalar function
    Function,

    // ========================================================================
    // Miscellaneous
    // ========================================================================
    /// Trace message (debug)
    Trace,

    /// Execute nested SQL
    SqlExec,

    /// Check for interrupt
    Checkpoint,

    /// Set up deferred table seek from index cursor
    DeferredSeek,

    /// Complete deferred table seek
    FinishSeek,

    /// Sequence value for autoincrement
    Sequence,

    /// Count rows
    Count,

    // ========================================================================
    // Foreign Key Operations
    // ========================================================================
    /// Increment/decrement deferred FK violation counter
    /// P1 = amount to add (positive or negative)
    /// P2 = database index
    FkCounter,

    /// Jump to P2 if deferred FK counter is zero (no violations)
    /// P1 = database index
    FkIfZero,

    /// Check immediate FK constraints for a row
    /// P1 = cursor for table being modified
    /// P2 = register containing rowid
    /// P3 = 0=INSERT, 1=DELETE, 2=UPDATE
    /// P4 = table name
    FkCheck,

    // ========================================================================
    // Trigger Operations
    // ========================================================================
    /// Execute a trigger subprogram
    /// P1 = subprogram context register
    /// P2 = return address
    /// P3 = trigger mask/flags
    /// P4 = SubProgram containing trigger bytecode
    Program,

    /// Access parameter from parent VDBE (for trigger body)
    /// P1 = which parameter (0 = OLD row, 1 = NEW row)
    /// P2 = column index (-1 for rowid)
    /// P3 = destination register
    Param,

    /// Set OLD/NEW row values for trigger execution
    /// P1 = 0 for OLD row, 1 for NEW row
    /// P2 = base register containing row values
    /// P3 = number of columns
    SetTriggerRow,

    // ========================================================================
    // RowSet Operations (for IN clause optimization)
    // ========================================================================
    /// Add integer P2 to the RowSet in register P1
    RowSetAdd,
    /// Extract smallest value from RowSet P1 into P3, jump to P2 if empty
    RowSetRead,
    /// If P3 is in RowSet P1, jump to P2; if P4>=0, also insert P3
    RowSetTest,

    // ========================================================================
    // Debug/Assertion Opcodes (SQLite parity)
    // ========================================================================
    /// Debug assertion that VDBE can be aborted (no-op in release)
    Abortable,
    /// Release registers from service (debug only)
    ReleaseReg,

    // ========================================================================
    // Additional Control Flow (SQLite parity)
    // ========================================================================
    /// Initialize coroutine - store return addr in P1, jump to P2 if P3 != 0
    InitCoroutine,
    /// Begin subroutine (same as Null - clears register range)
    BeginSubrtn,
    /// Halt if register P3 is null, fall through to Halt
    HaltIfNull,
    /// Jump to P2 if cursor P1 is empty
    IfEmpty,
    /// Decrement P1, jump to P2 if result is not zero
    IfNotZero,
    /// Jump to P2 if cursor P1 is not open
    IfNotOpen,
    /// Jump if no hope of finding key (optimization hint)
    IfNoHope,
    /// Jump to P2 if size of cursor P1 is between P3 and P4
    IfSizeBetween,

    // ========================================================================
    // Register Operations (SQLite parity)
    // ========================================================================
    /// Copy integer from P1 to P2
    IntCopy,
    /// Set register P1 to "soft null" (can be overwritten)
    SoftNull,
    /// Set register to zero or null based on P1
    ZeroOrNull,
    /// P2 = max(P2, P1) as integer
    MemMax,

    // ========================================================================
    // Type Operations (SQLite parity)
    // ========================================================================
    /// Convert register P1 to integer, jump to P2 if not possible
    MustBeInt,
    /// Apply REAL affinity to register P1
    RealAffinity,
    /// Check if value P1 matches type P3, jump to P2 if not
    IsType,
    /// Check if P1 is true (non-null and non-zero), store result in P2
    IsTrue,
    /// Check types in record at P3, raise error if mismatch
    TypeCheck,

    // ========================================================================
    // Subtype Operations (SQLite parity)
    // ========================================================================
    /// Get subtype from register P1 into P2
    GetSubtype,
    /// Set subtype of register P1 to P2
    SetSubtype,
    /// Clear subtype flag from register P1
    ClrSubtype,

    // ========================================================================
    // Collation Operations (SQLite parity)
    // ========================================================================
    /// Set collation sequence for comparisons (P4 = collation name)
    CollSeq,

    // ========================================================================
    // Comparison Operations (SQLite parity)
    // ========================================================================
    /// Used after Lt/Gt when previous was Eq - compare equal case
    ElseEq,
    /// Set permutation array for Compare opcode (P4 = permutation)
    Permutation,

    // ========================================================================
    // Cursor Operations (SQLite parity)
    // ========================================================================
    /// Clear all rows from table/index at root page P1
    Clear,
    /// Open duplicate cursor P1 on cursor P2's btree
    OpenDup,
    /// Reopen index cursor P1 on root page P2
    ReopenIdx,
    /// Seek cursor P1 to end of btree
    SeekEnd,
    /// Mark cursor P1 as having had a successful seek
    SeekHit,
    /// Seek scan optimization - P1=cursor, P2=jump if seek fails
    SeekScan,
    /// Get byte offset of cursor P1 into register P2
    Offset,
    /// Cursor hint for optimization (P4 = hint expression)
    CursorHint,
    /// Lock cursor P1 to prevent modification
    CursorLock,
    /// Unlock cursor P1
    CursorUnlock,
    /// Mark columns as used in cursor P1 (P4 = bitmask)
    ColumnsUsed,
    /// Like NotFound but fails on constraint violation
    NoConflict,

    // ========================================================================
    // Row Data Operations (SQLite parity)
    // ========================================================================
    /// Copy entire row data from cursor P1 into register P2
    RowData,
    /// Extract a single cell from record (P1=cursor, P2=cell, P3=dest)
    RowCell,

    // ========================================================================
    // Aggregation Operations (SQLite parity)
    // ========================================================================
    /// Aggregate step with single argument (P4 = function)
    AggStep1,
    /// Aggregate inverse for window functions
    AggInverse,

    // ========================================================================
    // Sorting Operations (SQLite parity)
    // ========================================================================
    /// Open a sorter cursor (same as SorterSort start)
    SorterOpen,
    /// Sort (alias for SorterSort)
    Sort,
    /// Test sequence value at P1, jump to P2 if not matching P3
    SequenceTest,

    // ========================================================================
    // Filter Operations (Bloom filter - SQLite parity)
    // ========================================================================
    /// Check if key in P3 might be in bloom filter P1, jump to P2 if not
    Filter,
    /// Add key in P3 to bloom filter in cursor P1
    FilterAdd,

    // ========================================================================
    // Schema Operations (SQLite parity)
    // ========================================================================
    /// Destroy btree at root page P1, store freed pages in P2
    Destroy,
    /// Drop table from schema (P4 = table name)
    DropTable,
    /// Drop index from schema (P4 = index name)
    DropIndex,
    /// Drop trigger from schema (P4 = trigger name)
    DropTrigger,
    /// Lock table P1 in database P2 (P3 = lock type)
    TableLock,
    /// Load analysis data from sqlite_stat tables
    LoadAnalysis,

    // ========================================================================
    // Database Operations (SQLite parity)
    // ========================================================================
    /// Incremental vacuum
    IncrVacuum,
    /// Full vacuum
    Vacuum,
    /// Get/set journal mode (P3 = new mode, result in P2)
    JournalMode,
    /// Get page count into register P2
    Pagecount,
    /// Get/set max page count (P2 = register for result, P3 = new max)
    MaxPgcnt,
    /// Run integrity check (P1 = root page, P2 = result register)
    IntegrityCk,
    /// Mark statement as expired (prepared statement cache)
    Expire,
    /// Reset change counter
    ResetCount,

    // ========================================================================
    // Function Operations (SQLite parity)
    // ========================================================================
    /// Call pure/deterministic function (can be factored out of loops)
    PureFunc,
    /// String opcode with explicit length P1 (P4 = string, P2 = dest)
    String,

    // ========================================================================
    // Virtual Table Operations (SQLite parity)
    // ========================================================================
    /// Begin virtual table transaction
    VBegin,
    /// Check virtual table constraint
    VCheck,
    /// Read column P2 from virtual table cursor P1 into register P3
    VColumn,
    /// Create virtual table (P4 = module args)
    VCreate,
    /// Destroy virtual table
    VDestroy,
    /// Initialize IN constraint for virtual table
    VInitIn,
    /// Advance to next row in virtual table cursor
    VNext,
    /// Open virtual table cursor
    VOpen,
    /// Rename virtual table
    VRename,
    /// Update virtual table (insert/delete/update)
    VUpdate,

    /// Maximum opcode value
    MaxOpcode,
}

impl Opcode {
    /// Check if this opcode is a jump instruction
    pub fn is_jump(&self) -> bool {
        matches!(
            self,
            Opcode::Goto
                | Opcode::Jump
                | Opcode::If
                | Opcode::IfNot
                | Opcode::Ne
                | Opcode::Eq
                | Opcode::Lt
                | Opcode::Le
                | Opcode::Gt
                | Opcode::Ge
                | Opcode::IsNull
                | Opcode::NotNull
                | Opcode::Gosub
                | Opcode::Return
                | Opcode::Yield
                | Opcode::Next
                | Opcode::Prev
                | Opcode::Rewind
                | Opcode::SeekGE
                | Opcode::SeekGT
                | Opcode::SeekLE
                | Opcode::SeekLT
                | Opcode::SeekRowid
                | Opcode::NotExists
                | Opcode::Found
                | Opcode::NotFound
                | Opcode::IdxGE
                | Opcode::IdxGT
                | Opcode::IdxLE
                | Opcode::IdxLT
                | Opcode::SorterNext
                | Opcode::SorterSort
                | Opcode::FkIfZero
                | Opcode::Program
                | Opcode::IfPos
                | Opcode::InitCoroutine
                | Opcode::HaltIfNull
                | Opcode::IfEmpty
                | Opcode::IfNotZero
                | Opcode::IfNotOpen
                | Opcode::IfNoHope
                | Opcode::IfSizeBetween
                | Opcode::MustBeInt
                | Opcode::IsType
                | Opcode::ElseEq
                | Opcode::SeekEnd
                | Opcode::SeekScan
                | Opcode::NoConflict
                | Opcode::Filter
                | Opcode::SequenceTest
                | Opcode::IncrVacuum
                | Opcode::Sort
                | Opcode::VNext
        )
    }

    /// Check if this opcode uses a cursor reference in P1
    pub fn uses_cursor(&self) -> bool {
        matches!(
            self,
            Opcode::OpenRead
                | Opcode::OpenWrite
                | Opcode::OpenEphemeral
                | Opcode::VFilter
                | Opcode::Close
                | Opcode::Rewind
                | Opcode::Last
                | Opcode::Next
                | Opcode::Prev
                | Opcode::SeekRowid
                | Opcode::SeekGE
                | Opcode::SeekGT
                | Opcode::SeekLE
                | Opcode::SeekLT
                | Opcode::NotExists
                | Opcode::Found
                | Opcode::NotFound
                | Opcode::Delete
                | Opcode::Column
                | Opcode::Rowid
                | Opcode::Insert
                | Opcode::InsertInt
                | Opcode::NewRowid
                | Opcode::IdxGE
                | Opcode::IdxGT
                | Opcode::IdxLE
                | Opcode::IdxLT
                | Opcode::IdxRowid
                | Opcode::IdxInsert
                | Opcode::IdxDelete
                | Opcode::FkCheck
                | Opcode::SorterSort
                | Opcode::SorterNext
                | Opcode::SorterData
                | Opcode::SorterInsert
                | Opcode::SorterConfig
                | Opcode::NullRow
                | Opcode::Clear
                | Opcode::OpenDup
                | Opcode::ReopenIdx
                | Opcode::SeekEnd
                | Opcode::SeekHit
                | Opcode::SeekScan
                | Opcode::Offset
                | Opcode::CursorHint
                | Opcode::CursorLock
                | Opcode::CursorUnlock
                | Opcode::ColumnsUsed
                | Opcode::NoConflict
                | Opcode::RowData
                | Opcode::RowCell
                | Opcode::SorterOpen
                | Opcode::Sort
                | Opcode::Filter
                | Opcode::FilterAdd
                | Opcode::IfEmpty
                | Opcode::IfNotOpen
                | Opcode::IfNoHope
                | Opcode::IfSizeBetween
                | Opcode::Destroy
                | Opcode::VColumn
                | Opcode::VNext
                | Opcode::VOpen
        )
    }

    /// Get opcode name as string
    pub fn name(&self) -> &'static str {
        match self {
            Opcode::Noop => "Noop",
            Opcode::Init => "Init",
            Opcode::Halt => "Halt",
            Opcode::Goto => "Goto",
            Opcode::If => "If",
            Opcode::IfNot => "IfNot",
            Opcode::Ne => "Ne",
            Opcode::Eq => "Eq",
            Opcode::Lt => "Lt",
            Opcode::Le => "Le",
            Opcode::Gt => "Gt",
            Opcode::Ge => "Ge",
            Opcode::IsNull => "IsNull",
            Opcode::NotNull => "NotNull",
            Opcode::IfNullRow => "IfNullRow",
            Opcode::Gosub => "Gosub",
            Opcode::Return => "Return",
            Opcode::EndCoroutine => "EndCoroutine",
            Opcode::Yield => "Yield",
            Opcode::IfPos => "IfPos",
            Opcode::DecrJumpZero => "DecrJumpZero",
            Opcode::OffsetLimit => "OffsetLimit",
            Opcode::Copy => "Copy",
            Opcode::Move => "Move",
            Opcode::SCopy => "SCopy",
            Opcode::Null => "Null",
            Opcode::NullRow => "NullRow",
            Opcode::Integer => "Integer",
            Opcode::Int64 => "Int64",
            Opcode::Real => "Real",
            Opcode::String8 => "String8",
            Opcode::Blob => "Blob",
            Opcode::Variable => "Variable",
            Opcode::Add => "Add",
            Opcode::AddImm => "AddImm",
            Opcode::Subtract => "Subtract",
            Opcode::Multiply => "Multiply",
            Opcode::Divide => "Divide",
            Opcode::Remainder => "Remainder",
            Opcode::Concat => "Concat",
            Opcode::Not => "Not",
            Opcode::BitNot => "BitNot",
            Opcode::BitAnd => "BitAnd",
            Opcode::BitOr => "BitOr",
            Opcode::ShiftLeft => "ShiftLeft",
            Opcode::ShiftRight => "ShiftRight",
            Opcode::And => "And",
            Opcode::Or => "Or",
            Opcode::Cast => "Cast",
            Opcode::Affinity => "Affinity",
            Opcode::Compare => "Compare",
            Opcode::Jump => "Jump",
            Opcode::Once => "Once",
            Opcode::Like => "Like",
            Opcode::Glob => "Glob",
            Opcode::Regexp => "Regexp",
            Opcode::Match => "Match",
            Opcode::OpenRead => "OpenRead",
            Opcode::OpenWrite => "OpenWrite",
            Opcode::OpenEphemeral => "OpenEphemeral",
            Opcode::OpenPseudo => "OpenPseudo",
            Opcode::OpenAutoindex => "OpenAutoindex",
            Opcode::Close => "Close",
            Opcode::Rewind => "Rewind",
            Opcode::Last => "Last",
            Opcode::Next => "Next",
            Opcode::Prev => "Prev",
            Opcode::SeekRowid => "SeekRowid",
            Opcode::SeekGE => "SeekGE",
            Opcode::SeekGT => "SeekGT",
            Opcode::SeekLE => "SeekLE",
            Opcode::SeekLT => "SeekLT",
            Opcode::NotExists => "NotExists",
            Opcode::Found => "Found",
            Opcode::NotFound => "NotFound",
            Opcode::VFilter => "VFilter",
            Opcode::Delete => "Delete",
            Opcode::ResetSorter => "ResetSorter",
            Opcode::Column => "Column",
            Opcode::Rowid => "Rowid",
            Opcode::MakeRecord => "MakeRecord",
            Opcode::DecodeRecord => "DecodeRecord",
            Opcode::ResultRow => "ResultRow",
            Opcode::Insert => "Insert",
            Opcode::InsertInt => "InsertInt",
            Opcode::NewRowid => "NewRowid",
            Opcode::IdxGE => "IdxGE",
            Opcode::IdxGT => "IdxGT",
            Opcode::IdxLE => "IdxLE",
            Opcode::IdxLT => "IdxLT",
            Opcode::IdxRowid => "IdxRowid",
            Opcode::IdxInsert => "IdxInsert",
            Opcode::IdxDelete => "IdxDelete",
            Opcode::AggStep => "AggStep",
            Opcode::AggFinal => "AggFinal",
            Opcode::AggStep0 => "AggStep0",
            Opcode::AggValue => "AggValue",
            Opcode::SorterInsert => "SorterInsert",
            Opcode::SorterSort => "SorterSort",
            Opcode::SorterNext => "SorterNext",
            Opcode::SorterData => "SorterData",
            Opcode::SorterCompare => "SorterCompare",
            Opcode::SorterConfig => "SorterConfig",
            Opcode::CreateBtree => "CreateBtree",
            Opcode::ParseSchema => "ParseSchema",
            Opcode::ParseSchemaIndex => "ParseSchemaIndex",
            Opcode::DropSchema => "DropSchema",
            Opcode::Transaction => "Transaction",
            Opcode::AutoCommit => "AutoCommit",
            Opcode::Savepoint => "Savepoint",
            Opcode::ReadCookie => "ReadCookie",
            Opcode::SetCookie => "SetCookie",
            Opcode::Function => "Function",
            Opcode::Trace => "Trace",
            Opcode::SqlExec => "SqlExec",
            Opcode::Checkpoint => "Checkpoint",
            Opcode::DeferredSeek => "DeferredSeek",
            Opcode::FinishSeek => "FinishSeek",
            Opcode::Sequence => "Sequence",
            Opcode::Count => "Count",
            Opcode::FkCounter => "FkCounter",
            Opcode::FkIfZero => "FkIfZero",
            Opcode::FkCheck => "FkCheck",
            Opcode::Program => "Program",
            Opcode::Param => "Param",
            Opcode::SetTriggerRow => "SetTriggerRow",
            Opcode::RowSetAdd => "RowSetAdd",
            Opcode::RowSetRead => "RowSetRead",
            Opcode::RowSetTest => "RowSetTest",
            // Debug/Assertion Opcodes
            Opcode::Abortable => "Abortable",
            Opcode::ReleaseReg => "ReleaseReg",
            // Additional Control Flow
            Opcode::InitCoroutine => "InitCoroutine",
            Opcode::BeginSubrtn => "BeginSubrtn",
            Opcode::HaltIfNull => "HaltIfNull",
            Opcode::IfEmpty => "IfEmpty",
            Opcode::IfNotZero => "IfNotZero",
            Opcode::IfNotOpen => "IfNotOpen",
            Opcode::IfNoHope => "IfNoHope",
            Opcode::IfSizeBetween => "IfSizeBetween",
            // Register Operations
            Opcode::IntCopy => "IntCopy",
            Opcode::SoftNull => "SoftNull",
            Opcode::ZeroOrNull => "ZeroOrNull",
            Opcode::MemMax => "MemMax",
            // Type Operations
            Opcode::MustBeInt => "MustBeInt",
            Opcode::RealAffinity => "RealAffinity",
            Opcode::IsType => "IsType",
            Opcode::IsTrue => "IsTrue",
            Opcode::TypeCheck => "TypeCheck",
            // Subtype Operations
            Opcode::GetSubtype => "GetSubtype",
            Opcode::SetSubtype => "SetSubtype",
            Opcode::ClrSubtype => "ClrSubtype",
            // Collation Operations
            Opcode::CollSeq => "CollSeq",
            // Comparison Operations
            Opcode::ElseEq => "ElseEq",
            Opcode::Permutation => "Permutation",
            // Cursor Operations
            Opcode::Clear => "Clear",
            Opcode::OpenDup => "OpenDup",
            Opcode::ReopenIdx => "ReopenIdx",
            Opcode::SeekEnd => "SeekEnd",
            Opcode::SeekHit => "SeekHit",
            Opcode::SeekScan => "SeekScan",
            Opcode::Offset => "Offset",
            Opcode::CursorHint => "CursorHint",
            Opcode::CursorLock => "CursorLock",
            Opcode::CursorUnlock => "CursorUnlock",
            Opcode::ColumnsUsed => "ColumnsUsed",
            Opcode::NoConflict => "NoConflict",
            // Row Data Operations
            Opcode::RowData => "RowData",
            Opcode::RowCell => "RowCell",
            // Aggregation Operations
            Opcode::AggStep1 => "AggStep1",
            Opcode::AggInverse => "AggInverse",
            // Sorting Operations
            Opcode::SorterOpen => "SorterOpen",
            Opcode::Sort => "Sort",
            Opcode::SequenceTest => "SequenceTest",
            // Filter Operations
            Opcode::Filter => "Filter",
            Opcode::FilterAdd => "FilterAdd",
            // Schema Operations
            Opcode::Destroy => "Destroy",
            Opcode::DropTable => "DropTable",
            Opcode::DropIndex => "DropIndex",
            Opcode::DropTrigger => "DropTrigger",
            Opcode::TableLock => "TableLock",
            Opcode::LoadAnalysis => "LoadAnalysis",
            // Database Operations
            Opcode::IncrVacuum => "IncrVacuum",
            Opcode::Vacuum => "Vacuum",
            Opcode::JournalMode => "JournalMode",
            Opcode::Pagecount => "Pagecount",
            Opcode::MaxPgcnt => "MaxPgcnt",
            Opcode::IntegrityCk => "IntegrityCk",
            Opcode::Expire => "Expire",
            Opcode::ResetCount => "ResetCount",
            // Function Operations
            Opcode::PureFunc => "PureFunc",
            Opcode::String => "String",
            // Virtual Table Operations
            Opcode::VBegin => "VBegin",
            Opcode::VCheck => "VCheck",
            Opcode::VColumn => "VColumn",
            Opcode::VCreate => "VCreate",
            Opcode::VDestroy => "VDestroy",
            Opcode::VInitIn => "VInitIn",
            Opcode::VNext => "VNext",
            Opcode::VOpen => "VOpen",
            Opcode::VRename => "VRename",
            Opcode::VUpdate => "VUpdate",
            Opcode::MaxOpcode => "MaxOpcode",
        }
    }
}

impl fmt::Display for Opcode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

// ============================================================================
// P4 Union Type
// ============================================================================

/// P4 operand - can hold various types of data
#[derive(Debug, Clone, PartialEq, Default)]
pub enum P4 {
    /// Not used
    #[default]
    Unused,
    /// 64-bit integer
    Int64(i64),
    /// Real number
    Real(f64),
    /// Text string
    Text(String),
    /// Binary blob
    Blob(Vec<u8>),
    /// Collation sequence name
    Collation(String),
    /// Function definition reference
    FuncDef(String),
    /// Key info for comparison
    KeyInfo(Arc<KeyInfo>),
    /// Memory cell reference
    Mem(i32),
    /// Virtual table reference
    Vtab(String),
    /// Subprogram
    Subprogram(Arc<SubProgram>),
    /// Table structure reference
    Table(String),
    /// Integer array (for IN lists)
    IntArray(Vec<i64>),
}

impl P4 {
    /// Check if P4 is unused
    pub fn is_unused(&self) -> bool {
        matches!(self, P4::Unused)
    }
}

// ============================================================================
// Key Info
// ============================================================================

/// Key comparison information
#[derive(Debug, Clone, PartialEq)]
pub struct KeyInfo {
    /// Collation sequences for each key column
    pub collations: Vec<String>,
    /// Sort orders for each key column (true = descending)
    pub sort_orders: Vec<bool>,
    /// Number of key columns
    pub n_key_field: u16,
}

impl KeyInfo {
    pub fn new(n_fields: usize) -> Self {
        Self {
            collations: vec!["BINARY".to_string(); n_fields],
            sort_orders: vec![false; n_fields],
            n_key_field: n_fields as u16,
        }
    }
}

// ============================================================================
// Subprogram
// ============================================================================

/// Subprogram for triggers/nested execution
#[derive(Debug, Clone, PartialEq)]
pub struct SubProgram {
    /// Instructions in the subprogram
    pub ops: Vec<VdbeOp>,
    /// Memory cells used
    pub n_mem: i32,
    /// Cursors used
    pub n_cursor: i32,
    /// Associated trigger name (if any)
    pub trigger: Option<String>,
}

// ============================================================================
// VDBE Instruction
// ============================================================================

/// A single VDBE instruction
#[derive(Debug, Clone, PartialEq)]
pub struct VdbeOp {
    /// Operation code
    pub opcode: Opcode,
    /// First operand (usually register or cursor)
    pub p1: i32,
    /// Second operand (usually jump target or register)
    pub p2: i32,
    /// Third operand
    pub p3: i32,
    /// Fourth operand (type varies by opcode)
    pub p4: P4,
    /// Fifth operand (flags/extra info)
    pub p5: u16,
    /// Comment for debugging/explain
    pub comment: Option<String>,
}

impl VdbeOp {
    /// Create a new instruction with minimal operands
    pub fn new(opcode: Opcode, p1: i32, p2: i32, p3: i32) -> Self {
        Self {
            opcode,
            p1,
            p2,
            p3,
            p4: P4::Unused,
            p5: 0,
            comment: None,
        }
    }

    /// Create instruction with P4
    pub fn with_p4(opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) -> Self {
        Self {
            opcode,
            p1,
            p2,
            p3,
            p4,
            p5: 0,
            comment: None,
        }
    }

    /// Set comment for debugging
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Set P5 flags
    pub fn with_p5(mut self, p5: u16) -> Self {
        self.p5 = p5;
        self
    }
}

impl fmt::Display for VdbeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:<12} {:>4} {:>4} {:>4}",
            self.opcode.name(),
            self.p1,
            self.p2,
            self.p3
        )?;

        match &self.p4 {
            P4::Unused => {}
            P4::Int64(i) => write!(f, "  {}", i)?,
            P4::Real(r) => write!(f, "  {}", r)?,
            P4::Text(s) => write!(f, "  \"{}\"", s)?,
            P4::Blob(b) => write!(f, "  x'{}'", hex::encode(b))?,
            P4::Collation(c) => write!(f, "  collseq({})", c)?,
            P4::FuncDef(n) => write!(f, "  func({})", n)?,
            P4::KeyInfo(k) => write!(f, "  k({} cols)", k.n_key_field)?,
            P4::Mem(m) => write!(f, "  r[{}]", m)?,
            P4::Vtab(v) => write!(f, "  vtab({})", v)?,
            P4::Subprogram(s) => write!(f, "  program({} ops)", s.ops.len())?,
            P4::Table(t) => write!(f, "  table({})", t)?,
            P4::IntArray(a) => write!(f, "  [{} ints]", a.len())?,
        }

        if let Some(ref comment) = self.comment {
            write!(f, "  ; {}", comment)?;
        }

        Ok(())
    }
}

// Helper for hex encoding blobs in Display
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

// ============================================================================
// Comparison Flags (P5)
// ============================================================================

/// Comparison flags for P5 operand
pub mod cmp_flags {
    /// NULL values compare equal
    pub const NULLEQ: u16 = 0x80;
    /// Jump if either operand is NULL
    pub const JUMPIFNULL: u16 = 0x10;
    /// Comparison result stored in register
    pub const STOREP2: u16 = 0x20;
    /// Affinity mask
    pub const AFFINITY_MASK: u16 = 0x0F;
}

/// Affinity values for P5
pub mod affinity {
    pub const BLOB: u16 = 0x00;
    pub const TEXT: u16 = 0x01;
    pub const NUMERIC: u16 = 0x02;
    pub const INTEGER: u16 = 0x03;
    pub const REAL: u16 = 0x04;
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opcode_is_jump() {
        assert!(Opcode::Goto.is_jump());
        assert!(Opcode::If.is_jump());
        assert!(Opcode::Next.is_jump());
        assert!(!Opcode::Null.is_jump());
        assert!(!Opcode::Integer.is_jump());
    }

    #[test]
    fn test_opcode_uses_cursor() {
        assert!(Opcode::OpenRead.uses_cursor());
        assert!(Opcode::Column.uses_cursor());
        assert!(Opcode::Next.uses_cursor());
        assert!(!Opcode::Integer.uses_cursor());
        assert!(!Opcode::Add.uses_cursor());
    }

    #[test]
    fn test_opcode_name() {
        assert_eq!(Opcode::Goto.name(), "Goto");
        assert_eq!(Opcode::Integer.name(), "Integer");
        assert_eq!(Opcode::SeekGE.name(), "SeekGE");
    }

    #[test]
    fn test_vdbe_op_new() {
        let op = VdbeOp::new(Opcode::Integer, 42, 1, 0);
        assert_eq!(op.opcode, Opcode::Integer);
        assert_eq!(op.p1, 42);
        assert_eq!(op.p2, 1);
        assert_eq!(op.p3, 0);
        assert!(op.p4.is_unused());
    }

    #[test]
    fn test_vdbe_op_with_p4() {
        let op = VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".to_string()));
        assert_eq!(op.opcode, Opcode::String8);
        assert_eq!(op.p4, P4::Text("hello".to_string()));
    }

    #[test]
    fn test_vdbe_op_display() {
        let op = VdbeOp::new(Opcode::Integer, 42, 1, 0);
        let s = format!("{}", op);
        assert!(s.contains("Integer"));
        assert!(s.contains("42"));

        let op = VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("test".to_string()))
            .with_comment("load string");
        let s = format!("{}", op);
        assert!(s.contains("\"test\""));
        assert!(s.contains("; load string"));
    }

    #[test]
    fn test_key_info() {
        let ki = KeyInfo::new(3);
        assert_eq!(ki.n_key_field, 3);
        assert_eq!(ki.collations.len(), 3);
        assert_eq!(ki.sort_orders.len(), 3);
        assert!(!ki.sort_orders[0]);
    }

    #[test]
    fn test_p4_variants() {
        assert!(P4::Unused.is_unused());
        assert!(!P4::Int64(42).is_unused());
        assert!(!P4::Text("test".to_string()).is_unused());
    }
}
