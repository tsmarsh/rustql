# Translate vdbe.c - VDBE Core

## Overview
Translate the Virtual Database Engine (VDBE) core execution loop. This is the bytecode interpreter that runs all SQL statements after they've been compiled into opcodes.

## Source Reference
- `sqlite3/src/vdbe.c` - 9,321 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Vdbe
Main virtual machine structure:
```rust
pub struct Vdbe {
    // Connection and state
    db: Arc<Connection>,           // Database connection
    pc: i32,                        // Program counter
    rc: ErrorCode,                  // Most recent result code

    // Program
    ops: Vec<VdbeOp>,              // Array of opcodes
    n_op: i32,                      // Number of instructions

    // Memory cells
    mem: Vec<Mem>,                  // Memory cells (registers)
    n_mem: i32,                     // Number of memory cells

    // Cursors
    cursors: Vec<Option<VdbeCursor>>, // Open cursors
    n_cursor: i32,                    // Number of cursor slots

    // Stack frames for subroutines
    frames: Vec<VdbeFrame>,

    // Execution state
    magic: u32,                     // Magic number for validation
    explain_mode: ExplainMode,      // EXPLAIN mode
    is_done: bool,                  // Execution complete
    has_result: bool,               // Has result row

    // Change tracking
    n_change: i64,                  // Rows modified by this statement
    start_time: Option<Instant>,    // For timeout checking

    // Bound parameters
    vars: Vec<Mem>,                 // Bound parameter values
    var_names: Vec<Option<String>>, // Parameter names (?NNN, :name, @name)
}
```

### VdbeOp
Single VDBE instruction:
```rust
#[derive(Debug, Clone)]
pub struct VdbeOp {
    pub opcode: Opcode,    // Operation code
    pub p1: i32,           // First operand
    pub p2: i32,           // Second operand (often jump target)
    pub p3: i32,           // Third operand
    pub p4: P4,            // Fourth operand (type varies)
    pub p5: u16,           // Fifth operand (flags)
    #[cfg(debug_assertions)]
    pub comment: Option<String>, // Debug comment
}
```

### P4 Union
Fourth operand can be various types:
```rust
#[derive(Debug, Clone)]
pub enum P4 {
    None,
    Int(i32),
    Int64(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    KeyInfo(Arc<KeyInfo>),     // For index operations
    CollSeq(Arc<CollSeq>),     // Collation sequence
    FuncDef(Arc<FuncDef>),     // Function definition
    Mem(Box<Mem>),             // Precomputed value
    Subprogram(Arc<SubProgram>), // For triggers
    Table(Arc<Table>),         // Table schema
    Vtab(Arc<dyn VTab>),       // Virtual table
}
```

## Key Opcodes

SQLite has ~180 opcodes. Key categories:

### Control Flow
```rust
pub enum Opcode {
    Init,       // Initialize program, jump to p2
    Goto,       // Unconditional jump to p2
    If,         // Jump to p2 if r[p1] is true
    IfNot,      // Jump to p2 if r[p1] is false
    Halt,       // Terminate with result code p1
    Return,     // Return from subroutine
    Gosub,      // Call subroutine at p2
    Yield,      // Coroutine yield

    // ... many more
}
```

### Data Movement
```rust
    Integer,    // Store p1 in r[p2]
    Int64,      // Store p4 (i64) in r[p2]
    Real,       // Store p4 (f64) in r[p2]
    String8,    // Store p4 (text) in r[p2]
    Blob,       // Store p4 (blob) in r[p2]
    Null,       // Store NULL in r[p2]
    Copy,       // Copy r[p1] to r[p2]
    SCopy,      // Shallow copy r[p1] to r[p2]
    Move,       // Move r[p1] to r[p2]
```

### Comparison
```rust
    Eq,         // Jump to p2 if r[p1] == r[p3]
    Ne,         // Jump to p2 if r[p1] != r[p3]
    Lt,         // Jump to p2 if r[p3] < r[p1]
    Le,         // Jump to p2 if r[p3] <= r[p1]
    Gt,         // Jump to p2 if r[p3] > r[p1]
    Ge,         // Jump to p2 if r[p3] >= r[p1]
```

### Cursor Operations
```rust
    OpenRead,   // Open cursor p1 on table p2 for reading
    OpenWrite,  // Open cursor p1 on table p2 for writing
    Close,      // Close cursor p1
    Rewind,     // Move cursor p1 to first row
    Next,       // Move cursor p1 to next row
    Prev,       // Move cursor p1 to previous row
    SeekGE,     // Position cursor where key >= r[p3]
    SeekGT,     // Position cursor where key > r[p3]
    SeekLE,     // Position cursor where key <= r[p3]
    SeekLT,     // Position cursor where key < r[p3]
    Column,     // Read column p2 of cursor p1 into r[p3]
    Rowid,      // Get rowid of cursor p1 into r[p2]
```

### Row Operations
```rust
    Insert,     // Insert row into cursor p1
    Delete,     // Delete current row of cursor p1
    NewRowid,   // Generate new rowid for cursor p1
    MakeRecord, // Build record from r[p1..p1+p2-1]
```

### Arithmetic
```rust
    Add,        // r[p3] = r[p1] + r[p2]
    Subtract,   // r[p3] = r[p2] - r[p1]
    Multiply,   // r[p3] = r[p1] * r[p2]
    Divide,     // r[p3] = r[p2] / r[p1]
    Remainder,  // r[p3] = r[p2] % r[p1]
```

### Functions
```rust
    Function,   // Call function p4, args at r[p2], result in r[p3]
    AggStep,    // Aggregate step function
    AggFinal,   // Aggregate finalize
```

## Main Execution Loop

```rust
impl Vdbe {
    /// Execute the virtual machine
    pub fn exec(&mut self) -> Result<ExecResult> {
        loop {
            // Fetch instruction
            let op = &self.ops[self.pc as usize];
            self.pc += 1;

            // Execute
            match op.opcode {
                Opcode::Init => {
                    // Jump to start of program
                    if op.p2 != 0 {
                        self.pc = op.p2;
                    }
                }

                Opcode::Goto => {
                    self.pc = op.p2;
                }

                Opcode::Halt => {
                    self.rc = ErrorCode::from_i32(op.p1);
                    if op.p1 == 0 {
                        return Ok(ExecResult::Done);
                    } else {
                        return Err(Error::new(self.rc));
                    }
                }

                Opcode::Integer => {
                    self.mem[op.p2 as usize].set_int(op.p1 as i64);
                }

                Opcode::ResultRow => {
                    // Return result row to caller
                    self.has_result = true;
                    return Ok(ExecResult::Row);
                }

                Opcode::OpenRead => {
                    self.open_cursor(op.p1, op.p2, false)?;
                }

                Opcode::Column => {
                    self.read_column(op.p1, op.p2, op.p3)?;
                }

                // ... handle all ~180 opcodes
                _ => {
                    return Err(Error::with_message(
                        ErrorCode::Internal,
                        format!("Unknown opcode: {:?}", op.opcode)
                    ));
                }
            }

            // Check for interrupt
            if self.db.is_interrupted() {
                return Err(Error::new(ErrorCode::Interrupt));
            }
        }
    }
}
```

## Key Helper Functions

```rust
impl Vdbe {
    /// Compare two Mem values
    fn compare(&self, a: &Mem, b: &Mem, coll: &CollSeq) -> Ordering { ... }

    /// Apply affinity to a value
    fn apply_affinity(&self, mem: &mut Mem, affinity: Affinity) { ... }

    /// Make a record from registers
    fn make_record(&self, start: i32, count: i32) -> Vec<u8> { ... }

    /// Open a cursor on a table or index
    fn open_cursor(&mut self, cursor_id: i32, root: i32, writable: bool) -> Result<()> { ... }

    /// Read a column value
    fn read_column(&mut self, cursor: i32, col: i32, dest: i32) -> Result<()> { ... }
}
```

## Execution Results

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecResult {
    Row,     // SQLITE_ROW - result row available
    Done,    // SQLITE_DONE - execution complete
}
```

## Rust Translation Considerations

### Performance
- The execution loop is extremely hot code
- Minimize allocations in the loop
- Consider using computed goto equivalent (match with explicit jumps)
- Profile and optimize critical opcodes

### Memory Safety
- Mem cells use union-like storage in C
- Use Rust enum for type safety
- Handle blob/string lifetime carefully

### Error Handling
- Each opcode can fail
- Must clean up properly on error
- Return appropriate error codes

## Dependencies
- `vdbeInt.h` - Internal structures
- `vdbeaux.c` - Auxiliary functions
- `vdbemem.c` - Memory management
- `btree.c` - Cursor operations

## Acceptance Criteria
- [ ] Vdbe struct with all necessary fields
- [ ] VdbeOp and P4 enums defined
- [ ] All ~180 opcodes implemented
- [ ] Main execution loop working
- [ ] Cursor operations (open/close/seek/next)
- [ ] Column reading and record making
- [ ] Comparison and arithmetic operations
- [ ] Function calls (scalar and aggregate)
- [ ] Proper error handling and cleanup
