# Translate vdbetrace.c and vdbevtab.c

## Overview
Translate VDBE tracing facilities and the bytecode virtual table for debugging and introspection.

## Source Reference
- `sqlite3/src/vdbetrace.c` - 192 lines
- `sqlite3/src/vdbevtab.c` - 446 lines

---

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## vdbetrace.c - VDBE Tracing

### Purpose
Provides SQL execution tracing for debugging. When enabled, outputs the SQL being executed with bound parameter values expanded.

### Key Functions

```rust
impl Vdbe {
    /// Generate expanded SQL with bound values
    /// Used for trace callbacks
    pub fn expanded_sql(&self) -> String {
        if self.sql.is_empty() {
            return String::new();
        }

        let mut result = String::with_capacity(self.sql.len() * 2);
        let mut param_idx = 0;
        let mut chars = self.sql.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '?' {
                // Check for numbered parameter ?NNN
                let mut num = String::new();
                while let Some(&d) = chars.peek() {
                    if d.is_ascii_digit() {
                        num.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                let idx = if num.is_empty() {
                    param_idx += 1;
                    param_idx
                } else {
                    num.parse().unwrap_or(0)
                };

                // Get parameter value
                if idx > 0 && idx <= self.vars.len() {
                    result.push_str(&self.vars[idx - 1].to_sql_literal());
                } else {
                    result.push('?');
                    result.push_str(&num);
                }
            } else if c == '$' || c == '@' || c == ':' {
                // Named parameter
                let mut name = String::new();
                name.push(c);
                while let Some(&d) = chars.peek() {
                    if d.is_alphanumeric() || d == '_' {
                        name.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                // Look up named parameter
                if let Some(idx) = self.find_param_index(&name) {
                    result.push_str(&self.vars[idx - 1].to_sql_literal());
                } else {
                    result.push_str(&name);
                }
            } else {
                result.push(c);
            }
        }

        result
    }

    fn find_param_index(&self, name: &str) -> Option<usize> {
        for (i, param_name) in self.var_names.iter().enumerate() {
            if let Some(n) = param_name {
                if n == name {
                    return Some(i + 1);
                }
            }
        }
        None
    }
}
```

### Trace Callback

```rust
/// Trace callback type
pub type TraceCallback = Box<dyn Fn(&str) + Send + Sync>;

impl Connection {
    /// Set trace callback
    /// sqlite3_trace_v2()
    pub fn trace(&mut self, callback: Option<TraceCallback>, mask: TraceFlags) {
        self.trace_callback = callback;
        self.trace_mask = mask;
    }

    /// Called before executing SQL
    pub fn trace_sql(&self, sql: &str) {
        if self.trace_mask.contains(TraceFlags::STMT) {
            if let Some(ref cb) = self.trace_callback {
                cb(sql);
            }
        }
    }

    /// Called when statement completes
    pub fn trace_profile(&self, stmt: &Statement, elapsed_ns: u64) {
        if self.trace_mask.contains(TraceFlags::PROFILE) {
            if let Some(ref cb) = self.trace_callback {
                // Format profile info
                let msg = format!("{} -- {} ns", stmt.sql(), elapsed_ns);
                cb(&msg);
            }
        }
    }
}

bitflags! {
    pub struct TraceFlags: u32 {
        const STMT = 0x01;    // Trace SQL statements
        const PROFILE = 0x02; // Profile statement timing
        const ROW = 0x04;     // Trace each result row
        const CLOSE = 0x08;   // Trace statement close
    }
}
```

---

## vdbevtab.c - Bytecode Virtual Table

### Purpose
Implements a virtual table that exposes VDBE bytecode for any SQL statement, allowing introspection via SQL queries.

### Schema

```sql
CREATE TABLE bytecode(
    addr INT,      -- Instruction address
    opcode TEXT,   -- Opcode name
    p1 INT,        -- First operand
    p2 INT,        -- Second operand
    p3 INT,        -- Third operand
    p4 TEXT,       -- Fourth operand (formatted)
    p5 INT,        -- Fifth operand
    comment TEXT,  -- Instruction comment
    subprog TEXT   -- Subprogram name (for triggers)
);
```

### Virtual Table Implementation

```rust
/// Bytecode virtual table module
pub struct BytecodeVTab {
    /// The SQL statement being examined
    sql: String,
}

/// Cursor for iterating bytecode
pub struct BytecodeCursor {
    /// Compiled statement
    stmt: Statement,

    /// Current instruction index
    addr: i32,

    /// Total instructions
    n_op: i32,

    /// EOF flag
    eof: bool,
}

impl VTab for BytecodeVTab {
    type Cursor = BytecodeCursor;

    fn create(db: &Connection, args: &[&str]) -> Result<Self> {
        // args[3] should be the SQL to analyze
        let sql = args.get(3).unwrap_or(&"").to_string();
        Ok(BytecodeVTab { sql })
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Full table scan
        info.estimated_cost = 1000.0;
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        // Compile the SQL
        let stmt = prepare(&self.db, &self.sql)?;
        let n_op = stmt.vdbe().ops.len() as i32;

        Ok(BytecodeCursor {
            stmt,
            addr: 0,
            n_op,
            eof: n_op == 0,
        })
    }
}

impl VTabCursor for BytecodeCursor {
    fn filter(&mut self, _idx: i32, _args: &[Value]) -> Result<()> {
        self.addr = 0;
        self.eof = self.n_op == 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.addr += 1;
        if self.addr >= self.n_op {
            self.eof = true;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, ctx: &mut FunctionContext, col: i32) -> Result<()> {
        let op = &self.stmt.vdbe().ops[self.addr as usize];

        match col {
            0 => ctx.result_i64(self.addr as i64),      // addr
            1 => ctx.result_text(&format!("{:?}", op.opcode)), // opcode
            2 => ctx.result_i64(op.p1 as i64),         // p1
            3 => ctx.result_i64(op.p2 as i64),         // p2
            4 => ctx.result_i64(op.p3 as i64),         // p3
            5 => ctx.result_text(&format_p4(&op.p4)), // p4
            6 => ctx.result_i64(op.p5 as i64),         // p5
            7 => {                                      // comment
                #[cfg(debug_assertions)]
                if let Some(c) = &op.comment {
                    ctx.result_text(c);
                } else {
                    ctx.result_null();
                }
                #[cfg(not(debug_assertions))]
                ctx.result_null();
            }
            8 => ctx.result_null(),                    // subprog
            _ => ctx.result_null(),
        }
        Ok(())
    }

    fn rowid(&self) -> i64 {
        self.addr as i64
    }
}
```

### Usage

```sql
-- Examine bytecode of a query
SELECT * FROM bytecode('SELECT * FROM users WHERE id = 5');

-- Find all column reads
SELECT addr, p1, p2
FROM bytecode('SELECT name, email FROM users')
WHERE opcode = 'Column';

-- Count opcodes by type
SELECT opcode, COUNT(*)
FROM bytecode('SELECT * FROM orders JOIN users ON orders.user_id = users.id')
GROUP BY opcode;
```

### Registration

```rust
impl Connection {
    /// Register the bytecode virtual table
    pub fn register_bytecode_vtab(&self) -> Result<()> {
        self.create_module("bytecode", BytecodeVTab::module())?;
        Ok(())
    }
}
```

## Acceptance Criteria

### vdbetrace.c
- [ ] expanded_sql() generates SQL with bound values
- [ ] Handle positional parameters (?1, ?2, ?)
- [ ] Handle named parameters ($name, :name, @name)
- [ ] TraceCallback type defined
- [ ] trace() to set callback
- [ ] TraceFlags for filtering events
- [ ] Profile timing support

### vdbevtab.c
- [ ] BytecodeVTab virtual table struct
- [ ] BytecodeCursor for iteration
- [ ] Expose all opcode fields
- [ ] Format P4 operand as text
- [ ] Register as "bytecode" module
- [ ] Support debug comments when available
