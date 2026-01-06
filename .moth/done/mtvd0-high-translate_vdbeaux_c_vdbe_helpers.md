# Translate vdbeaux.c - VDBE Helpers

## Overview
Translate VDBE auxiliary functions for building programs, managing cursors, preparing for execution, and cleanup after execution.

## Source Reference
- `sqlite3/src/vdbeaux.c` - 5,584 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### Program Building

```rust
impl Vdbe {
    /// Create a new VDBE for a connection
    pub fn new(db: Arc<Connection>) -> Self {
        Vdbe {
            db,
            pc: 0,
            rc: ErrorCode::Ok,
            ops: Vec::new(),
            n_op: 0,
            mem: Vec::new(),
            n_mem: 0,
            cursors: Vec::new(),
            n_cursor: 0,
            frames: Vec::new(),
            magic: VDBE_MAGIC_INIT,
            // ... other fields
        }
    }

    /// Add an opcode to the program
    /// Returns the address of the new opcode
    pub fn add_op(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32) -> i32 {
        let addr = self.ops.len() as i32;
        self.ops.push(VdbeOp {
            opcode,
            p1,
            p2,
            p3,
            p4: P4::None,
            p5: 0,
            #[cfg(debug_assertions)]
            comment: None,
        });
        self.n_op = self.ops.len() as i32;
        addr
    }

    /// Add an opcode with P4 operand
    pub fn add_op4(
        &mut self,
        opcode: Opcode,
        p1: i32,
        p2: i32,
        p3: i32,
        p4: P4,
    ) -> i32 {
        let addr = self.add_op(opcode, p1, p2, p3);
        self.ops[addr as usize].p4 = p4;
        addr
    }

    /// Get current address (for jump targets)
    pub fn current_addr(&self) -> i32 {
        self.ops.len() as i32
    }

    /// Change the P2 operand of instruction at addr
    pub fn change_p2(&mut self, addr: i32, p2: i32) {
        if addr >= 0 && (addr as usize) < self.ops.len() {
            self.ops[addr as usize].p2 = p2;
        }
    }

    /// Resolve a label to an address
    pub fn resolve_label(&mut self, label: i32) {
        let addr = self.current_addr();
        // Label is negative, actual instruction is at -label-1
        let label_addr = (-label - 1) as usize;
        if label_addr < self.ops.len() {
            self.ops[label_addr].p2 = addr;
        }
    }
}
```

### Resource Allocation

```rust
impl Vdbe {
    /// Allocate memory cells
    pub fn alloc_mem(&mut self, n: i32) -> i32 {
        let start = self.n_mem;
        self.n_mem += n;
        while self.mem.len() < self.n_mem as usize {
            self.mem.push(Mem::null());
        }
        start + 1 // 1-indexed
    }

    /// Allocate cursor slots
    pub fn alloc_cursor(&mut self, n: i32) -> i32 {
        let start = self.n_cursor;
        self.n_cursor += n;
        while self.cursors.len() < self.n_cursor as usize {
            self.cursors.push(None);
        }
        start
    }
}
```

### Program Finalization

```rust
impl Vdbe {
    /// Prepare VDBE for execution
    pub fn make_ready(&mut self) -> Result<()> {
        // Allocate memory cells
        self.mem.resize(self.n_mem as usize, Mem::null());

        // Allocate cursor slots
        self.cursors.resize(self.n_cursor as usize, None);

        // Set magic number
        self.magic = VDBE_MAGIC_RUN;

        Ok(())
    }

    /// Reset VDBE for re-execution
    pub fn reset(&mut self) -> Result<ErrorCode> {
        // Close all cursors
        for cursor in &mut self.cursors {
            *cursor = None;
        }

        // Clear memory cells
        for mem in &mut self.mem {
            *mem = Mem::null();
        }

        // Reset state
        self.pc = 0;
        self.has_result = false;
        self.is_done = false;
        self.magic = VDBE_MAGIC_RESET;

        Ok(self.rc)
    }

    /// Finalize and free VDBE resources
    pub fn finalize(mut self) -> Result<ErrorCode> {
        let rc = self.reset()?;

        // Clear bound parameters
        self.vars.clear();

        // Set magic to dead
        self.magic = VDBE_MAGIC_DEAD;

        Ok(rc)
    }
}
```

### Cursor Management

```rust
impl Vdbe {
    /// Allocate and initialize a cursor
    pub fn open_cursor(
        &mut self,
        cursor_idx: i32,
        root: Pgno,
        key_info: Option<Arc<KeyInfo>>,
        writable: bool,
    ) -> Result<()> {
        // Get the B-tree for this database
        let btree = self.db.main_btree();

        // Create B-tree cursor
        let bt_cursor = btree.cursor(root, writable, key_info.clone())?;

        // Store in cursor array
        self.cursors[cursor_idx as usize] = Some(VdbeCursor {
            cursor_type: CursorType::BTree,
            idx: cursor_idx,
            root,
            writable,
            btree_cursor: Some(bt_cursor),
            pseudo_data: None,
            sorter: None,
            cached_columns: Vec::new(),
            payload: None,
            key_info,
            null_row: false,
            seek_result: 0,
        });

        Ok(())
    }

    /// Close a cursor
    pub fn close_cursor(&mut self, cursor_idx: i32) {
        if let Some(cursor) = self.cursors.get_mut(cursor_idx as usize) {
            *cursor = None;
        }
    }

    /// Close all cursors
    pub fn close_all_cursors(&mut self) {
        for cursor in &mut self.cursors {
            *cursor = None;
        }
    }
}
```

### Record Handling

```rust
impl Vdbe {
    /// Decode a record header
    pub fn decode_record_header(data: &[u8]) -> Result<(Vec<SerialType>, usize)> {
        let (header_size, mut offset) = get_varint(data);
        let header_end = header_size as usize;
        let mut types = Vec::new();

        while offset < header_end {
            let (type_code, consumed) = get_varint(&data[offset..]);
            types.push(SerialType::from_code(type_code as u32));
            offset += consumed;
        }

        Ok((types, header_end))
    }

    /// Build a record from memory cells
    pub fn make_record(&self, start: i32, count: i32) -> Vec<u8> {
        let mut header = Vec::new();
        let mut body = Vec::new();

        // Build header (serial types) and body (data)
        for i in 0..count {
            let mem = &self.mem[(start + i) as usize];
            let serial_type = mem.serial_type();

            // Add type to header
            put_varint(&mut header, serial_type.code() as u64);

            // Add data to body
            mem.serialize(&mut body);
        }

        // Combine: header_size + header + body
        let mut record = Vec::new();
        put_varint(&mut record, (header.len() + 1) as u64);
        record.extend(header);
        record.extend(body);

        record
    }
}
```

### EXPLAIN Support

```rust
impl Vdbe {
    /// Get SQL for EXPLAIN output
    pub fn explain_sql(&self) -> String {
        let mut output = String::new();
        for (i, op) in self.ops.iter().enumerate() {
            output.push_str(&format!(
                "{:4}  {:15}  {:4}  {:4}  {:4}  {}\n",
                i,
                format!("{:?}", op.opcode),
                op.p1,
                op.p2,
                op.p3,
                self.format_p4(&op.p4),
            ));
        }
        output
    }

    fn format_p4(&self, p4: &P4) -> String {
        match p4 {
            P4::None => String::new(),
            P4::Int(i) => i.to_string(),
            P4::Int64(i) => i.to_string(),
            P4::Real(r) => r.to_string(),
            P4::Text(s) => format!("'{}'", s),
            P4::Blob(b) => format!("x'{}'", hex::encode(b)),
            P4::KeyInfo(ki) => format!("k({})", ki.n_key_field),
            P4::CollSeq(cs) => cs.name.clone(),
            P4::FuncDef(fd) => fd.name.clone(),
            _ => String::from("..."),
        }
    }
}
```

## Serial Types

```rust
#[derive(Debug, Clone, Copy)]
pub enum SerialType {
    Null,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    Float64,
    Zero,
    One,
    Reserved10,
    Reserved11,
    Blob(u32),  // (N-12)/2 bytes
    Text(u32),  // (N-13)/2 bytes
}

impl SerialType {
    pub fn from_code(code: u32) -> Self {
        match code {
            0 => SerialType::Null,
            1 => SerialType::Int8,
            2 => SerialType::Int16,
            3 => SerialType::Int24,
            4 => SerialType::Int32,
            5 => SerialType::Int48,
            6 => SerialType::Int64,
            7 => SerialType::Float64,
            8 => SerialType::Zero,
            9 => SerialType::One,
            10 => SerialType::Reserved10,
            11 => SerialType::Reserved11,
            n if n >= 12 && n % 2 == 0 => SerialType::Blob((n - 12) / 2),
            n if n >= 13 && n % 2 == 1 => SerialType::Text((n - 13) / 2),
            _ => SerialType::Null,
        }
    }

    pub fn code(&self) -> u32 {
        match self {
            SerialType::Null => 0,
            SerialType::Int8 => 1,
            SerialType::Int16 => 2,
            SerialType::Int24 => 3,
            SerialType::Int32 => 4,
            SerialType::Int48 => 5,
            SerialType::Int64 => 6,
            SerialType::Float64 => 7,
            SerialType::Zero => 8,
            SerialType::One => 9,
            SerialType::Reserved10 => 10,
            SerialType::Reserved11 => 11,
            SerialType::Blob(n) => 12 + n * 2,
            SerialType::Text(n) => 13 + n * 2,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            SerialType::Null => 0,
            SerialType::Int8 => 1,
            SerialType::Int16 => 2,
            SerialType::Int24 => 3,
            SerialType::Int32 => 4,
            SerialType::Int48 => 6,
            SerialType::Int64 => 8,
            SerialType::Float64 => 8,
            SerialType::Zero | SerialType::One => 0,
            SerialType::Reserved10 | SerialType::Reserved11 => 0,
            SerialType::Blob(n) | SerialType::Text(n) => *n as usize,
        }
    }
}
```

## Acceptance Criteria
- [ ] VDBE creation and initialization
- [ ] Opcode adding functions (add_op, add_op4, etc.)
- [ ] Memory cell allocation
- [ ] Cursor slot allocation
- [ ] make_ready() preparation
- [ ] reset() for re-execution
- [ ] finalize() for cleanup
- [ ] Cursor open/close operations
- [ ] Record encoding/decoding
- [ ] Serial type handling
- [ ] EXPLAIN output generation
