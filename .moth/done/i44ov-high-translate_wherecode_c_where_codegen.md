# Translate wherecode.c - WHERE Code Generation

## Overview
Translate WHERE clause code generation which produces VDBE bytecode for the query plan.

## Source Reference
- `sqlite3/src/wherecode.c` - 2,936 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### Loop Code Generation
- `sqlite3WhereCodeOneLoopStart()` - Generate code for one loop level
- `codeAllEqualityTerms()` - Code for equality constraints
- `codeApplyAffinity()` - Apply type affinity
- `whereIndexExprTrans()` - Translate expressions for index

### Index Scan Code
```rust
impl<'a> Parse<'a> {
    /// Generate code for index range scan
    fn code_index_scan(
        &mut self,
        level: &WhereLevel,
        index: &Index,
    ) -> Result<()> {
        let cursor = level.cursor;

        // Open index cursor
        self.add_op(Opcode::OpenRead, cursor, index.root_page as i32, 0);

        // Code equality constraints
        let start_reg = self.code_equality_terms(level)?;

        // Seek to start position
        if level.plan.has_start_constraint() {
            self.add_op(Opcode::SeekGE, cursor, level.addr_cont, start_reg);
        } else {
            self.add_op(Opcode::Rewind, cursor, level.addr_cont, 0);
        }

        // Loop body marker
        let loop_top = self.current_addr();

        // Check end constraint
        if let Some(end_expr) = level.plan.end_constraint() {
            let end_reg = self.compile_expr(end_expr)?;
            self.add_op(Opcode::IdxGT, cursor, level.addr_cont, end_reg);
        }

        Ok(())
    }
}
```

### Full Scan Code
```rust
impl<'a> Parse<'a> {
    fn code_full_scan(&mut self, level: &WhereLevel) -> Result<()> {
        let cursor = level.cursor;

        // Rewind to start
        self.add_op(Opcode::Rewind, cursor, level.addr_cont, 0);

        // Loop top
        level.addr_first = self.current_addr();

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Full table scan code generation
- [ ] Index seek (SeekGE, SeekGT, SeekLE, SeekLT)
- [ ] Index range scan
- [ ] Rowid lookup
- [ ] IN clause handling
- [ ] OR clause handling
- [ ] Loop continuation/exit jumps
- [ ] Affinity application
