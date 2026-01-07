# Translate trigger.c - Triggers

## Overview
Translate trigger implementation including CREATE TRIGGER, trigger firing, and OLD/NEW row access.

## Source Reference
- `sqlite3/src/trigger.c` - 1,572 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Trigger
```rust
pub struct Trigger {
    /// Trigger name
    pub name: String,
    /// Table the trigger is on
    pub table: String,
    /// BEFORE or AFTER
    pub timing: TriggerTiming,
    /// DELETE, INSERT, or UPDATE
    pub event: TriggerEvent,
    /// Columns for UPDATE OF
    pub columns: Option<Vec<String>>,
    /// WHEN expression
    pub when: Option<Expr>,
    /// Trigger body statements
    pub body: Vec<TriggerStep>,
    /// FOR EACH ROW (always true in SQLite)
    pub for_each_row: bool,
    /// CREATE TRIGGER sql
    pub sql: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, Copy)]
pub enum TriggerEvent {
    Delete,
    Insert,
    Update,
}

pub struct TriggerStep {
    pub op: TriggerStepOp,
    pub target: Option<String>,
    pub expr_list: Option<Vec<Expr>>,
    pub select: Option<SelectStmt>,
    pub where_clause: Option<Expr>,
}

pub enum TriggerStepOp {
    Insert,
    Update,
    Delete,
    Select,
}
```

## Key Functions

### Trigger Creation
```rust
impl Schema {
    pub fn create_trigger(
        &mut self,
        db: &mut Connection,
        create: &CreateTriggerStmt,
    ) -> Result<()> {
        // Validate table exists
        let table = self.tables.get(&create.table)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", create.table)
            ))?;

        // Check for INSTEAD OF on regular table
        if create.timing == TriggerTiming::InsteadOf && !table.is_view {
            return Err(Error::with_message(
                ErrorCode::Error,
                "INSTEAD OF triggers are only for views"
            ));
        }

        // Build trigger
        let trigger = Trigger {
            name: create.name.clone(),
            table: create.table.clone(),
            timing: create.timing,
            event: create.event,
            columns: create.columns.clone(),
            when: create.when.clone(),
            body: create.body.clone(),
            for_each_row: true,
            sql: Some(create.to_sql()),
        };

        // Store in schema
        self.triggers.insert(trigger.name.clone(), Arc::new(trigger));

        // Write to sqlite_master
        self.insert_into_master(
            db, "trigger", &trigger.name, &trigger.table, 0,
            trigger.sql.as_ref().unwrap()
        )?;

        Ok(())
    }
}
```

### Trigger Firing
```rust
impl<'a> Parse<'a> {
    /// Generate code to fire triggers
    pub fn fire_triggers(
        &mut self,
        table: &Table,
        timing: TriggerTiming,
        event: TriggerEvent,
        old_reg: Option<i32>,
        new_reg: Option<i32>,
    ) -> Result<()> {
        // Find matching triggers
        let triggers = self.find_triggers(table, timing, event);

        for trigger in triggers {
            // Evaluate WHEN condition
            if let Some(ref when) = trigger.when {
                let cond_reg = self.compile_expr(when)?;
                let skip_label = self.make_label();
                self.add_op(Opcode::IfNot, cond_reg, skip_label, 0);
            }

            // Execute trigger body
            for step in &trigger.body {
                self.compile_trigger_step(step, old_reg, new_reg)?;
            }

            // Skip label
            if trigger.when.is_some() {
                self.resolve_label(skip_label);
            }
        }

        Ok(())
    }

    fn compile_trigger_step(
        &mut self,
        step: &TriggerStep,
        old_reg: Option<i32>,
        new_reg: Option<i32>,
    ) -> Result<()> {
        // Set up OLD and NEW pseudo-tables
        // These are accessible in the trigger body

        match step.op {
            TriggerStepOp::Insert => {
                // Compile nested INSERT
            }
            TriggerStepOp::Update => {
                // Compile nested UPDATE
            }
            TriggerStepOp::Delete => {
                // Compile nested DELETE
            }
            TriggerStepOp::Select => {
                // Compile SELECT (side effects only)
            }
        }

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] CREATE TRIGGER parsing
- [ ] BEFORE/AFTER/INSTEAD OF timing
- [ ] INSERT/UPDATE/DELETE events
- [ ] UPDATE OF column list
- [ ] WHEN condition
- [ ] Trigger body statements
- [ ] OLD and NEW row access
- [ ] Trigger firing order
- [ ] Recursive trigger prevention
- [ ] DROP TRIGGER
- [ ] RAISE() function

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `trigger1.test` - Basic trigger functionality
- `trigger2.test` - Trigger timing (BEFORE/AFTER)
- `trigger3.test` - INSTEAD OF triggers on views
- `trigger4.test` - Trigger body statements
- `trigger5.test` - OLD/NEW row access
- `trigger6.test` - Nested triggers
- `trigger7.test` - Trigger and transactions
- `trigger8.test` - Trigger edge cases
- `trigger9.test` - Recursive triggers
- `triggerA.test` - RAISE function
- `triggerB.test` - Trigger and constraints
- `triggerC.test` - Additional trigger tests
- `triggerD.test` - Trigger performance
- `altertrig.test` - ALTER TABLE with triggers
- `droptrig.test` - DROP TRIGGER tests
- `e_droptrigger.test` - DROP TRIGGER expressions
