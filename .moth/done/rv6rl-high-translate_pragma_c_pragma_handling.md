# Translate pragma.c - PRAGMA Handling

## Overview
Translate PRAGMA statement processing for database configuration and introspection.

## Source Reference
- `sqlite3/src/pragma.c` - 3,093 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Pragma Definition
```rust
pub struct PragmaDef {
    /// Pragma name
    pub name: &'static str,
    /// Pragma type
    pub pragma_type: PragmaType,
    /// Flags
    pub flags: PragmaFlags,
    /// Column names for table-valued pragmas
    pub columns: &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
pub enum PragmaType {
    /// Returns header info
    HeaderValue,
    /// Get/set schema value
    SchemaValue,
    /// Get/set flag
    Flag,
    /// Returns result set
    TableResult,
    /// Custom handler
    Custom,
}

bitflags! {
    pub struct PragmaFlags: u32 {
        const NEED_SCHEMA = 0x01;
        const READ_ONLY = 0x02;
        const NO_COLUMNS = 0x04;
        const NO_COLUMNS1 = 0x08;
        const RESULT_INT = 0x10;
        const SINGLE_ROW = 0x20;
    }
}
```

### Pragma Registry
```rust
lazy_static! {
    pub static ref PRAGMAS: HashMap<&'static str, PragmaDef> = {
        let mut m = HashMap::new();

        // Schema pragmas
        m.insert("table_info", PragmaDef {
            name: "table_info",
            pragma_type: PragmaType::TableResult,
            flags: PragmaFlags::NEED_SCHEMA,
            columns: &["cid", "name", "type", "notnull", "dflt_value", "pk"],
        });

        m.insert("index_list", PragmaDef {
            name: "index_list",
            pragma_type: PragmaType::TableResult,
            flags: PragmaFlags::NEED_SCHEMA,
            columns: &["seq", "name", "unique", "origin", "partial"],
        });

        m.insert("index_info", PragmaDef {
            name: "index_info",
            pragma_type: PragmaType::TableResult,
            flags: PragmaFlags::NEED_SCHEMA,
            columns: &["seqno", "cid", "name"],
        });

        m.insert("foreign_key_list", PragmaDef {
            name: "foreign_key_list",
            pragma_type: PragmaType::TableResult,
            flags: PragmaFlags::NEED_SCHEMA,
            columns: &["id", "seq", "table", "from", "to", "on_update", "on_delete", "match"],
        });

        // Database pragmas
        m.insert("database_list", PragmaDef {
            name: "database_list",
            pragma_type: PragmaType::TableResult,
            flags: PragmaFlags::empty(),
            columns: &["seq", "name", "file"],
        });

        m.insert("page_size", PragmaDef {
            name: "page_size",
            pragma_type: PragmaType::HeaderValue,
            flags: PragmaFlags::RESULT_INT | PragmaFlags::SINGLE_ROW,
            columns: &[],
        });

        m.insert("page_count", PragmaDef {
            name: "page_count",
            pragma_type: PragmaType::HeaderValue,
            flags: PragmaFlags::READ_ONLY | PragmaFlags::RESULT_INT | PragmaFlags::SINGLE_ROW,
            columns: &[],
        });

        m.insert("cache_size", PragmaDef {
            name: "cache_size",
            pragma_type: PragmaType::SchemaValue,
            flags: PragmaFlags::RESULT_INT | PragmaFlags::SINGLE_ROW,
            columns: &[],
        });

        // Journal/WAL pragmas
        m.insert("journal_mode", PragmaDef {
            name: "journal_mode",
            pragma_type: PragmaType::Custom,
            flags: PragmaFlags::SINGLE_ROW,
            columns: &[],
        });

        m.insert("wal_checkpoint", PragmaDef {
            name: "wal_checkpoint",
            pragma_type: PragmaType::Custom,
            flags: PragmaFlags::empty(),
            columns: &["busy", "log", "checkpointed"],
        });

        // Safety pragmas
        m.insert("synchronous", PragmaDef {
            name: "synchronous",
            pragma_type: PragmaType::SchemaValue,
            flags: PragmaFlags::RESULT_INT | PragmaFlags::SINGLE_ROW,
            columns: &[],
        });

        // Flag pragmas
        m.insert("foreign_keys", PragmaDef {
            name: "foreign_keys",
            pragma_type: PragmaType::Flag,
            flags: PragmaFlags::RESULT_INT | PragmaFlags::SINGLE_ROW,
            columns: &[],
        });

        m.insert("recursive_triggers", PragmaDef {
            name: "recursive_triggers",
            pragma_type: PragmaType::Flag,
            flags: PragmaFlags::RESULT_INT | PragmaFlags::SINGLE_ROW,
            columns: &[],
        });

        m
    };
}
```

## Pragma Execution

### Main Dispatcher
```rust
impl<'a> Parse<'a> {
    pub fn compile_pragma(&mut self, pragma: &PragmaStmt) -> Result<()> {
        let name = pragma.name.to_lowercase();

        // Find pragma definition
        let def = PRAGMAS.get(name.as_str())
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("unknown pragma: {}", name)
            ))?;

        // Check permissions
        if let Some(ref auth) = self.conn.authorizer {
            let rc = auth(
                AuthAction::Pragma,
                &pragma.name,
                pragma.value.as_deref(),
                &self.db_name(),
            );
            if rc != AuthResult::Ok {
                return Err(Error::with_code(ErrorCode::Auth));
            }
        }

        // Load schema if needed
        if def.flags.contains(PragmaFlags::NEED_SCHEMA) {
            self.conn.load_schema(&pragma.schema)?;
        }

        // Dispatch to handler
        match name.as_str() {
            "table_info" => self.pragma_table_info(pragma),
            "index_list" => self.pragma_index_list(pragma),
            "index_info" => self.pragma_index_info(pragma),
            "foreign_key_list" => self.pragma_foreign_key_list(pragma),
            "database_list" => self.pragma_database_list(pragma),
            "page_size" => self.pragma_page_size(pragma),
            "page_count" => self.pragma_page_count(pragma),
            "cache_size" => self.pragma_cache_size(pragma),
            "journal_mode" => self.pragma_journal_mode(pragma),
            "synchronous" => self.pragma_synchronous(pragma),
            "foreign_keys" => self.pragma_foreign_keys(pragma),
            "integrity_check" => self.pragma_integrity_check(pragma),
            "quick_check" => self.pragma_quick_check(pragma),
            "wal_checkpoint" => self.pragma_wal_checkpoint(pragma),
            "optimize" => self.pragma_optimize(pragma),
            _ => Err(Error::with_message(
                ErrorCode::Error,
                format!("unimplemented pragma: {}", name)
            )),
        }
    }
}
```

### Schema Introspection Pragmas
```rust
impl<'a> Parse<'a> {
    fn pragma_table_info(&mut self, pragma: &PragmaStmt) -> Result<()> {
        let table_name = pragma.value.as_ref()
            .ok_or_else(|| Error::with_message(ErrorCode::Error, "table name required"))?;

        let schema = self.conn.schema.read().unwrap();
        let table = schema.tables.get(table_name)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", table_name)
            ))?;

        // Set up result columns
        self.set_num_columns(6);
        self.set_column_names(&["cid", "name", "type", "notnull", "dflt_value", "pk"]);

        for (i, col) in table.columns.iter().enumerate() {
            let row_start = self.make_label();
            self.resolve_label(row_start);

            // cid
            self.add_op(Opcode::Integer, i as i32, 1, 0);
            // name
            self.add_string_op(2, &col.name);
            // type
            self.add_string_op(3, &col.col_type.to_string());
            // notnull
            self.add_op(Opcode::Integer, col.not_null as i32, 4, 0);
            // dflt_value
            if let Some(ref default) = col.default_value {
                self.add_string_op(5, &default.to_sql());
            } else {
                self.add_op(Opcode::Null, 0, 5, 0);
            }
            // pk
            self.add_op(Opcode::Integer, col.is_pk as i32, 6, 0);

            self.add_op(Opcode::ResultRow, 1, 6, 0);
        }

        Ok(())
    }

    fn pragma_index_list(&mut self, pragma: &PragmaStmt) -> Result<()> {
        let table_name = pragma.value.as_ref()
            .ok_or_else(|| Error::with_message(ErrorCode::Error, "table name required"))?;

        let schema = self.conn.schema.read().unwrap();
        let table = schema.tables.get(table_name)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", table_name)
            ))?;

        self.set_num_columns(5);
        self.set_column_names(&["seq", "name", "unique", "origin", "partial"]);

        for (i, index) in table.indexes.iter().enumerate() {
            // seq
            self.add_op(Opcode::Integer, i as i32, 1, 0);
            // name
            self.add_string_op(2, &index.name);
            // unique
            self.add_op(Opcode::Integer, index.unique as i32, 3, 0);
            // origin
            let origin = if index.is_pk { "pk" } else { "c" };
            self.add_string_op(4, origin);
            // partial
            self.add_op(Opcode::Integer, index.where_clause.is_some() as i32, 5, 0);

            self.add_op(Opcode::ResultRow, 1, 5, 0);
        }

        Ok(())
    }

    fn pragma_database_list(&mut self, pragma: &PragmaStmt) -> Result<()> {
        self.set_num_columns(3);
        self.set_column_names(&["seq", "name", "file"]);

        for (i, db) in self.conn.dbs.iter().enumerate() {
            self.add_op(Opcode::Integer, i as i32, 1, 0);
            self.add_string_op(2, &db.name);

            let file = db.btree.as_ref()
                .map(|b| b.filename())
                .unwrap_or("");
            self.add_string_op(3, file);

            self.add_op(Opcode::ResultRow, 1, 3, 0);
        }

        Ok(())
    }
}
```

### Configuration Pragmas
```rust
impl<'a> Parse<'a> {
    fn pragma_page_size(&mut self, pragma: &PragmaStmt) -> Result<()> {
        let db = self.get_db(&pragma.schema)?;

        if let Some(ref value) = pragma.value {
            // Set page size
            let size: i32 = value.parse()
                .map_err(|_| Error::with_message(ErrorCode::Error, "invalid page size"))?;

            // Validate (must be power of 2 between 512 and 65536)
            if !size.is_power_of_two() || size < 512 || size > 65536 {
                return Err(Error::with_message(ErrorCode::Error, "invalid page size"));
            }

            // Can only change before writing
            if let Some(ref btree) = db.btree {
                btree.set_page_size(size)?;
            }
        }

        // Return current value
        let page_size = db.btree.as_ref()
            .map(|b| b.page_size())
            .unwrap_or(4096);

        self.set_num_columns(1);
        self.add_op(Opcode::Integer, page_size, 1, 0);
        self.add_op(Opcode::ResultRow, 1, 1, 0);

        Ok(())
    }

    fn pragma_journal_mode(&mut self, pragma: &PragmaStmt) -> Result<()> {
        let db = self.get_db(&pragma.schema)?;

        if let Some(ref value) = pragma.value {
            let mode = match value.to_lowercase().as_str() {
                "delete" => JournalMode::Delete,
                "truncate" => JournalMode::Truncate,
                "persist" => JournalMode::Persist,
                "memory" => JournalMode::Memory,
                "off" => JournalMode::Off,
                "wal" => JournalMode::Wal,
                _ => return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("unknown journal mode: {}", value)
                )),
            };

            if let Some(ref btree) = db.btree {
                btree.pager().set_journal_mode(mode)?;
            }
        }

        // Return current mode
        let mode = db.btree.as_ref()
            .map(|b| b.pager().journal_mode())
            .unwrap_or(JournalMode::Delete);

        self.set_num_columns(1);
        self.add_string_op(1, &mode.to_string());
        self.add_op(Opcode::ResultRow, 1, 1, 0);

        Ok(())
    }

    fn pragma_synchronous(&mut self, pragma: &PragmaStmt) -> Result<()> {
        let db = self.get_db_mut(&pragma.schema)?;

        if let Some(ref value) = pragma.value {
            let level = match value.to_lowercase().as_str() {
                "off" | "0" => SafetyLevel::Off,
                "normal" | "1" => SafetyLevel::Normal,
                "full" | "2" => SafetyLevel::Full,
                "extra" | "3" => SafetyLevel::Extra,
                _ => return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("unknown synchronous level: {}", value)
                )),
            };

            db.safety_level = level;
        }

        self.set_num_columns(1);
        self.add_op(Opcode::Integer, db.safety_level as i32, 1, 0);
        self.add_op(Opcode::ResultRow, 1, 1, 0);

        Ok(())
    }
}
```

### Integrity Checking
```rust
impl<'a> Parse<'a> {
    fn pragma_integrity_check(&mut self, pragma: &PragmaStmt) -> Result<()> {
        let max_errors = pragma.value.as_ref()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        self.set_num_columns(1);
        self.set_column_names(&["integrity_check"]);

        // Generate code to check each database
        for db in &self.conn.dbs {
            if let Some(ref btree) = db.btree {
                // Check B-tree integrity
                let errors = self.generate_btree_check(btree, max_errors)?;

                for error in errors {
                    self.add_string_op(1, &error);
                    self.add_op(Opcode::ResultRow, 1, 1, 0);
                }
            }
        }

        // Return "ok" if no errors
        self.add_string_op(1, "ok");
        self.add_op(Opcode::ResultRow, 1, 1, 0);

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Pragma parsing and dispatch
- [ ] table_info pragma
- [ ] index_list pragma
- [ ] index_info pragma
- [ ] foreign_key_list pragma
- [ ] database_list pragma
- [ ] page_size pragma
- [ ] page_count pragma
- [ ] cache_size pragma
- [ ] journal_mode pragma
- [ ] synchronous pragma
- [ ] foreign_keys pragma
- [ ] integrity_check pragma
- [ ] wal_checkpoint pragma
- [ ] optimize pragma
- [ ] auto_vacuum pragma
- [ ] encoding pragma
- [ ] Schema-specific pragmas (schema.pragma)
