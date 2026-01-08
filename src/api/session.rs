//! Session change tracking (sqlite3session.c)
//!
//! This module provides a Rust translation of SQLite's session extension APIs
//! for capturing and applying database changes. The implementation focuses on
//! preserving observable behavior while integrating with RustQL's schema and
//! statement execution model.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Table;
use crate::types::{StepResult, Value};
use crate::vdbe::aux::{get_varint, put_varint};

use super::connection::SqliteConnection;
use super::stmt::{sqlite3_bind_value, sqlite3_column_value, sqlite3_prepare_v2, sqlite3_step};

const CHANGESET_END: u8 = 0;
const CHANGESET_INSERT: u8 = 1;
const CHANGESET_UPDATE: u8 = 2;
const CHANGESET_DELETE: u8 = 3;
const CHANGESET_INDIRECT: u8 = 0x80;

const VALUE_NULL: u8 = 0;
const VALUE_INTEGER: u8 = 1;
const VALUE_REAL: u8 = 2;
const VALUE_TEXT: u8 = 3;
const VALUE_BLOB: u8 = 4;

static STREAM_CHUNK_SIZE: AtomicUsize = AtomicUsize::new(1024);

// ============================================================================
// Public Types
// ============================================================================

/// Session change tracking object (sqlite3_session)
pub struct Session {
    db: *mut SqliteConnection,
    db_name: String,
    tables: HashMap<String, SessionTable>,
    enabled: bool,
    indirect: bool,
    attach_all: bool,
    table_filter: Option<Box<dyn Fn(&str) -> bool + Send + Sync>>,
    track_rowid: bool,
    changeset_size_enabled: bool,
}

/// Options for sqlite3session_object_config().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SessionObjConfig {
    Size = 1,
    RowId = 2,
}

/// Options for sqlite3session_config().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SessionConfigOp {
    StrmSize = 1,
}

/// Change operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// Single change entry
#[derive(Debug, Clone)]
pub struct SessionChange {
    pub op: ChangeOp,
    pub old: Option<Vec<Value>>,
    pub new: Option<Vec<Value>>,
    pub indirect: bool,
}

/// Serialized changeset
#[derive(Debug, Clone)]
pub struct Changeset {
    data: Vec<u8>,
}

/// Serialized patchset
#[derive(Debug, Clone)]
pub struct Patchset {
    data: Vec<u8>,
}

/// Iterator over a changeset or patchset
pub struct ChangesetIter<'a> {
    data: &'a [u8],
    pos: usize,
    header: Option<ChangesetHeader>,
}

/// Single change returned by a changeset iterator
#[derive(Debug, Clone)]
pub struct ChangesetChange {
    pub table: String,
    pub op: ChangeOp,
    pub n_col: i32,
    pub pk: Vec<bool>,
    pub old: Option<Vec<Value>>,
    pub new: Option<Vec<Value>>,
    pub indirect: bool,
}

/// Conflict types returned by changeset application
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType {
    Data,
    NotFound,
    Constraint,
    Other,
}

/// Conflict actions for changeset application
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictAction {
    Omit,
    Replace,
    Abort,
}

// ============================================================================
// Internal Types
// ============================================================================

#[derive(Debug, Clone)]
struct SessionTable {
    name: String,
    n_col: i32,
    pk_cols: Vec<bool>,
    changes: HashMap<Vec<u8>, SessionChange>,
}

#[derive(Debug, Clone)]
struct ChangesetHeader {
    table: String,
    n_col: i32,
    pk: Vec<bool>,
}

// ============================================================================
// Session Implementation
// ============================================================================

impl Session {
    fn new(db: &mut SqliteConnection, db_name: &str) -> Result<Self> {
        if db.find_db(db_name).is_none() {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("no such database: {}", db_name),
            ));
        }

        Ok(Self {
            db: db as *mut SqliteConnection,
            db_name: db_name.to_string(),
            tables: HashMap::new(),
            enabled: true,
            indirect: false,
            attach_all: false,
            table_filter: None,
            track_rowid: false,
            changeset_size_enabled: false,
        })
    }

    fn attach_table(&mut self, table_name: &str) -> Result<()> {
        if let Some(ref filter) = self.table_filter {
            if !filter(table_name) {
                return Ok(());
            }
        }

        let table_key = table_name.to_lowercase();
        if self.tables.contains_key(&table_key) {
            return Ok(());
        }

        let (n_col, pk_cols) = self.table_info(table_name)?;
        self.tables.insert(
            table_key,
            SessionTable {
                name: table_name.to_string(),
                n_col,
                pk_cols,
                changes: HashMap::new(),
            },
        );
        Ok(())
    }

    fn table_info(&self, table_name: &str) -> Result<(i32, Vec<bool>)> {
        let conn = unsafe { &*self.db };
        let db = conn
            .find_db(&self.db_name)
            .ok_or_else(|| Error::new(ErrorCode::Error))?;
        let schema = db
            .schema
            .as_ref()
            .ok_or_else(|| Error::new(ErrorCode::Error))?;
        let schema = schema.read().unwrap();
        let table = schema.table(table_name).ok_or_else(|| {
            Error::with_message(ErrorCode::Error, format!("no such table: {}", table_name))
        })?;

        let (has_pk, mut pk_cols) = table_pk_flags(&table);
        let mut n_col = table.columns.len();
        if !has_pk && self.track_rowid {
            n_col += 1;
            pk_cols.insert(0, true);
        }
        Ok((n_col as i32, pk_cols))
    }

    fn ensure_table_attached(&mut self, table_name: &str) -> Result<()> {
        let table_key = table_name.to_lowercase();
        if self.tables.contains_key(&table_key) {
            return Ok(());
        }
        if !self.attach_all {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("table not attached: {}", table_name),
            ));
        }
        self.attach_table(table_name)
    }

    pub(crate) fn record_change(
        &mut self,
        table_name: &str,
        op: ChangeOp,
        old: Option<Vec<Value>>,
        new: Option<Vec<Value>>,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        self.ensure_table_attached(table_name)?;

        let table_key = table_name.to_lowercase();
        let table = self
            .tables
            .get_mut(&table_key)
            .ok_or_else(|| Error::new(ErrorCode::Error))?;
        let pk_key = pk_key_for_change(table, op, old.as_ref(), new.as_ref())?;
        let indirect = self.indirect;

        if let Some(existing) = table.changes.get_mut(&pk_key) {
            if existing.op == ChangeOp::Insert && op == ChangeOp::Delete {
                table.changes.remove(&pk_key);
                return Ok(());
            }
            coalesce_change(existing, op, old, new, indirect);
        } else {
            table.changes.insert(
                pk_key,
                SessionChange {
                    op,
                    old,
                    new,
                    indirect,
                },
            );
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.tables.values().all(|table| table.changes.is_empty())
    }

    fn changeset(&self) -> Result<Changeset> {
        let mut data = Vec::new();
        let mut table_names: Vec<&SessionTable> = self.tables.values().collect();
        table_names.sort_by(|a, b| a.name.cmp(&b.name));

        for table in table_names {
            if table.changes.is_empty() {
                continue;
            }
            encode_table_header(&mut data, table)?;

            let mut change_keys: Vec<&Vec<u8>> = table.changes.keys().collect();
            change_keys.sort();
            for key in change_keys {
                let change = table
                    .changes
                    .get(key)
                    .ok_or_else(|| Error::new(ErrorCode::Error))?;
                encode_change(&mut data, table, change)?;
            }
            data.push(CHANGESET_END);
        }

        Ok(Changeset { data })
    }

    fn patchset(&self) -> Result<Patchset> {
        let changeset = self.changeset()?;
        let patchset = Patchset::from_changeset(&changeset)?;
        Ok(patchset)
    }

    fn changeset_size(&self) -> i64 {
        if !self.changeset_size_enabled {
            return 0;
        }
        self.changeset().map(|c| c.data.len() as i64).unwrap_or(0)
    }

    fn memory_used(&self) -> i64 {
        let mut total = self.db_name.len() as i64;
        for table in self.tables.values() {
            total += table.name.len() as i64;
            total += table.pk_cols.len() as i64;
            for (key, change) in &table.changes {
                total += key.len() as i64;
                total += change_values_bytes(change) as i64;
            }
        }
        total
    }
}

// ============================================================================
// Changeset Helpers
// ============================================================================

impl Changeset {
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn iter(&self) -> ChangesetIter<'_> {
        ChangesetIter {
            data: &self.data,
            pos: 0,
            header: None,
        }
    }

    pub fn invert(&self) -> Result<Changeset> {
        let mut data = Vec::new();
        let mut iter = self.iter();

        while let Some(change) = iter.next()? {
            encode_inverted_change(&mut data, &change)?;
        }

        Ok(Changeset { data })
    }
}

impl Patchset {
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn from_changeset(changeset: &Changeset) -> Result<Self> {
        let mut data = Vec::new();
        let mut iter = changeset.iter();
        while let Some(change) = iter.next()? {
            encode_patchset_change(&mut data, &change)?;
        }
        Ok(Self { data })
    }
}

impl<'a> ChangesetIter<'a> {
    pub fn next(&mut self) -> Result<Option<ChangesetChange>> {
        loop {
            if self.pos >= self.data.len() {
                return Ok(None);
            }

            if self.header.is_none() {
                self.header = Some(decode_table_header(self.data, &mut self.pos)?);
            }

            let header = self
                .header
                .clone()
                .ok_or_else(|| Error::new(ErrorCode::Error))?;
            if self.pos >= self.data.len() {
                return Ok(None);
            }

            let op_byte = self.data[self.pos];
            self.pos += 1;
            if op_byte == CHANGESET_END {
                self.header = None;
                continue;
            }

            let indirect = op_byte & CHANGESET_INDIRECT != 0;
            let op = match op_byte & !CHANGESET_INDIRECT {
                CHANGESET_INSERT => ChangeOp::Insert,
                CHANGESET_UPDATE => ChangeOp::Update,
                CHANGESET_DELETE => ChangeOp::Delete,
                _ => return Err(Error::new(ErrorCode::Error)),
            };

            let old = match op {
                ChangeOp::Insert => None,
                ChangeOp::Update | ChangeOp::Delete => Some(decode_values(
                    self.data,
                    &mut self.pos,
                    header.n_col as usize,
                )?),
            };
            let new = match op {
                ChangeOp::Delete => None,
                ChangeOp::Insert | ChangeOp::Update => Some(decode_values(
                    self.data,
                    &mut self.pos,
                    header.n_col as usize,
                )?),
            };

            return Ok(Some(ChangesetChange {
                table: header.table,
                op,
                n_col: header.n_col,
                pk: header.pk,
                old,
                new,
                indirect,
            }));
        }
    }
}

// ============================================================================
// Public API Functions
// ============================================================================

pub fn sqlite3session_create(conn: &mut SqliteConnection, db_name: &str) -> Result<Session> {
    Session::new(conn, db_name)
}

pub fn sqlite3session_delete(_session: Session) {}

pub fn sqlite3session_object_config(
    session: &mut Session,
    op: SessionObjConfig,
    value: &mut i32,
) -> Result<()> {
    if !session.tables.is_empty() || session.attach_all {
        return Err(Error::new(ErrorCode::Misuse));
    }

    match op {
        SessionObjConfig::Size => {
            if *value >= 0 {
                session.changeset_size_enabled = *value > 0;
            }
            *value = i32::from(session.changeset_size_enabled);
        }
        SessionObjConfig::RowId => {
            if *value >= 0 {
                session.track_rowid = *value > 0;
            }
            *value = i32::from(session.track_rowid);
        }
    }

    Ok(())
}

pub fn sqlite3session_enable(session: &mut Session, enable: i32) -> i32 {
    let prev = session.enabled;
    if enable >= 0 {
        session.enabled = enable > 0;
    }
    i32::from(prev)
}

pub fn sqlite3session_indirect(session: &mut Session, indirect: i32) -> i32 {
    let prev = session.indirect;
    if indirect >= 0 {
        session.indirect = indirect > 0;
    }
    i32::from(prev)
}

pub fn sqlite3session_attach(session: &mut Session, table: Option<&str>) -> Result<()> {
    match table {
        Some(name) => session.attach_table(name),
        None => {
            session.attach_all = true;
            let conn = unsafe { &*session.db };
            let db = conn
                .find_db(&session.db_name)
                .ok_or_else(|| Error::new(ErrorCode::Error))?;
            let schema = db
                .schema
                .as_ref()
                .ok_or_else(|| Error::new(ErrorCode::Error))?;
            let schema = schema.read().unwrap();
            let mut table_names: Vec<String> =
                schema.tables.keys().map(|name| name.to_string()).collect();
            table_names.sort();
            for name in table_names {
                session.attach_table(&name)?;
            }
            Ok(())
        }
    }
}

pub fn sqlite3session_table_filter<F>(session: &mut Session, filter: Option<F>)
where
    F: Fn(&str) -> bool + Send + Sync + 'static,
{
    session.table_filter = filter.map(|f| Box::new(f) as _);
}

pub fn sqlite3session_changeset(session: &Session) -> Result<Changeset> {
    session.changeset()
}

pub fn sqlite3session_patchset(session: &Session) -> Result<Patchset> {
    session.patchset()
}

pub fn sqlite3session_changeset_strm<F>(session: &Session, mut output: F) -> Result<()>
where
    F: FnMut(&[u8]) -> Result<()>,
{
    let changeset = session.changeset()?;
    stream_output(&changeset.data, &mut output)
}

pub fn sqlite3session_patchset_strm<F>(session: &Session, mut output: F) -> Result<()>
where
    F: FnMut(&[u8]) -> Result<()>,
{
    let patchset = session.patchset()?;
    stream_output(&patchset.data, &mut output)
}

pub fn sqlite3session_diff(
    _session: &mut Session,
    _from_db: &str,
    _to_db: &str,
) -> Result<Changeset> {
    Err(Error::with_message(
        ErrorCode::Internal,
        "sqlite3session_diff not yet implemented",
    ))
}

pub fn sqlite3session_isempty(session: &Session) -> i32 {
    i32::from(session.is_empty())
}

pub fn sqlite3session_memory_used(session: &Session) -> i64 {
    session.memory_used()
}

pub fn sqlite3session_changeset_size(session: &Session) -> i64 {
    session.changeset_size()
}

pub fn sqlite3session_config(op: SessionConfigOp, value: &mut i32) -> Result<()> {
    match op {
        SessionConfigOp::StrmSize => {
            if *value > 0 {
                STREAM_CHUNK_SIZE.store(*value as usize, Ordering::SeqCst);
            }
            *value = STREAM_CHUNK_SIZE.load(Ordering::SeqCst) as i32;
            Ok(())
        }
    }
}

pub fn sqlite3changeset_apply<F, G>(
    conn: &mut SqliteConnection,
    changeset: &Changeset,
    filter: Option<F>,
    conflict: Option<G>,
) -> Result<()>
where
    F: Fn(&str) -> bool,
    G: Fn(&ChangesetChange, ConflictType) -> ConflictAction,
{
    let mut iter = changeset.iter();
    while let Some(change) = iter.next()? {
        if let Some(ref filter_fn) = filter {
            if !filter_fn(&change.table) {
                continue;
            }
        }

        let result = apply_single_change(conn, &change);
        if let Err(err) = result {
            let conflict_type = classify_conflict(&err);
            if let Some(ref conflict_fn) = conflict {
                match conflict_fn(&change, conflict_type) {
                    ConflictAction::Omit => continue,
                    ConflictAction::Replace => {
                        apply_change_with_replace(conn, &change)?;
                    }
                    ConflictAction::Abort => return Err(err),
                }
            } else {
                return Err(err);
            }
        }
    }

    Ok(())
}

// ============================================================================
// Encoding / Decoding
// ============================================================================

fn encode_table_header(data: &mut Vec<u8>, table: &SessionTable) -> Result<()> {
    put_varint(data, table.name.len() as u64);
    data.extend_from_slice(table.name.as_bytes());
    put_varint(data, table.n_col as u64);
    for &pk in &table.pk_cols {
        data.push(if pk { 1 } else { 0 });
    }
    Ok(())
}

fn encode_change(data: &mut Vec<u8>, table: &SessionTable, change: &SessionChange) -> Result<()> {
    let op_byte = match change.op {
        ChangeOp::Insert => CHANGESET_INSERT,
        ChangeOp::Update => CHANGESET_UPDATE,
        ChangeOp::Delete => CHANGESET_DELETE,
    };
    let mut header = op_byte;
    if change.indirect {
        header |= CHANGESET_INDIRECT;
    }
    data.push(header);

    if let Some(ref old) = change.old {
        encode_values(data, table.n_col as usize, old)?;
    }
    if let Some(ref new) = change.new {
        encode_values(data, table.n_col as usize, new)?;
    }

    Ok(())
}

fn encode_inverted_change(data: &mut Vec<u8>, change: &ChangesetChange) -> Result<()> {
    let inverted_op = match change.op {
        ChangeOp::Insert => ChangeOp::Delete,
        ChangeOp::Delete => ChangeOp::Insert,
        ChangeOp::Update => ChangeOp::Update,
    };
    let header = ChangesetHeader {
        table: change.table.clone(),
        n_col: change.n_col,
        pk: change.pk.clone(),
    };
    let table = SessionTable {
        name: header.table.clone(),
        n_col: header.n_col,
        pk_cols: header.pk.clone(),
        changes: HashMap::new(),
    };
    encode_table_header(data, &table)?;
    let inverted = SessionChange {
        op: inverted_op,
        old: change.new.clone(),
        new: change.old.clone(),
        indirect: change.indirect,
    };
    encode_change(data, &table, &inverted)?;
    data.push(CHANGESET_END);
    Ok(())
}

fn encode_patchset_change(data: &mut Vec<u8>, change: &ChangesetChange) -> Result<()> {
    let header = ChangesetHeader {
        table: change.table.clone(),
        n_col: change.n_col,
        pk: change.pk.clone(),
    };
    let table = SessionTable {
        name: header.table.clone(),
        n_col: header.n_col,
        pk_cols: header.pk.clone(),
        changes: HashMap::new(),
    };
    encode_table_header(data, &table)?;

    let mut new_change = change.clone();
    if new_change.op == ChangeOp::Update {
        new_change.old = None;
    }
    encode_change(
        data,
        &table,
        &SessionChange {
            op: new_change.op,
            old: new_change.old.clone(),
            new: new_change.new.clone(),
            indirect: new_change.indirect,
        },
    )?;
    data.push(CHANGESET_END);
    Ok(())
}

fn decode_table_header(data: &[u8], pos: &mut usize) -> Result<ChangesetHeader> {
    let (name_len, consumed) = get_varint(&data[*pos..]);
    if consumed == 0 {
        return Err(Error::new(ErrorCode::Error));
    }
    *pos += consumed;
    let name_len = name_len as usize;
    if *pos + name_len > data.len() {
        return Err(Error::new(ErrorCode::Error));
    }
    let table = std::str::from_utf8(&data[*pos..*pos + name_len])
        .map_err(|_| Error::new(ErrorCode::Error))?
        .to_string();
    *pos += name_len;

    let (n_col, consumed) = get_varint(&data[*pos..]);
    if consumed == 0 {
        return Err(Error::new(ErrorCode::Error));
    }
    *pos += consumed;
    let n_col = n_col as usize;
    if *pos + n_col > data.len() {
        return Err(Error::new(ErrorCode::Error));
    }
    let mut pk = Vec::with_capacity(n_col);
    for i in 0..n_col {
        pk.push(data[*pos + i] != 0);
    }
    *pos += n_col;

    Ok(ChangesetHeader {
        table,
        n_col: n_col as i32,
        pk,
    })
}

fn encode_values(data: &mut Vec<u8>, n_col: usize, values: &[Value]) -> Result<()> {
    if values.len() != n_col {
        return Err(Error::new(ErrorCode::Mismatch));
    }
    for value in values {
        encode_value(data, value);
    }
    Ok(())
}

fn encode_value(data: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => data.push(VALUE_NULL),
        Value::Integer(v) => {
            data.push(VALUE_INTEGER);
            put_varint(data, encode_i64(*v));
        }
        Value::Real(v) => {
            data.push(VALUE_REAL);
            data.extend_from_slice(&v.to_be_bytes());
        }
        Value::Text(v) => {
            data.push(VALUE_TEXT);
            put_varint(data, v.len() as u64);
            data.extend_from_slice(v.as_bytes());
        }
        Value::Blob(v) => {
            data.push(VALUE_BLOB);
            put_varint(data, v.len() as u64);
            data.extend_from_slice(v);
        }
    }
}

fn decode_values(data: &[u8], pos: &mut usize, n_col: usize) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(n_col);
    for _ in 0..n_col {
        values.push(decode_value(data, pos)?);
    }
    Ok(values)
}

fn decode_value(data: &[u8], pos: &mut usize) -> Result<Value> {
    if *pos >= data.len() {
        return Err(Error::new(ErrorCode::Error));
    }
    let tag = data[*pos];
    *pos += 1;
    match tag {
        VALUE_NULL => Ok(Value::Null),
        VALUE_INTEGER => {
            let (value, consumed) = get_varint(&data[*pos..]);
            if consumed == 0 {
                return Err(Error::new(ErrorCode::Error));
            }
            *pos += consumed;
            Ok(Value::Integer(decode_i64(value)))
        }
        VALUE_REAL => {
            if *pos + 8 > data.len() {
                return Err(Error::new(ErrorCode::Error));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&data[*pos..*pos + 8]);
            *pos += 8;
            Ok(Value::Real(f64::from_be_bytes(buf)))
        }
        VALUE_TEXT | VALUE_BLOB => {
            let (len, consumed) = get_varint(&data[*pos..]);
            if consumed == 0 {
                return Err(Error::new(ErrorCode::Error));
            }
            *pos += consumed;
            let len = len as usize;
            if *pos + len > data.len() {
                return Err(Error::new(ErrorCode::Error));
            }
            let slice = &data[*pos..*pos + len];
            *pos += len;
            if tag == VALUE_TEXT {
                Ok(Value::Text(
                    std::str::from_utf8(slice)
                        .map_err(|_| Error::new(ErrorCode::Error))?
                        .to_string(),
                ))
            } else {
                Ok(Value::Blob(slice.to_vec()))
            }
        }
        _ => Err(Error::new(ErrorCode::Error)),
    }
}

// ============================================================================
// Apply Helpers
// ============================================================================

fn apply_single_change(conn: &mut SqliteConnection, change: &ChangesetChange) -> Result<()> {
    match change.op {
        ChangeOp::Insert => apply_insert(conn, change, false),
        ChangeOp::Update => apply_update(conn, change),
        ChangeOp::Delete => apply_delete(conn, change),
    }
}

fn apply_change_with_replace(conn: &mut SqliteConnection, change: &ChangesetChange) -> Result<()> {
    match change.op {
        ChangeOp::Insert => apply_insert(conn, change, true),
        ChangeOp::Update => apply_update(conn, change),
        ChangeOp::Delete => apply_delete(conn, change),
    }
}

fn apply_insert(
    conn: &mut SqliteConnection,
    change: &ChangesetChange,
    replace: bool,
) -> Result<()> {
    let columns = resolve_table_columns(conn, &change.table, change.n_col as usize)?;
    let placeholders = vec!["?"; columns.len()].join(", ");
    let verb = if replace {
        "INSERT OR REPLACE"
    } else {
        "INSERT"
    };
    let sql = format!(
        "{} INTO \"{}\" ({}) VALUES ({})",
        verb,
        change.table,
        columns.join(", "),
        placeholders
    );
    let (mut stmt, _) = sqlite3_prepare_v2(conn, &sql)?;
    let values = change
        .new
        .as_ref()
        .ok_or_else(|| Error::new(ErrorCode::Error))?;
    for (i, value) in values.iter().enumerate() {
        sqlite3_bind_value(&mut stmt, (i + 1) as i32, value)?;
    }
    let _ = sqlite3_step(&mut stmt)?;
    Ok(())
}

fn apply_delete(conn: &mut SqliteConnection, change: &ChangesetChange) -> Result<()> {
    let columns = resolve_table_columns(conn, &change.table, change.n_col as usize)?;
    let mut pk_cols = Vec::new();
    for (idx, is_pk) in change.pk.iter().enumerate() {
        if *is_pk {
            pk_cols.push((idx, columns[idx].clone()));
        }
    }
    if pk_cols.is_empty() {
        return Err(Error::new(ErrorCode::Error));
    }
    let mut where_parts = Vec::new();
    for (_, name) in &pk_cols {
        where_parts.push(format!("\"{}\" = ?", name));
    }
    let sql = format!(
        "DELETE FROM \"{}\" WHERE {}",
        change.table,
        where_parts.join(" AND ")
    );
    let (mut stmt, _) = sqlite3_prepare_v2(conn, &sql)?;
    let values = change
        .old
        .as_ref()
        .ok_or_else(|| Error::new(ErrorCode::Error))?;
    let mut bind_idx = 1;
    for (idx, _) in pk_cols {
        sqlite3_bind_value(&mut stmt, bind_idx, &values[idx])?;
        bind_idx += 1;
    }
    let _ = sqlite3_step(&mut stmt)?;
    Ok(())
}

fn apply_update(conn: &mut SqliteConnection, change: &ChangesetChange) -> Result<()> {
    let columns = resolve_table_columns(conn, &change.table, change.n_col as usize)?;
    let mut set_parts = Vec::new();
    let mut where_parts = Vec::new();
    let mut set_indices = Vec::new();
    let mut where_indices = Vec::new();

    for (idx, is_pk) in change.pk.iter().enumerate() {
        if *is_pk {
            where_parts.push(format!("\"{}\" = ?", columns[idx]));
            where_indices.push(idx);
        } else {
            set_parts.push(format!("\"{}\" = ?", columns[idx]));
            set_indices.push(idx);
        }
    }

    if where_parts.is_empty() {
        return Err(Error::new(ErrorCode::Error));
    }

    let sql = format!(
        "UPDATE \"{}\" SET {} WHERE {}",
        change.table,
        set_parts.join(", "),
        where_parts.join(" AND ")
    );
    let (mut stmt, _) = sqlite3_prepare_v2(conn, &sql)?;
    let new_values = change
        .new
        .as_ref()
        .ok_or_else(|| Error::new(ErrorCode::Error))?;
    let old_values = change.old.as_ref().unwrap_or(new_values);

    let mut bind_idx = 1;
    for idx in set_indices {
        sqlite3_bind_value(&mut stmt, bind_idx, &new_values[idx])?;
        bind_idx += 1;
    }
    for idx in where_indices {
        sqlite3_bind_value(&mut stmt, bind_idx, &old_values[idx])?;
        bind_idx += 1;
    }
    let _ = sqlite3_step(&mut stmt)?;
    Ok(())
}

fn resolve_table_columns(
    conn: &SqliteConnection,
    table_name: &str,
    n_col: usize,
) -> Result<Vec<String>> {
    let db = conn
        .find_db("main")
        .ok_or_else(|| Error::new(ErrorCode::Error))?;
    let schema = db
        .schema
        .as_ref()
        .ok_or_else(|| Error::new(ErrorCode::Error))?;
    let schema = schema.read().unwrap();
    let table = schema.table(table_name).ok_or_else(|| {
        Error::with_message(ErrorCode::Error, format!("no such table: {}", table_name))
    })?;
    let mut columns: Vec<String> = table.columns.iter().map(|col| col.name.clone()).collect();
    if columns.len() + 1 == n_col {
        columns.insert(0, "rowid".to_string());
    }
    if columns.len() != n_col {
        return Err(Error::new(ErrorCode::Mismatch));
    }
    Ok(columns)
}

fn classify_conflict(err: &Error) -> ConflictType {
    match err.code {
        ErrorCode::NotFound => ConflictType::NotFound,
        ErrorCode::Constraint => ConflictType::Constraint,
        ErrorCode::Error => ConflictType::Data,
        _ => ConflictType::Other,
    }
}

// ============================================================================
// Utility Helpers
// ============================================================================

fn table_pk_flags(table: &Table) -> (bool, Vec<bool>) {
    let mut pk = vec![false; table.columns.len()];
    let mut has_pk = false;
    if let Some(ref pk_cols) = table.primary_key {
        for &idx in pk_cols {
            if idx < pk.len() {
                pk[idx] = true;
                has_pk = true;
            }
        }
    } else {
        for (idx, col) in table.columns.iter().enumerate() {
            if col.is_primary_key {
                pk[idx] = true;
                has_pk = true;
            }
        }
    }
    (has_pk, pk)
}

fn pk_key_for_change(
    table: &SessionTable,
    op: ChangeOp,
    old: Option<&Vec<Value>>,
    new: Option<&Vec<Value>>,
) -> Result<Vec<u8>> {
    let values = match op {
        ChangeOp::Insert => new,
        ChangeOp::Update => old.or(new),
        ChangeOp::Delete => old,
    }
    .ok_or_else(|| Error::new(ErrorCode::Error))?;

    if values.len() != table.n_col as usize {
        return Err(Error::new(ErrorCode::Mismatch));
    }
    let mut key = Vec::new();
    for (idx, is_pk) in table.pk_cols.iter().enumerate() {
        if *is_pk {
            encode_value(&mut key, &values[idx]);
        }
    }
    Ok(key)
}

fn coalesce_change(
    existing: &mut SessionChange,
    op: ChangeOp,
    old: Option<Vec<Value>>,
    new: Option<Vec<Value>>,
    indirect: bool,
) {
    existing.indirect = existing.indirect && indirect;
    match (existing.op, op) {
        (ChangeOp::Insert, ChangeOp::Update) => {
            if let Some(new_values) = new {
                existing.new = Some(new_values);
            }
        }
        (ChangeOp::Update, ChangeOp::Delete) => {
            existing.op = ChangeOp::Delete;
            if existing.old.is_none() {
                existing.old = old;
            }
            existing.new = None;
        }
        (ChangeOp::Delete, ChangeOp::Insert) => {
            existing.op = ChangeOp::Update;
            existing.new = new;
        }
        (_, ChangeOp::Insert) => {
            if let Some(new_values) = new {
                existing.new = Some(new_values);
            }
        }
        (_, ChangeOp::Update) => {
            if let Some(new_values) = new {
                existing.new = Some(new_values);
            }
            if existing.old.is_none() {
                existing.old = old;
            }
        }
        (_, ChangeOp::Delete) => {
            existing.op = ChangeOp::Delete;
            if existing.old.is_none() {
                existing.old = old;
            }
            existing.new = None;
        }
    }
}

fn encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn decode_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

fn stream_output<F>(data: &[u8], output: &mut F) -> Result<()>
where
    F: FnMut(&[u8]) -> Result<()>,
{
    let chunk_size = STREAM_CHUNK_SIZE.load(Ordering::SeqCst).max(1);
    for chunk in data.chunks(chunk_size) {
        output(chunk)?;
    }
    Ok(())
}

fn change_values_bytes(change: &SessionChange) -> usize {
    let mut total = 0;
    if let Some(ref old) = change.old {
        total += values_bytes(old);
    }
    if let Some(ref new) = change.new {
        total += values_bytes(new);
    }
    total
}

fn values_bytes(values: &[Value]) -> usize {
    values.iter().map(|v| v.bytes()).sum()
}

// ============================================================================
// Changegroup Implementation
// ============================================================================

/// A changegroup combines multiple changesets or patchsets.
pub struct Changegroup {
    tables: HashMap<String, ChangeGroupTable>,
    table_order: Vec<String>,
}

#[derive(Debug, Clone)]
struct ChangeGroupTable {
    name: String,
    n_col: i32,
    pk: Vec<bool>,
    changes: HashMap<Vec<u8>, SessionChange>,
    change_order: Vec<Vec<u8>>,
}

impl Changegroup {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            table_order: Vec::new(),
        }
    }

    fn add_changeset(&mut self, changeset: &Changeset) -> Result<()> {
        let mut iter = changeset.iter();
        while let Some(change) = iter.next()? {
            self.add_change(&change)?;
        }
        Ok(())
    }

    fn add_change(&mut self, change: &ChangesetChange) -> Result<()> {
        let table_key = change.table.to_lowercase();

        // Create table entry if it doesn't exist
        if !self.tables.contains_key(&table_key) {
            self.tables.insert(
                table_key.clone(),
                ChangeGroupTable {
                    name: change.table.clone(),
                    n_col: change.n_col,
                    pk: change.pk.clone(),
                    changes: HashMap::new(),
                    change_order: Vec::new(),
                },
            );
            self.table_order.push(table_key.clone());
        }

        let table = self
            .tables
            .get_mut(&table_key)
            .ok_or_else(|| Error::new(ErrorCode::Error))?;

        // Validate schema consistency
        if table.n_col != change.n_col || table.pk != change.pk {
            return Err(Error::new(ErrorCode::Schema));
        }

        // Compute PK key for this change
        let pk_key = compute_pk_key_from_change(change)?;

        // Merge with existing change if present
        if let Some(existing) = table.changes.get(&pk_key) {
            let merged = merge_changes(existing, change)?;
            if let Some(merged_change) = merged {
                table.changes.insert(pk_key, merged_change);
            } else {
                // Changes cancel out - remove from table
                table.changes.remove(&pk_key);
                table.change_order.retain(|k| k != &pk_key);
            }
        } else {
            table.change_order.push(pk_key.clone());
            table.changes.insert(
                pk_key,
                SessionChange {
                    op: change.op,
                    old: change.old.clone(),
                    new: change.new.clone(),
                    indirect: change.indirect,
                },
            );
        }

        Ok(())
    }

    fn output(&self) -> Result<Changeset> {
        let mut data = Vec::new();

        for table_key in &self.table_order {
            if let Some(table) = self.tables.get(table_key) {
                if table.changes.is_empty() {
                    continue;
                }

                // Write table header
                put_varint(&mut data, table.name.len() as u64);
                data.extend_from_slice(table.name.as_bytes());
                put_varint(&mut data, table.n_col as u64);
                for &is_pk in &table.pk {
                    data.push(if is_pk { 1 } else { 0 });
                }

                // Write changes in order
                for pk_key in &table.change_order {
                    if let Some(change) = table.changes.get(pk_key) {
                        let op_byte = match change.op {
                            ChangeOp::Insert => CHANGESET_INSERT,
                            ChangeOp::Update => CHANGESET_UPDATE,
                            ChangeOp::Delete => CHANGESET_DELETE,
                        };
                        let mut header = op_byte;
                        if change.indirect {
                            header |= CHANGESET_INDIRECT;
                        }
                        data.push(header);

                        if let Some(ref old) = change.old {
                            for value in old {
                                encode_value(&mut data, value);
                            }
                        }
                        if let Some(ref new) = change.new {
                            for value in new {
                                encode_value(&mut data, value);
                            }
                        }
                    }
                }
                data.push(CHANGESET_END);
            }
        }

        Ok(Changeset { data })
    }
}

fn compute_pk_key_from_change(change: &ChangesetChange) -> Result<Vec<u8>> {
    let values = match change.op {
        ChangeOp::Insert => change.new.as_ref(),
        ChangeOp::Update => change.old.as_ref().or(change.new.as_ref()),
        ChangeOp::Delete => change.old.as_ref(),
    }
    .ok_or_else(|| Error::new(ErrorCode::Error))?;

    let mut key = Vec::new();
    for (idx, is_pk) in change.pk.iter().enumerate() {
        if *is_pk && idx < values.len() {
            encode_value(&mut key, &values[idx]);
        }
    }
    Ok(key)
}

/// Merge two changes on the same primary key.
/// Returns None if the changes cancel out.
fn merge_changes(existing: &SessionChange, new: &ChangesetChange) -> Result<Option<SessionChange>> {
    match (existing.op, new.op) {
        // INSERT + INSERT: Keep existing (shouldn't happen in practice)
        (ChangeOp::Insert, ChangeOp::Insert) => Ok(Some(existing.clone())),

        // INSERT + UPDATE: Update the INSERT's new values
        (ChangeOp::Insert, ChangeOp::Update) => {
            let merged_new = merge_values(existing.new.as_ref(), new.new.as_ref());
            Ok(Some(SessionChange {
                op: ChangeOp::Insert,
                old: None,
                new: Some(merged_new),
                indirect: existing.indirect && new.indirect,
            }))
        }

        // INSERT + DELETE: Cancel out
        (ChangeOp::Insert, ChangeOp::Delete) => Ok(None),

        // UPDATE + INSERT: Keep existing UPDATE (shouldn't happen in practice)
        (ChangeOp::Update, ChangeOp::Insert) => Ok(Some(existing.clone())),

        // UPDATE + UPDATE: Merge the updates
        (ChangeOp::Update, ChangeOp::Update) => {
            let merged_new = merge_values(existing.new.as_ref(), new.new.as_ref());
            Ok(Some(SessionChange {
                op: ChangeOp::Update,
                old: existing.old.clone(),
                new: Some(merged_new),
                indirect: existing.indirect && new.indirect,
            }))
        }

        // UPDATE + DELETE: Becomes DELETE with original old values
        (ChangeOp::Update, ChangeOp::Delete) => Ok(Some(SessionChange {
            op: ChangeOp::Delete,
            old: existing.old.clone(),
            new: None,
            indirect: existing.indirect && new.indirect,
        })),

        // DELETE + INSERT: Becomes UPDATE (or nothing if identical)
        (ChangeOp::Delete, ChangeOp::Insert) => {
            let old_vals = existing.old.as_ref();
            let new_vals = new.new.as_ref();
            if old_vals == new_vals {
                Ok(None) // Identical row restored
            } else {
                Ok(Some(SessionChange {
                    op: ChangeOp::Update,
                    old: existing.old.clone(),
                    new: new_vals.cloned(),
                    indirect: existing.indirect && new.indirect,
                }))
            }
        }

        // DELETE + UPDATE: Keep existing (shouldn't happen in practice)
        (ChangeOp::Delete, ChangeOp::Update) => Ok(Some(existing.clone())),

        // DELETE + DELETE: Keep existing (shouldn't happen in practice)
        (ChangeOp::Delete, ChangeOp::Delete) => Ok(Some(existing.clone())),
    }
}

fn merge_values(existing: Option<&Vec<Value>>, new: Option<&Vec<Value>>) -> Vec<Value> {
    match (existing, new) {
        (Some(e), Some(n)) => {
            let mut result = e.clone();
            for (i, val) in n.iter().enumerate() {
                if i < result.len() && !matches!(val, Value::Null) {
                    result[i] = val.clone();
                }
            }
            result
        }
        (Some(e), None) => e.clone(),
        (None, Some(n)) => n.clone(),
        (None, None) => Vec::new(),
    }
}

// ============================================================================
// Changegroup Public API
// ============================================================================

pub fn sqlite3changegroup_new() -> Changegroup {
    Changegroup::new()
}

pub fn sqlite3changegroup_add(group: &mut Changegroup, changeset: &Changeset) -> Result<()> {
    group.add_changeset(changeset)
}

pub fn sqlite3changegroup_add_change(
    group: &mut Changegroup,
    change: &ChangesetChange,
) -> Result<()> {
    group.add_change(change)
}

pub fn sqlite3changegroup_output(group: &Changegroup) -> Result<Changeset> {
    group.output()
}

pub fn sqlite3changegroup_delete(_group: Changegroup) {
    // Rust drops automatically
}

// ============================================================================
// Changeset Concat
// ============================================================================

pub fn sqlite3changeset_concat(a: &Changeset, b: &Changeset) -> Result<Changeset> {
    let mut group = sqlite3changegroup_new();
    sqlite3changegroup_add(&mut group, a)?;
    sqlite3changegroup_add(&mut group, b)?;
    sqlite3changegroup_output(&group)
}

// ============================================================================
// Rebaser Implementation
// ============================================================================

/// Rebase buffer entry representing a conflict resolution
#[derive(Debug, Clone)]
struct RebaseEntry {
    table: String,
    pk: Vec<u8>,
    action: ConflictAction,
    conflict_type: ConflictType,
    old_values: Option<Vec<Value>>,
    new_values: Option<Vec<Value>>,
}

/// Rebaser for adjusting changesets based on conflict resolutions
pub struct Rebaser {
    entries: Vec<RebaseEntry>,
}

impl Rebaser {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn configure(&mut self, rebase_data: &[u8]) -> Result<()> {
        // Parse rebase buffer format
        // Format: [table_name_len][table_name][pk_len][pk][action][conflict_type][old?][new?]...
        let mut pos = 0;
        while pos < rebase_data.len() {
            // Read table name
            let (name_len, consumed) = get_varint(&rebase_data[pos..]);
            if consumed == 0 {
                break;
            }
            pos += consumed;

            if pos + name_len as usize > rebase_data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let table = std::str::from_utf8(&rebase_data[pos..pos + name_len as usize])
                .map_err(|_| Error::new(ErrorCode::Corrupt))?
                .to_string();
            pos += name_len as usize;

            // Read PK
            let (pk_len, consumed) = get_varint(&rebase_data[pos..]);
            if consumed == 0 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            pos += consumed;

            if pos + pk_len as usize > rebase_data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let pk = rebase_data[pos..pos + pk_len as usize].to_vec();
            pos += pk_len as usize;

            // Read action and conflict type
            if pos + 2 > rebase_data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let action = match rebase_data[pos] {
                0 => ConflictAction::Omit,
                1 => ConflictAction::Replace,
                2 => ConflictAction::Abort,
                _ => return Err(Error::new(ErrorCode::Corrupt)),
            };
            pos += 1;

            let conflict_type = match rebase_data[pos] {
                1 => ConflictType::Data,
                2 => ConflictType::NotFound,
                3 => ConflictType::Constraint,
                _ => ConflictType::Other,
            };
            pos += 1;

            self.entries.push(RebaseEntry {
                table,
                pk,
                action,
                conflict_type,
                old_values: None,
                new_values: None,
            });
        }
        Ok(())
    }

    fn rebase(&self, changeset: &Changeset) -> Result<Changeset> {
        let mut output = Vec::new();
        let mut iter = changeset.iter();

        while let Some(change) = iter.next()? {
            let pk_key = compute_pk_key_from_change(&change)?;

            // Find matching rebase entry
            let entry = self
                .entries
                .iter()
                .find(|e| e.table.eq_ignore_ascii_case(&change.table) && e.pk == pk_key);

            let rebased = match entry {
                Some(entry) => self.rebase_change(&change, entry)?,
                None => Some(change.clone()),
            };

            if let Some(rebased_change) = rebased {
                encode_full_change(&mut output, &rebased_change)?;
            }
        }

        Ok(Changeset { data: output })
    }

    fn rebase_change(
        &self,
        change: &ChangesetChange,
        entry: &RebaseEntry,
    ) -> Result<Option<ChangesetChange>> {
        match entry.action {
            ConflictAction::Replace => {
                // For REPLACE, the local change is overridden
                match change.op {
                    ChangeOp::Insert => Ok(None),                 // INSERT overridden
                    ChangeOp::Update => Ok(None),                 // UPDATE overridden
                    ChangeOp::Delete => Ok(Some(change.clone())), // Keep DELETE
                }
            }
            ConflictAction::Omit => {
                // For OMIT, adjust the change based on conflict type
                match (change.op, entry.conflict_type) {
                    (ChangeOp::Insert, ConflictType::Data) => {
                        // INSERT conflicted - convert to UPDATE
                        Ok(Some(ChangesetChange {
                            table: change.table.clone(),
                            op: ChangeOp::Update,
                            n_col: change.n_col,
                            pk: change.pk.clone(),
                            old: entry.new_values.clone(),
                            new: change.new.clone(),
                            indirect: change.indirect,
                        }))
                    }
                    (ChangeOp::Delete, ConflictType::NotFound) => {
                        // DELETE target not found - omit
                        Ok(None)
                    }
                    _ => Ok(Some(change.clone())),
                }
            }
            ConflictAction::Abort => {
                // Abort should not appear in rebase data
                Ok(Some(change.clone()))
            }
        }
    }
}

fn encode_full_change(data: &mut Vec<u8>, change: &ChangesetChange) -> Result<()> {
    // Write table header
    put_varint(data, change.table.len() as u64);
    data.extend_from_slice(change.table.as_bytes());
    put_varint(data, change.n_col as u64);
    for &is_pk in &change.pk {
        data.push(if is_pk { 1 } else { 0 });
    }

    // Write change
    let op_byte = match change.op {
        ChangeOp::Insert => CHANGESET_INSERT,
        ChangeOp::Update => CHANGESET_UPDATE,
        ChangeOp::Delete => CHANGESET_DELETE,
    };
    let mut header = op_byte;
    if change.indirect {
        header |= CHANGESET_INDIRECT;
    }
    data.push(header);

    if let Some(ref old) = change.old {
        for value in old {
            encode_value(data, value);
        }
    }
    if let Some(ref new) = change.new {
        for value in new {
            encode_value(data, value);
        }
    }
    data.push(CHANGESET_END);

    Ok(())
}

// ============================================================================
// Rebaser Public API
// ============================================================================

pub fn sqlite3rebaser_create() -> Rebaser {
    Rebaser::new()
}

pub fn sqlite3rebaser_configure(rebaser: &mut Rebaser, rebase_data: &[u8]) -> Result<()> {
    rebaser.configure(rebase_data)
}

pub fn sqlite3rebaser_rebase(rebaser: &Rebaser, changeset: &Changeset) -> Result<Changeset> {
    rebaser.rebase(changeset)
}

pub fn sqlite3rebaser_delete(_rebaser: Rebaser) {
    // Rust drops automatically
}

// ============================================================================
// Streaming Input API
// ============================================================================

use std::io::Read;

/// Streaming changeset input for low-memory environments
pub struct ChangesetInputStream<R: Read> {
    reader: R,
    buffer: Vec<u8>,
    pos: usize,
    header: Option<ChangesetHeader>,
    exhausted: bool,
}

impl<R: Read> ChangesetInputStream<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(4096),
            pos: 0,
            header: None,
            exhausted: false,
        }
    }

    fn fill_buffer(&mut self, min_bytes: usize) -> Result<bool> {
        if self.exhausted {
            return Ok(self.buffer.len() - self.pos >= min_bytes);
        }

        // Compact buffer if needed
        if self.pos > 0 {
            self.buffer.drain(..self.pos);
            self.pos = 0;
        }

        // Read more data if needed
        while self.buffer.len() < min_bytes {
            let chunk_size = STREAM_CHUNK_SIZE.load(Ordering::SeqCst).max(64);
            let old_len = self.buffer.len();
            self.buffer.resize(old_len + chunk_size, 0);

            match self.reader.read(&mut self.buffer[old_len..]) {
                Ok(0) => {
                    self.buffer.truncate(old_len);
                    self.exhausted = true;
                    break;
                }
                Ok(n) => {
                    self.buffer.truncate(old_len + n);
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => return Err(Error::new(ErrorCode::IoErr)),
            }
        }

        Ok(self.buffer.len() >= min_bytes)
    }

    pub fn next(&mut self) -> Result<Option<ChangesetChange>> {
        loop {
            // Ensure we have at least some data
            if !self.fill_buffer(1)? {
                return Ok(None);
            }

            if self.pos >= self.buffer.len() {
                return Ok(None);
            }

            // Parse table header if needed
            if self.header.is_none() {
                // Need enough data for header
                if !self.fill_buffer(self.pos + 16)? && self.pos >= self.buffer.len() {
                    return Ok(None);
                }
                self.header = Some(decode_table_header(&self.buffer, &mut self.pos)?);
            }

            let header = self
                .header
                .clone()
                .ok_or_else(|| Error::new(ErrorCode::Error))?;

            // Ensure we have data for next op
            if !self.fill_buffer(self.pos + 1)? {
                return Ok(None);
            }

            let op_byte = self.buffer[self.pos];
            self.pos += 1;

            if op_byte == CHANGESET_END {
                self.header = None;
                continue;
            }

            let indirect = op_byte & CHANGESET_INDIRECT != 0;
            let op = match op_byte & !CHANGESET_INDIRECT {
                CHANGESET_INSERT => ChangeOp::Insert,
                CHANGESET_UPDATE => ChangeOp::Update,
                CHANGESET_DELETE => ChangeOp::Delete,
                _ => return Err(Error::new(ErrorCode::Corrupt)),
            };

            // Estimate bytes needed for values
            let n_col = header.n_col as usize;
            let estimated_size = n_col * 10; // rough estimate
            self.fill_buffer(self.pos + estimated_size)?;

            let old = match op {
                ChangeOp::Insert => None,
                ChangeOp::Update | ChangeOp::Delete => {
                    Some(decode_values(&self.buffer, &mut self.pos, n_col)?)
                }
            };
            let new = match op {
                ChangeOp::Delete => None,
                ChangeOp::Insert | ChangeOp::Update => {
                    Some(decode_values(&self.buffer, &mut self.pos, n_col)?)
                }
            };

            return Ok(Some(ChangesetChange {
                table: header.table,
                op,
                n_col: header.n_col,
                pk: header.pk,
                old,
                new,
                indirect,
            }));
        }
    }
}

/// Apply a changeset from a stream
pub fn sqlite3changeset_apply_strm<R, F, G>(
    conn: &mut SqliteConnection,
    reader: R,
    filter: Option<F>,
    conflict: Option<G>,
) -> Result<()>
where
    R: Read,
    F: Fn(&str) -> bool,
    G: Fn(&ChangesetChange, ConflictType) -> ConflictAction,
{
    let mut stream = ChangesetInputStream::new(reader);
    while let Some(change) = stream.next()? {
        if let Some(ref filter_fn) = filter {
            if !filter_fn(&change.table) {
                continue;
            }
        }

        let result = apply_single_change(conn, &change);
        if let Err(err) = result {
            let conflict_type = classify_conflict(&err);
            if let Some(ref conflict_fn) = conflict {
                match conflict_fn(&change, conflict_type) {
                    ConflictAction::Omit => continue,
                    ConflictAction::Replace => {
                        apply_change_with_replace(conn, &change)?;
                    }
                    ConflictAction::Abort => return Err(err),
                }
            } else {
                return Err(err);
            }
        }
    }
    Ok(())
}

// ============================================================================
// Changeset Statistics
// ============================================================================

/// Statistics about a changeset
#[derive(Debug, Default, Clone)]
pub struct ChangesetStats {
    pub inserts: i32,
    pub updates: i32,
    pub deletes: i32,
    pub tables: i32,
    pub size: usize,
}

impl ChangesetStats {
    pub fn from_changeset(changeset: &Changeset) -> Result<Self> {
        let mut stats = ChangesetStats {
            size: changeset.data.len(),
            ..Default::default()
        };
        let mut seen_tables = std::collections::HashSet::new();
        let mut iter = changeset.iter();

        while let Some(change) = iter.next()? {
            if !seen_tables.contains(&change.table) {
                seen_tables.insert(change.table.clone());
                stats.tables += 1;
            }
            match change.op {
                ChangeOp::Insert => stats.inserts += 1,
                ChangeOp::Update => stats.updates += 1,
                ChangeOp::Delete => stats.deletes += 1,
            }
        }

        Ok(stats)
    }
}

pub fn sqlite3changeset_stats(changeset: &Changeset) -> Result<ChangesetStats> {
    ChangesetStats::from_changeset(changeset)
}

// ============================================================================
// Session Diff Implementation
// ============================================================================

pub fn sqlite3session_diff_table(
    session: &mut Session,
    from_db: &str,
    table_name: &str,
) -> Result<()> {
    // Attach the table if not already attached
    session.attach_table(table_name)?;

    let conn = unsafe { &mut *session.db };

    // Get table info
    let (n_col, pk_cols) = session.table_info(table_name)?;

    // Build column list for the table
    let columns = resolve_table_columns(conn, table_name, n_col as usize)?;
    let col_list = columns.join(", ");
    let pk_conditions: Vec<String> = pk_cols
        .iter()
        .enumerate()
        .filter(|(_, &is_pk)| is_pk)
        .map(|(i, _)| format!("t1.\"{}\" = t2.\"{}\"", columns[i], columns[i]))
        .collect();

    if pk_conditions.is_empty() {
        // No PK, can't diff
        return Ok(());
    }

    let pk_where = pk_conditions.join(" AND ");

    // Find rows in to_db but not in from_db (INSERT)
    let insert_sql = format!(
        "SELECT {} FROM \"{}\".\"{}\" t1 WHERE NOT EXISTS \
         (SELECT 1 FROM \"{}\".\"{}\" t2 WHERE {})",
        col_list, session.db_name, table_name, from_db, table_name, pk_where
    );
    let (mut stmt, _) = sqlite3_prepare_v2(conn, &insert_sql)?;
    loop {
        match sqlite3_step(&mut stmt)? {
            StepResult::Row => {
                let mut values = Vec::with_capacity(n_col as usize);
                for i in 0..n_col {
                    values.push(sqlite3_column_value(&stmt, i));
                }
                session.record_change(table_name, ChangeOp::Insert, None, Some(values))?;
            }
            StepResult::Done => break,
        }
    }

    // Find rows in from_db but not in to_db (DELETE)
    let delete_sql = format!(
        "SELECT {} FROM \"{}\".\"{}\" t2 WHERE NOT EXISTS \
         (SELECT 1 FROM \"{}\".\"{}\" t1 WHERE {})",
        col_list, from_db, table_name, session.db_name, table_name, pk_where
    );
    let (mut stmt, _) = sqlite3_prepare_v2(conn, &delete_sql)?;
    loop {
        match sqlite3_step(&mut stmt)? {
            StepResult::Row => {
                let mut values = Vec::with_capacity(n_col as usize);
                for i in 0..n_col {
                    values.push(sqlite3_column_value(&stmt, i));
                }
                session.record_change(table_name, ChangeOp::Delete, Some(values), None)?;
            }
            StepResult::Done => break,
        }
    }

    // Find rows that differ (UPDATE)
    let non_pk_cols: Vec<String> = columns
        .iter()
        .enumerate()
        .filter(|(i, _)| !pk_cols.get(*i).copied().unwrap_or(false))
        .map(|(_, name)| name.clone())
        .collect();

    if !non_pk_cols.is_empty() {
        let diff_conditions: Vec<String> = non_pk_cols
            .iter()
            .map(|col| format!("(t1.\"{}\" IS NOT t2.\"{}\")", col, col))
            .collect();
        let diff_where = diff_conditions.join(" OR ");

        let t1_cols: String = columns
            .iter()
            .map(|c| format!("t1.\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");
        let t2_cols: String = columns
            .iter()
            .map(|c| format!("t2.\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let update_sql = format!(
            "SELECT {}, {} FROM \"{}\".\"{}\" t1, \"{}\".\"{}\" t2 \
             WHERE {} AND ({})",
            t1_cols,
            t2_cols,
            session.db_name,
            table_name,
            from_db,
            table_name,
            pk_where,
            diff_where
        );
        let (mut stmt, _) = sqlite3_prepare_v2(conn, &update_sql)?;
        loop {
            match sqlite3_step(&mut stmt)? {
                StepResult::Row => {
                    let mut new_values = Vec::with_capacity(n_col as usize);
                    let mut old_values = Vec::with_capacity(n_col as usize);
                    for i in 0..n_col {
                        new_values.push(sqlite3_column_value(&stmt, i));
                    }
                    for i in 0..n_col {
                        old_values.push(sqlite3_column_value(&stmt, n_col + i));
                    }
                    session.record_change(
                        table_name,
                        ChangeOp::Update,
                        Some(old_values),
                        Some(new_values),
                    )?;
                }
                StepResult::Done => break,
            }
        }
    }

    Ok(())
}

// ============================================================================
// Changeset Utility Functions
// ============================================================================

/// Check if a changeset is empty
pub fn sqlite3changeset_is_empty(changeset: &Changeset) -> bool {
    changeset.data.is_empty()
}

/// Get the size of a changeset in bytes
pub fn sqlite3changeset_size(changeset: &Changeset) -> usize {
    changeset.data.len()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, Table};
    use std::sync::Arc;

    fn build_table() -> Table {
        let mut table = Table::new("t1");
        let mut col = Column::new("id");
        col.is_primary_key = true;
        table.columns.push(col);
        let mut col2 = Column::new("name");
        table.columns.push(col2);
        table
    }

    fn setup_conn_with_table() -> SqliteConnection {
        let mut conn = SqliteConnection::new();
        let schema = conn.find_db_mut("main").unwrap().schema.clone().unwrap();
        let mut schema = schema.write().unwrap();
        let table = build_table();
        schema
            .tables
            .insert(table.name.to_lowercase(), Arc::new(table));
        conn
    }

    #[test]
    fn test_attach_and_record_change() {
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
            )
            .unwrap();

        let changeset = sqlite3session_changeset(&session).unwrap();
        let mut iter = changeset.iter();
        let change = iter.next().unwrap().unwrap();
        assert_eq!(change.op, ChangeOp::Insert);
        assert_eq!(change.table, "t1");
    }

    #[test]
    fn test_changeset_invert() {
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        session
            .record_change(
                "t1",
                ChangeOp::Delete,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
                None,
            )
            .unwrap();

        let changeset = sqlite3session_changeset(&session).unwrap();
        let inverted = changeset.invert().unwrap();
        let mut iter = inverted.iter();
        let change = iter.next().unwrap().unwrap();
        assert_eq!(change.op, ChangeOp::Insert);
    }

    #[test]
    fn test_changegroup_basic() {
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        // First changeset: INSERT id=1
        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
            )
            .unwrap();
        let cs1 = sqlite3session_changeset(&session).unwrap();

        // Reset session for second changeset
        drop(session);
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        // Second changeset: INSERT id=2
        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(2), Value::Text("b".into())]),
            )
            .unwrap();
        let cs2 = sqlite3session_changeset(&session).unwrap();

        // Combine using changegroup
        let mut group = sqlite3changegroup_new();
        sqlite3changegroup_add(&mut group, &cs1).unwrap();
        sqlite3changegroup_add(&mut group, &cs2).unwrap();
        let combined = sqlite3changegroup_output(&group).unwrap();

        // Verify combined has 2 inserts
        let stats = sqlite3changeset_stats(&combined).unwrap();
        assert_eq!(stats.inserts, 2);
        assert_eq!(stats.tables, 1);
    }

    #[test]
    fn test_changegroup_insert_then_delete() {
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        // First changeset: INSERT id=1
        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
            )
            .unwrap();
        let cs1 = sqlite3session_changeset(&session).unwrap();

        // Reset session for second changeset
        drop(session);
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        // Second changeset: DELETE id=1
        session
            .record_change(
                "t1",
                ChangeOp::Delete,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
                None,
            )
            .unwrap();
        let cs2 = sqlite3session_changeset(&session).unwrap();

        // Combine - should cancel out
        let mut group = sqlite3changegroup_new();
        sqlite3changegroup_add(&mut group, &cs1).unwrap();
        sqlite3changegroup_add(&mut group, &cs2).unwrap();
        let combined = sqlite3changegroup_output(&group).unwrap();

        // Verify combined is empty
        assert!(sqlite3changeset_is_empty(&combined));
    }

    #[test]
    fn test_changeset_concat() {
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
            )
            .unwrap();
        let cs1 = sqlite3session_changeset(&session).unwrap();

        drop(session);
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(2), Value::Text("b".into())]),
            )
            .unwrap();
        let cs2 = sqlite3session_changeset(&session).unwrap();

        let combined = sqlite3changeset_concat(&cs1, &cs2).unwrap();
        let stats = sqlite3changeset_stats(&combined).unwrap();
        assert_eq!(stats.inserts, 2);
    }

    #[test]
    fn test_changeset_stats() {
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
            )
            .unwrap();
        session
            .record_change(
                "t1",
                ChangeOp::Update,
                Some(vec![Value::Integer(2), Value::Text("old".into())]),
                Some(vec![Value::Integer(2), Value::Text("new".into())]),
            )
            .unwrap();
        session
            .record_change(
                "t1",
                ChangeOp::Delete,
                Some(vec![Value::Integer(3), Value::Text("c".into())]),
                None,
            )
            .unwrap();

        let changeset = sqlite3session_changeset(&session).unwrap();
        let stats = sqlite3changeset_stats(&changeset).unwrap();

        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.updates, 1);
        assert_eq!(stats.deletes, 1);
        assert_eq!(stats.tables, 1);
        assert!(stats.size > 0);
    }

    #[test]
    fn test_changeset_size_and_empty() {
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        let empty_changeset = sqlite3session_changeset(&session).unwrap();
        assert!(sqlite3changeset_is_empty(&empty_changeset));
        assert_eq!(sqlite3changeset_size(&empty_changeset), 0);

        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
            )
            .unwrap();

        let changeset = sqlite3session_changeset(&session).unwrap();
        assert!(!sqlite3changeset_is_empty(&changeset));
        assert!(sqlite3changeset_size(&changeset) > 0);
    }

    #[test]
    fn test_rebaser_basic() {
        // Create a simple rebaser
        let rebaser = sqlite3rebaser_create();

        // Create a changeset
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(1), Value::Text("a".into())]),
            )
            .unwrap();

        let changeset = sqlite3session_changeset(&session).unwrap();

        // Rebase (with no configuration, should be a no-op)
        let rebased = sqlite3rebaser_rebase(&rebaser, &changeset).unwrap();

        // Verify the rebased changeset has the same content
        let mut iter = rebased.iter();
        let change = iter.next().unwrap().unwrap();
        assert_eq!(change.op, ChangeOp::Insert);
        assert_eq!(change.table, "t1");
    }

    #[test]
    fn test_streaming_input() {
        let mut conn = setup_conn_with_table();
        let mut session = sqlite3session_create(&mut conn, "main").unwrap();
        sqlite3session_attach(&mut session, Some("t1")).unwrap();

        session
            .record_change(
                "t1",
                ChangeOp::Insert,
                None,
                Some(vec![Value::Integer(1), Value::Text("test".into())]),
            )
            .unwrap();

        let changeset = sqlite3session_changeset(&session).unwrap();
        let data = changeset.data().to_vec();

        // Create a stream from the data
        let cursor = std::io::Cursor::new(data);
        let mut stream = ChangesetInputStream::new(cursor);

        // Read from stream
        let change = stream.next().unwrap().unwrap();
        assert_eq!(change.op, ChangeOp::Insert);
        assert_eq!(change.table, "t1");
        assert_eq!(change.n_col, 2);

        // No more changes
        assert!(stream.next().unwrap().is_none());
    }
}
