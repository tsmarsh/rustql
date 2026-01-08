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
use crate::types::Value;
use crate::vdbe::aux::{get_varint, put_varint};

use super::connection::SqliteConnection;
use super::stmt::{sqlite3_bind_value, sqlite3_prepare_v2, sqlite3_step};

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
}
