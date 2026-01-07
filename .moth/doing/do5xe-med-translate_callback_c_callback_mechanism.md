# Translate callback.c - Callback Mechanism

## Overview
Translate callback infrastructure for function lookup, collation sequences, and authorization.

## Source Reference
- `sqlite3/src/callback.c` - ~600 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Function Lookup
```rust
/// Function signature for lookup
pub struct FuncKey {
    /// Function name (case-insensitive)
    pub name: String,
    /// Number of arguments (-1 for any)
    pub n_arg: i32,
    /// Text encoding
    pub encoding: TextEncoding,
}

impl FuncKey {
    pub fn new(name: &str, n_arg: i32, encoding: TextEncoding) -> Self {
        Self {
            name: name.to_lowercase(),
            n_arg,
            encoding,
        }
    }
}

impl Hash for FuncKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.n_arg.hash(state);
    }
}

impl PartialEq for FuncKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.n_arg == other.n_arg
    }
}

impl Eq for FuncKey {}
```

### Collation Sequence
```rust
/// Collation comparison function
pub type CollationCmp = fn(&[u8], &[u8]) -> std::cmp::Ordering;

/// Collation sequence
pub struct Collation {
    /// Collation name
    pub name: String,
    /// Text encoding
    pub encoding: TextEncoding,
    /// Comparison function
    pub cmp: CollationCmp,
    /// User data
    pub user_data: Option<*mut ()>,
    /// Destructor for user data
    pub destroy: Option<fn(*mut ())>,
}

impl Collation {
    pub fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        (self.cmp)(a, b)
    }
}

impl Drop for Collation {
    fn drop(&mut self) {
        if let (Some(destroy), Some(data)) = (self.destroy, self.user_data) {
            destroy(data);
        }
    }
}
```

### Authorization
```rust
/// Authorization actions
#[derive(Debug, Clone, Copy)]
pub enum AuthAction {
    CreateIndex = 1,
    CreateTable = 2,
    CreateTempIndex = 3,
    CreateTempTable = 4,
    CreateTempTrigger = 5,
    CreateTempView = 6,
    CreateTrigger = 7,
    CreateView = 8,
    Delete = 9,
    DropIndex = 10,
    DropTable = 11,
    DropTempIndex = 12,
    DropTempTable = 13,
    DropTempTrigger = 14,
    DropTempView = 15,
    DropTrigger = 16,
    DropView = 17,
    Insert = 18,
    Pragma = 19,
    Read = 20,
    Select = 21,
    Transaction = 22,
    Update = 23,
    Attach = 24,
    Detach = 25,
    AlterTable = 26,
    Reindex = 27,
    Analyze = 28,
    CreateVtable = 29,
    DropVtable = 30,
    Function = 31,
    Savepoint = 32,
    Recursive = 33,
}

/// Authorization callback result
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AuthResult {
    /// Allow the operation
    Ok = 0,
    /// Deny with error
    Deny = 1,
    /// Silently ignore
    Ignore = 2,
}

/// Authorization callback type
pub type AuthCallback = fn(
    action: AuthAction,
    arg1: Option<&str>,
    arg2: Option<&str>,
    arg3: Option<&str>,
    arg4: Option<&str>,
) -> AuthResult;
```

## Function Lookup

```rust
impl Connection {
    /// Find a function by name and argument count
    pub fn find_function(&self, name: &str, n_arg: i32) -> Option<&FuncDef> {
        // Try exact match first
        let key = FuncKey::new(name, n_arg, self.encoding);
        if let Some(func) = self.functions.get(&key) {
            return Some(func);
        }

        // Try variadic (-1 args)
        let key_any = FuncKey::new(name, -1, self.encoding);
        if let Some(func) = self.functions.get(&key_any) {
            return Some(func);
        }

        // Try built-in functions
        self.find_builtin_function(name, n_arg)
    }

    /// Register a scalar function
    pub fn create_function(
        &mut self,
        name: &str,
        n_arg: i32,
        flags: FuncFlags,
        user_data: Option<*mut ()>,
        func: ScalarFunc,
        destroy: Option<fn(*mut ())>,
    ) -> Result<()> {
        let key = FuncKey::new(name, n_arg, self.encoding);

        let func_def = FuncDef {
            n_arg: n_arg as i8,
            flags,
            user_data: user_data.map(|p| unsafe { Box::from_raw(p as *mut ()) }),
            name: Box::leak(name.to_lowercase().into_boxed_str()),
            x_func: Some(func),
            x_step: None,
            x_final: None,
            x_inverse: None,
            x_value: None,
        };

        self.functions.insert(key, func_def);
        Ok(())
    }

    /// Register an aggregate function
    pub fn create_aggregate(
        &mut self,
        name: &str,
        n_arg: i32,
        flags: FuncFlags,
        user_data: Option<*mut ()>,
        step: AggStep,
        finalize: AggFinal,
    ) -> Result<()> {
        let key = FuncKey::new(name, n_arg, self.encoding);

        let func_def = FuncDef {
            n_arg: n_arg as i8,
            flags,
            user_data: user_data.map(|p| unsafe { Box::from_raw(p as *mut ()) }),
            name: Box::leak(name.to_lowercase().into_boxed_str()),
            x_func: None,
            x_step: Some(step),
            x_final: Some(finalize),
            x_inverse: None,
            x_value: None,
        };

        self.functions.insert(key, func_def);
        Ok(())
    }

    /// Register a window function
    pub fn create_window_function(
        &mut self,
        name: &str,
        n_arg: i32,
        flags: FuncFlags,
        step: AggStep,
        finalize: AggFinal,
        value: AggFinal,
        inverse: AggStep,
    ) -> Result<()> {
        let key = FuncKey::new(name, n_arg, self.encoding);

        let func_def = FuncDef {
            n_arg: n_arg as i8,
            flags,
            user_data: None,
            name: Box::leak(name.to_lowercase().into_boxed_str()),
            x_func: None,
            x_step: Some(step),
            x_final: Some(finalize),
            x_inverse: Some(inverse),
            x_value: Some(value),
        };

        self.functions.insert(key, func_def);
        Ok(())
    }
}
```

## Collation Handling

```rust
impl Connection {
    /// Find a collation by name
    pub fn find_collation(&self, name: &str) -> Option<Arc<Collation>> {
        self.collations.get(&name.to_uppercase()).cloned()
    }

    /// Register a collation
    pub fn create_collation(
        &mut self,
        name: &str,
        encoding: TextEncoding,
        user_data: Option<*mut ()>,
        cmp: CollationCmp,
        destroy: Option<fn(*mut ())>,
    ) -> Result<()> {
        let collation = Collation {
            name: name.to_string(),
            encoding,
            cmp,
            user_data,
            destroy,
        };

        self.collations.insert(name.to_uppercase(), Arc::new(collation));
        Ok(())
    }

    /// Register needed collation callback
    pub fn collation_needed(
        &mut self,
        callback: impl Fn(&Connection, &str) + Send + Sync + 'static,
    ) {
        self.collation_needed_callback = Some(Box::new(callback));
    }

    /// Request a missing collation
    fn request_collation(&self, name: &str) -> Option<Arc<Collation>> {
        if let Some(ref callback) = self.collation_needed_callback {
            callback(self, name);
            // Try again after callback
            self.collations.get(&name.to_uppercase()).cloned()
        } else {
            None
        }
    }
}

/// Built-in collations
pub fn register_builtin_collations(conn: &mut Connection) {
    // BINARY - byte comparison
    conn.create_collation(
        "BINARY",
        TextEncoding::Utf8,
        None,
        |a, b| a.cmp(b),
        None,
    ).ok();

    // NOCASE - case-insensitive ASCII
    conn.create_collation(
        "NOCASE",
        TextEncoding::Utf8,
        None,
        |a, b| {
            let a_str = String::from_utf8_lossy(a).to_lowercase();
            let b_str = String::from_utf8_lossy(b).to_lowercase();
            a_str.cmp(&b_str)
        },
        None,
    ).ok();

    // RTRIM - trailing space ignored
    conn.create_collation(
        "RTRIM",
        TextEncoding::Utf8,
        None,
        |a, b| {
            let a_trimmed = std::str::from_utf8(a)
                .map(|s| s.trim_end())
                .unwrap_or_default();
            let b_trimmed = std::str::from_utf8(b)
                .map(|s| s.trim_end())
                .unwrap_or_default();
            a_trimmed.cmp(b_trimmed)
        },
        None,
    ).ok();
}
```

## Authorization

```rust
impl Connection {
    /// Set the authorization callback
    pub fn set_authorizer(&mut self, auth: Option<AuthCallback>) {
        self.authorizer = auth;
    }

    /// Check authorization for an action
    pub fn authorize(
        &self,
        action: AuthAction,
        arg1: Option<&str>,
        arg2: Option<&str>,
        arg3: Option<&str>,
        arg4: Option<&str>,
    ) -> AuthResult {
        match &self.authorizer {
            Some(auth) => auth(action, arg1, arg2, arg3, arg4),
            None => AuthResult::Ok,
        }
    }
}

impl<'a> Parse<'a> {
    /// Check authorization during compilation
    fn auth_check(&self, action: AuthAction, arg1: &str, arg2: Option<&str>) -> Result<()> {
        let result = self.conn.authorize(
            action,
            Some(arg1),
            arg2,
            Some(&self.db_name()),
            None,
        );

        match result {
            AuthResult::Ok => Ok(()),
            AuthResult::Deny => Err(Error::with_code(ErrorCode::Auth)),
            AuthResult::Ignore => {
                // For read operations, return NULL instead of actual value
                // This is handled at the codegen level
                Ok(())
            }
        }
    }

    /// Check read authorization for a column
    fn auth_read(&self, table: &str, column: &str) -> Result<AuthResult> {
        let result = self.conn.authorize(
            AuthAction::Read,
            Some(table),
            Some(column),
            Some(&self.db_name()),
            None,
        );

        if result == AuthResult::Deny {
            return Err(Error::with_message(
                ErrorCode::Auth,
                format!("not authorized to read {}.{}", table, column)
            ));
        }

        Ok(result)
    }
}
```

## Busy Handler

```rust
impl Connection {
    /// Set busy handler callback
    pub fn busy_handler(
        &mut self,
        handler: Option<impl Fn(i32) -> bool + Send + Sync + 'static>,
    ) {
        self.busy_handler = handler.map(|h| Box::new(h) as Box<dyn Fn(i32) -> bool + Send + Sync>);
    }

    /// Set busy timeout
    pub fn busy_timeout(&mut self, ms: i32) {
        self.busy_timeout_ms = ms;
        if ms > 0 {
            self.busy_handler = Some(Box::new(move |count| {
                let delay = if count < 12 {
                    (count + 1) * (count + 1)
                } else {
                    100
                };
                std::thread::sleep(std::time::Duration::from_millis(delay as u64));
                (count * delay) < ms
            }));
        } else {
            self.busy_handler = None;
        }
    }

    /// Invoke busy handler
    pub fn invoke_busy_handler(&self, count: i32) -> bool {
        match &self.busy_handler {
            Some(handler) => handler(count),
            None => false,
        }
    }
}
```

## Acceptance Criteria
- [ ] Function lookup by name and arg count
- [ ] Scalar function registration
- [ ] Aggregate function registration
- [ ] Window function registration
- [ ] Collation lookup by name
- [ ] Collation registration
- [ ] Built-in collations (BINARY, NOCASE, RTRIM)
- [ ] Collation needed callback
- [ ] Authorization callback
- [ ] All AuthAction types
- [ ] AuthResult handling (Ok, Deny, Ignore)
- [ ] Busy handler callback
- [ ] Busy timeout
- [ ] Progress handler callback
