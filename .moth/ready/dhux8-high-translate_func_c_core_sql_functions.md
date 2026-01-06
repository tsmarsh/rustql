# Translate func.c - Core SQL Functions

## Overview
Translate built-in SQL functions including string manipulation, math, aggregate functions, and type conversion.

## Source Reference
- `sqlite3/src/func.c` - 3,461 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Function Definition
```rust
pub struct FuncDef {
    /// Number of arguments (-1 for variadic)
    pub n_arg: i8,
    /// Function flags (SQLITE_FUNC_*)
    pub flags: FuncFlags,
    /// User data passed to function
    pub user_data: Option<Box<dyn Any>>,
    /// Function name
    pub name: &'static str,
    /// Scalar function implementation
    pub x_func: Option<ScalarFunc>,
    /// Step function for aggregates
    pub x_step: Option<AggStep>,
    /// Finalize function for aggregates
    pub x_final: Option<AggFinal>,
    /// Inverse for window functions
    pub x_inverse: Option<AggStep>,
    /// Value function for window aggregates
    pub x_value: Option<AggFinal>,
}

bitflags! {
    pub struct FuncFlags: u32 {
        const DETERMINISTIC = 0x0800;
        const INNOCUOUS = 0x200000;
        const SUBTYPE = 0x100000;
        const NEEDCOLL = 0x0020;
        const LIKE = 0x0004;
        const LENGTH = 0x0040;
        const TYPEOF = 0x0080;
        const COUNT = 0x0100;
        const UNLIKELY = 0x0400;
        const CONSTANT = 0x2000;
        const MINMAX = 0x1000;
    }
}

pub type ScalarFunc = fn(&mut Context, &[&Value]) -> Result<()>;
pub type AggStep = fn(&mut Context, &[&Value]) -> Result<()>;
pub type AggFinal = fn(&mut Context) -> Result<()>;
```

### Context for Functions
```rust
pub struct Context {
    /// Output value
    pub result: Value,
    /// Aggregate context (for step functions)
    pub agg: Option<Box<dyn Any>>,
    /// Error message
    pub error: Option<String>,
    /// Collating sequence
    pub coll: Option<Arc<Collation>>,
    /// Database connection
    pub db: *mut Connection,
}

impl Context {
    pub fn result_int(&mut self, v: i64);
    pub fn result_double(&mut self, v: f64);
    pub fn result_text(&mut self, v: &str);
    pub fn result_blob(&mut self, v: &[u8]);
    pub fn result_null(&mut self);
    pub fn result_error(&mut self, msg: &str);

    pub fn aggregate_context<T: Default>(&mut self) -> &mut T;
}
```

## Built-in Functions

### String Functions
```rust
fn length_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    match &args[0] {
        Value::Null => ctx.result_null(),
        Value::Blob(b) => ctx.result_int(b.len() as i64),
        Value::Text(s) => ctx.result_int(s.chars().count() as i64),
        _ => ctx.result_int(args[0].to_string().chars().count() as i64),
    }
    Ok(())
}

fn substr_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let s = args[0].as_str();
    let start = args[1].as_int() as usize;
    let len = args.get(2).map(|v| v.as_int() as usize);

    let chars: Vec<char> = s.chars().collect();
    let start_idx = if start > 0 { start - 1 } else { 0 };

    let result: String = match len {
        Some(n) => chars.iter().skip(start_idx).take(n).collect(),
        None => chars.iter().skip(start_idx).collect(),
    };

    ctx.result_text(&result);
    Ok(())
}

fn upper_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    ctx.result_text(&args[0].as_str().to_uppercase());
    Ok(())
}

fn lower_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    ctx.result_text(&args[0].as_str().to_lowercase());
    Ok(())
}

fn trim_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let s = args[0].as_str();
    let chars = args.get(1).map(|v| v.as_str()).unwrap_or(" ");
    ctx.result_text(s.trim_matches(|c| chars.contains(c)));
    Ok(())
}

fn replace_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let s = args[0].as_str();
    let from = args[1].as_str();
    let to = args[2].as_str();
    ctx.result_text(&s.replace(from, to));
    Ok(())
}

fn instr_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let haystack = args[0].as_str();
    let needle = args[1].as_str();
    match haystack.find(needle) {
        Some(pos) => ctx.result_int((pos + 1) as i64),
        None => ctx.result_int(0),
    }
    Ok(())
}
```

### Math Functions
```rust
fn abs_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    match &args[0] {
        Value::Null => ctx.result_null(),
        Value::Integer(i) => ctx.result_int(i.abs()),
        Value::Real(r) => ctx.result_double(r.abs()),
        _ => ctx.result_double(args[0].as_real().abs()),
    }
    Ok(())
}

fn round_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let v = args[0].as_real();
    let precision = args.get(1).map(|a| a.as_int()).unwrap_or(0) as i32;
    let multiplier = 10f64.powi(precision);
    ctx.result_double((v * multiplier).round() / multiplier);
    Ok(())
}

fn random_func(ctx: &mut Context, _args: &[&Value]) -> Result<()> {
    ctx.result_int(rand::random::<i64>());
    Ok(())
}

fn randomblob_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let n = args[0].as_int() as usize;
    let mut blob = vec![0u8; n];
    rand::fill(&mut blob);
    ctx.result_blob(&blob);
    Ok(())
}
```

### Type Functions
```rust
fn typeof_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let type_name = match &args[0] {
        Value::Null => "null",
        Value::Integer(_) => "integer",
        Value::Real(_) => "real",
        Value::Text(_) => "text",
        Value::Blob(_) => "blob",
    };
    ctx.result_text(type_name);
    Ok(())
}

fn coalesce_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    for arg in args {
        if !arg.is_null() {
            ctx.set_result((*arg).clone());
            return Ok(());
        }
    }
    ctx.result_null();
    Ok(())
}

fn nullif_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args[0] == args[1] {
        ctx.result_null();
    } else {
        ctx.set_result(args[0].clone());
    }
    Ok(())
}

fn ifnull_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args[0].is_null() {
        ctx.set_result(args[1].clone());
    } else {
        ctx.set_result(args[0].clone());
    }
    Ok(())
}
```

### Aggregate Functions
```rust
struct SumAgg {
    sum: f64,
    count: i64,
    is_int: bool,
    has_value: bool,
}

fn sum_step(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let agg: &mut SumAgg = ctx.aggregate_context();

    if !args[0].is_null() {
        agg.has_value = true;
        match &args[0] {
            Value::Integer(i) => {
                agg.sum += *i as f64;
            }
            Value::Real(r) => {
                agg.sum += r;
                agg.is_int = false;
            }
            _ => {}
        }
    }
    Ok(())
}

fn sum_final(ctx: &mut Context) -> Result<()> {
    let agg: &SumAgg = ctx.aggregate_context();
    if !agg.has_value {
        ctx.result_null();
    } else if agg.is_int && agg.sum >= i64::MIN as f64 && agg.sum <= i64::MAX as f64 {
        ctx.result_int(agg.sum as i64);
    } else {
        ctx.result_double(agg.sum);
    }
    Ok(())
}

fn count_step(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let count: &mut i64 = ctx.aggregate_context();
    if args.is_empty() || !args[0].is_null() {
        *count += 1;
    }
    Ok(())
}

fn count_final(ctx: &mut Context) -> Result<()> {
    let count: &i64 = ctx.aggregate_context();
    ctx.result_int(*count);
    Ok(())
}

fn avg_final(ctx: &mut Context) -> Result<()> {
    let agg: &SumAgg = ctx.aggregate_context();
    if agg.count == 0 {
        ctx.result_null();
    } else {
        ctx.result_double(agg.sum / agg.count as f64);
    }
    Ok(())
}
```

### Hex/Blob Functions
```rust
fn hex_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let bytes = args[0].as_blob();
    let hex: String = bytes.iter().map(|b| format!("{:02X}", b)).collect();
    ctx.result_text(&hex);
    Ok(())
}

fn unhex_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let s = args[0].as_str();
    match hex::decode(s) {
        Ok(bytes) => ctx.result_blob(&bytes),
        Err(_) => ctx.result_null(),
    }
    Ok(())
}

fn zeroblob_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let n = args[0].as_int() as usize;
    ctx.result_blob(&vec![0u8; n]);
    Ok(())
}

fn quote_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    match &args[0] {
        Value::Null => ctx.result_text("NULL"),
        Value::Integer(i) => ctx.result_text(&i.to_string()),
        Value::Real(r) => ctx.result_text(&r.to_string()),
        Value::Text(s) => {
            let escaped = s.replace("'", "''");
            ctx.result_text(&format!("'{}'", escaped));
        }
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            ctx.result_text(&format!("X'{}'", hex));
        }
    }
    Ok(())
}
```

## Function Registration
```rust
impl Connection {
    pub fn register_builtin_functions(&mut self) {
        let functions = [
            // String functions
            FuncDef::scalar("length", 1, FuncFlags::DETERMINISTIC, length_func),
            FuncDef::scalar("substr", -1, FuncFlags::DETERMINISTIC, substr_func),
            FuncDef::scalar("upper", 1, FuncFlags::DETERMINISTIC, upper_func),
            FuncDef::scalar("lower", 1, FuncFlags::DETERMINISTIC, lower_func),
            FuncDef::scalar("trim", -1, FuncFlags::DETERMINISTIC, trim_func),
            FuncDef::scalar("ltrim", -1, FuncFlags::DETERMINISTIC, ltrim_func),
            FuncDef::scalar("rtrim", -1, FuncFlags::DETERMINISTIC, rtrim_func),
            FuncDef::scalar("replace", 3, FuncFlags::DETERMINISTIC, replace_func),
            FuncDef::scalar("instr", 2, FuncFlags::DETERMINISTIC, instr_func),

            // Math functions
            FuncDef::scalar("abs", 1, FuncFlags::DETERMINISTIC, abs_func),
            FuncDef::scalar("round", -1, FuncFlags::DETERMINISTIC, round_func),
            FuncDef::scalar("random", 0, FuncFlags::empty(), random_func),
            FuncDef::scalar("randomblob", 1, FuncFlags::empty(), randomblob_func),

            // Type functions
            FuncDef::scalar("typeof", 1, FuncFlags::DETERMINISTIC, typeof_func),
            FuncDef::scalar("coalesce", -1, FuncFlags::DETERMINISTIC, coalesce_func),
            FuncDef::scalar("nullif", 2, FuncFlags::DETERMINISTIC, nullif_func),
            FuncDef::scalar("ifnull", 2, FuncFlags::DETERMINISTIC, ifnull_func),
            FuncDef::scalar("iif", 3, FuncFlags::DETERMINISTIC, iif_func),

            // Hex/blob
            FuncDef::scalar("hex", 1, FuncFlags::DETERMINISTIC, hex_func),
            FuncDef::scalar("unhex", -1, FuncFlags::DETERMINISTIC, unhex_func),
            FuncDef::scalar("zeroblob", 1, FuncFlags::DETERMINISTIC, zeroblob_func),
            FuncDef::scalar("quote", 1, FuncFlags::DETERMINISTIC, quote_func),

            // Aggregates
            FuncDef::aggregate("sum", 1, sum_step, sum_final),
            FuncDef::aggregate("total", 1, sum_step, total_final),
            FuncDef::aggregate("avg", 1, sum_step, avg_final),
            FuncDef::aggregate("count", -1, count_step, count_final),
            FuncDef::aggregate("min", 1, min_step, min_final),
            FuncDef::aggregate("max", 1, max_step, max_final),
            FuncDef::aggregate("group_concat", -1, group_concat_step, group_concat_final),
        ];

        for func in functions {
            self.functions.insert(func.name.to_lowercase(), func);
        }
    }
}
```

## Acceptance Criteria
- [ ] String functions: length, substr, upper, lower, trim, replace, instr
- [ ] Math functions: abs, round, random, randomblob
- [ ] Type functions: typeof, coalesce, nullif, ifnull, iif
- [ ] Aggregate functions: sum, total, avg, count, min, max, group_concat
- [ ] Blob functions: hex, unhex, zeroblob, quote
- [ ] LIKE pattern matching with % and _
- [ ] GLOB pattern matching
- [ ] printf function
- [ ] Custom function registration
- [ ] Deterministic function optimization
