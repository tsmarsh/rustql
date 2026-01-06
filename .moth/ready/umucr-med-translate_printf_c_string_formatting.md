# Translate printf.c - String Formatting

## Overview
Translate SQLite's printf implementation for formatted string output including SQL-specific extensions.

## Source Reference
- `sqlite3/src/printf.c` - 1,558 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Format Info
```rust
/// Printf format specification
struct PrintfSpec {
    /// Flags
    pub flags: FormatFlags,
    /// Minimum field width
    pub width: i32,
    /// Precision
    pub precision: i32,
    /// Conversion specifier
    pub specifier: char,
}

bitflags! {
    pub struct FormatFlags: u8 {
        const LEFT_ALIGN = 0x01;   // '-'
        const PLUS_SIGN = 0x02;    // '+'
        const SPACE_SIGN = 0x04;  // ' '
        const ZERO_PAD = 0x08;     // '0'
        const ALT_FORM = 0x10;     // '#'
    }
}

/// Accumulator for building output string
pub struct StrAccum {
    /// Output buffer
    pub buf: String,
    /// Maximum allowed length
    pub max_len: usize,
    /// Has an error occurred?
    pub accum_error: bool,
    /// Memory allocation limit
    pub mem_limit: usize,
}
```

## Printf Implementation

### Main Function
```rust
impl StrAccum {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            max_len: usize::MAX,
            accum_error: false,
            mem_limit: 1_000_000_000, // 1GB default
        }
    }

    /// Append formatted string
    pub fn printf(&mut self, format: &str, args: &[&Value]) -> Result<()> {
        let mut arg_idx = 0;
        let mut chars = format.chars().peekable();

        while let Some(c) = chars.next() {
            if c != '%' {
                self.append_char(c);
                continue;
            }

            // Parse format specification
            let spec = self.parse_format(&mut chars, args, &mut arg_idx)?;

            if spec.specifier == '%' {
                self.append_char('%');
                continue;
            }

            // Get the argument
            let arg = args.get(arg_idx).ok_or_else(|| {
                Error::msg("not enough arguments for printf")
            })?;
            arg_idx += 1;

            // Format the argument
            self.format_arg(&spec, arg)?;
        }

        Ok(())
    }

    fn parse_format(
        &self,
        chars: &mut std::iter::Peekable<std::str::Chars>,
        args: &[&Value],
        arg_idx: &mut usize,
    ) -> Result<PrintfSpec> {
        let mut spec = PrintfSpec {
            flags: FormatFlags::empty(),
            width: 0,
            precision: -1,
            specifier: ' ',
        };

        // Parse flags
        loop {
            match chars.peek() {
                Some('-') => { spec.flags.insert(FormatFlags::LEFT_ALIGN); chars.next(); }
                Some('+') => { spec.flags.insert(FormatFlags::PLUS_SIGN); chars.next(); }
                Some(' ') => { spec.flags.insert(FormatFlags::SPACE_SIGN); chars.next(); }
                Some('0') => { spec.flags.insert(FormatFlags::ZERO_PAD); chars.next(); }
                Some('#') => { spec.flags.insert(FormatFlags::ALT_FORM); chars.next(); }
                _ => break,
            }
        }

        // Parse width
        if chars.peek() == Some(&'*') {
            chars.next();
            let w = args.get(*arg_idx).map(|v| v.as_int()).unwrap_or(0);
            *arg_idx += 1;
            if w < 0 {
                spec.flags.insert(FormatFlags::LEFT_ALIGN);
                spec.width = (-w) as i32;
            } else {
                spec.width = w as i32;
            }
        } else {
            spec.width = self.parse_number(chars);
        }

        // Parse precision
        if chars.peek() == Some(&'.') {
            chars.next();
            if chars.peek() == Some(&'*') {
                chars.next();
                spec.precision = args.get(*arg_idx).map(|v| v.as_int() as i32).unwrap_or(0);
                *arg_idx += 1;
            } else {
                spec.precision = self.parse_number(chars);
            }
        }

        // Skip length modifiers (we handle all as 64-bit)
        while matches!(chars.peek(), Some('l') | Some('h') | Some('L') | Some('z')) {
            chars.next();
        }

        // Get specifier
        spec.specifier = chars.next().ok_or_else(|| {
            Error::msg("incomplete format specifier")
        })?;

        Ok(spec)
    }

    fn parse_number(&self, chars: &mut std::iter::Peekable<std::str::Chars>) -> i32 {
        let mut n = 0i32;
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                n = n * 10 + (c as i32 - '0' as i32);
                chars.next();
            } else {
                break;
            }
        }
        n
    }
}
```

### Format Specifiers
```rust
impl StrAccum {
    fn format_arg(&mut self, spec: &PrintfSpec, arg: &Value) -> Result<()> {
        match spec.specifier {
            'd' | 'i' => self.format_decimal(spec, arg.as_int()),
            'u' => self.format_unsigned(spec, arg.as_int() as u64),
            'x' | 'X' => self.format_hex(spec, arg.as_int() as u64),
            'o' => self.format_octal(spec, arg.as_int() as u64),
            'f' | 'F' => self.format_float(spec, arg.as_real()),
            'e' | 'E' => self.format_exp(spec, arg.as_real()),
            'g' | 'G' => self.format_general(spec, arg.as_real()),
            's' => self.format_string(spec, arg.as_str()),
            'c' => self.format_char(spec, arg.as_int() as u8 as char),
            'p' => self.format_pointer(spec, arg),
            'n' => Ok(()), // Not supported for security
            'q' => self.format_sql_escaped(spec, arg.as_str()),
            'Q' => self.format_sql_quoted(spec, arg.as_str()),
            'w' => self.format_sql_identifier(spec, arg.as_str()),
            'z' => self.format_sqlite_free(spec, arg),
            _ => Err(Error::msg(format!("unknown format specifier: {}", spec.specifier))),
        }
    }

    fn format_decimal(&mut self, spec: &PrintfSpec, value: i64) -> Result<()> {
        let abs_val = value.abs() as u64;
        let negative = value < 0;

        let mut digits = String::new();
        let mut n = abs_val;
        if n == 0 {
            digits.push('0');
        } else {
            while n > 0 {
                digits.push(char::from_digit((n % 10) as u32, 10).unwrap());
                n /= 10;
            }
        }
        digits = digits.chars().rev().collect();

        // Apply precision (minimum digits)
        let precision = if spec.precision >= 0 { spec.precision as usize } else { 1 };
        while digits.len() < precision {
            digits.insert(0, '0');
        }

        // Add sign
        let sign = if negative {
            "-"
        } else if spec.flags.contains(FormatFlags::PLUS_SIGN) {
            "+"
        } else if spec.flags.contains(FormatFlags::SPACE_SIGN) {
            " "
        } else {
            ""
        };

        let formatted = format!("{}{}", sign, digits);
        self.apply_width(spec, &formatted);
        Ok(())
    }

    fn format_hex(&mut self, spec: &PrintfSpec, value: u64) -> Result<()> {
        let hex_str = if spec.specifier == 'X' {
            format!("{:X}", value)
        } else {
            format!("{:x}", value)
        };

        let prefix = if spec.flags.contains(FormatFlags::ALT_FORM) && value != 0 {
            if spec.specifier == 'X' { "0X" } else { "0x" }
        } else {
            ""
        };

        let formatted = format!("{}{}", prefix, hex_str);
        self.apply_width(spec, &formatted);
        Ok(())
    }

    fn format_float(&mut self, spec: &PrintfSpec, value: f64) -> Result<()> {
        let precision = if spec.precision >= 0 { spec.precision as usize } else { 6 };

        let formatted = if value.is_nan() {
            "NaN".to_string()
        } else if value.is_infinite() {
            if value > 0.0 { "Inf" } else { "-Inf" }.to_string()
        } else {
            format!("{:.prec$}", value, prec = precision)
        };

        self.apply_width(spec, &formatted);
        Ok(())
    }

    fn format_string(&mut self, spec: &PrintfSpec, value: &str) -> Result<()> {
        let s = if spec.precision >= 0 {
            &value[..std::cmp::min(spec.precision as usize, value.len())]
        } else {
            value
        };

        self.apply_width(spec, s);
        Ok(())
    }
}
```

### SQL-Specific Formats
```rust
impl StrAccum {
    /// %q - SQL string escape (double single quotes)
    fn format_sql_escaped(&mut self, spec: &PrintfSpec, value: &str) -> Result<()> {
        let escaped = value.replace("'", "''");
        self.apply_width(spec, &escaped);
        Ok(())
    }

    /// %Q - SQL string literal with quotes, or NULL
    fn format_sql_quoted(&mut self, spec: &PrintfSpec, value: &str) -> Result<()> {
        if value.is_empty() {
            self.append_str("NULL");
        } else {
            let escaped = value.replace("'", "''");
            let quoted = format!("'{}'", escaped);
            self.apply_width(spec, &quoted);
        }
        Ok(())
    }

    /// %w - SQL identifier escape (double double quotes)
    fn format_sql_identifier(&mut self, spec: &PrintfSpec, value: &str) -> Result<()> {
        let escaped = value.replace("\"", "\"\"");
        let quoted = format!("\"{}\"", escaped);
        self.apply_width(spec, &quoted);
        Ok(())
    }

    fn apply_width(&mut self, spec: &PrintfSpec, s: &str) {
        let width = spec.width as usize;

        if s.len() >= width {
            self.append_str(s);
        } else {
            let padding = width - s.len();
            let pad_char = if spec.flags.contains(FormatFlags::ZERO_PAD) { '0' } else { ' ' };

            if spec.flags.contains(FormatFlags::LEFT_ALIGN) {
                self.append_str(s);
                for _ in 0..padding {
                    self.append_char(pad_char);
                }
            } else {
                for _ in 0..padding {
                    self.append_char(pad_char);
                }
                self.append_str(s);
            }
        }
    }

    fn append_str(&mut self, s: &str) {
        if self.buf.len() + s.len() <= self.max_len {
            self.buf.push_str(s);
        } else {
            self.accum_error = true;
        }
    }

    fn append_char(&mut self, c: char) {
        if self.buf.len() < self.max_len {
            self.buf.push(c);
        } else {
            self.accum_error = true;
        }
    }
}
```

### SQL printf Function
```rust
fn printf_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args.is_empty() {
        ctx.result_null();
        return Ok(());
    }

    let format = args[0].as_str();
    let mut accum = StrAccum::new();

    accum.printf(format, &args[1..])?;

    if accum.accum_error {
        ctx.result_error("string too long");
    } else {
        ctx.result_text(&accum.buf);
    }

    Ok(())
}

fn format_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    // Alias for printf
    printf_func(ctx, args)
}
```

### Error Logging
```rust
impl Connection {
    /// Log error message using printf format
    pub fn error_log(&self, format: &str, args: &[&dyn std::fmt::Display]) {
        let mut accum = StrAccum::new();
        // Format the message
        if let Ok(()) = accum.printf_display(format, args) {
            eprintln!("SQLite error: {}", accum.buf);
        }
    }

    /// mprintf - malloc'd printf
    pub fn mprintf(format: &str, args: &[&Value]) -> Result<String> {
        let mut accum = StrAccum::new();
        accum.printf(format, args)?;
        Ok(accum.buf)
    }

    /// snprintf - bounded printf
    pub fn snprintf(buf: &mut String, n: usize, format: &str, args: &[&Value]) -> Result<usize> {
        let mut accum = StrAccum::new();
        accum.max_len = n;
        accum.printf(format, args)?;
        *buf = accum.buf.clone();
        Ok(accum.buf.len())
    }
}
```

## Acceptance Criteria
- [ ] Basic format specifiers: %d, %i, %u, %x, %X, %o
- [ ] Floating point: %f, %F, %e, %E, %g, %G
- [ ] String: %s, %c
- [ ] Width and precision
- [ ] Flags: -, +, space, 0, #
- [ ] SQL escape: %q (escape single quotes)
- [ ] SQL quote: %Q (quote string or NULL)
- [ ] SQL identifier: %w (escape double quotes)
- [ ] Star width/precision (*)
- [ ] printf() SQL function
- [ ] format() SQL function (alias)
- [ ] mprintf() - malloc'd result
- [ ] snprintf() - bounded output
- [ ] Error handling for malformed formats
