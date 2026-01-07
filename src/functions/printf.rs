//! printf() implementation translated from SQLite printf.c.

use bitflags::bitflags;

use crate::error::{Error, ErrorCode, Result};
use crate::types::Value;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct FormatFlags: u8 {
        const LEFT_ALIGN = 0x01;
        const PLUS_SIGN = 0x02;
        const SPACE_SIGN = 0x04;
        const ZERO_PAD = 0x08;
        const ALT_FORM = 0x10;
    }
}

#[derive(Debug, Clone)]
struct PrintfSpec {
    flags: FormatFlags,
    width: i32,
    precision: i32,
    specifier: char,
}

#[derive(Debug, Clone)]
pub struct StrAccum {
    buf: String,
    max_len: usize,
    accum_error: bool,
}

impl StrAccum {
    pub fn new(max_len: usize) -> Self {
        Self {
            buf: String::new(),
            max_len,
            accum_error: false,
        }
    }

    pub fn printf(&mut self, format: &str, args: &[Value]) -> Result<()> {
        let mut arg_idx = 0usize;
        let mut chars = format.chars().peekable();

        while let Some(c) = chars.next() {
            if c != '%' {
                self.append_char(c);
                continue;
            }

            let spec = self.parse_format(&mut chars, args, &mut arg_idx)?;
            if spec.specifier == '%' {
                self.append_char('%');
                continue;
            }

            let arg = args.get(arg_idx).ok_or_else(|| {
                Error::with_message(ErrorCode::Error, "not enough arguments for printf")
            })?;
            arg_idx += 1;
            let formatted = format_arg(&spec, arg)?;
            self.append_str(&formatted);
        }

        Ok(())
    }

    pub fn into_string(self) -> Result<String> {
        if self.accum_error {
            return Err(Error::with_message(ErrorCode::TooBig, "string too long"));
        }
        Ok(self.buf)
    }

    fn append_char(&mut self, c: char) {
        if self.buf.len() + c.len_utf8() <= self.max_len {
            self.buf.push(c);
        } else {
            self.accum_error = true;
        }
    }

    fn append_str(&mut self, s: &str) {
        if self.buf.len() + s.len() <= self.max_len {
            self.buf.push_str(s);
        } else {
            self.accum_error = true;
        }
    }

    fn parse_format(
        &self,
        chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
        args: &[Value],
        arg_idx: &mut usize,
    ) -> Result<PrintfSpec> {
        let mut spec = PrintfSpec {
            flags: FormatFlags::empty(),
            width: 0,
            precision: -1,
            specifier: ' ',
        };

        loop {
            match chars.peek().copied() {
                Some('-') => {
                    spec.flags.insert(FormatFlags::LEFT_ALIGN);
                    chars.next();
                }
                Some('+') => {
                    spec.flags.insert(FormatFlags::PLUS_SIGN);
                    chars.next();
                }
                Some(' ') => {
                    spec.flags.insert(FormatFlags::SPACE_SIGN);
                    chars.next();
                }
                Some('0') => {
                    spec.flags.insert(FormatFlags::ZERO_PAD);
                    chars.next();
                }
                Some('#') => {
                    spec.flags.insert(FormatFlags::ALT_FORM);
                    chars.next();
                }
                _ => break,
            }
        }

        if chars.peek() == Some(&'*') {
            chars.next();
            let w = args.get(*arg_idx).map(|v| v.to_i64()).unwrap_or(0);
            *arg_idx += 1;
            if w < 0 {
                spec.flags.insert(FormatFlags::LEFT_ALIGN);
                spec.width = (-w) as i32;
            } else {
                spec.width = w as i32;
            }
        } else {
            spec.width = parse_number(chars);
        }

        if chars.peek() == Some(&'.') {
            chars.next();
            if chars.peek() == Some(&'*') {
                chars.next();
                let p = args.get(*arg_idx).map(|v| v.to_i64()).unwrap_or(0);
                *arg_idx += 1;
                spec.precision = if p < 0 { -1 } else { p as i32 };
            } else {
                spec.precision = parse_number(chars);
            }
        }

        while matches!(chars.peek(), Some('l') | Some('h') | Some('L') | Some('z')) {
            chars.next();
        }

        spec.specifier = chars
            .next()
            .ok_or_else(|| Error::with_message(ErrorCode::Error, "incomplete format specifier"))?;

        Ok(spec)
    }
}

pub fn printf_format(format: &str, args: &[Value]) -> Result<String> {
    let mut accum = StrAccum::new(usize::MAX);
    accum.printf(format, args)?;
    accum.into_string()
}

pub fn snprintf(max_len: usize, format: &str, args: &[Value]) -> Result<String> {
    let mut accum = StrAccum::new(max_len);
    accum.printf(format, args)?;
    accum.into_string()
}

fn parse_number(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> i32 {
    let mut n = 0i32;
    while let Some(c) = chars.peek().copied() {
        if c.is_ascii_digit() {
            n = n * 10 + (c as i32 - '0' as i32);
            chars.next();
        } else {
            break;
        }
    }
    n
}

fn format_arg(spec: &PrintfSpec, arg: &Value) -> Result<String> {
    match spec.specifier {
        'd' | 'i' => format_integer(spec, arg.to_i64(), 10, true, false),
        'u' => format_unsigned(spec, arg.to_i64() as u64, 10, false),
        'x' | 'X' => format_unsigned(spec, arg.to_i64() as u64, 16, spec.specifier == 'X'),
        'o' => format_unsigned(spec, arg.to_i64() as u64, 8, false),
        'f' | 'F' => format_float(spec, arg.to_f64(), 'f'),
        'e' | 'E' => format_float(spec, arg.to_f64(), spec.specifier),
        'g' | 'G' => format_float(spec, arg.to_f64(), spec.specifier),
        's' => format_string(spec, &arg.to_text()),
        'c' => {
            let c = std::char::from_u32(arg.to_i64() as u32).unwrap_or('\u{FFFD}');
            format_string(spec, &c.to_string())
        }
        'p' => format_pointer(spec, arg.to_i64() as u64),
        'q' => format_sql_escape(spec, &arg.to_text()),
        'Q' => format_sql_quote(spec, arg),
        'w' => format_sql_ident(spec, &arg.to_text()),
        _ => Err(Error::with_message(
            ErrorCode::Error,
            format!("unknown format specifier: {}", spec.specifier),
        )),
    }
}

fn format_integer(
    spec: &PrintfSpec,
    value: i64,
    base: u32,
    signed: bool,
    upper: bool,
) -> Result<String> {
    let mut sign = "";
    let mut unsigned = value as i128;
    if signed && value < 0 {
        sign = "-";
        unsigned = -(value as i128);
    } else if signed && spec.flags.contains(FormatFlags::PLUS_SIGN) {
        sign = "+";
    } else if signed && spec.flags.contains(FormatFlags::SPACE_SIGN) {
        sign = " ";
    }

    let mut digits = if base == 10 {
        format!("{}", unsigned)
    } else {
        let mut buf = String::new();
        let mut n = unsigned as u128;
        if n == 0 {
            buf.push('0');
        } else {
            while n > 0 {
                let d = (n % base as u128) as u32;
                let ch = std::char::from_digit(d, base).unwrap_or('0');
                buf.push(if upper { ch.to_ascii_uppercase() } else { ch });
                n /= base as u128;
            }
            buf = buf.chars().rev().collect();
        }
        buf
    };

    if spec.precision >= 0 {
        let prec = spec.precision as usize;
        while digits.len() < prec {
            digits.insert(0, '0');
        }
    }

    let mut prefix = String::new();
    if spec.flags.contains(FormatFlags::ALT_FORM) {
        if base == 16 && digits != "0" {
            prefix.push_str(if upper { "0X" } else { "0x" });
        } else if base == 8 && !digits.starts_with('0') {
            prefix.push('0');
        }
    }

    let combined = format!("{}{}{}", sign, prefix, digits);
    Ok(apply_width(spec, &combined, sign.len() + prefix.len()))
}

fn format_unsigned(spec: &PrintfSpec, value: u64, base: u32, upper: bool) -> Result<String> {
    let mut digits = if base == 10 {
        format!("{}", value)
    } else {
        let mut buf = String::new();
        let mut n = value;
        if n == 0 {
            buf.push('0');
        } else {
            while n > 0 {
                let d = (n % base as u64) as u32;
                let ch = std::char::from_digit(d, base).unwrap_or('0');
                buf.push(if upper { ch.to_ascii_uppercase() } else { ch });
                n /= base as u64;
            }
            buf = buf.chars().rev().collect();
        }
        buf
    };

    if spec.precision >= 0 {
        let prec = spec.precision as usize;
        while digits.len() < prec {
            digits.insert(0, '0');
        }
    }

    let mut prefix = String::new();
    if spec.flags.contains(FormatFlags::ALT_FORM) {
        if base == 16 && digits != "0" {
            prefix.push_str(if upper { "0X" } else { "0x" });
        } else if base == 8 && !digits.starts_with('0') {
            prefix.push('0');
        }
    }

    let combined = format!("{}{}", prefix, digits);
    Ok(apply_width(spec, &combined, prefix.len()))
}

fn format_float(spec: &PrintfSpec, value: f64, mode: char) -> Result<String> {
    let precision = if spec.precision >= 0 {
        spec.precision as usize
    } else {
        6
    };

    let mut sign = "";
    if value >= 0.0 {
        if spec.flags.contains(FormatFlags::PLUS_SIGN) {
            sign = "+";
        } else if spec.flags.contains(FormatFlags::SPACE_SIGN) {
            sign = " ";
        }
    }

    let mut body = match mode {
        'e' => format!("{:.*e}", precision, value),
        'E' => format!("{:.*E}", precision, value),
        'f' | 'F' => format!("{:.*}", precision, value),
        'g' => format!("{:.*}", precision, value),
        'G' => format!("{:.*}", precision, value).to_uppercase(),
        _ => format!("{:.*}", precision, value),
    };

    if mode == 'F' {
        body = body.to_uppercase();
    }

    let combined = format!("{}{}", sign, body);
    Ok(apply_width(spec, &combined, sign.len()))
}

fn format_string(spec: &PrintfSpec, value: &str) -> Result<String> {
    let mut s = value.to_string();
    if spec.precision >= 0 {
        let limit = spec.precision as usize;
        s = s.chars().take(limit).collect();
    }
    Ok(apply_width(spec, &s, 0))
}

fn format_pointer(spec: &PrintfSpec, value: u64) -> Result<String> {
    let s = format!("0x{:x}", value);
    Ok(apply_width(spec, &s, 2))
}

fn format_sql_escape(spec: &PrintfSpec, value: &str) -> Result<String> {
    let escaped = value.replace('\'', "''");
    Ok(apply_width(spec, &escaped, 0))
}

fn format_sql_quote(spec: &PrintfSpec, value: &Value) -> Result<String> {
    if value.is_null() {
        return Ok(apply_width(spec, "NULL", 0));
    }
    let escaped = value.to_text().replace('\'', "''");
    let quoted = format!("'{}'", escaped);
    Ok(apply_width(spec, &quoted, 0))
}

fn format_sql_ident(spec: &PrintfSpec, value: &str) -> Result<String> {
    let escaped = value.replace('"', "\"\"");
    let quoted = format!("\"{}\"", escaped);
    Ok(apply_width(spec, &quoted, 0))
}

fn apply_width(spec: &PrintfSpec, input: &str, prefix_len: usize) -> String {
    let width = spec.width.max(0) as usize;
    if input.len() >= width {
        return input.to_string();
    }
    let pad_len = width - input.len();
    let pad_char = if spec.flags.contains(FormatFlags::ZERO_PAD) && spec.precision < 0 {
        '0'
    } else {
        ' '
    };
    if spec.flags.contains(FormatFlags::LEFT_ALIGN) {
        let mut out = String::with_capacity(width);
        out.push_str(input);
        for _ in 0..pad_len {
            out.push(pad_char);
        }
        return out;
    }
    if pad_char == '0' && prefix_len > 0 {
        let mut out = String::with_capacity(width);
        let (prefix, rest) = input.split_at(prefix_len);
        out.push_str(prefix);
        for _ in 0..pad_len {
            out.push(pad_char);
        }
        out.push_str(rest);
        return out;
    }
    let mut out = String::with_capacity(width);
    for _ in 0..pad_len {
        out.push(pad_char);
    }
    out.push_str(input);
    out
}

pub fn mprintf(format: &str, args: &[Value]) -> Result<String> {
    printf_format(format, args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_printf_basic() {
        let args = vec![Value::Text("x".to_string()), Value::Integer(3)];
        let out = printf_format("hello %s %d", &args).unwrap();
        assert_eq!(out, "hello x 3");
    }

    #[test]
    fn test_printf_sql_quote() {
        let args = vec![Value::Text("a'b".to_string())];
        let out = printf_format("%Q", &args).unwrap();
        assert_eq!(out, "'a''b'");
    }
}
