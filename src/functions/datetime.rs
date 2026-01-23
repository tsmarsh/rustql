//! Date/time functions translated from SQLite date.c.

use crate::error::{Error, ErrorCode, Result};
use crate::types::Value;

const MS_PER_DAY: i64 = 86_400_000;
const JD_UNIX_EPOCH_MS: i64 = 210_866_760_000_000;

#[derive(Debug, Clone)]
struct DateTime {
    i_jd: i64,
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: f64,
    tz: i32,
    valid_jd: bool,
    valid_ymd: bool,
    valid_hms: bool,
    raw_s: bool,
    is_error: bool,
    use_subsec: bool,
    is_utc: bool,
    is_local: bool,
}

impl DateTime {
    fn new() -> Self {
        Self {
            i_jd: 0,
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0.0,
            tz: 0,
            valid_jd: false,
            valid_ymd: false,
            valid_hms: false,
            raw_s: false,
            is_error: false,
            use_subsec: false,
            is_utc: false,
            is_local: false,
        }
    }

    fn datetime_error(&mut self) {
        *self = DateTime::new();
        self.is_error = true;
    }

    fn parse_value(value: &Value) -> Result<Self> {
        let mut dt = DateTime::new();
        match value {
            Value::Text(text) => dt.parse_date_or_time(text)?,
            Value::Integer(i) => dt.set_raw_date_number(*i as f64),
            Value::Real(r) => dt.set_raw_date_number(*r),
            Value::Null => {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "cannot parse NULL as date/time",
                ))
            }
            _ => {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "invalid date/time argument",
                ))
            }
        }
        Ok(dt)
    }

    fn parse_date_or_time(&mut self, input: &str) -> Result<()> {
        let trimmed = input.trim();
        if self.parse_yyyy_mm_dd(trimmed)? {
            return Ok(());
        }
        if self.parse_hh_mm_ss(trimmed)? {
            return Ok(());
        }
        if trimmed.eq_ignore_ascii_case("now") {
            return self.set_current_time();
        }
        if let Ok(num) = trimmed.parse::<f64>() {
            self.set_raw_date_number(num);
            return Ok(());
        }
        Err(Error::with_message(
            ErrorCode::Error,
            format!("cannot parse date/time: {}", trimmed),
        ))
    }

    fn parse_yyyy_mm_dd(&mut self, input: &str) -> Result<bool> {
        let bytes = input.as_bytes();
        let mut idx = 0;

        // Check for leading + which is invalid
        if bytes.first() == Some(&b'+') {
            return Ok(false);
        }

        let neg = if bytes.first() == Some(&b'-') {
            idx += 1;
            true
        } else {
            false
        };

        // Year must be exactly 4 digits
        let year = match parse_fixed_digits(bytes, idx, 4) {
            Some(v) => v,
            None => return Ok(false),
        };
        idx += 4;
        if bytes.get(idx) != Some(&b'-') {
            return Ok(false);
        }
        idx += 1;

        // Month must be exactly 2 digits
        let month = match parse_fixed_digits(bytes, idx, 2) {
            Some(v) => v,
            None => return Ok(false),
        };
        if !(1..=12).contains(&month) {
            return Ok(false); // Invalid month - return false to signal parse failure
        }
        idx += 2;
        if bytes.get(idx) != Some(&b'-') {
            return Ok(false);
        }
        idx += 1;

        // Day must be exactly 2 digits
        let day = match parse_fixed_digits(bytes, idx, 2) {
            Some(v) => v,
            None => return Ok(false),
        };

        // Validate day against the actual month
        let year_val = if neg { -year } else { year };
        let max_day = days_in_month(year_val, month);
        if day < 1 || day > max_day {
            return Ok(false); // Invalid day for this month
        }
        idx += 2;
        let mut rest = &input[idx..];
        rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == 'T');
        if !rest.is_empty() {
            if !self.parse_hh_mm_ss(rest)? {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "invalid time suffix in date",
                ));
            }
        } else {
            self.valid_hms = false;
        }

        self.valid_jd = false;
        self.valid_ymd = true;
        self.year = if neg { -year } else { year };
        self.month = month;
        self.day = day;
        if self.tz != 0 {
            self.compute_jd();
        }
        Ok(true)
    }

    fn parse_hh_mm_ss(&mut self, input: &str) -> Result<bool> {
        let bytes = input.as_bytes();
        if bytes.len() < 5 {
            return Ok(false);
        }
        let hour = match parse_fixed_digits(bytes, 0, 2) {
            Some(v) => v,
            None => return Ok(false),
        };
        if hour > 24 {
            return Err(Error::with_message(ErrorCode::Error, "invalid hour"));
        }
        if bytes.get(2) != Some(&b':') {
            return Ok(false);
        }
        let minute = match parse_fixed_digits(bytes, 3, 2) {
            Some(v) => v,
            None => return Ok(false),
        };
        if minute > 59 {
            return Err(Error::with_message(ErrorCode::Error, "invalid minute"));
        }
        let mut second = 0.0;
        let mut idx = 5;
        if bytes.get(idx) == Some(&b':') {
            idx += 1;
            let sec_int = match parse_fixed_digits(bytes, idx, 2) {
                Some(v) => v,
                None => return Ok(false),
            };
            if sec_int > 59 {
                return Err(Error::with_message(ErrorCode::Error, "invalid second"));
            }
            second = sec_int as f64;
            idx += 2;
            if bytes.get(idx) == Some(&b'.') {
                idx += 1;
                let (frac, frac_len) = parse_fraction(bytes, idx);
                if frac_len == 0 {
                    return Ok(false);
                }
                second += frac;
                idx += frac_len;
            }
        }
        let rest = &input[idx..];
        self.parse_timezone(rest)?;

        self.valid_jd = false;
        self.raw_s = false;
        self.valid_hms = true;
        self.hour = hour;
        self.minute = minute;
        self.second = second;
        Ok(true)
    }

    fn parse_timezone(&mut self, input: &str) -> Result<()> {
        let mut s = input.trim();
        self.tz = 0;
        if s.is_empty() {
            return Ok(());
        }
        let first = s.as_bytes()[0];
        if first == b'Z' || first == b'z' {
            self.is_local = false;
            self.is_utc = true;
            s = &s[1..];
        } else if first == b'+' || first == b'-' {
            let sign = if first == b'-' { -1 } else { 1 };
            s = &s[1..];
            if s.len() < 5 {
                return Err(Error::with_message(ErrorCode::Error, "invalid timezone"));
            }
            let hour = parse_fixed_digits(s.as_bytes(), 0, 2)
                .ok_or_else(|| Error::with_message(ErrorCode::Error, "invalid timezone"))?;
            if s.as_bytes().get(2) != Some(&b':') {
                return Err(Error::with_message(ErrorCode::Error, "invalid timezone"));
            }
            let minute = parse_fixed_digits(s.as_bytes(), 3, 2)
                .ok_or_else(|| Error::with_message(ErrorCode::Error, "invalid timezone"))?;
            self.tz = sign * (hour * 60 + minute);
            s = &s[5..];
            if self.tz == 0 {
                self.is_local = false;
                self.is_utc = true;
            }
        } else {
            return Err(Error::with_message(ErrorCode::Error, "invalid timezone"));
        }
        if !s.trim().is_empty() {
            return Err(Error::with_message(ErrorCode::Error, "invalid timezone"));
        }
        Ok(())
    }

    fn set_current_time(&mut self) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::with_message(ErrorCode::Error, "system time before epoch"))?;
        let ms = now.as_secs() as i64 * 1000 + now.subsec_millis() as i64;
        self.i_jd = ms + JD_UNIX_EPOCH_MS;
        self.valid_jd = true;
        self.is_utc = true;
        self.is_local = false;
        self.clear_ymd_hms_tz();
        Ok(())
    }

    fn set_raw_date_number(&mut self, r: f64) {
        self.second = r;
        self.raw_s = true;
        if (0.0..5_373_484.5).contains(&r) {
            self.i_jd = (r * MS_PER_DAY as f64 + 0.5) as i64;
            self.valid_jd = true;
        }
    }

    fn clear_ymd_hms_tz(&mut self) {
        self.valid_ymd = false;
        self.valid_hms = false;
        self.tz = 0;
    }

    fn compute_jd(&mut self) {
        if self.valid_jd {
            return;
        }
        let (mut y, mut m, d) = if self.valid_ymd {
            (self.year, self.month, self.day)
        } else {
            (2000, 1, 1)
        };
        if !(-4713..=9999).contains(&y) || self.raw_s {
            self.datetime_error();
            return;
        }
        if m <= 2 {
            y -= 1;
            m += 12;
        }
        let a = (y + 4800) / 100;
        let b = 38 - a + (a / 4);
        let x1 = 36525 * (y + 4716) / 100;
        let x2 = 306001 * (m + 1) / 10000;
        self.i_jd = (((x1 + x2 + d + b) as f64 - 1524.5) * MS_PER_DAY as f64) as i64;
        self.valid_jd = true;
        if self.valid_hms {
            self.i_jd += self.hour as i64 * 3_600_000
                + self.minute as i64 * 60_000
                + (self.second * 1000.0 + 0.5) as i64;
            if self.tz != 0 {
                self.i_jd -= self.tz as i64 * 60_000;
                self.valid_ymd = false;
                self.valid_hms = false;
                self.tz = 0;
                self.is_utc = true;
                self.is_local = false;
            }
        }
    }

    fn compute_ymd(&mut self) {
        if self.valid_ymd {
            return;
        }
        if !self.valid_jd {
            self.year = 2000;
            self.month = 1;
            self.day = 1;
            self.valid_ymd = true;
            return;
        }
        let z = (self.i_jd + 43_200_000) / MS_PER_DAY;
        let alpha = ((z as f64 + 32044.75) / 36524.25) as i64 - 52;
        let a = z + 1 + alpha - ((alpha + 100) / 4) + 25;
        let b = a + 1524;
        let c = ((b as f64 - 122.1) / 365.25) as i64;
        let d = (36525 * (c & 32767)) / 100;
        let e = ((b - d) as f64 / 30.6001) as i64;
        let x1 = (30.6001 * e as f64) as i64;
        self.day = (b - d - x1) as i32;
        self.month = if e < 14 {
            (e - 1) as i32
        } else {
            (e - 13) as i32
        };
        self.year = if self.month > 2 {
            (c - 4716) as i32
        } else {
            (c - 4715) as i32
        };
        self.valid_ymd = true;
    }

    fn compute_hms(&mut self) {
        if self.valid_hms {
            return;
        }
        self.compute_jd();
        let day_ms = (self.i_jd + 43_200_000) % MS_PER_DAY;
        let day_min = day_ms / 60_000;
        self.second = (day_ms % 60_000) as f64 / 1000.0;
        self.minute = (day_min % 60) as i32;
        self.hour = (day_min / 60) as i32;
        self.raw_s = false;
        self.valid_hms = true;
    }

    fn compute_ymd_hms(&mut self) {
        self.compute_ymd();
        self.compute_hms();
    }

    fn auto_adjust(&mut self) {
        if !self.raw_s || self.valid_jd {
            self.raw_s = false;
            return;
        }
        if self.second >= -210_866_760_000.0 && self.second <= 253_402_307_999.0 {
            let r = self.second * 1000.0 + JD_UNIX_EPOCH_MS as f64;
            self.clear_ymd_hms_tz();
            self.i_jd = (r + 0.5) as i64;
            self.valid_jd = true;
            self.raw_s = false;
        }
    }

    fn apply_modifier(&mut self, modifier: &str, idx: usize) -> Result<()> {
        let lower = modifier.trim().to_ascii_lowercase();
        if lower.is_empty() {
            return Ok(());
        }
        match lower.as_str() {
            "julianday" => {
                if idx > 0 {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "julianday modifier must be first",
                    ));
                }
                if self.valid_jd && self.raw_s {
                    self.raw_s = false;
                }
                return Ok(());
            }
            "unixepoch" => {
                if idx > 0 {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "unixepoch modifier must be first",
                    ));
                }
                if self.raw_s {
                    let r = self.second * 1000.0 + JD_UNIX_EPOCH_MS as f64;
                    if (0.0..464_269_060_800_000.0).contains(&r) {
                        self.clear_ymd_hms_tz();
                        self.i_jd = (r + 0.5) as i64;
                        self.valid_jd = true;
                        self.raw_s = false;
                        return Ok(());
                    }
                }
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "unixepoch modifier requires numeric input",
                ));
            }
            "auto" => {
                if idx > 0 {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "auto modifier must be first",
                    ));
                }
                self.auto_adjust();
                return Ok(());
            }
            "start of month" => {
                self.compute_ymd();
                self.day = 1;
                self.hour = 0;
                self.minute = 0;
                self.second = 0.0;
                self.valid_jd = false;
                self.valid_hms = true;
                return Ok(());
            }
            "start of year" => {
                self.compute_ymd();
                self.month = 1;
                self.day = 1;
                self.hour = 0;
                self.minute = 0;
                self.second = 0.0;
                self.valid_jd = false;
                self.valid_hms = true;
                return Ok(());
            }
            "start of day" => {
                self.compute_ymd_hms();
                self.hour = 0;
                self.minute = 0;
                self.second = 0.0;
                self.valid_jd = false;
                self.valid_hms = true;
                return Ok(());
            }
            "localtime" => {
                self.to_localtime()?;
                return Ok(());
            }
            "utc" => {
                self.to_utc()?;
                return Ok(());
            }
            _ => {}
        }

        if let Some(day) = parse_weekday(&lower) {
            self.compute_ymd_hms();
            self.tz = 0;
            self.valid_jd = false;
            self.compute_jd();
            let mut z = ((self.i_jd + 129_600_000) / MS_PER_DAY) % 7;
            if z > day as i64 {
                z -= 7;
            }
            self.i_jd += (day as i64 - z) * MS_PER_DAY;
            self.clear_ymd_hms_tz();
            return Ok(());
        }

        if let Some((amount, unit)) = parse_amount_unit(&lower) {
            match unit {
                "second" => self.add_seconds(amount),
                "minute" => self.add_seconds(amount * 60.0),
                "hour" => self.add_seconds(amount * 3600.0),
                "day" => self.add_seconds(amount * 86_400.0),
                "month" => self.add_months(amount as i32),
                "year" => self.add_years(amount as i32),
                _ => {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "unknown date/time modifier",
                    ))
                }
            }
            return Ok(());
        }

        Err(Error::with_message(
            ErrorCode::Error,
            "unknown date/time modifier",
        ))
    }

    fn add_seconds(&mut self, seconds: f64) {
        self.compute_jd();
        self.i_jd += (seconds * 1000.0) as i64;
        self.clear_ymd_hms_tz();
        self.valid_jd = true;
    }

    fn add_months(&mut self, months: i32) {
        self.compute_ymd_hms();
        let mut y = self.year;
        let mut m = self.month + months;
        while m > 12 {
            m -= 12;
            y += 1;
        }
        while m < 1 {
            m += 12;
            y -= 1;
        }
        let max_day = days_in_month(y, m);
        let d = self.day.min(max_day);
        self.year = y;
        self.month = m;
        self.day = d;
        self.valid_jd = false;
        self.valid_ymd = true;
    }

    fn add_years(&mut self, years: i32) {
        self.compute_ymd_hms();
        let y = self.year + years;
        let max_day = days_in_month(y, self.month);
        self.year = y;
        self.day = self.day.min(max_day);
        self.valid_jd = false;
        self.valid_ymd = true;
    }

    fn days_after_jan01(&mut self) -> i32 {
        let mut jan01 = self.clone();
        jan01.valid_jd = false;
        jan01.month = 1;
        jan01.day = 1;
        jan01.compute_jd();
        ((self.i_jd - jan01.i_jd + 43_200_000) / MS_PER_DAY) as i32
    }

    fn days_after_monday(&self) -> i32 {
        ((self.i_jd + 43_200_000) / MS_PER_DAY % 7) as i32
    }

    fn days_after_sunday(&self) -> i32 {
        ((self.i_jd + 129_600_000) / MS_PER_DAY % 7) as i32
    }

    fn to_julian_day(&mut self) -> f64 {
        self.compute_jd();
        self.i_jd as f64 / MS_PER_DAY as f64
    }

    fn to_unix_seconds(&mut self) -> f64 {
        self.compute_jd();
        (self.i_jd - JD_UNIX_EPOCH_MS) as f64 / 1000.0
    }

    fn strftime(&mut self, fmt: &str) -> Result<String> {
        self.compute_jd();
        self.compute_ymd_hms();
        let mut out = String::new();
        let mut chars = fmt.chars().peekable();
        while let Some(c) = chars.next() {
            if c != '%' {
                out.push(c);
                continue;
            }
            let spec = match chars.next() {
                Some(s) => s,
                None => {
                    out.push('%');
                    break;
                }
            };
            match spec {
                'd' => out.push_str(&format!("{:02}", self.day)),
                'e' => out.push_str(&format!("{:2}", self.day)),
                'f' => out.push_str(&format!("{:06.3}", self.second)),
                'F' => out.push_str(&format!(
                    "{:04}-{:02}-{:02}",
                    self.year, self.month, self.day
                )),
                'G' | 'g' => {
                    let mut y = self.clone();
                    let offset = 3 - y.days_after_monday();
                    y.i_jd += offset as i64 * MS_PER_DAY;
                    y.valid_ymd = false;
                    y.compute_ymd();
                    if spec == 'g' {
                        out.push_str(&format!("{:02}", y.year % 100));
                    } else {
                        out.push_str(&format!("{:04}", y.year));
                    }
                }
                'H' => out.push_str(&format!("{:02}", self.hour)),
                'k' => out.push_str(&format!("{:2}", self.hour)),
                'I' | 'l' => {
                    let mut h = self.hour;
                    if h > 12 {
                        h -= 12;
                    }
                    if h == 0 {
                        h = 12;
                    }
                    if spec == 'I' {
                        out.push_str(&format!("{:02}", h));
                    } else {
                        out.push_str(&format!("{:2}", h));
                    }
                }
                'j' => out.push_str(&format!("{:03}", self.days_after_jan01() + 1)),
                'J' => out.push_str(&format!("{:.16}", self.to_julian_day())),
                'm' => out.push_str(&format!("{:02}", self.month)),
                'M' => out.push_str(&format!("{:02}", self.minute)),
                'p' | 'P' => {
                    if self.hour >= 12 {
                        out.push_str(if spec == 'p' { "PM" } else { "pm" });
                    } else {
                        out.push_str(if spec == 'p' { "AM" } else { "am" });
                    }
                }
                'R' => out.push_str(&format!("{:02}:{:02}", self.hour, self.minute)),
                's' => {
                    if self.use_subsec {
                        out.push_str(&format!("{:.3}", self.to_unix_seconds()));
                    } else {
                        out.push_str(&format!("{}", self.to_unix_seconds() as i64));
                    }
                }
                'S' => out.push_str(&format!("{:02}", self.second as i32)),
                'T' => out.push_str(&format!(
                    "{:02}:{:02}:{:02}",
                    self.hour, self.minute, self.second as i32
                )),
                'u' | 'w' => {
                    let mut c = (self.days_after_sunday() as u8 + b'0') as char;
                    if c == '0' && spec == 'u' {
                        c = '7';
                    }
                    out.push(c);
                }
                'U' => out.push_str(&format!(
                    "{:02}",
                    (self.days_after_jan01() - self.days_after_sunday() + 7) / 7
                )),
                'V' => {
                    let mut y = self.clone();
                    let offset = 3 - y.days_after_monday();
                    y.i_jd += offset as i64 * MS_PER_DAY;
                    y.valid_ymd = false;
                    y.compute_ymd();
                    out.push_str(&format!("{:02}", y.days_after_jan01() / 7 + 1));
                }
                'W' => out.push_str(&format!(
                    "{:02}",
                    (self.days_after_jan01() - self.days_after_monday() + 7) / 7
                )),
                'Y' => out.push_str(&format!("{:04}", self.year)),
                '%' => out.push('%'),
                _ => {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "unsupported strftime format",
                    ))
                }
            }
        }
        Ok(out)
    }

    fn to_localtime(&mut self) -> Result<()> {
        self.compute_jd();
        if self.is_local {
            return Ok(());
        }
        #[cfg(unix)]
        {
            use libc::{localtime_r, time_t, tm};
            let mut tm_out: tm = unsafe { std::mem::zeroed() };
            let secs = self.i_jd / 1000 - 21086676_i64 * 10000;
            let t = secs as time_t;
            let res = unsafe { localtime_r(&t, &mut tm_out) };
            if res.is_null() {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "local time unavailable",
                ));
            }
            self.year = tm_out.tm_year + 1900;
            self.month = tm_out.tm_mon + 1;
            self.day = tm_out.tm_mday;
            self.hour = tm_out.tm_hour;
            self.minute = tm_out.tm_min;
            self.second = tm_out.tm_sec as f64 + (self.i_jd % 1000) as f64 * 0.001;
            self.valid_ymd = true;
            self.valid_hms = true;
            self.valid_jd = false;
            self.raw_s = false;
            self.tz = 0;
            self.is_error = false;
            self.is_local = true;
            self.is_utc = false;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Err(Error::with_message(
                ErrorCode::Error,
                "localtime conversion not supported",
            ))
        }
    }

    fn to_utc(&mut self) -> Result<()> {
        self.compute_ymd_hms();
        if self.is_utc {
            return Ok(());
        }
        #[cfg(unix)]
        {
            use libc::{mktime, tm};
            let mut tm_in: tm = unsafe { std::mem::zeroed() };
            tm_in.tm_year = self.year - 1900;
            tm_in.tm_mon = self.month - 1;
            tm_in.tm_mday = self.day;
            tm_in.tm_hour = self.hour;
            tm_in.tm_min = self.minute;
            tm_in.tm_sec = self.second as i32;
            let t = unsafe { mktime(&mut tm_in) };
            if t == -1 {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "utc conversion failed",
                ));
            }
            let secs = t as i64;
            self.i_jd = secs * 1000 + JD_UNIX_EPOCH_MS;
            self.valid_jd = true;
            self.clear_ymd_hms_tz();
            self.is_utc = true;
            self.is_local = false;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Err(Error::with_message(
                ErrorCode::Error,
                "utc conversion not supported",
            ))
        }
    }
}

fn parse_fixed_digits(bytes: &[u8], start: usize, count: usize) -> Option<i32> {
    if start + count > bytes.len() {
        return None;
    }
    let mut val = 0i32;
    for i in start..start + count {
        let b = bytes[i];
        if !b.is_ascii_digit() {
            return None;
        }
        val = val * 10 + (b - b'0') as i32;
    }
    Some(val)
}

fn parse_fraction(bytes: &[u8], start: usize) -> (f64, usize) {
    let mut val = 0.0;
    let mut scale = 1.0;
    let mut idx = start;
    while idx < bytes.len() {
        let b = bytes[idx];
        if !b.is_ascii_digit() {
            break;
        }
        val = val * 10.0 + (b - b'0') as f64;
        scale *= 10.0;
        idx += 1;
    }
    if idx == start {
        (0.0, 0)
    } else {
        (val / scale, idx - start)
    }
}

fn parse_weekday(modifier: &str) -> Option<i32> {
    let mut parts = modifier.split_whitespace();
    if parts.next()? != "weekday" {
        return None;
    }
    let day = parts.next()?.parse::<i32>().ok()?;
    if (0..=6).contains(&day) {
        Some(day)
    } else {
        None
    }
}

fn parse_amount_unit(modifier: &str) -> Option<(f64, &str)> {
    let mut parts = modifier.split_whitespace();
    let amount = parts.next()?.parse::<f64>().ok()?;
    let unit = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let unit = unit.trim_end_matches('s');
    Some((amount, unit))
}

fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn render_year(year: i32) -> String {
    if year < 0 {
        format!("-{:04}", -year)
    } else {
        format!("{:04}", year)
    }
}

fn apply_modifiers(mut dt: DateTime, args: &[Value]) -> Result<DateTime> {
    for (idx, arg) in args.iter().enumerate() {
        if arg.is_null() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "modifier argument is NULL",
            ));
        }
        let modifier = arg.to_text();
        dt.apply_modifier(&modifier, idx)?;
    }
    Ok(dt)
}

pub fn func_date(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args[0].is_null() {
        return Ok(Value::Null);
    }
    let dt = match DateTime::parse_value(&args[0]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    let mut dt = match apply_modifiers(dt, &args[1..]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    dt.compute_ymd();
    if dt.is_error {
        return Ok(Value::Null);
    }
    Ok(Value::Text(format!(
        "{}-{:02}-{:02}",
        render_year(dt.year),
        dt.month,
        dt.day
    )))
}

pub fn func_time(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args[0].is_null() {
        return Ok(Value::Null);
    }
    let dt = match DateTime::parse_value(&args[0]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    let mut dt = match apply_modifiers(dt, &args[1..]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    dt.compute_hms();
    if dt.is_error {
        return Ok(Value::Null);
    }
    Ok(Value::Text(format!(
        "{:02}:{:02}:{:02}",
        dt.hour, dt.minute, dt.second as i32
    )))
}

pub fn func_datetime(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args[0].is_null() {
        return Ok(Value::Null);
    }
    let dt = match DateTime::parse_value(&args[0]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    let mut dt = match apply_modifiers(dt, &args[1..]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    dt.compute_ymd_hms();
    if dt.is_error {
        return Ok(Value::Null);
    }
    Ok(Value::Text(format!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
        render_year(dt.year),
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second as i32
    )))
}

pub fn func_julianday(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args[0].is_null() {
        return Ok(Value::Null);
    }
    let dt = match DateTime::parse_value(&args[0]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    let mut dt = match apply_modifiers(dt, &args[1..]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    if dt.is_error {
        return Ok(Value::Null);
    }
    Ok(Value::Real(dt.to_julian_day()))
}

pub fn func_unixepoch(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args[0].is_null() {
        return Ok(Value::Null);
    }
    let dt = match DateTime::parse_value(&args[0]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    let mut dt = match apply_modifiers(dt, &args[1..]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    if dt.is_error {
        return Ok(Value::Null);
    }
    Ok(Value::Integer(dt.to_unix_seconds() as i64))
}

pub fn func_strftime(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }
    let format = args[0].to_text();
    let dt = match DateTime::parse_value(&args[1]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    let mut dt = match apply_modifiers(dt, &args[2..]) {
        Ok(dt) => dt,
        Err(_) => return Ok(Value::Null),
    };
    if dt.is_error {
        return Ok(Value::Null);
    }
    Ok(Value::Text(match dt.strftime(&format) {
        Ok(text) => text,
        Err(_) => return Ok(Value::Null),
    }))
}

pub fn func_current_date(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            "current_date() takes no arguments",
        ));
    }
    func_date(&[Value::Text("now".to_string())])
}

pub fn func_current_time(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            "current_time() takes no arguments",
        ));
    }
    func_time(&[Value::Text("now".to_string())])
}

pub fn func_current_timestamp(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            "current_timestamp() takes no arguments",
        ));
    }
    func_datetime(&[Value::Text("now".to_string())])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_simple() {
        let v = Value::Text("2024-01-02".to_string());
        let out = func_date(&[v]).unwrap();
        assert_eq!(out, Value::Text("2024-01-02".to_string()));
    }

    #[test]
    fn test_time_simple() {
        let v = Value::Text("12:34:56".to_string());
        let out = func_time(&[v]).unwrap();
        assert_eq!(out, Value::Text("12:34:56".to_string()));
    }

    #[test]
    fn test_datetime_simple() {
        let v = Value::Text("2024-01-02 03:04:05".to_string());
        let out = func_datetime(&[v]).unwrap();
        assert_eq!(out, Value::Text("2024-01-02 03:04:05".to_string()));
    }

    #[test]
    fn test_strftime_basic() {
        let fmt = Value::Text("%Y-%m-%d".to_string());
        let ts = Value::Text("2024-01-02".to_string());
        let out = func_strftime(&[fmt, ts]).unwrap();
        assert_eq!(out, Value::Text("2024-01-02".to_string()));
    }
}
