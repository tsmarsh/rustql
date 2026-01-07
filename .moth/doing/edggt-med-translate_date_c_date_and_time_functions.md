# Translate date.c - Date and Time Functions

## Overview
Translate date and time functions including parsing, formatting, and arithmetic operations on dates.

## Source Reference
- `sqlite3/src/date.c` - 1,822 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### DateTime
```rust
/// Internal date/time representation
pub struct DateTime {
    /// Julian day number
    pub julian_day: f64,
    /// Year (-4713 to ...)
    pub year: i32,
    /// Month (1-12)
    pub month: u8,
    /// Day (1-31)
    pub day: u8,
    /// Hour (0-23)
    pub hour: u8,
    /// Minute (0-59)
    pub minute: u8,
    /// Second (0-59.999...)
    pub second: f64,
    /// Timezone offset in minutes
    pub tz_offset: Option<i32>,
    /// Which fields are valid
    pub valid_flags: DateTimeFlags,
}

bitflags! {
    pub struct DateTimeFlags: u8 {
        const JULIAN = 0x01;
        const YMD = 0x02;
        const HMS = 0x04;
        const RAWTIME = 0x08;
        const LOCAL_TIME = 0x10;
    }
}
```

### Time Value Types
```rust
#[derive(Debug, Clone)]
pub enum TimeValue {
    /// Julian day number
    Julian(f64),
    /// ISO-8601 date/time string
    String(String),
    /// Unix timestamp
    Unix(i64),
    /// Special value: 'now'
    Now,
}
```

## Date Functions

### Core Parsing
```rust
impl DateTime {
    /// Parse date/time from value
    pub fn parse(value: &Value) -> Result<Self> {
        match value {
            Value::Text(s) => Self::parse_string(s),
            Value::Integer(i) => Self::from_julian(*i as f64),
            Value::Real(r) => Self::from_julian(*r),
            Value::Null => Err(Error::msg("cannot parse NULL as date")),
            _ => Err(Error::msg("invalid date type")),
        }
    }

    /// Parse ISO-8601 date string
    fn parse_string(s: &str) -> Result<Self> {
        let s = s.trim();

        // Try YYYY-MM-DD HH:MM:SS.SSS format
        if let Some(dt) = Self::parse_iso8601(s) {
            return Ok(dt);
        }

        // Try YYYY-MM-DD
        if let Some(dt) = Self::parse_date_only(s) {
            return Ok(dt);
        }

        // Try HH:MM:SS
        if let Some(dt) = Self::parse_time_only(s) {
            return Ok(dt);
        }

        // Try Julian day number
        if let Ok(jd) = s.parse::<f64>() {
            return Self::from_julian(jd);
        }

        // Try 'now'
        if s.eq_ignore_ascii_case("now") {
            return Self::now();
        }

        Err(Error::msg(format!("cannot parse date: {}", s)))
    }

    fn parse_iso8601(s: &str) -> Option<Self> {
        // YYYY-MM-DD HH:MM:SS.SSS or YYYY-MM-DDTHH:MM:SS.SSS
        let re = regex::Regex::new(
            r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?$"
        ).ok()?;

        let caps = re.captures(s)?;

        Some(DateTime {
            year: caps[1].parse().ok()?,
            month: caps[2].parse().ok()?,
            day: caps[3].parse().ok()?,
            hour: caps[4].parse().ok()?,
            minute: caps[5].parse().ok()?,
            second: {
                let secs: f64 = caps[6].parse().ok()?;
                if let Some(frac) = caps.get(7) {
                    secs + format!("0.{}", frac.as_str()).parse::<f64>().unwrap_or(0.0)
                } else {
                    secs
                }
            },
            tz_offset: Self::parse_timezone(caps.get(8).map(|m| m.as_str())),
            julian_day: 0.0, // Computed later
            valid_flags: DateTimeFlags::YMD | DateTimeFlags::HMS,
        })
    }
}
```

### Julian Day Conversion
```rust
impl DateTime {
    /// Convert to Julian day number
    pub fn to_julian(&self) -> f64 {
        if self.valid_flags.contains(DateTimeFlags::JULIAN) {
            return self.julian_day;
        }

        let y = self.year as f64;
        let m = self.month as f64;
        let d = self.day as f64;

        // Julian day calculation
        let a = ((14.0 - m) / 12.0).floor();
        let y_adj = y + 4800.0 - a;
        let m_adj = m + 12.0 * a - 3.0;

        let jdn = d + ((153.0 * m_adj + 2.0) / 5.0).floor()
            + 365.0 * y_adj
            + (y_adj / 4.0).floor()
            - (y_adj / 100.0).floor()
            + (y_adj / 400.0).floor()
            - 32045.0;

        // Add time component
        let time_frac = (self.hour as f64 - 12.0) / 24.0
            + self.minute as f64 / 1440.0
            + self.second / 86400.0;

        jdn + time_frac
    }

    /// Create from Julian day number
    pub fn from_julian(jd: f64) -> Result<Self> {
        let z = jd.floor() as i64;
        let f = jd - z as f64;

        let alpha = ((z as f64 - 1867216.25) / 36524.25).floor() as i64;
        let a = z + 1 + alpha - alpha / 4;
        let b = a + 1524;
        let c = ((b as f64 - 122.1) / 365.25).floor() as i64;
        let d = (365.25 * c as f64).floor() as i64;
        let e = ((b - d) as f64 / 30.6001).floor() as i64;

        let day = b - d - (30.6001 * e as f64).floor() as i64;
        let month = if e < 14 { e - 1 } else { e - 13 };
        let year = if month > 2 { c - 4716 } else { c - 4715 };

        // Extract time
        let time = (f + 0.5) * 24.0;
        let hour = time.floor() as u8;
        let min_frac = (time - hour as f64) * 60.0;
        let minute = min_frac.floor() as u8;
        let second = (min_frac - minute as f64) * 60.0;

        Ok(DateTime {
            julian_day: jd,
            year: year as i32,
            month: month as u8,
            day: day as u8,
            hour,
            minute,
            second,
            tz_offset: None,
            valid_flags: DateTimeFlags::JULIAN | DateTimeFlags::YMD | DateTimeFlags::HMS,
        })
    }
}
```

### Date/Time Modifiers
```rust
impl DateTime {
    /// Apply modifier to date/time
    pub fn apply_modifier(&mut self, modifier: &str) -> Result<()> {
        let modifier = modifier.trim().to_lowercase();

        match modifier.as_str() {
            "start of month" => self.start_of_month(),
            "start of year" => self.start_of_year(),
            "start of day" => self.start_of_day(),
            "weekday 0" | "weekday 1" | "weekday 2" | "weekday 3" |
            "weekday 4" | "weekday 5" | "weekday 6" => {
                let target = modifier.chars().last().unwrap().to_digit(10).unwrap() as u8;
                self.next_weekday(target)
            }
            "localtime" => self.to_localtime(),
            "utc" => self.to_utc(),
            "unixepoch" => self.interpret_as_unixepoch(),
            "julianday" => self.interpret_as_julian(),
            _ => self.apply_offset(&modifier),
        }
    }

    fn apply_offset(&mut self, modifier: &str) -> Result<()> {
        // Parse "+N unit" or "-N unit"
        let re = regex::Regex::new(r"^([+-]?\d+(?:\.\d+)?)\s+(year|month|day|hour|minute|second)s?$")
            .map_err(|_| Error::msg("invalid modifier"))?;

        if let Some(caps) = re.captures(modifier) {
            let amount: f64 = caps[1].parse().map_err(|_| Error::msg("invalid number"))?;
            let unit = &caps[2];

            match unit {
                "year" => self.add_years(amount as i32),
                "month" => self.add_months(amount as i32),
                "day" => self.add_days(amount),
                "hour" => self.add_hours(amount),
                "minute" => self.add_minutes(amount),
                "second" => self.add_seconds(amount),
                _ => Err(Error::msg("unknown unit")),
            }
        } else {
            Err(Error::msg(format!("unknown modifier: {}", modifier)))
        }
    }

    fn add_days(&mut self, days: f64) -> Result<()> {
        self.julian_day += days;
        self.recalculate_from_julian();
        Ok(())
    }

    fn start_of_month(&mut self) -> Result<()> {
        self.day = 1;
        self.hour = 0;
        self.minute = 0;
        self.second = 0.0;
        self.valid_flags.remove(DateTimeFlags::JULIAN);
        Ok(())
    }
}
```

### SQL Functions
```rust
fn date_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args.is_empty() {
        ctx.result_null();
        return Ok(());
    }

    let mut dt = DateTime::parse(args[0])?;

    // Apply modifiers
    for arg in &args[1..] {
        if let Value::Text(modifier) = arg {
            dt.apply_modifier(modifier)?;
        }
    }

    // Return YYYY-MM-DD
    ctx.result_text(&format!("{:04}-{:02}-{:02}", dt.year, dt.month, dt.day));
    Ok(())
}

fn time_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args.is_empty() {
        ctx.result_null();
        return Ok(());
    }

    let mut dt = DateTime::parse(args[0])?;

    for arg in &args[1..] {
        if let Value::Text(modifier) = arg {
            dt.apply_modifier(modifier)?;
        }
    }

    // Return HH:MM:SS
    ctx.result_text(&format!("{:02}:{:02}:{:02}", dt.hour, dt.minute, dt.second as u8));
    Ok(())
}

fn datetime_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args.is_empty() {
        ctx.result_null();
        return Ok(());
    }

    let mut dt = DateTime::parse(args[0])?;

    for arg in &args[1..] {
        if let Value::Text(modifier) = arg {
            dt.apply_modifier(modifier)?;
        }
    }

    // Return YYYY-MM-DD HH:MM:SS
    ctx.result_text(&format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second as u8
    ));
    Ok(())
}

fn julianday_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args.is_empty() {
        ctx.result_null();
        return Ok(());
    }

    let mut dt = DateTime::parse(args[0])?;

    for arg in &args[1..] {
        if let Value::Text(modifier) = arg {
            dt.apply_modifier(modifier)?;
        }
    }

    ctx.result_double(dt.to_julian());
    Ok(())
}

fn unixepoch_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args.is_empty() {
        ctx.result_null();
        return Ok(());
    }

    let mut dt = DateTime::parse(args[0])?;

    for arg in &args[1..] {
        if let Value::Text(modifier) = arg {
            dt.apply_modifier(modifier)?;
        }
    }

    // Unix epoch: seconds since 1970-01-01 00:00:00 UTC
    let jd_epoch = 2440587.5; // Julian day of Unix epoch
    let unix_time = (dt.to_julian() - jd_epoch) * 86400.0;
    ctx.result_int(unix_time as i64);
    Ok(())
}

fn strftime_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args.len() < 2 {
        ctx.result_null();
        return Ok(());
    }

    let format = args[0].as_str();
    let mut dt = DateTime::parse(args[1])?;

    for arg in &args[2..] {
        if let Value::Text(modifier) = arg {
            dt.apply_modifier(modifier)?;
        }
    }

    let result = dt.strftime(format)?;
    ctx.result_text(&result);
    Ok(())
}
```

### strftime Implementation
```rust
impl DateTime {
    pub fn strftime(&self, format: &str) -> Result<String> {
        let mut result = String::new();
        let mut chars = format.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '%' {
                match chars.next() {
                    Some('Y') => result.push_str(&format!("{:04}", self.year)),
                    Some('m') => result.push_str(&format!("{:02}", self.month)),
                    Some('d') => result.push_str(&format!("{:02}", self.day)),
                    Some('H') => result.push_str(&format!("{:02}", self.hour)),
                    Some('M') => result.push_str(&format!("{:02}", self.minute)),
                    Some('S') => result.push_str(&format!("{:02}", self.second as u8)),
                    Some('f') => {
                        let frac = (self.second.fract() * 1000.0) as u32;
                        result.push_str(&format!("{:03}", frac));
                    }
                    Some('j') => {
                        let doy = self.day_of_year();
                        result.push_str(&format!("{:03}", doy));
                    }
                    Some('W') => {
                        let week = self.week_of_year();
                        result.push_str(&format!("{:02}", week));
                    }
                    Some('w') => result.push_str(&format!("{}", self.day_of_week())),
                    Some('s') => {
                        let jd_epoch = 2440587.5;
                        let unix_time = ((self.to_julian() - jd_epoch) * 86400.0) as i64;
                        result.push_str(&format!("{}", unix_time));
                    }
                    Some('J') => result.push_str(&format!("{}", self.to_julian())),
                    Some('%') => result.push('%'),
                    Some(other) => {
                        result.push('%');
                        result.push(other);
                    }
                    None => result.push('%'),
                }
            } else {
                result.push(c);
            }
        }

        Ok(result)
    }
}
```

## Acceptance Criteria
- [ ] date() function
- [ ] time() function
- [ ] datetime() function
- [ ] julianday() function
- [ ] unixepoch() function
- [ ] strftime() function
- [ ] ISO-8601 date parsing
- [ ] Julian day conversion
- [ ] Date modifiers (+/- N days/months/years)
- [ ] start of month/year/day
- [ ] weekday modifier
- [ ] localtime/utc conversion
- [ ] All strftime format specifiers
