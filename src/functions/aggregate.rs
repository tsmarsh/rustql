//! Aggregate SQL functions
//!
//! This module implements SQLite's aggregate functions like COUNT, SUM, AVG, etc.
//! Aggregate functions accumulate values across multiple rows.

use crate::error::Result;
use crate::types::Value;
use crate::vdbe::mem::format_real_sqlite;

// ============================================================================
// Aggregate State
// ============================================================================

/// State maintained during aggregation
#[derive(Debug, Clone)]
pub enum AggregateState {
    /// COUNT(*) or COUNT(x) state
    Count { count: i64 },

    /// SUM(x) state
    Sum {
        sum: f64,
        has_value: bool,
        is_integer: bool,
    },

    /// AVG(x) state
    Avg { sum: f64, count: i64 },

    /// MIN(x) state
    Min { value: Option<Value> },

    /// MAX(x) state
    Max { value: Option<Value> },

    /// TOTAL(x) state (always returns float, 0.0 for no rows)
    Total { sum: f64 },

    /// GROUP_CONCAT(x) or GROUP_CONCAT(x, sep) state
    GroupConcat {
        values: Vec<String>,
        separator: String,
    },

    /// MD5SUM(x) state - computes MD5 hash of concatenated values
    Md5Sum { data: Vec<u8> },

    /// DECIMAL_SUM(x) state
    DecimalSum { sum: f64, has_value: bool },
}

impl AggregateState {
    /// Create initial state for an aggregate function
    pub fn new(func_name: &str) -> Option<Self> {
        match func_name.to_uppercase().as_str() {
            "COUNT" => Some(AggregateState::Count { count: 0 }),
            "SUM" => Some(AggregateState::Sum {
                sum: 0.0,
                has_value: false,
                is_integer: true,
            }),
            "AVG" => Some(AggregateState::Avg { sum: 0.0, count: 0 }),
            "MIN" => Some(AggregateState::Min { value: None }),
            "MAX" => Some(AggregateState::Max { value: None }),
            "TOTAL" => Some(AggregateState::Total { sum: 0.0 }),
            "GROUP_CONCAT" | "STRING_AGG" => Some(AggregateState::GroupConcat {
                values: Vec::new(),
                separator: ",".to_string(),
            }),
            "MD5SUM" => Some(AggregateState::Md5Sum { data: Vec::new() }),
            "DECIMAL_SUM" => Some(AggregateState::DecimalSum { sum: 0.0, has_value: false }),
            _ => None,
        }
    }

    /// Step: add a value to the aggregate
    /// Returns true if this value became the new min/max (for bare column affinity)
    pub fn step(&mut self, args: &[Value]) -> Result<bool> {
        match self {
            AggregateState::Count { count } => {
                // COUNT(*) counts all rows, COUNT(x) counts non-NULL x
                if args.is_empty() || !matches!(args.first(), Some(Value::Null)) {
                    *count += 1;
                }
                Ok(false)
            }

            AggregateState::Sum {
                sum,
                has_value,
                is_integer,
            } => {
                if let Some(val) = args.first() {
                    if !matches!(val, Value::Null) {
                        *has_value = true;
                        match val {
                            Value::Integer(n) => *sum += *n as f64,
                            Value::Real(f) => {
                                *sum += f;
                                *is_integer = false;
                            }
                            Value::Text(s) => {
                                if let Ok(n) = s.parse::<i64>() {
                                    *sum += n as f64;
                                } else if let Ok(f) = s.parse::<f64>() {
                                    *sum += f;
                                    *is_integer = false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok(false)
            }

            AggregateState::Avg { sum, count } => {
                if let Some(val) = args.first() {
                    if !matches!(val, Value::Null) {
                        *count += 1;
                        match val {
                            Value::Integer(n) => *sum += *n as f64,
                            Value::Real(f) => *sum += f,
                            Value::Text(s) => {
                                if let Ok(n) = s.parse::<f64>() {
                                    *sum += n;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok(false)
            }

            AggregateState::Min { value } => {
                let mut changed = false;
                if let Some(val) = args.first() {
                    if !matches!(val, Value::Null) {
                        if let Some(current) = value {
                            if compare_values(val, current) < 0 {
                                *value = Some(val.clone());
                                changed = true;
                            }
                        } else {
                            *value = Some(val.clone());
                            changed = true;
                        }
                    }
                }
                Ok(changed)
            }

            AggregateState::Max { value } => {
                let mut changed = false;
                if let Some(val) = args.first() {
                    if !matches!(val, Value::Null) {
                        if let Some(current) = value {
                            if compare_values(val, current) > 0 {
                                *value = Some(val.clone());
                                changed = true;
                            }
                        } else {
                            *value = Some(val.clone());
                            changed = true;
                        }
                    }
                }
                Ok(changed)
            }

            AggregateState::Total { sum } => {
                if let Some(val) = args.first() {
                    match val {
                        Value::Integer(n) => *sum += *n as f64,
                        Value::Real(f) => *sum += f,
                        Value::Text(s) => {
                            if let Ok(n) = s.parse::<f64>() {
                                *sum += n;
                            }
                        }
                        _ => {}
                    }
                }
                Ok(false)
            }

            AggregateState::GroupConcat { values, separator } => {
                if let Some(val) = args.first() {
                    if !matches!(val, Value::Null) {
                        let s = value_to_string(val);
                        values.push(s);
                    }
                }
                // Optional separator in second argument
                // NULL separator means empty string (no separator)
                if let Some(sep) = args.get(1) {
                    if matches!(sep, Value::Null) {
                        *separator = String::new();
                    } else {
                        *separator = value_to_string(sep);
                    }
                }
                Ok(false)
            }

            AggregateState::Md5Sum { data } => {
                // Concatenate all argument values as bytes
                for val in args {
                    match val {
                        Value::Null => {}
                        Value::Integer(n) => data.extend_from_slice(n.to_string().as_bytes()),
                        Value::Real(f) => data.extend_from_slice(format_real_sqlite(*f).as_bytes()),
                        Value::Text(s) => data.extend_from_slice(s.as_bytes()),
                        Value::Blob(b) => data.extend_from_slice(b),
                    }
                }
                Ok(false)
            }

            AggregateState::DecimalSum { sum, has_value } => {
                if let Some(val) = args.first() {
                    if !matches!(val, Value::Null) {
                        *has_value = true;
                        match val {
                            Value::Integer(n) => *sum += *n as f64,
                            Value::Real(f) => *sum += f,
                            Value::Text(s) => { if let Ok(f) = s.parse::<f64>() { *sum += f; } }
                            _ => {}
                        }
                    }
                }
                Ok(false)
            }
        }
    }

    /// Finalize: return the final aggregate value
    pub fn finalize(&self) -> Result<Value> {
        match self {
            AggregateState::Count { count } => Ok(Value::Integer(*count)),

            AggregateState::Sum {
                sum,
                has_value,
                is_integer,
            } => {
                if !has_value {
                    Ok(Value::Null)
                } else if *is_integer && *sum >= i64::MIN as f64 && *sum <= i64::MAX as f64 {
                    Ok(Value::Integer(*sum as i64))
                } else {
                    Ok(Value::Real(*sum))
                }
            }

            AggregateState::Avg { sum, count } => {
                if *count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Real(*sum / *count as f64))
                }
            }

            AggregateState::Min { value } => Ok(value.clone().unwrap_or(Value::Null)),

            AggregateState::Max { value } => Ok(value.clone().unwrap_or(Value::Null)),

            AggregateState::Total { sum } => Ok(Value::Real(*sum)),

            AggregateState::GroupConcat { values, separator } => {
                if values.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Text(values.join(separator)))
                }
            }

            AggregateState::Md5Sum { data } => {
                let hash = compute_md5(data);
                // Convert to lowercase hex string
                let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
                Ok(Value::Text(hex))
            }

            AggregateState::DecimalSum { sum, has_value } => {
                if *has_value { Ok(Value::Real(*sum)) } else { Ok(Value::Null) }
            }
        }
    }
}

// ============================================================================
// MD5 Implementation (from RFC 1321)
// ============================================================================

fn compute_md5(data: &[u8]) -> [u8; 16] {
    let mut state: [u32; 4] = [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476];

    // Process full 64-byte blocks
    let mut i = 0;
    while i + 64 <= data.len() {
        md5_transform(&mut state, &data[i..i + 64]);
        i += 64;
    }

    // Handle remaining bytes with padding
    let remaining = data.len() - i;
    let mut buffer = [0u8; 128];
    buffer[..remaining].copy_from_slice(&data[i..]);
    buffer[remaining] = 0x80;

    let bit_len = (data.len() as u64) * 8;
    if remaining >= 56 {
        // Need two blocks
        md5_transform(&mut state, &buffer[..64]);
        buffer[..56].fill(0);
        buffer[56..64].copy_from_slice(&bit_len.to_le_bytes());
        md5_transform(&mut state, &buffer[..64]);
    } else {
        buffer[56..64].copy_from_slice(&bit_len.to_le_bytes());
        md5_transform(&mut state, &buffer[..64]);
    }

    // Convert state to bytes
    let mut result = [0u8; 16];
    for (i, &word) in state.iter().enumerate() {
        result[i * 4..i * 4 + 4].copy_from_slice(&word.to_le_bytes());
    }
    result
}

fn md5_transform(state: &mut [u32; 4], block: &[u8]) {
    let mut words = [0u32; 16];
    for (i, chunk) in block.chunks_exact(4).enumerate() {
        words[i] = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    }

    let [mut a, mut b, mut c, mut d] = *state;

    // Round 1
    const S1: [u32; 4] = [7, 12, 17, 22];
    const K1: [u32; 16] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821,
    ];
    for i in 0..16 {
        let f = (b & c) | ((!b) & d);
        let g = i;
        let temp = d;
        d = c;
        c = b;
        b = b.wrapping_add(
            (a.wrapping_add(f).wrapping_add(K1[i]).wrapping_add(words[g])).rotate_left(S1[i % 4]),
        );
        a = temp;
    }

    // Round 2
    const S2: [u32; 4] = [5, 9, 14, 20];
    const K2: [u32; 16] = [
        0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681,
        0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8,
        0x676f02d9, 0x8d2a4c8a,
    ];
    for i in 0..16 {
        let f = (d & b) | ((!d) & c);
        let g = (5 * i + 1) % 16;
        let temp = d;
        d = c;
        c = b;
        b = b.wrapping_add(
            (a.wrapping_add(f).wrapping_add(K2[i]).wrapping_add(words[g])).rotate_left(S2[i % 4]),
        );
        a = temp;
    }

    // Round 3
    const S3: [u32; 4] = [4, 11, 16, 23];
    const K3: [u32; 16] = [
        0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60,
        0xbebfbc70, 0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5,
        0x1fa27cf8, 0xc4ac5665,
    ];
    for i in 0..16 {
        let f = b ^ c ^ d;
        let g = (3 * i + 5) % 16;
        let temp = d;
        d = c;
        c = b;
        b = b.wrapping_add(
            (a.wrapping_add(f).wrapping_add(K3[i]).wrapping_add(words[g])).rotate_left(S3[i % 4]),
        );
        a = temp;
    }

    // Round 4
    const S4: [u32; 4] = [6, 10, 15, 21];
    const K4: [u32; 16] = [
        0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d,
        0x85845dd1, 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235,
        0x2ad7d2bb, 0xeb86d391,
    ];
    for i in 0..16 {
        let f = c ^ (b | (!d));
        let g = (7 * i) % 16;
        let temp = d;
        d = c;
        c = b;
        b = b.wrapping_add(
            (a.wrapping_add(f).wrapping_add(K4[i]).wrapping_add(words[g])).rotate_left(S4[i % 4]),
        );
        a = temp;
    }

    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
}

// ============================================================================
// Aggregate Function Registry
// ============================================================================

/// Check if a function name is an aggregate function
pub fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "TOTAL"
            | "GROUP_CONCAT"
            | "STRING_AGG"
            | "MD5SUM"
            | "DECIMAL_SUM"
    )
}

/// Get aggregate function info
pub fn get_aggregate_function(name: &str) -> Option<AggregateInfo> {
    let name_upper = name.to_uppercase();
    match name_upper.as_str() {
        "COUNT" => Some(AggregateInfo {
            name: name_upper,
            min_args: 0,
            max_args: 1,
        }),
        "SUM" => Some(AggregateInfo {
            name: name_upper,
            min_args: 1,
            max_args: 1,
        }),
        "AVG" => Some(AggregateInfo {
            name: name_upper,
            min_args: 1,
            max_args: 1,
        }),
        "MIN" => Some(AggregateInfo {
            name: name_upper,
            min_args: 1,
            max_args: 1,
        }),
        "MAX" => Some(AggregateInfo {
            name: name_upper,
            min_args: 1,
            max_args: 1,
        }),
        "TOTAL" => Some(AggregateInfo {
            name: name_upper,
            min_args: 1,
            max_args: 1,
        }),
        "GROUP_CONCAT" => Some(AggregateInfo {
            name: name_upper,
            min_args: 1,
            max_args: 2,
        }),
        "STRING_AGG" => Some(AggregateInfo {
            name: name_upper,
            min_args: 2,
            max_args: 2,
        }),
        "MD5SUM" => Some(AggregateInfo {
            name: name_upper,
            min_args: 1,
            max_args: 255, // Can take multiple arguments
        }),
        "DECIMAL_SUM" => Some(AggregateInfo {
            name: name_upper,
            min_args: 1,
            max_args: 1,
        }),
        _ => None,
    }
}

/// Information about an aggregate function
#[derive(Debug, Clone)]
pub struct AggregateInfo {
    pub name: String,
    pub min_args: usize,
    pub max_args: usize,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert Value to String
fn value_to_string(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Integer(n) => n.to_string(),
        Value::Real(f) => format_real_sqlite(*f),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => String::from_utf8_lossy(b).to_string(),
    }
}

/// Compare two values (returns -1, 0, or 1)
fn compare_values(a: &Value, b: &Value) -> i32 {
    match (a, b) {
        (Value::Null, Value::Null) => 0,
        (Value::Null, _) => -1,
        (_, Value::Null) => 1,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y) as i32,
        (Value::Real(x), Value::Real(y)) => {
            if x < y {
                -1
            } else if x > y {
                1
            } else {
                0
            }
        }
        (Value::Integer(x), Value::Real(y)) => {
            let fx = *x as f64;
            if fx < *y {
                -1
            } else if fx > *y {
                1
            } else {
                0
            }
        }
        (Value::Real(x), Value::Integer(y)) => {
            let fy = *y as f64;
            if *x < fy {
                -1
            } else if *x > fy {
                1
            } else {
                0
            }
        }
        (Value::Text(x), Value::Text(y)) => x.cmp(y) as i32,
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y) as i32,
        (Value::Integer(_), Value::Text(_)) | (Value::Real(_), Value::Text(_)) => -1,
        (Value::Text(_), Value::Integer(_)) | (Value::Text(_), Value::Real(_)) => 1,
        (Value::Blob(_), _) => 1,
        (_, Value::Blob(_)) => -1,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count() {
        let mut state = AggregateState::new("COUNT").unwrap();
        state.step(&[Value::Integer(1)]).unwrap();
        state.step(&[Value::Integer(2)]).unwrap();
        state.step(&[Value::Null]).unwrap();
        state.step(&[Value::Integer(3)]).unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Integer(3)); // NULL not counted
    }

    #[test]
    fn test_count_star() {
        let mut state = AggregateState::new("COUNT").unwrap();
        state.step(&[]).unwrap();
        state.step(&[]).unwrap();
        state.step(&[]).unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Integer(3));
    }

    #[test]
    fn test_sum() {
        let mut state = AggregateState::new("SUM").unwrap();
        state.step(&[Value::Integer(10)]).unwrap();
        state.step(&[Value::Integer(20)]).unwrap();
        state.step(&[Value::Integer(30)]).unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Integer(60));
    }

    #[test]
    fn test_sum_with_null() {
        let mut state = AggregateState::new("SUM").unwrap();
        state.step(&[Value::Integer(10)]).unwrap();
        state.step(&[Value::Null]).unwrap();
        state.step(&[Value::Integer(20)]).unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Integer(30));
    }

    #[test]
    fn test_sum_empty() {
        let state = AggregateState::new("SUM").unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Null);
    }

    #[test]
    fn test_avg() {
        let mut state = AggregateState::new("AVG").unwrap();
        state.step(&[Value::Integer(10)]).unwrap();
        state.step(&[Value::Integer(20)]).unwrap();
        state.step(&[Value::Integer(30)]).unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Real(20.0));
    }

    #[test]
    fn test_min_max() {
        let mut min_state = AggregateState::new("MIN").unwrap();
        let mut max_state = AggregateState::new("MAX").unwrap();

        for v in &[Value::Integer(5), Value::Integer(2), Value::Integer(8)] {
            min_state.step(&[v.clone()]).unwrap();
            max_state.step(&[v.clone()]).unwrap();
        }

        assert_eq!(min_state.finalize().unwrap(), Value::Integer(2));
        assert_eq!(max_state.finalize().unwrap(), Value::Integer(8));
    }

    #[test]
    fn test_total() {
        let mut state = AggregateState::new("TOTAL").unwrap();
        state.step(&[Value::Integer(10)]).unwrap();
        state.step(&[Value::Integer(20)]).unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Real(30.0));
    }

    #[test]
    fn test_total_empty() {
        let state = AggregateState::new("TOTAL").unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Real(0.0));
    }

    #[test]
    fn test_group_concat() {
        let mut state = AggregateState::new("GROUP_CONCAT").unwrap();
        state.step(&[Value::Text("a".to_string())]).unwrap();
        state.step(&[Value::Text("b".to_string())]).unwrap();
        state.step(&[Value::Text("c".to_string())]).unwrap();
        assert_eq!(state.finalize().unwrap(), Value::Text("a,b,c".to_string()));
    }

    #[test]
    fn test_group_concat_custom_separator() {
        let mut state = AggregateState::new("GROUP_CONCAT").unwrap();
        state
            .step(&[Value::Text("a".to_string()), Value::Text("; ".to_string())])
            .unwrap();
        state
            .step(&[Value::Text("b".to_string()), Value::Text("; ".to_string())])
            .unwrap();
        state
            .step(&[Value::Text("c".to_string()), Value::Text("; ".to_string())])
            .unwrap();
        assert_eq!(
            state.finalize().unwrap(),
            Value::Text("a; b; c".to_string())
        );
    }

    #[test]
    fn test_is_aggregate_function() {
        assert!(is_aggregate_function("COUNT"));
        assert!(is_aggregate_function("count"));
        assert!(is_aggregate_function("SUM"));
        assert!(is_aggregate_function("AVG"));
        assert!(!is_aggregate_function("LENGTH"));
        assert!(!is_aggregate_function("UPPER"));
    }
}
