//! Aggregate SQL functions
//!
//! This module implements SQLite's aggregate functions like COUNT, SUM, AVG, etc.
//! Aggregate functions accumulate values across multiple rows.

use crate::error::Result;
use crate::types::Value;

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
            _ => None,
        }
    }

    /// Step: add a value to the aggregate
    pub fn step(&mut self, args: &[Value]) -> Result<()> {
        match self {
            AggregateState::Count { count } => {
                // COUNT(*) counts all rows, COUNT(x) counts non-NULL x
                if args.is_empty() || !matches!(args.first(), Some(Value::Null)) {
                    *count += 1;
                }
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
            }

            AggregateState::Min { value } => {
                if let Some(val) = args.first() {
                    if !matches!(val, Value::Null) {
                        if let Some(current) = value {
                            if compare_values(val, current) < 0 {
                                *value = Some(val.clone());
                            }
                        } else {
                            *value = Some(val.clone());
                        }
                    }
                }
            }

            AggregateState::Max { value } => {
                if let Some(val) = args.first() {
                    if !matches!(val, Value::Null) {
                        if let Some(current) = value {
                            if compare_values(val, current) > 0 {
                                *value = Some(val.clone());
                            }
                        } else {
                            *value = Some(val.clone());
                        }
                    }
                }
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
            }
        }

        Ok(())
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
        }
    }
}

// ============================================================================
// Aggregate Function Registry
// ============================================================================

/// Check if a function name is an aggregate function
pub fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "TOTAL" | "GROUP_CONCAT" | "STRING_AGG"
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
        Value::Real(f) => f.to_string(),
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
