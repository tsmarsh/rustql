//! SQL functions: scalar, aggregate, date/time, JSON

pub mod aggregate;
pub mod datetime;
#[cfg(feature = "fts3")]
pub mod fts3;
pub mod json;
pub mod printf;
pub mod scalar;

pub use aggregate::{get_aggregate_function, is_aggregate_function, AggregateInfo, AggregateState};
pub use scalar::{get_scalar_function, ScalarFunc};
