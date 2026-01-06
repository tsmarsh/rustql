//! SQL functions: scalar, aggregate, date/time, JSON

pub mod scalar;
pub mod aggregate;
pub mod datetime;
pub mod json;

pub use scalar::{get_scalar_function, ScalarFunc};
pub use aggregate::{
    is_aggregate_function, get_aggregate_function,
    AggregateState, AggregateInfo,
};
