//! Error types and Result aliases for RustQL

/// Placeholder error type (to be implemented in issue e4mac)
#[derive(Debug)]
pub struct Error;

/// Result type alias for RustQL operations
pub type Result<T> = std::result::Result<T, Error>;
