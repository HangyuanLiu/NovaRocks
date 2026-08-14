//! Narrow SQL syntax handoff for application admission.
//!
//! Frontend may normalize and parse a statement before it freezes
//! application-owned facts. Parser implementation details stay private.

pub use super::parser::{normalize_for_raw_parse, parse_normalized_sql_raw};
