//! Query logical rewrite rule library.
//!
//! The execution driver lives in `sql::optimizer::rewrite`. This module keeps
//! the existing query rule implementations and utility helpers while the new
//! rewrite framework owns traversal, fixed-point iteration, disabling, and
//! tracing.

pub(crate) mod rules;
pub(crate) mod utils;
