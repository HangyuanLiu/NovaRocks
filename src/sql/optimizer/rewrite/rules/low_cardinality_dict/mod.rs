//! LowCardinalityDictionaryRewrite — query rewrite rule that pushes
//! string-column reads through their dictionary encoding wherever it is
//! cheap to operate on Int32 dict ids instead of strings, and inserts a
//! `Decode` boundary just before the rest of the plan needs the
//! original string value.
//!
//! Scope (Task 7): Scan / Project / Aggregate / Sort / TopN / Limit.
//! Join / Union / CTE / derived-dictionary-expression coverage is the
//! responsibility of Task 8 and is marked with `TODO(task-8)` markers.

pub(crate) mod collector;
pub(crate) mod context;
pub(crate) mod expr;
pub(crate) mod rewriter;
pub(crate) mod rule;

pub(crate) use rule::LowCardinalityDictionaryRewriteRule;
