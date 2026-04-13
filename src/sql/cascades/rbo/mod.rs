//! Rule-based optimization (RBO) phase: tree-level fixed-point rewrites
//! over `LogicalPlan` before Memo insertion.
//!
//! Mirrors StarRocks FE's `TaskScheduler.rewriteIterative` model: a
//! shared `RewriteRule` trait, a single bottom-up driver, and a central
//! `RuleSet`. Rules are pure local rewrites; the driver owns traversal
//! and iteration.

pub(crate) mod driver;
pub(crate) mod rule;
pub(crate) mod rules;
