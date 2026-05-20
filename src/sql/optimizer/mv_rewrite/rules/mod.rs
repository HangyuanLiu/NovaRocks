//! MV rewrite cascades rules.
//!
//! Each rule is gated on `MvRewriteCtx::enabled()`. Rules are
//! registered in `super::super::rules::all_transformation_rules`
//! only when MV rewrite is enabled (see crate::sql::optimizer::rules).
