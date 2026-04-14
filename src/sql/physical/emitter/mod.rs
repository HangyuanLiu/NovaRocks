//! Emitter utilities — helpers and window-grouping logic used by the
//! cascades fragment builder.

pub mod emit_window;
pub(crate) mod helpers;

// Re-export for expr_compiler.rs which references super::emitter::agg_call_display_name_from_parts
pub(crate) use helpers::agg_call_display_name_from_parts;
