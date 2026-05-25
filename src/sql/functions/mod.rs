//! Single-source function signature registry.
//!
//! Before this module landed, analyzer and codegen each carried their own
//! private "given a function name and argument types, what is the return
//! type?" logic — analyzer in [`crate::sql::analyzer::functions`] and codegen
//! in [`crate::sql::codegen::expr_compiler`]. The two copies were drifting
//! (the codegen side, for example, recognised `parse_url -> Utf8` while
//! analyzer did not), and adding a new SQL function meant patching both
//! sides at once.
//!
//! This module follows StarRocks' [`functions.py`] approach: every supported
//! scalar function (and operator) is described once, by a [`Signature`] of
//! parameter types and a return type. Resolving a call is then a lookup
//! against that table (`strict → polymorphic → cast`), and both analyzer
//! and codegen share the same answer.
//!
//! Step A of the migration deliberately covers only the high-frequency
//! function families (string / numeric / condition / a few array helpers).
//! Anything not yet registered here falls through to the legacy
//! hand-written `infer_*` paths so existing behaviour is preserved.
//!
//! [`functions.py`]: https://github.com/StarRocks/starrocks/blob/main/gensrc/script/functions.py

pub(crate) mod registry;
pub(crate) mod resolver;
pub(crate) mod signature;

pub(crate) use resolver::{ResolveError, resolve_scalar_function};
pub(crate) use signature::{Signature, TypeSpec};
