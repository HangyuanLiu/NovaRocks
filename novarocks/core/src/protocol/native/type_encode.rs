//! Frontend-owned native `TypeDesc` encoding seam.
//!
//! The implementation remains beside the temporary decode half until the
//! backend decoder cutover. New encoder call sites must use this module rather
//! than depending on the bidirectional compatibility module directly.

#[allow(unused_imports)]
pub(crate) use super::type_mapping::encode_type;

#[cfg(test)]
pub(crate) use super::type_mapping::encode_field_type;
