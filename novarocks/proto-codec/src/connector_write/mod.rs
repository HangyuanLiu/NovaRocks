// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Structural validation and canonical encoding for the central connector
//! write carriers.
//!
//! Two carriers cross the process boundary in opposite directions: a logical
//! writer handle travels FE to BE inside a plan node, and a commit fragment
//! travels BE to FE inside the root write relation. Both are closed
//! per-category `oneof`s, and this module is the only place that turns
//! untrusted bytes into a value the rest of the process will act on.
//!
//! What it does NOT do is interpret provider semantics. It proves a carrier is
//! structurally a canonical, in-bounds Iceberg write carrier; deciding whether
//! the facts inside it describe a legal Iceberg write is the provider's job,
//! and lives behind the provider's own constructors.

mod fragment;
mod handle;
mod runtime_codec;
mod shared;

pub use fragment::ValidatedCommitFragment;
pub use handle::ValidatedWriterHandle;
pub use runtime_codec::{
    ConnectorWriteCodecError, ConnectorWriteFragmentDecoder, ConnectorWriteFragmentEncoder,
    ConnectorWriteHandleDecoder, ConnectorWriteHandleEncoder,
};

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

// Hard bounds. Every one of these is a wire-visible budget: exceeding it is a
// typed rejection before any connector I/O or external side effect.
//
// The two encoded-size caps mirror the SPI budgets they enforce
// (`MAX_CONNECTOR_WRITER_HANDLE_BYTES`, `MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES`).
// They are restated here because the codec is the trust boundary: a carrier is
// rejected before it is parsed, not after a caller happens to check.
pub const MAX_WRITER_HANDLE_ENCODED_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_COMMIT_FRAGMENT_ENCODED_BYTES: usize = 1024 * 1024;

pub const MAX_PATH_BYTES: usize = 16 * 1024;
pub const MAX_NAME_BYTES: usize = 1024;
pub const MAX_SCHEMA_JSON_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_PARTITION_VALUES: usize = 4096;
pub const MAX_PARTITION_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_COLUMN_STAT_ENTRIES: usize = 4096;
pub const MAX_COLUMN_STAT_BOUND_BYTES: usize = 64 * 1024;
pub const MAX_SPLIT_OFFSETS: usize = 4096;
pub const MAX_PARTITION_COLUMNS: usize = 4096;
pub const MAX_TRANSFORM_EXPRS: usize = 4096;
pub const MAX_TRANSFORM_EXPR_BYTES: usize = 64 * 1024;
pub const MAX_OLD_DELETE_MERGE_TARGETS: usize = 16_384;
pub const MAX_OLD_DELETE_REFERENCES: usize = 1024;
pub const MAX_MERGED_OLD_REFERENCES: usize = 1024;
pub const MAX_EQUALITY_DELETE_COLUMNS: usize = 4096;

pub(crate) fn missing(path: FieldPath, detail: &'static str) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::MissingField, detail)
}

pub(crate) fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

pub(crate) fn out_of_range(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::OutOfRange, detail)
}

pub(crate) fn inconsistent(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InconsistentFields, detail)
}

pub(crate) fn invalid_enum(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidEnum, detail)
}

pub(crate) fn bounded_text(
    value: &str,
    max_bytes: usize,
    path: FieldPath,
    allow_empty: bool,
) -> Result<(), ProtocolError> {
    if !allow_empty && value.is_empty() {
        return Err(invalid(path, "value must not be empty"));
    }
    if value.len() > max_bytes {
        return Err(out_of_range(
            path,
            format!("value exceeds {max_bytes} bytes"),
        ));
    }
    Ok(())
}

pub(crate) fn bounded_bytes(
    value: &[u8],
    max_bytes: usize,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if value.len() > max_bytes {
        return Err(out_of_range(
            path,
            format!("value exceeds {max_bytes} bytes"),
        ));
    }
    Ok(())
}

pub(crate) fn nonnegative_i64(
    value: i64,
    path: FieldPath,
    label: &'static str,
) -> Result<i64, ProtocolError> {
    if value < 0 {
        return Err(out_of_range(path, format!("{label} must be nonnegative")));
    }
    Ok(value)
}

pub(crate) fn bounded_count(
    actual: usize,
    max: usize,
    path: FieldPath,
    label: &'static str,
) -> Result<(), ProtocolError> {
    if actual > max {
        return Err(out_of_range(
            path,
            format!("{label} count {actual} exceeds the hard limit {max}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::write_stack::{
        MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_WRITER_HANDLE_BYTES,
    };

    use super::{MAX_COMMIT_FRAGMENT_ENCODED_BYTES, MAX_WRITER_HANDLE_ENCODED_BYTES};

    /// The two encoded-size caps are restated here so the codec can reject a
    /// carrier before parsing it, but they are the SPI's budgets and nothing
    /// else. Restating a number is only safe while something proves the two
    /// copies still agree: widening one alone would silently open the trust
    /// boundary that the other still believes it closes.
    #[test]
    fn the_codec_size_caps_are_the_spi_budgets_they_restate() {
        assert_eq!(
            MAX_WRITER_HANDLE_ENCODED_BYTES,
            MAX_CONNECTOR_WRITER_HANDLE_BYTES
        );
        assert_eq!(
            MAX_COMMIT_FRAGMENT_ENCODED_BYTES,
            MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES
        );
    }
}
