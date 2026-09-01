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

//! The two internal row relations of the distributed write data plane.
//!
//! This module is the **single definition point** for both relations. The SQL
//! planner declares them, the execution engine materializes them, the backend
//! decodes them, and the frontend reads them back — so a divergence between any
//! two of those would be a silent wire bug that no single crate's tests could
//! see. Everyone imports the schema and the invariants from here.
//!
//! Both relations use the same four primitive columns and differ only in their
//! invariants:
//!
//! | column | writer output | root result |
//! |---|---|---|
//! | `kind` | `1 = ROW_COUNT`, `2 = COMMIT_FRAGMENT` | `1 = SUMMARY`, `2 = PREPARED_FRAGMENT` |
//! | `write_target_ordinal` | non-null, always this writer's target | null on `SUMMARY` |
//! | `row_count` | non-null exactly on `ROW_COUNT` | non-null exactly on `SUMMARY` |
//! | `commit_fragment` | non-null exactly on `COMMIT_FRAGMENT` | non-null exactly on `PREPARED_FRAGMENT` |
//!
//! The `kind` column and the nullable columns must always agree. An unknown
//! kind, two non-null payloads, two null payloads, or an ordinal outside the
//! sealed target set is rejected at the nearest ingress — never repaired.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::connector::{ConnectorError, ConnectorErrorKind};

pub const WRITE_RELATION_COLUMN_COUNT: usize = 4;

pub const WRITE_RELATION_KIND_COLUMN: &str = "kind";
pub const WRITE_RELATION_TARGET_COLUMN: &str = "write_target_ordinal";
pub const WRITE_RELATION_ROW_COUNT_COLUMN: &str = "row_count";
pub const WRITE_RELATION_FRAGMENT_COLUMN: &str = "commit_fragment";

pub const WRITE_RELATION_KIND_INDEX: usize = 0;
pub const WRITE_RELATION_TARGET_INDEX: usize = 1;
pub const WRITE_RELATION_ROW_COUNT_INDEX: usize = 2;
pub const WRITE_RELATION_FRAGMENT_INDEX: usize = 3;

/// A row a `TableWriter` operator emits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterRowKind {
    /// The rows this operator accepted. Exactly one per successful writer.
    RowCount,
    /// One staged provider artifact.
    CommitFragment,
}

impl WriterRowKind {
    pub const ROW_COUNT: u8 = 1;
    pub const COMMIT_FRAGMENT: u8 = 2;

    pub fn from_wire(kind: u8) -> Result<Self, ConnectorError> {
        match kind {
            Self::ROW_COUNT => Ok(Self::RowCount),
            Self::COMMIT_FRAGMENT => Ok(Self::CommitFragment),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "unknown connector writer row kind",
            )),
        }
    }

    pub const fn to_wire(self) -> u8 {
        match self {
            Self::RowCount => Self::ROW_COUNT,
            Self::CommitFragment => Self::COMMIT_FRAGMENT,
        }
    }
}

/// A row the single `TableFinish` operator emits into the result sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootRowKind {
    /// The whole write's checked row count. Exactly one per complete set.
    Summary,
    /// One staged provider artifact, tagged with its logical target.
    PreparedFragment,
}

impl RootRowKind {
    pub const SUMMARY: u8 = 1;
    pub const PREPARED_FRAGMENT: u8 = 2;

    pub fn from_wire(kind: u8) -> Result<Self, ConnectorError> {
        match kind {
            Self::SUMMARY => Ok(Self::Summary),
            Self::PREPARED_FRAGMENT => Ok(Self::PreparedFragment),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "unknown connector write root row kind",
            )),
        }
    }

    pub const fn to_wire(self) -> u8 {
        match self {
            Self::Summary => Self::SUMMARY,
            Self::PreparedFragment => Self::PREPARED_FRAGMENT,
        }
    }
}

fn relation_schema(target_nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(WRITE_RELATION_KIND_COLUMN, DataType::UInt8, false),
        Field::new(
            WRITE_RELATION_TARGET_COLUMN,
            DataType::UInt32,
            target_nullable,
        ),
        Field::new(WRITE_RELATION_ROW_COUNT_COLUMN, DataType::UInt64, true),
        Field::new(WRITE_RELATION_FRAGMENT_COLUMN, DataType::Binary, true),
    ]))
}

/// The schema every `TableWriter` operator emits. `write_target_ordinal` is
/// non-null because a writer always knows which logical target it serves.
pub fn writer_output_schema() -> SchemaRef {
    relation_schema(false)
}

/// The schema the single `TableFinish` operator emits. `write_target_ordinal`
/// is nullable because the one `SUMMARY` row belongs to no single target.
pub fn root_output_schema() -> SchemaRef {
    relation_schema(true)
}

/// Check one writer row's payload against its kind.
pub fn validate_writer_row(
    kind: WriterRowKind,
    row_count: Option<u64>,
    fragment_len: Option<usize>,
) -> Result<(), ConnectorError> {
    let consistent = match kind {
        WriterRowKind::RowCount => row_count.is_some() && fragment_len.is_none(),
        WriterRowKind::CommitFragment => row_count.is_none() && fragment_len.is_some(),
    };
    if consistent {
        return Ok(());
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "connector writer row payload does not match its row kind",
    ))
}

/// Check one root row's payload against its kind. A `SUMMARY` row carries no
/// target ordinal precisely because it aggregates every target.
pub fn validate_root_row(
    kind: RootRowKind,
    target: Option<u32>,
    row_count: Option<u64>,
    fragment_len: Option<usize>,
) -> Result<(), ConnectorError> {
    let consistent = match kind {
        RootRowKind::Summary => target.is_none() && row_count.is_some() && fragment_len.is_none(),
        RootRowKind::PreparedFragment => {
            target.is_some() && row_count.is_none() && fragment_len.is_some()
        }
    };
    if consistent {
        return Ok(());
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "connector write root row payload does not match its row kind",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_relations_share_the_same_four_primitive_columns() {
        for schema in [writer_output_schema(), root_output_schema()] {
            assert_eq!(schema.fields().len(), WRITE_RELATION_COLUMN_COUNT);
            assert_eq!(
                schema.field(WRITE_RELATION_KIND_INDEX).data_type(),
                &DataType::UInt8
            );
            assert!(!schema.field(WRITE_RELATION_KIND_INDEX).is_nullable());
            assert_eq!(
                schema.field(WRITE_RELATION_TARGET_INDEX).data_type(),
                &DataType::UInt32
            );
            assert_eq!(
                schema.field(WRITE_RELATION_ROW_COUNT_INDEX).data_type(),
                &DataType::UInt64
            );
            assert!(schema.field(WRITE_RELATION_ROW_COUNT_INDEX).is_nullable());
            assert_eq!(
                schema.field(WRITE_RELATION_FRAGMENT_INDEX).data_type(),
                &DataType::Binary
            );
            assert!(schema.field(WRITE_RELATION_FRAGMENT_INDEX).is_nullable());
        }
    }

    #[test]
    fn only_the_root_relation_allows_a_null_target_ordinal() {
        assert!(
            !writer_output_schema()
                .field(WRITE_RELATION_TARGET_INDEX)
                .is_nullable()
        );
        assert!(
            root_output_schema()
                .field(WRITE_RELATION_TARGET_INDEX)
                .is_nullable()
        );
    }

    #[test]
    fn unknown_row_kinds_are_corrupt_data() {
        for kind in [0_u8, 3, 255] {
            assert_eq!(
                WriterRowKind::from_wire(kind).expect_err("unknown").kind(),
                ConnectorErrorKind::CorruptData
            );
            assert_eq!(
                RootRowKind::from_wire(kind).expect_err("unknown").kind(),
                ConnectorErrorKind::CorruptData
            );
        }
        assert_eq!(
            WriterRowKind::from_wire(WriterRowKind::ROW_COUNT).expect("known"),
            WriterRowKind::RowCount
        );
        assert_eq!(
            RootRowKind::from_wire(RootRowKind::PREPARED_FRAGMENT).expect("known"),
            RootRowKind::PreparedFragment
        );
    }

    #[test]
    fn writer_rows_must_carry_exactly_the_payload_their_kind_names() {
        assert!(validate_writer_row(WriterRowKind::RowCount, Some(7), None).is_ok());
        assert!(validate_writer_row(WriterRowKind::CommitFragment, None, Some(9)).is_ok());
        // both non-null
        assert!(validate_writer_row(WriterRowKind::RowCount, Some(7), Some(9)).is_err());
        // both null
        assert!(validate_writer_row(WriterRowKind::RowCount, None, None).is_err());
        assert!(validate_writer_row(WriterRowKind::CommitFragment, None, None).is_err());
        // payload belongs to the other kind
        assert!(validate_writer_row(WriterRowKind::CommitFragment, Some(7), None).is_err());
    }

    #[test]
    fn a_summary_row_carries_no_target_and_a_fragment_row_must() {
        assert!(validate_root_row(RootRowKind::Summary, None, Some(7), None).is_ok());
        assert!(validate_root_row(RootRowKind::PreparedFragment, Some(0), None, Some(9)).is_ok());
        assert!(validate_root_row(RootRowKind::Summary, Some(0), Some(7), None).is_err());
        assert!(validate_root_row(RootRowKind::PreparedFragment, None, None, Some(9)).is_err());
        assert!(validate_root_row(RootRowKind::Summary, None, None, None).is_err());
        assert!(
            validate_root_row(RootRowKind::PreparedFragment, Some(0), Some(7), Some(9)).is_err()
        );
    }
}
