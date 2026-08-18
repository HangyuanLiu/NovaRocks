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

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::durable::{DurableOpaqueBytes, DurableRecord, DurableRecordError};

/// Provider object identity is the only table-private durable value. Handles,
/// snapshots, and schemas remain attempt-local.
pub(crate) const MAX_DURABLE_STATISTICS_TABLE_OBJECT_ID_BYTES: usize =
    novarocks_spi::connector::MAX_CONNECTOR_TABLE_OBJECT_ID_BYTES;
pub(crate) const MAX_DURABLE_STATISTICS_BASIS_DATA_VERSION_BYTES: usize = 1024;
pub(crate) const MAX_DURABLE_STATISTICS_PUBLICATION_EVIDENCE_BYTES: usize = 1024;
pub(crate) const STATISTICS_JOB_RECORD_ENCODED_LIMIT: usize = 60 * 1024;

pub(crate) const MAX_STATISTICS_TARGET_COMPONENT_BYTES: usize = 128;
pub(crate) const MAX_STATISTICS_EXPLICIT_COLUMNS: usize = 256;
pub(crate) const MAX_STATISTICS_EXPLICIT_COLUMN_BYTES: usize = 128;
pub(crate) const MAX_STATISTICS_ERROR_MESSAGE_BYTES: usize = 256;

/// The durable schema carried by a statistics job record.
pub const STATISTICS_JOB_SCHEMA_VERSION: u8 = 3;

/// A stable table reference for a submitted ANALYZE request.
///
/// It deliberately contains no scan artifact, sketch, or runtime handle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatisticsJobTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobCreate {
    pub target: StatisticsJobTarget,
    pub connector_instance_id: String,
    pub object_id: Vec<u8>,
    pub columns: super::application::StatisticsColumnIntent,
    pub submitted_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatisticsJobState {
    Submitted,
    Preparing,
    Running,
    Publishing,
    Succeeded,
    Failed,
    Stale,
    Cancelled,
}

impl StatisticsJobState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Stale | Self::Cancelled
        )
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Submitted, Self::Preparing)
                | (Self::Preparing, Self::Running)
                | (Self::Running, Self::Publishing)
                // A new fenced owner may replay only work that has not
                // crossed the external publish boundary. Re-claiming the
                // returned SUBMITTED job increments the same durable
                // operation's attempt counter.
                | (Self::Preparing, Self::Submitted)
                | (Self::Running, Self::Submitted)
                | (Self::Publishing, Self::Succeeded)
                | (Self::Preparing, Self::Failed)
                | (Self::Running, Self::Failed)
                | (Self::Publishing, Self::Failed)
                | (Self::Preparing, Self::Stale)
                | (Self::Running, Self::Stale)
                | (Self::Submitted, Self::Cancelled)
                | (Self::Preparing, Self::Cancelled)
                | (Self::Running, Self::Cancelled)
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatisticsJobErrorKind {
    Configuration,
    Connector,
    Collection,
    Publish,
    TargetReplaced,
    TargetMissing,
    Cancelled,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatisticsJobError {
    pub kind: StatisticsJobErrorKind,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJob {
    pub job_id: Uuid,
    pub operation_id: Uuid,
    pub target: StatisticsJobTarget,
    pub connector_instance_id: String,
    pub object_id: Vec<u8>,
    pub columns: super::application::StatisticsColumnIntent,
    pub state: StatisticsJobState,
    pub attempt: u32,
    pub retry_not_before_ms: Option<i64>,
    /// Bounded opaque operation evidence used only to reconcile a publish
    /// whose external commit outcome became unknown. It is not a statistics
    /// artifact, sketch, or execution handle.
    pub publication_evidence: Option<Vec<u8>>,
    /// Provider-private version of the data whose statistics were published.
    /// This is terminal diagnostics/policy evidence, never an attempt input.
    pub basis_data_version: Option<Vec<u8>>,
    /// Client intent only. The fenced worker performs the state transition to
    /// CANCELLED, so an unfenced session cannot race publication.
    pub cancel_requested: bool,
    pub error: Option<StatisticsJobError>,
    pub submitted_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
// Design: ADR-0084 keeps only durable target identity and terminal evidence here.
pub(crate) struct StoredStatisticsJobV3 {
    pub schema_version: u8,
    pub job_id: Uuid,
    pub operation_id: Uuid,
    pub target: StatisticsJobTarget,
    pub connector_instance_id: String,
    pub object_id: DurableOpaqueBytes<MAX_DURABLE_STATISTICS_TABLE_OBJECT_ID_BYTES>,
    pub columns: super::application::StatisticsColumnIntent,
    pub state: StatisticsJobState,
    pub attempt: u32,
    #[serde(default)]
    pub retry_not_before_ms: Option<i64>,
    #[serde(default)]
    pub publication_evidence:
        Option<DurableOpaqueBytes<MAX_DURABLE_STATISTICS_PUBLICATION_EVIDENCE_BYTES>>,
    #[serde(default)]
    pub basis_data_version:
        Option<DurableOpaqueBytes<MAX_DURABLE_STATISTICS_BASIS_DATA_VERSION_BYTES>>,
    #[serde(default)]
    pub cancel_requested: bool,
    pub error: Option<StatisticsJobError>,
    pub submitted_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

impl StoredStatisticsJobV3 {
    pub(crate) fn try_new(
        job_id: Uuid,
        operation_id: Uuid,
        request: StatisticsJobCreate,
    ) -> Result<Self, DurableRecordError> {
        Ok(Self {
            schema_version: STATISTICS_JOB_SCHEMA_VERSION,
            job_id,
            operation_id,
            target: request.target,
            connector_instance_id: request.connector_instance_id,
            object_id: DurableOpaqueBytes::try_new(request.object_id)?,
            columns: request.columns,
            state: StatisticsJobState::Submitted,
            attempt: 0,
            retry_not_before_ms: None,
            publication_evidence: None,
            basis_data_version: None,
            cancel_requested: false,
            error: None,
            submitted_at_ms: request.submitted_at_ms,
            updated_at_ms: request.submitted_at_ms,
            completed_at_ms: None,
        })
    }
}

impl DurableRecord for StoredStatisticsJobV3 {
    const RECORD_KIND: &'static str = "statistics-job";
    const SCHEMA_VERSION: u8 = STATISTICS_JOB_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = STATISTICS_JOB_RECORD_ENCODED_LIMIT;
}

impl From<&StoredStatisticsJobV3> for StatisticsJob {
    fn from(value: &StoredStatisticsJobV3) -> Self {
        Self {
            job_id: value.job_id,
            operation_id: value.operation_id,
            target: value.target.clone(),
            connector_instance_id: value.connector_instance_id.clone(),
            object_id: value.object_id.as_bytes().to_vec(),
            columns: value.columns.clone(),
            state: value.state,
            attempt: value.attempt,
            retry_not_before_ms: value.retry_not_before_ms,
            publication_evidence: value
                .publication_evidence
                .as_ref()
                .map(|evidence| evidence.as_bytes().to_vec()),
            basis_data_version: value
                .basis_data_version
                .as_ref()
                .map(|version| version.as_bytes().to_vec()),
            cancel_requested: value.cancel_requested,
            error: value.error.clone(),
            submitted_at_ms: value.submitted_at_ms,
            updated_at_ms: value.updated_at_ms,
            completed_at_ms: value.completed_at_ms,
        }
    }
}

#[cfg(test)]
mod durable_record_budget_tests {
    use novarocks_spi::state_store::{MAX_VALUE_BYTES, StateStoreLimits};

    use super::*;
    use crate::durable::DurableRecordStore;

    const SENTINEL: &str = "statistics-opaque-budget-sentinel";

    fn opaque<const MAX_BYTES: usize>(bytes: usize) -> DurableOpaqueBytes<MAX_BYTES> {
        DurableOpaqueBytes::try_new(
            SENTINEL
                .as_bytes()
                .iter()
                .copied()
                .cycle()
                .take(bytes)
                .collect(),
        )
        .expect("bounded opaque payload")
    }

    #[test]
    fn maximal_statistics_record_fits_and_fails_before_a_store_write_when_restricted() {
        let record = StoredStatisticsJobV3 {
            schema_version: STATISTICS_JOB_SCHEMA_VERSION,
            job_id: Uuid::nil(),
            operation_id: Uuid::nil(),
            target: StatisticsJobTarget {
                catalog: "c".repeat(MAX_STATISTICS_TARGET_COMPONENT_BYTES),
                namespace: "n".repeat(MAX_STATISTICS_TARGET_COMPONENT_BYTES),
                table: "t".repeat(MAX_STATISTICS_TARGET_COMPONENT_BYTES),
            },
            connector_instance_id: "i".repeat(MAX_STATISTICS_TARGET_COMPONENT_BYTES),
            object_id: opaque(MAX_DURABLE_STATISTICS_TABLE_OBJECT_ID_BYTES),
            columns: super::super::application::StatisticsColumnIntent::Explicit(
                (0..MAX_STATISTICS_EXPLICIT_COLUMNS)
                    .map(|_| "m".repeat(MAX_STATISTICS_EXPLICIT_COLUMN_BYTES))
                    .collect(),
            ),
            state: StatisticsJobState::Failed,
            attempt: u32::MAX,
            retry_not_before_ms: Some(i64::MAX),
            publication_evidence: Some(opaque(MAX_DURABLE_STATISTICS_PUBLICATION_EVIDENCE_BYTES)),
            basis_data_version: Some(opaque(MAX_DURABLE_STATISTICS_BASIS_DATA_VERSION_BYTES)),
            cancel_requested: true,
            error: Some(StatisticsJobError {
                kind: StatisticsJobErrorKind::Internal,
                message: SENTINEL.repeat(MAX_STATISTICS_ERROR_MESSAGE_BYTES / SENTINEL.len()),
            }),
            submitted_at_ms: i64::MAX,
            updated_at_ms: i64::MAX,
            completed_at_ms: Some(i64::MAX),
        };
        let standard = DurableRecordStore::with_limits(StateStoreLimits::default());
        let encoded = standard
            .encode(&record)
            .expect("maximal bounded statistics record must fit");
        let actual_bytes = encoded.as_bytes().len();
        assert!(actual_bytes <= STATISTICS_JOB_RECORD_ENCODED_LIMIT);
        assert!(STATISTICS_JOB_RECORD_ENCODED_LIMIT <= MAX_VALUE_BYTES);

        let same_target = StoredStatisticsJobV3 {
            job_id: Uuid::new_v4(),
            operation_id: Uuid::new_v4(),
            ..record.clone()
        };
        let same_target_encoded = standard
            .encode(&same_target)
            .expect("same target record must fit");
        assert_eq!(
            actual_bytes,
            same_target_encoded.as_bytes().len(),
            "durable record size must not depend on provider metadata"
        );

        // This encoder has no StateStore handle: rejection proves the error
        // is raised before a transaction, job value, or index can be written.
        let mut restricted = StateStoreLimits::default();
        restricted.max_value_bytes = actual_bytes - 1;
        let error = DurableRecordStore::with_limits(restricted)
            .encode(&record)
            .expect_err("restricted record budget must reject before writing");
        assert!(matches!(
            error,
            DurableRecordError::BudgetExceeded {
                record_kind: "statistics-job",
                schema_version: STATISTICS_JOB_SCHEMA_VERSION,
                actual_bytes: actual,
                limit_bytes,
            } if actual == actual_bytes && limit_bytes == actual_bytes - 1
        ));
        assert!(!format!("{error}").contains(SENTINEL));
        assert!(!format!("{error:?}").contains(SENTINEL));
    }
}
