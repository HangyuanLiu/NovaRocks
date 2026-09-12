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

//! Provider-neutral facts and terminal classifications for MV maintenance.

use std::fmt;

/// Product classification of a failed background capability invocation.
///
/// The Frontend adapter supplies this classification; product policy consumes
/// it without observing a provider, query, or Native transport type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvBackgroundEngineErrorKind {
    TargetGone,
    TransientUnavailable,
    InvalidDefinition,
    TerminalFailure,
    Corruption,
    InvariantViolation,
    ShutdownCancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvBackgroundEngineError {
    kind: MvBackgroundEngineErrorKind,
    message: String,
}

impl MvBackgroundEngineError {
    pub fn new(kind: MvBackgroundEngineErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> MvBackgroundEngineErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvBackgroundEngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvBackgroundEngineError {}

/// Facts needed to apply automatic-maintenance policy without a provider
/// metadata object or property-map fallback. Absent facts remain unknown.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvMaintenanceFacts {
    pub current_snapshot_id: Option<i64>,
    pub total_data_files: Option<i64>,
    pub max_compactable_data_files: Option<i64>,
    pub total_delete_files: Option<i64>,
    pub total_files_size_bytes: Option<i64>,
    pub oldest_snapshot_timestamp_ms: Option<i64>,
    pub snapshot_count: usize,
    pub non_default_reference_count: usize,
    pub downstream_floor_ts_ms: Option<i64>,
    pub downstream_floor_unknown: bool,
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::{MvBackgroundEngineError, MvBackgroundEngineErrorKind};

    #[test]
    fn typed_error_keeps_retry_policy_out_of_display_text() {
        let error = MvBackgroundEngineError::new(
            MvBackgroundEngineErrorKind::TransientUnavailable,
            "connector lease is temporarily unavailable",
        );
        assert_eq!(
            error.kind(),
            MvBackgroundEngineErrorKind::TransientUnavailable
        );
        assert_eq!(
            error.to_string(),
            "connector lease is temporarily unavailable"
        );
    }
}
