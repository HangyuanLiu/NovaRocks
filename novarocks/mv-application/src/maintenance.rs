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

/// Product policy configuration for process-local automatic MV maintenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceCoordinatorConfig {
    pub enabled: bool,
    pub tick_interval_ms: u64,
    pub max_concurrent: usize,
    pub compaction_min_data_files: i64,
    pub dv_min_delete_files: i64,
    pub action_cooldown_ms: i64,
    pub max_consecutive_failures: u32,
}

impl MaintenanceCoordinatorConfig {
    pub const fn new(
        enabled: bool,
        tick_interval_ms: u64,
        max_concurrent: usize,
        compaction_min_data_files: i64,
        dv_min_delete_files: i64,
        action_cooldown_ms: i64,
        max_consecutive_failures: u32,
    ) -> Self {
        Self {
            enabled,
            tick_interval_ms,
            max_concurrent,
            compaction_min_data_files,
            dv_min_delete_files,
            action_cooldown_ms,
            max_consecutive_failures,
        }
    }
}

impl Default for MaintenanceCoordinatorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick_interval_ms: 60_000,
            max_concurrent: 1,
            compaction_min_data_files: 100,
            dv_min_delete_files: 10,
            action_cooldown_ms: 3_600_000,
            max_consecutive_failures: 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MaintenanceActionKind {
    Expire,
    RewritePositionDeletes,
    Optimize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutomaticMaintenanceAction {
    ExpireSnapshots {
        older_than_ms: i64,
        retain_last: u32,
    },
    RewritePositionDeletes {
        min_input_files: usize,
    },
    Optimize,
}

impl AutomaticMaintenanceAction {
    pub const fn kind(&self) -> MaintenanceActionKind {
        match self {
            Self::ExpireSnapshots { .. } => MaintenanceActionKind::Expire,
            Self::RewritePositionDeletes { .. } => MaintenanceActionKind::RewritePositionDeletes,
            Self::Optimize => MaintenanceActionKind::Optimize,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceSkipReason {
    Disabled,
    NonDefaultRefs,
    DownstreamFloorUnknown,
    NothingToExpire,
    SnapshotUnchanged,
    MissingSummaryStats,
    BelowThreshold,
    SuppressedByOptimize,
    Cooldown,
    FailureBackoff,
    CircuitBroken,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MaintenanceEvaluation {
    pub actions: Vec<AutomaticMaintenanceAction>,
    pub skips: Vec<(MaintenanceActionKind, MaintenanceSkipReason)>,
}

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
