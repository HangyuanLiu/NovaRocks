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

use crate::connector::iceberg::changes::{
    ChangeError, IcebergChangePolicySignal, policy_signal_from_change_error,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MvRefreshPolicy {
    NoOp {
        current_snapshot_id: i64,
    },
    FullRefresh {
        target_snapshot_id: Option<i64>,
        reason: FullRefreshReason,
    },
    Incremental {
        previous_snapshot_id: i64,
        current_snapshot_id: i64,
    },
    Unsupported {
        reason: UnsupportedRefreshReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum FullRefreshReason {
    InitialRefresh,
    InsertOverwrite {
        snapshot_id: i64,
    },
    LineageExpired {
        previous_snapshot_id: i64,
    },
    BaseTableRecreated {
        previous_uuid: String,
        current_uuid: String,
    },
    SchemaEvolutionSafeFallback {
        detail: String,
    },
}

impl std::fmt::Display for FullRefreshReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FullRefreshReason::InitialRefresh => write!(f, "initial refresh"),
            FullRefreshReason::InsertOverwrite { snapshot_id } => {
                write!(f, "insert overwrite snapshot {snapshot_id}")
            }
            FullRefreshReason::LineageExpired {
                previous_snapshot_id,
            } => write!(f, "lineage expired after snapshot {previous_snapshot_id}"),
            FullRefreshReason::BaseTableRecreated {
                previous_uuid,
                current_uuid,
            } => write!(
                f,
                "base table recreated (previous uuid {previous_uuid}, current uuid {current_uuid})"
            ),
            FullRefreshReason::SchemaEvolutionSafeFallback { detail } => {
                write!(f, "schema evolution safe fallback: {detail}")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UnsupportedRefreshReason {
    SchemaEvolution { detail: String },
    ReplaceValidationFailed { snapshot_id: i64, reason: String },
    InternalInconsistency { detail: String },
}

impl std::fmt::Display for UnsupportedRefreshReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnsupportedRefreshReason::SchemaEvolution { detail } => {
                write!(f, "schema evolution unsupported: {detail}")
            }
            UnsupportedRefreshReason::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => write!(
                f,
                "replace snapshot {snapshot_id} failed validation: {reason}"
            ),
            UnsupportedRefreshReason::InternalInconsistency { detail } => {
                write!(f, "internal inconsistency: {detail}")
            }
        }
    }
}

pub(crate) fn choose_snapshot_refresh_policy(
    previous_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> Result<MvRefreshPolicy, String> {
    match (previous_snapshot_id, current_snapshot_id) {
        (None, current) => Ok(MvRefreshPolicy::FullRefresh {
            target_snapshot_id: current,
            reason: FullRefreshReason::InitialRefresh,
        }),
        (Some(previous), Some(current)) if previous == current => Ok(MvRefreshPolicy::NoOp {
            current_snapshot_id: current,
        }),
        (Some(previous), Some(current)) => Ok(MvRefreshPolicy::Incremental {
            previous_snapshot_id: previous,
            current_snapshot_id: current,
        }),
        (Some(previous), None) => Err(format!(
            "materialized view refresh cannot advance from snapshot {previous}: base table has no current snapshot"
        )),
    }
}

pub(crate) fn policy_from_change_error(err: ChangeError) -> MvRefreshPolicy {
    match (policy_signal_from_change_error(&err), err) {
        (
            IcebergChangePolicySignal::FullRefresh { .. },
            ChangeError::UnsupportedOperation { snapshot_id, op },
        ) if op == "overwrite" => MvRefreshPolicy::FullRefresh {
            target_snapshot_id: Some(snapshot_id),
            reason: FullRefreshReason::InsertOverwrite { snapshot_id },
        },
        (
            IcebergChangePolicySignal::FullRefresh { .. },
            ChangeError::LineageBroken { previous_snapshot },
        ) => MvRefreshPolicy::FullRefresh {
            target_snapshot_id: None,
            reason: FullRefreshReason::LineageExpired {
                previous_snapshot_id: previous_snapshot,
            },
        },
        (IcebergChangePolicySignal::FullRefresh { reason }, _) => MvRefreshPolicy::FullRefresh {
            target_snapshot_id: None,
            reason: FullRefreshReason::SchemaEvolutionSafeFallback { detail: reason },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::SchemaEvolutionUnsupported { detail },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::SchemaEvolution { detail },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::ReplaceValidationFailed {
                snapshot_id,
                reason,
            },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::UnsupportedOperation { snapshot_id, op },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!("unsupported iceberg snapshot operation `{op}` in {snapshot_id}"),
            },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::InternalInconsistency(detail),
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency { detail },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::PrimaryKeyMissingFromBase { pk_col }
            | ChangeError::PrimaryKeyNullable { pk_col }
            | ChangeError::PrimaryKeyTypeUnsupported { pk_col, .. },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!(
                    "CREATE-time primary key validation reached refresh path: {pk_col}"
                ),
            },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::PrimaryKeyValueNull { row_info },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!("primary key value became NULL during refresh: {row_info}"),
            },
        },
        (
            IcebergChangePolicySignal::Unsupported { .. },
            ChangeError::IcebergFormatUnsupported { format_version },
        ) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!(
                    "unsupported Iceberg format reached refresh path: {format_version}"
                ),
            },
        },
        (IcebergChangePolicySignal::Unsupported { reason }, _) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency { detail: reason },
        },
        (IcebergChangePolicySignal::Incremental, _) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: "incremental signal is invalid for change planning errors".to_string(),
            },
        },
    }
}
