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

//! Application-owned table-maintenance primitives.
//!
//! This crate owns no SQL session, provider client, executor, or workload
//! policy. Products supply their exact target binding and execution adapter;
//! this crate supplies the single target conflict gate and current-process job
//! observation semantics shared by SQL and MV.

use std::collections::BTreeMap;

pub mod activity;
pub mod gc_observation;
pub mod runtime;

/// Stable product identity of one external table-maintenance target.
///
/// The target contains no connector handle, snapshot, or session state. The
/// caller captures and rebinds those provider facts around this durable
/// process-local identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MaintenanceTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

/// Provider receipt facts retained by one OPTIMIZE job.
///
/// Optional counts mean that the provider did not prove the fact; they are not
/// a known zero.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJobOutcome {
    pub target_snapshot_id: Option<i64>,
    pub rewritten_data_files: i64,
    pub deleted_data_files: i64,
    pub added_data_files: Option<i64>,
    pub added_delete_files: Option<i64>,
    pub output_record_count: Option<i64>,
}

/// Current-process OPTIMIZE job state owned by the table-maintenance product.
pub type OptimizeJob = runtime::JobRecord<MaintenanceTarget, OptimizeJobOutcome>;

/// Current-process OPTIMIZE job ledger and target permit owner.
pub type OptimizeProcessRuntime = runtime::ProcessRuntime<
    MaintenanceTarget,
    OptimizeJobOutcome,
    activity::MaintenanceActivityPermit,
>;

/// One typed maintenance operation after SQL lowering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceActionRequest {
    RewriteDataFiles {
        target: MaintenanceTarget,
        base_snapshot_id: i64,
        job_id: Option<i64>,
        options: BTreeMap<String, String>,
        branch: Option<String>,
        where_clause: Option<String>,
    },
    RewriteManifests {
        target: MaintenanceTarget,
        use_caching: Option<bool>,
        spec_id: Option<i32>,
    },
    ExpireSnapshots {
        target: MaintenanceTarget,
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
    RemoveOrphanFiles {
        target: MaintenanceTarget,
        older_than_ms: i64,
    },
    RewritePositionDeleteFiles {
        target: MaintenanceTarget,
        options: BTreeMap<String, String>,
        where_clause: Option<String>,
    },
}

/// Provider facts projected by one completed maintenance operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceActionOutcome {
    RewriteDataFiles {
        target_snapshot_id: Option<i64>,
        rewritten_data_files_count: i32,
        /// `None` is an externally committed rewrite whose provider receipt
        /// does not prove an output file count. It is not a known zero.
        added_data_files_count: Option<i32>,
        /// Includes position deletes, deletion vectors, and equality deletes
        /// in the provider's typed receipt projection. `None` is unknown.
        added_delete_files_count: Option<i32>,
        rewritten_bytes_count: i64,
        failed_data_files_count: i32,
        removed_delete_files_count: i32,
        /// `None` is an unknown publication fact, distinct from zero rows.
        output_record_count: Option<i64>,
    },
    RewriteManifests {
        rewritten_manifests_count: i32,
        added_manifests_count: i32,
    },
    ExpireSnapshots {
        deleted_data_files_count: Option<i64>,
        deleted_position_delete_files_count: Option<i64>,
        deleted_equality_delete_files_count: Option<i64>,
        deleted_manifest_files_count: Option<i64>,
        deleted_manifest_lists_count: Option<i64>,
        deleted_statistics_files_count: Option<i64>,
    },
    RemoveOrphanFiles {
        orphan_file_locations: Vec<String>,
    },
    RewritePositionDeleteFiles {
        rewritten_delete_files_count: i32,
        /// `None` is an unknown publication fact, distinct from zero files.
        added_delete_files_count: Option<i32>,
        rewritten_bytes_count: i64,
        added_bytes_count: i64,
    },
}

/// Projects the only provider outcome accepted by an OPTIMIZE job into its
/// stable product receipt.
///
/// The conversion preserves unknown output facts as `None`; an OPTIMIZE job
/// must not turn a provider's missing proof into zero.
pub fn optimize_job_outcome_from_action(
    outcome: MaintenanceActionOutcome,
) -> Result<OptimizeJobOutcome, String> {
    let MaintenanceActionOutcome::RewriteDataFiles {
        target_snapshot_id,
        rewritten_data_files_count,
        added_data_files_count,
        added_delete_files_count,
        removed_delete_files_count,
        output_record_count,
        ..
    } = outcome
    else {
        return Err("optimize job expected a RewriteDataFiles outcome".to_string());
    };
    Ok(OptimizeJobOutcome {
        target_snapshot_id,
        rewritten_data_files: i64::from(rewritten_data_files_count),
        deleted_data_files: i64::from(removed_delete_files_count),
        added_data_files: added_data_files_count.map(i64::from),
        added_delete_files: added_delete_files_count.map(i64::from),
        output_record_count,
    })
}

/// Result of rebinding one durable maintenance target to its current table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceTargetRebind {
    Bound,
    Replaced,
    Missing,
}

/// Named current-process submission for one exact OPTIMIZE job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizeSubmission {
    Submitted { job_id: i64 },
    AlreadyActive,
}

impl OptimizeSubmission {
    /// `AlreadyActive` has no handle: joining another caller's job would
    /// silently transfer its business responsibility and conflict gate.
    pub const fn handle(self) -> Option<runtime::JobHandle> {
        match self {
            Self::Submitted { job_id } => Some(runtime::JobHandle::new(job_id)),
            Self::AlreadyActive => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MaintenanceActionOutcome, OptimizeSubmission, optimize_job_outcome_from_action};

    #[test]
    fn submitted_handle_names_only_its_exact_job() {
        assert_eq!(
            OptimizeSubmission::Submitted { job_id: 41 }
                .handle()
                .expect("submitted job has a handle")
                .job_id(),
            41
        );
        assert_eq!(OptimizeSubmission::AlreadyActive.handle(), None);
    }

    #[test]
    fn optimize_receipt_preserves_unknown_output_facts() {
        let receipt =
            optimize_job_outcome_from_action(MaintenanceActionOutcome::RewriteDataFiles {
                target_snapshot_id: Some(19),
                rewritten_data_files_count: 2,
                added_data_files_count: None,
                added_delete_files_count: Some(3),
                rewritten_bytes_count: 0,
                failed_data_files_count: 0,
                removed_delete_files_count: 4,
                output_record_count: None,
            })
            .expect("rewrite data files is an optimize outcome");
        assert_eq!(receipt.target_snapshot_id, Some(19));
        assert_eq!(receipt.rewritten_data_files, 2);
        assert_eq!(receipt.deleted_data_files, 4);
        assert_eq!(receipt.added_data_files, None);
        assert_eq!(receipt.added_delete_files, Some(3));
        assert_eq!(receipt.output_record_count, None);
    }

    #[test]
    fn optimize_rejects_a_non_rewrite_provider_outcome() {
        assert_eq!(
            optimize_job_outcome_from_action(MaintenanceActionOutcome::RemoveOrphanFiles {
                orphan_file_locations: Vec::new(),
            })
            .expect_err("cleanup cannot complete an optimize job"),
            "optimize job expected a RewriteDataFiles outcome"
        );
    }
}
