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

//! Provider-owned metadata commit primitives.
//!
//! These modules contain only Iceberg catalog/file-format facts and do not
//! depend on Core SQL, execution, or application state.

pub mod abort;
pub mod mv_provenance;
pub mod mv_refresh_ref;
pub mod puffin_dv;
pub mod ref_action;
pub mod retry;
pub mod snapshot_lifecycle_helpers;
pub mod statistics;

pub use abort::{AbortLog, CleanupError};
pub use mv_provenance::{
    MV_PROVENANCE_V1_PROP, MV_PROVENANCE_VERSION, MV_REFRESH_ROW_COUNT_PROP, MvProvenanceV1,
    ProvenanceBase, RefreshTechnique,
};
pub use mv_refresh_ref::{
    MV_ID_PROP, MV_REFRESH_ID_PROP, MV_REFRESH_TOKEN_PROP, MvRefreshPublishOutcome,
    MvRefreshPublishPlan, MvRefreshSnapshotMarker, publish_staging_branch_to_main,
    snapshot_matches_refresh_marker,
};
pub use puffin_dv::{
    DeletionVector, DeletionVectorBlobInput, WrittenPuffinDv, read_deletion_vector_puffin,
    read_deletion_vector_puffin_with_range_reader, write_multi_deletion_vector_puffin,
    write_single_deletion_vector_puffin,
};
pub use ref_action::{
    RefAction, RefActionOutcome, RefActionPlan, execute_ref_action, lower_ref_action,
};
pub use retry::{
    COMMIT_RETRY_BACKOFF_MS, COMMIT_RETRY_MAX_ATTEMPTS, commit_with_retry,
    is_retryable_commit_conflict,
};
pub use snapshot_lifecycle_helpers::{
    FileSet, build_dv_index, compute_live_snapshot_set, enumerate_files_for_snapshots,
    is_puffin_path, puffin_half_reference_protection,
};
