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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshExternalOutcome {
    pub target_snapshot_id: Option<i64>,
    pub commit_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshCommitMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvRefresh {
    pub refresh_id: i64,
    pub mv_id: i64,
    #[serde(default)]
    pub operation_id: Option<i64>,
    pub state: MvRefreshState,
    #[serde(default)]
    pub target_catalog: Option<String>,
    #[serde(default)]
    pub target_namespace: Option<String>,
    #[serde(default)]
    pub target_table: Option<String>,
    #[serde(default)]
    pub staging_branch: Option<String>,
    #[serde(default)]
    pub expected_main_snapshot_id: Option<i64>,
    #[serde(default)]
    pub staging_snapshot_id: Option<i64>,
    #[serde(default)]
    pub published_snapshot_id: Option<i64>,
    #[serde(default)]
    pub target_snapshots: BTreeMap<String, i64>,
    #[serde(default)]
    pub base_table_uuids: BTreeMap<String, String>,
    #[serde(default)]
    pub rows: Option<i64>,
    #[serde(default)]
    pub marker: Option<RefreshCommitMarker>,
    #[serde(default)]
    pub external_outcome: Option<RefreshExternalOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvRefreshState {
    IntentCreated,
    StagingCommitted,
    #[serde(alias = "EXTERNAL_COMMITTED")]
    PublishCommitted,
    Finalized,
    AbortRequested,
    Aborted,
    CommitUnknown,
}

impl MvRefreshState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IntentCreated => "INTENT_CREATED",
            Self::StagingCommitted => "STAGING_COMMITTED",
            Self::PublishCommitted => "PUBLISH_COMMITTED",
            Self::Finalized => "FINALIZED",
            Self::AbortRequested => "ABORT_REQUESTED",
            Self::Aborted => "ABORTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginIcebergMvRefreshRequest {
    pub mv_id: i64,
    pub operation_id: Option<i64>,
    pub target_catalog: String,
    pub target_namespace: String,
    pub target_table: String,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub marker_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordStagingCommitRequest {
    pub refresh_id: i64,
    pub staging_snapshot_id: i64,
    pub rows: i64,
    pub base_table_uuids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPublishCommitRequest {
    pub refresh_id: i64,
    pub published_snapshot_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshFinalizeRequest {
    pub refresh_id: i64,
    pub rows: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
    pub target_snapshot_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateStarRocksMvRefreshSummaryRequest {
    pub mv_id: i64,
    pub last_refresh_ms: i64,
    pub last_refresh_rows: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
}
