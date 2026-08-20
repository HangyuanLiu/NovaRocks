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

use novarocks_spi::connector::ConnectorTableObjectId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::common::persisted_query_definition::PersistedQueryDefinition;
use crate::mv::domain::persistence::schema::{MvPartitionContract, MvSchemaContract};

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) const MV_DEFINITION_SUBJECT: &str = "mv.definition";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvDefinition {
    pub mv_id: i64,
    /// The exact user query source and the frozen resolution context it needs
    /// after a Frontend restart. This is an all-or-nothing v4 contract.
    pub query_definition: PersistedQueryDefinition,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    #[serde(default)]
    pub schema_contract: Option<MvSchemaContract>,
    #[serde(default)]
    pub partition_spec: Option<MvPartitionContract>,
    #[serde(default)]
    pub partition_state_complete: bool,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub last_refresh_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
    pub refresh_in_progress: bool,
    #[serde(default)]
    pub active_refresh_id: Option<i64>,
    pub refresh_target_snapshots: BTreeMap<String, i64>,
    #[serde(default)]
    pub refresh_policy: StoredMvRefreshPolicy,
    #[serde(default)]
    pub refresh_paused: bool,
    #[serde(default)]
    pub refresh_interval_ms: Option<i64>,
    #[serde(default)]
    pub max_staleness_ms: Option<i64>,
    #[serde(default)]
    pub last_scheduler_error: Option<String>,
    #[serde(default)]
    pub next_refresh_after_ms: Option<i64>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvDefinitionRequest {
    pub query_definition: PersistedQueryDefinition,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub schema_contract: Option<MvSchemaContract>,
    pub partition_spec: Option<MvPartitionContract>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMvRefreshMetadataRequest {
    pub mv_id: i64,
    pub refresh_policy: StoredMvRefreshPolicy,
    pub refresh_paused: bool,
    pub refresh_interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
    pub last_scheduler_error: Option<String>,
    pub next_refresh_after_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StoredMvRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval,
}

impl StoredMvRefreshPolicy {
    pub fn as_sql_str(&self) -> &'static str {
        match self {
            Self::Manual => "DEFERRED_MANUAL",
            Self::AsyncOnChange => "ASYNC_ON_CHANGE",
            Self::AsyncInterval => "ASYNC_INTERVAL",
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn accepts_interval(&self) -> bool {
        matches!(self, Self::AsyncInterval)
    }
}
