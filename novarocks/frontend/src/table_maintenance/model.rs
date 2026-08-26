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

//! Current-process OPTIMIZE DTOs.
//!
//! These values intentionally have no durable codec, schema version, or
//! StateStore representation. A frontend restart discards them all.

use crate::maintenance::MaintenanceTarget;
use crate::query_execution::maintenance::OptimizeJobState;
use novarocks_spi::connector::ConnectorTableObjectId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJobCreate {
    pub target: MaintenanceTarget,
    pub object_id: ConnectorTableObjectId,
    pub base_snapshot_id: i64,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJobOutcome {
    pub target_snapshot_id: Option<i64>,
    pub rewritten_data_files: i64,
    pub deleted_data_files: i64,
    pub added_data_files: i64,
    pub output_record_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJob {
    pub job_id: i64,
    pub target: MaintenanceTarget,
    /// Captured only for this process's exact pre-dispatch rebind.
    pub object_id: Vec<u8>,
    pub base_snapshot_id: i64,
    pub state: OptimizeJobState,
    pub outcome: Option<OptimizeJobOutcome>,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}
