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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJobOutcome {
    pub target_snapshot_id: Option<i64>,
    pub rewritten_data_files: i64,
    pub deleted_data_files: i64,
    /// Provider receipt fact; absence is unknown, not zero.
    pub added_data_files: Option<i64>,
    /// Provider receipt fact; absence is unknown, not zero.
    pub added_delete_files: Option<i64>,
    /// Provider receipt fact; absence is unknown, not zero.
    pub output_record_count: Option<i64>,
}

pub type OptimizeJob =
    novarocks_table_maintenance::runtime::JobRecord<MaintenanceTarget, OptimizeJobOutcome>;
