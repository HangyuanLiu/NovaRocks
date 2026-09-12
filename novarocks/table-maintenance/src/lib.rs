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

pub mod activity;
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
