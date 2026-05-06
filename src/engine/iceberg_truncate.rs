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

//! Standalone-mode iceberg `TRUNCATE TABLE` entry point.
//!
//! Routes from `statement::execute_truncate_table_statement` for any iceberg
//! target (with optional branch suffix `t.branch_<name>` resolved at parse
//! time and threaded through as `target_ref`).
//!
//! Phase 1 scope (per spec §3): this module is a placeholder. The real
//! `TruncateCommit` lowering and commit-action pipeline land in subsequent
//! tasks; for now the function returns a TODO error so that the dispatch
//! wiring can be exercised end-to-end without changing semantics.

use std::sync::Arc;

use crate::connector::backend::ResolvedTable;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::{StandaloneState, StatementResult};

/// Placeholder iceberg TRUNCATE entry point. The real implementation lands in
/// a later task; for now this surfaces a clear TODO error at runtime so the
/// dispatch wiring (parser -> engine -> iceberg flow) can be verified.
pub(crate) fn execute_iceberg_truncate_table(
    _state: &Arc<StandaloneState>,
    _target: &TargetBackend,
    _resolved: &ResolvedTable,
    _target_ref: &str,
) -> Result<StatementResult, String> {
    Err("TODO: TruncateCommit not yet implemented".to_string())
}
