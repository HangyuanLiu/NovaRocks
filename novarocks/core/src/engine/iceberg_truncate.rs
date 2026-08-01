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
//! The statement boundary constructs only a provider-neutral mutation intent.
//! Exact-generation metadata loading, planning, commit and reconciliation are
//! owned by the connector data-mutation bridge.

use crate::engine::backend_resolver::TargetBackend;
use crate::engine::{StandaloneState, StatementResult};
use std::sync::Arc;

pub(crate) fn execute_iceberg_truncate_table(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    target_ref: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    debug_assert_eq!(target.backend_name, "iceberg");
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    crate::connector::data_mutation::execute_data_mutation(
        state.connector_control.as_ref(),
        state.as_ref(),
        &instance_id,
        novarocks_spi::connector::ConnectorMutationOperationId::new(),
        novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        },
        crate::connector::data_mutation::DataMutationIntent::truncate(target_ref),
        connector_context.clone(),
    )?;

    Ok(StatementResult::Ok)
}
