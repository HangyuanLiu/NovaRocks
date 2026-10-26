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

//! Provider-frozen rewrite sealing without an independent execution owner.

use crate::query_execution::contract::DistributedQueryError;
use crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession;
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorDistributedRewritePlan, ConnectorRequestContext,
};

/// Stateless sealing author; execution is owned by the QA logical supervisor.
#[derive(Clone, Default)]
pub struct QueryExecutionService;

impl QueryExecutionService {
    pub fn new() -> Self {
        Self
    }

    /// Seal a provider-frozen distributed rewrite against the composite lease
    /// that selected its metadata, rewrite and C1 write capabilities together.
    pub fn begin_distributed_rewrite_operation_with_lease(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        write_stack: novarocks_catalog_application::ConnectorWriteStackLease,
        table: &novarocks_spi::connector::ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorDistributedRewriteSession, DistributedQueryError> {
        ConnectorDistributedRewriteSession::try_begin(plan, lease, write_stack, table, context)
            .map_err(|error| {
                DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::Failed,
                    format!("seal distributed rewrite operation cohorts: {error}"),
                )
            })
    }
}
