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

//! Narrow query-lifecycle runtime facade owned by core.

use std::num::NonZeroU64;
use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::query_execution::contract::QueryId;
use crate::query_execution::lifecycle::{QueryExecutionId, RuntimeFilterContribution};
use crate::runtime::query_context::{
    QueryContextManager, QueryExecutionKey, QueryId as RuntimeQueryId, query_context_manager,
};
use crate::runtime_filter::port::identity::DeploymentEpoch;

#[derive(Clone)]
pub struct NativeQueryLifecycleRuntime {
    manager: Arc<QueryContextManager>,
}

impl NativeQueryLifecycleRuntime {
    pub fn global() -> Self {
        Self {
            manager: query_context_manager(),
        }
    }

    pub fn install_runtime_filter_contribution(
        &self,
        execution_id: QueryExecutionId,
        contribution: RuntimeFilterContribution,
    ) -> Result<(), String> {
        if contribution.install().epoch().get() != execution_id.attempt_id().get() {
            return Err(
                "runtime filter contribution epoch does not match query attempt".to_string(),
            );
        }
        self.manager
            .ensure_native_context_execution(
                runtime_execution_key(execution_id),
                false,
                contribution.lifecycle().delivery_expire,
                contribution.lifecycle().query_expire,
            )
            .map_err(|error| format!("prepare attempt-scoped runtime filter context: {error}"))?;
        self.manager
            .install_runtime_filter_deployment(
                runtime_query_id(execution_id.query_id()),
                contribution.lifecycle(),
                contribution.install().clone(),
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub fn abort_runtime_filter_contribution(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<(), String> {
        self.manager
            .abort_runtime_filter_deployment(
                runtime_query_id(execution_id.query_id()),
                DeploymentEpoch::new(execution_id.attempt_id().get()),
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub fn cancel_query(&self, query_id: QueryId, reason: String) -> Vec<UniqueId> {
        self.manager
            .cancel_query(runtime_query_id(query_id), reason)
    }

    pub fn cancel_execution(
        &self,
        execution_id: QueryExecutionId,
        reason: String,
    ) -> Vec<UniqueId> {
        self.manager
            .cancel_query_execution(runtime_execution_key(execution_id), reason)
    }
}

const fn runtime_query_id(query_id: QueryId) -> RuntimeQueryId {
    RuntimeQueryId::new(query_id.high(), query_id.low())
}

fn runtime_execution_key(execution_id: QueryExecutionId) -> QueryExecutionKey {
    QueryExecutionKey::native_attempt(
        runtime_query_id(execution_id.query_id()),
        NonZeroU64::new(execution_id.attempt_id().get())
            .expect("QueryExecutionId always has a nonzero attempt"),
    )
}
