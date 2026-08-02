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

//! Explicit value injection for distributed query execution.

use std::sync::Arc;

use crate::query_execution::contract::{
    ConnectorWriteOperationRegistration, DistributedQueryCoordinator, DistributedQueryError,
    DistributedQueryOutcome, DistributedQueryRequest,
};
use crate::query_execution::write_operation::ConnectorWriteOperationSession;
use novarocks_spi::connector::ConnectorWriteLease;

#[derive(Clone)]
pub struct QueryExecutionService {
    coordinator: Arc<dyn DistributedQueryCoordinator>,
}

impl QueryExecutionService {
    pub fn new(coordinator: Arc<dyn DistributedQueryCoordinator>) -> Self {
        Self { coordinator }
    }

    /// Submit a fully prepared request to the frontend-owned coordinator.
    pub fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.coordinator.execute(request)
    }

    /// Acquire one exact frontend control generation and seal every cohort
    /// before any distributed writer attempt is staged.
    pub fn begin_write_operation(
        &self,
        registration: ConnectorWriteOperationRegistration,
    ) -> Result<ConnectorWriteOperationSession, DistributedQueryError> {
        self.coordinator.begin_write_operation(registration)
    }

    /// Seal a write operation against a caller-retained exact control binding.
    ///
    /// This is the only path for a prepared refresh artifact whose scan
    /// preparation already observed a concrete connector generation. It must
    /// never substitute a later current generation.
    pub fn begin_write_operation_with_lease(
        &self,
        registration: ConnectorWriteOperationRegistration,
        lease: ConnectorWriteLease,
    ) -> Result<ConnectorWriteOperationSession, DistributedQueryError> {
        ConnectorWriteOperationSession::try_begin(registration, lease).map_err(|error| {
            DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::Failed,
                format!("seal connector write operation cohorts: {error}"),
            )
        })
    }
}
