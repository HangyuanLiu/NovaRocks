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

//! Move-only protocol settlement for governed query application output.

use crate::api::{
    ExecutionHandle, ExecutionOutput, QueryExecutionError, QueryExecutionErrorKind,
    QueryResultStream, ResultDelivery, ResultFailureView, SchemaDelivery,
};
use crate::cancellation::QueryCancellationView;
use crate::session_control::{
    GovernedQueryStatementOwner, GovernedStatementFinishOutcome,
    GovernedStatementVisibilitySealOutcome,
};
use novarocks_workload_control::{LocalResourceAuthority, WorkScope};

/// Shared move-only owner for any governed query result presented to a client.
#[must_use = "the governed protocol owner must be settled by its protocol adapter"]
pub struct GovernedProtocolOwner {
    statement: Option<GovernedQueryStatementOwner>,
    resources: LocalResourceAuthority,
    settled: bool,
}

impl GovernedProtocolOwner {
    pub fn new(statement: GovernedQueryStatementOwner, resources: LocalResourceAuthority) -> Self {
        Self {
            statement: Some(statement),
            resources,
            settled: false,
        }
    }

    pub fn cancellation(&self) -> QueryCancellationView {
        let statement = self
            .statement
            .as_ref()
            .expect("protocol result retains its governed owner");
        QueryCancellationView::governed(statement.cancellation().clone(), statement.timeout_ms())
    }

    pub fn reservation_inputs(&self) -> (LocalResourceAuthority, WorkScope) {
        let statement = self
            .statement
            .as_ref()
            .expect("protocol result retains its governed owner");
        (self.resources.clone(), statement.scope().clone())
    }

    pub fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        self.statement
            .as_mut()
            .expect("protocol result retains its governed owner")
            .seal_success_visibility()
    }

    pub fn complete(&mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.statement
            .take()
            .expect("protocol result retains its governed owner")
            .finish()
    }

    pub fn settle_cancellation(&mut self) -> GovernedStatementFinishOutcome {
        self.complete()
    }

    pub fn fail(&mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.statement
            .take()
            .expect("protocol result retains its governed owner")
            .protocol_fail()
    }

    pub fn client_disconnected(&mut self) -> GovernedStatementFinishOutcome {
        self.fail_with_reason(novarocks_workload_control::CancellationReason::ClientDisconnected)
    }

    fn fail_with_reason(
        &mut self,
        reason: novarocks_workload_control::CancellationReason,
    ) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.statement
            .take()
            .expect("protocol result retains its governed owner")
            .fail(reason)
    }
}

impl Drop for GovernedProtocolOwner {
    fn drop(&mut self) {
        if !self.settled {
            drop(self.statement.take());
        }
    }
}

/// Query Application row stream retained by a protocol adapter.
///
/// The owner holds the logical execution control handle and statement permit
/// through the final protocol outcome. Dropping it cancels the execution and
/// lets the logical actor settle undelivered stream items.
#[must_use = "the streaming statement must be completed or explicitly failed by its protocol owner"]
pub struct StreamingStatementResult {
    execution: ExecutionHandle,
    stream: QueryResultStream,
    resources: LocalResourceAuthority,
    protocol: GovernedProtocolOwner,
    settled: bool,
}

impl StreamingStatementResult {
    pub fn try_from_execution(
        mut execution: ExecutionHandle,
        resources: LocalResourceAuthority,
        statement: GovernedQueryStatementOwner,
    ) -> Result<Self, QueryExecutionError> {
        let stream = match execution.take_output() {
            Some(ExecutionOutput::Rows(stream)) => stream,
            Some(ExecutionOutput::Completion) => {
                let _ = execution.request_cancel();
                return Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    "read execution returned completion-only output",
                ));
            }
            None => {
                let _ = execution.request_cancel();
                return Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    "read execution output was already transferred",
                ));
            }
        };
        Ok(Self {
            execution,
            stream,
            resources: resources.clone(),
            protocol: GovernedProtocolOwner::new(statement, resources),
            settled: false,
        })
    }

    pub fn begin_schema(&mut self) -> Option<SchemaDelivery> {
        self.stream.begin_schema()
    }

    pub async fn next_delivery(&mut self) -> Result<Option<ResultDelivery>, QueryExecutionError> {
        self.stream.next().await
    }

    pub fn failure_view(&self) -> Option<ResultFailureView> {
        self.stream.failure_view()
    }

    pub const fn resources(&self) -> &LocalResourceAuthority {
        &self.resources
    }

    pub fn reservation_inputs(&self) -> (LocalResourceAuthority, WorkScope) {
        self.protocol.reservation_inputs()
    }

    pub fn request_cancel(&self) -> Result<(), QueryExecutionError> {
        self.execution.request_cancel()
    }

    pub fn cancellation(&self) -> QueryCancellationView {
        self.protocol.cancellation()
    }

    pub fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        self.protocol.seal_success_visibility()
    }

    pub fn complete(mut self) -> GovernedStatementFinishOutcome {
        self.settled = true;
        self.protocol.complete()
    }

    pub fn fail(mut self) -> GovernedStatementFinishOutcome {
        let _ = self.execution.request_cancel();
        self.settled = true;
        self.protocol.fail()
    }

    pub fn settle_cancellation(mut self) -> GovernedStatementFinishOutcome {
        let _ = self.execution.request_cancel();
        self.settled = true;
        self.protocol.settle_cancellation()
    }

    pub fn client_disconnected(mut self) -> GovernedStatementFinishOutcome {
        let _ = self.execution.request_cancel();
        self.settled = true;
        self.protocol.client_disconnected()
    }
}

impl Drop for StreamingStatementResult {
    fn drop(&mut self) {
        if !self.settled {
            let _ = self.execution.request_cancel();
        }
    }
}
