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
    ExecutionHandle, ExecutionOutput, QueryExecutionError, QueryExecutionErrorKind, QueryResult,
    QueryResultStream, ResultDelivery, ResultFailureView, SchemaDelivery,
    decoded_result_batch_governance_charge,
};
use crate::cancellation::QueryCancellationView;
use crate::session_control::{
    GovernedQueryStatementOwner, GovernedStatementFinishOutcome,
    GovernedStatementVisibilitySealOutcome,
};
use crate::session_error::QueryServiceError;
use arrow::record_batch::RecordBatch;
use novarocks_workload_control::{
    LocalResourceAuthority, ResultCredit, ResultCreditStage, WorkError, WorkScope,
};

/// A move-only decoded Arrow batch for an immediate statement result.
///
/// Immediate statements have no execution identity or actor receipt, but the
/// Arrow backing and its protocol bytes still require the same result-credit
/// transitions as a streamed delivery.
pub struct ImmediateResultBatch {
    batch: Option<RecordBatch>,
    decoded_bytes: u64,
    credit: Option<ResultCredit>,
}

pub struct ImmediateResultBatchReservationError {
    error: WorkError,
    batch: ImmediateResultBatch,
}

impl ImmediateResultBatchReservationError {
    pub const fn error(&self) -> &WorkError {
        &self.error
    }

    pub fn into_parts(self) -> (WorkError, ImmediateResultBatch) {
        (self.error, self.batch)
    }
}

impl std::fmt::Debug for ImmediateResultBatchReservationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ImmediateResultBatchReservationError")
            .field("error", &self.error)
            .field("decoded_bytes", &self.batch.decoded_bytes)
            .finish()
    }
}

impl ImmediateResultBatch {
    pub fn try_new(batch: RecordBatch, credit: ResultCredit) -> Result<Self, QueryExecutionError> {
        let decoded_bytes = decoded_result_batch_governance_charge(&batch)?;
        if credit.stage() != ResultCreditStage::DecodedQueued {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                format!(
                    "immediate result batch credit must be DecodedQueued, got {:?}",
                    credit.stage()
                ),
            ));
        }
        if credit.held_bytes() != decoded_bytes {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                format!(
                    "immediate result batch holds {decoded_bytes} bytes but its credit holds {}",
                    credit.held_bytes()
                ),
            ));
        }
        Ok(Self {
            batch: Some(batch),
            decoded_bytes,
            credit: Some(credit),
        })
    }

    pub fn batch(&self) -> &RecordBatch {
        self.batch
            .as_ref()
            .expect("immediate result batch retains its Arrow backing before completion")
    }

    pub const fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }

    pub async fn reserve_protocol_when_available(
        mut self,
        authority: &LocalResourceAuthority,
        bytes: u64,
    ) -> Result<Self, ImmediateResultBatchReservationError> {
        let credit = self
            .credit
            .take()
            .expect("immediate result batch owns credit while awaiting protocol capacity");
        match credit
            .reserve_protocol_when_available(authority, bytes)
            .await
        {
            Ok(credit) => {
                self.credit = Some(credit);
                Ok(self)
            }
            Err(rejection) => {
                let (error, credit) = rejection.into_parts();
                self.credit = Some(credit);
                Err(ImmediateResultBatchReservationError { error, batch: self })
            }
        }
    }

    pub fn begin_protocol_write(mut self, bytes: u64) -> Result<Self, QueryExecutionError> {
        let credit = self
            .credit
            .take()
            .expect("immediate result batch owns credit before protocol write");
        match credit.begin_protocol_write(bytes) {
            Ok(credit) => {
                self.credit = Some(credit);
                Ok(self)
            }
            Err(rejection) => {
                let (error, credit) = rejection.into_parts();
                drop(self.batch.take());
                drop(credit);
                Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    format!("begin immediate result protocol write: {error}"),
                ))
            }
        }
    }

    pub fn complete(mut self) -> Result<(), QueryExecutionError> {
        drop(self.batch.take());
        let credit = self
            .credit
            .take()
            .expect("immediate result batch owns credit before completion");
        credit.consume().map_err(|error| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                format!("consume immediate result protocol credit: {error}"),
            )
        })
    }

    pub fn fail(mut self) {
        drop(self.batch.take());
        drop(self.credit.take());
    }
}

impl Drop for ImmediateResultBatch {
    fn drop(&mut self) {
        drop(self.batch.take());
        drop(self.credit.take());
    }
}

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

/// Query-application output delivered to a client-session protocol adapter.
///
/// The adapter owns wire framing, while these values retain every application
/// lifetime that must remain live until the terminal protocol outcome.
pub enum QuerySessionOutput {
    Query(QueryResult),
    GovernedQuery(GovernedImmediateStatementResult),
    StreamingQuery(StreamingStatementResult),
    GovernedCompletion(GovernedCompletionStatementResult),
    GovernedError(GovernedErrorStatementResult),
    Ok,
}

impl std::fmt::Debug for QuerySessionOutput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(result) => formatter.debug_tuple("Query").field(result).finish(),
            Self::GovernedQuery(_) => formatter.write_str("GovernedQuery(..)"),
            Self::StreamingQuery(_) => formatter.write_str("StreamingQuery(..)"),
            Self::GovernedCompletion(_) => formatter.write_str("GovernedCompletion(..)"),
            Self::GovernedError(_) => formatter.write_str("GovernedError(..)"),
            Self::Ok => formatter.write_str("Ok"),
        }
    }
}

/// Fully materialized result whose statement permit remains live through the
/// final protocol outcome.
#[must_use = "the governed query result must be settled by its protocol owner"]
pub struct GovernedImmediateStatementResult {
    result: QueryResult,
    protocol: GovernedProtocolOwner,
}

impl GovernedImmediateStatementResult {
    pub fn new(
        result: QueryResult,
        resources: LocalResourceAuthority,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            result,
            protocol: GovernedProtocolOwner::new(statement, resources),
        }
    }

    pub fn into_parts(self) -> (QueryResult, GovernedProtocolOwner) {
        (self.result, self.protocol)
    }
}

/// Completion-only output that retains its statement permit through the
/// terminal protocol OK packet.
#[must_use = "the governed completion must be settled by its protocol owner"]
pub struct GovernedCompletionStatementResult {
    protocol: GovernedProtocolOwner,
}

impl GovernedCompletionStatementResult {
    pub fn new(resources: LocalResourceAuthority, statement: GovernedQueryStatementOwner) -> Self {
        Self {
            protocol: GovernedProtocolOwner::new(statement, resources),
        }
    }

    pub fn into_protocol(self) -> GovernedProtocolOwner {
        self.protocol
    }
}

/// Error output that retains its statement permit through the terminal
/// protocol error packet.
#[must_use = "the governed error must be settled by its protocol owner"]
pub struct GovernedErrorStatementResult {
    error: QueryServiceError,
    protocol: GovernedProtocolOwner,
}

impl GovernedErrorStatementResult {
    pub fn new(
        error: QueryServiceError,
        resources: LocalResourceAuthority,
        statement: GovernedQueryStatementOwner,
    ) -> Self {
        Self {
            error,
            protocol: GovernedProtocolOwner::new(statement, resources),
        }
    }

    pub fn into_parts(self) -> (QueryServiceError, GovernedProtocolOwner) {
        (self.error, self.protocol)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;

    #[tokio::test]
    async fn immediate_batch_keeps_result_credit_through_protocol_write() {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("workload control");
        control.mark_ready().expect("workload ready");
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query root");
        let authority = control.resources();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
        )
        .expect("record batch");
        let decoded_bytes = decoded_result_batch_governance_charge(&batch).expect("charge");
        let credit = authority
            .reserve_result_credit(&root.owner.scope(), decoded_bytes)
            .expect("fetch credit")
            .begin_fetch()
            .expect("begin fetch")
            .retain_raw(decoded_bytes)
            .expect("retain raw")
            .reserve_decode(&authority, decoded_bytes)
            .expect("reserve decode")
            .queue_decoded(decoded_bytes)
            .expect("queue decoded");

        let batch = ImmediateResultBatch::try_new(batch, credit).expect("immediate batch");
        assert_eq!(
            authority.snapshot().result_credit.held_bytes(),
            decoded_bytes
        );
        let batch = batch
            .reserve_protocol_when_available(&authority, 64)
            .await
            .expect("reserve protocol")
            .begin_protocol_write(64)
            .expect("begin protocol write");
        assert!(authority.snapshot().result_credit.protocol_writing_bytes > 0);

        batch.complete().expect("consume immediate batch");
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(root);
    }
}
