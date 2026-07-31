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

use std::sync::Arc;

use novarocks::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks::query_execution::lifecycle::{
    QueryLifecycleError, QueryLifecycleErrorCode, QueryTerminalIngress, QueryTerminalReportAck,
    QueryTerminalReportOutcome, QueryTerminalSnapshot,
};
use novarocks::query_execution::report::{NativeReportHandler, NativeReportHandlerError};
use novarocks::query_execution::write::{NativeExecutionReport, decode_native_execution_report};

use super::query_registry::FrontendQueryRegistry;

#[derive(Clone)]
pub struct FrontendCoordinatorReportHandler {
    registry: Arc<FrontendQueryRegistry>,
}

impl FrontendCoordinatorReportHandler {
    pub(crate) fn new(registry: Arc<FrontendQueryRegistry>) -> Self {
        Self { registry }
    }

    pub fn handle_native_report(
        &self,
        report: NativeExecutionReport,
    ) -> Result<(), DistributedQueryError> {
        self.registry.record_report(report)
    }
}

impl NativeReportHandler for FrontendCoordinatorReportHandler {
    fn handle_native_report(
        &self,
        report: novarocks_protocol::novarocks::ExecStatusReport,
    ) -> Result<(), NativeReportHandlerError> {
        let report = decode_native_execution_report(report)
            .map_err(NativeReportHandlerError::contract_violation)?;
        FrontendCoordinatorReportHandler::handle_native_report(self, report).map_err(|error| {
            match error.kind() {
                DistributedQueryErrorKind::Rejected => {
                    NativeReportHandlerError::query_gone(error.message())
                }
                DistributedQueryErrorKind::ContractViolation => {
                    NativeReportHandlerError::contract_violation(error.message())
                }
                DistributedQueryErrorKind::Failed => {
                    NativeReportHandlerError::failed(error.message())
                }
            }
        })
    }
}

/// Typed unary fallback owner for QLC-4 terminal delivery.  It shares the
/// query registry with stream delivery but is intentionally not a fragment
/// ReportExecStatus handler.
#[derive(Clone)]
pub struct FrontendCoordinatorTerminalIngress {
    registry: Arc<FrontendQueryRegistry>,
}

impl FrontendCoordinatorTerminalIngress {
    pub(crate) fn new(registry: Arc<FrontendQueryRegistry>) -> Self {
        Self { registry }
    }
}

impl QueryTerminalIngress for FrontendCoordinatorTerminalIngress {
    fn report_query_terminal(
        &self,
        snapshot: QueryTerminalSnapshot,
    ) -> Result<QueryTerminalReportAck, QueryLifecycleError> {
        match self.registry.report_query_terminal(snapshot) {
            Ok(true) => Ok(QueryTerminalReportAck::new(
                QueryTerminalReportOutcome::Accepted,
                "terminal snapshot stored",
            )),
            Ok(false) => Ok(QueryTerminalReportAck::new(
                QueryTerminalReportOutcome::AlreadyAccepted,
                "terminal snapshot already stored",
            )),
            Err(error) if matches!(error.kind(), DistributedQueryErrorKind::Rejected) => {
                Ok(QueryTerminalReportAck::new(
                    QueryTerminalReportOutcome::RejectedGone,
                    error.message(),
                ))
            }
            Err(error) if matches!(error.kind(), DistributedQueryErrorKind::ContractViolation) => {
                Ok(QueryTerminalReportAck::new(
                    QueryTerminalReportOutcome::RejectedConflict,
                    error.message(),
                ))
            }
            Err(error) => Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                error.message(),
            )),
        }
    }
}
